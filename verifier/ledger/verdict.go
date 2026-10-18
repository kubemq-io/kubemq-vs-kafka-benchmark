package ledger

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Outcome is the verdict of one scenario for one connector.
type Outcome string

const (
	// Pass means every acked id was observed. Nothing else earns a pass.
	Pass Outcome = "PASS"
	// Fail means at least one acked id was absent. No tolerance, no threshold.
	Fail Outcome = "FAIL"
	// Void means the instrument was unusable, so the run says nothing either way.
	// Void is NOT a soft fail and must never be reported as one.
	Void Outcome = "VOID"
)

// Verdict is written to verdict.json. It is computed, never hand-written.
type Verdict struct {
	Connector  string   `json:"connector"`
	Scenario   string   `json:"scenario"`
	Acked      int      `json:"acked"`
	Observed   int      `json:"observed"`
	Missing    int      `json:"missing"`
	Duplicates int      `json:"duplicates"`
	Outcome    Outcome  `json:"verdict"`
	Reason     string   `json:"reason,omitempty"`
	MissingIDs []string `json:"missing_ids,omitempty"`
	// AllMissingIDs is every missing id, never truncated. Spilled to
	// missing-ids.txt by Write() so a later adjudication is possible at all.
	AllMissingIDs []string `json:"-"`

	// ReaderDrops is how many times a no-replay reader lost its connection during
	// the scenario, from its ConnLog. See AttachReaderState for why this FLAGS a
	// verdict rather than changing it.
	ReaderDrops int    `json:"reader_drops,omitempty"`
	TriageNote  string `json:"triage_note,omitempty"`

	// Ordering is a reported PROPERTY, never a defect. Delivery order differs
	// legitimately by connector: Google Pub/Sub reorders freely unless ordering keys
	// are used, Kafka orders within a partition and not across, MQTT and AMQP order
	// per-link. Customers ask what a connector actually does, so the campaign
	// measures it — but a reordered run with no missing messages is a PASS, and
	// nothing here may change a verdict.
	Ordering   string `json:"ordering"`
	Inversions int    `json:"inversions"`

	// Publish timing, recorded so an id can be converted to a wall-clock time
	// WITHOUT assuming anything. A release-verification adjudication had to borrow
	// another run's rate to place a loss against a teardown, which made the whole
	// arithmetic conditional on an assumption that was never checked.
	PublishStart      time.Time `json:"publish_start,omitempty"`
	PublishEnd        time.Time `json:"publish_end,omitempty"`
	PublishRatePerSec float64   `json:"publish_rate_per_sec,omitempty"`

	// Publish-availability, from the Publisher wrapper. How long this connector's
	// PRODUCER path was actually unusable through the scenario — a customer-facing
	// number, and one nobody has measured for these connectors. Reported, never
	// judged: an outage during a deliberate pod kill is the behaviour under test,
	// not a defect, and the verdict is decided by the ledger as always.
	PublishGaps           int   `json:"publish_gaps,omitempty"`
	PublishUnavailableMs  int64 `json:"publish_unavailable_ms,omitempty"`
	PublishNeverRecovered bool  `json:"publish_never_recovered,omitempty"`
}

// measureOrdering compares the order ids were observed in against the order they
// were acked in, and counts inversions — pairs delivered out of their acked order.
//
// Reported, never judged. See the Ordering field for why.
func measureOrdering(ackedIDs, observedIDs []string) (string, int) {
	if len(observedIDs) < 2 {
		return "not-measurable", 0
	}
	rank := make(map[string]int, len(ackedIDs))
	for i, id := range ackedIDs {
		rank[id] = i
	}
	// Only ids we actually acked carry a meaningful position; anything else is a
	// duplicate or a stray and would make the count meaningless.
	seq := make([]int, 0, len(observedIDs))
	seen := make(map[string]bool, len(observedIDs))
	for _, id := range observedIDs {
		if r, ok := rank[id]; ok && !seen[id] {
			seen[id] = true
			seq = append(seq, r)
		}
	}
	if len(seq) < 2 {
		return "not-measurable", 0
	}
	inversions := 0
	for i := 1; i < len(seq); i++ {
		if seq[i] < seq[i-1] {
			inversions++
		}
	}
	if inversions == 0 {
		return "in-order", 0
	}
	return "reordered", inversions
}

// AttachReaderState folds a no-replay reader's connection record into the verdict.
//
// It deliberately does NOT convert a FAIL into a VOID, and the reasoning is the
// whole point of the mechanism. During chaos a reader disconnect is EXPECTED — that
// is the event under test. For a durable MQTT session the broker is supposed to hold
// QoS 1 messages while the subscriber is away and redeliver them on reconnect. So:
//
//   - if the session works as promised, the messages arrive late and the verdict is
//     a legitimate PASS despite the drops;
//   - if the session does not survive the move, the messages are genuinely gone —
//     and that is a PRODUCT FINDING, exactly what R3 exists to catch.
//
// Auto-voiding on any disconnect would suppress the second case, which is the most
// valuable result this connector can produce. So the drops are recorded and
// surfaced, and the verdict stands: a human or the scenario runner decides
// attribution with the evidence in front of them instead of guessing afterwards.
func (v *Verdict) AttachReaderState(connLogPath string) {
	dropped, n := DroppedDuring(connLogPath)
	if !dropped {
		return
	}
	v.ReaderDrops = n
	switch v.Outcome {
	case Fail:
		v.TriageNote = fmt.Sprintf("the reader lost its connection %d time(s) during this scenario. "+
			"For a no-replay connector this FAIL is not attributable until triaged: either the durable "+
			"session failed to redeliver what it held (a product finding) or the reader was absent when "+
			"the messages arrived (an instrument gap). reader-state.jsonl has the timestamps.", n)
	default:
		v.TriageNote = fmt.Sprintf("the reader lost its connection %d time(s) and the run still passed — "+
			"the durable session redelivered across the gap, which is the behaviour under test.", n)
	}
}

// VoidReason is a caller-supplied reason to void a scenario for something the ledger
// itself cannot see — digest drift mid-scenario, or a client that could never have
// connected. Empty means "not voided by the caller".
type VoidReason string

// Compute derives the verdict from the two ledger files.
//
// The rules, in the order they are applied:
//
//  1. A caller-supplied void reason wins over everything. Digest drift means the
//     cluster was not the cluster we meant to test, whatever the ids say.
//  2. Zero acked on a scenario that claimed load is VOID, not PASS. An instrument
//     pointed at nothing agrees with everything, and that has already produced two
//     false greens in this programme.
//  3. An empty read with non-zero acks is VOID, not FAIL. That shape is
//     reader-shaped, not loss-shaped — the previous phase reported "89 ACKED
//     messages are gone" on a cluster that had lost nothing, and the only sound
//     retry is a fresh channel, i.e. re-running the scenario.
//  4. Any acked id absent from the observed set is FAIL.
//  5. Otherwise PASS. Duplicates are counted and reported but never fail a run.
func Compute(connector, scenario, ackedPath, observedPath string, claimedLoad bool, void VoidReason) (*Verdict, error) {
	ackedIDs, ackedLines, err := ReadIDs(ackedPath)
	if err != nil {
		return nil, err
	}
	observedIDs, observedLines, err := ReadIDs(observedPath)
	if err != nil {
		return nil, err
	}

	v := &Verdict{
		Connector: connector,
		Scenario:  scenario,
		Acked:     ackedLines,
		Observed:  observedLines,
	}

	seen := make(map[string]int, len(observedIDs))
	for _, id := range observedIDs {
		seen[id]++
	}
	for _, n := range seen {
		if n > 1 {
			v.Duplicates += n - 1
		}
	}

	ackedSet := make(map[string]struct{}, len(ackedIDs))
	for _, id := range ackedIDs {
		ackedSet[id] = struct{}{}
	}
	// An observed id that was never acked is also a duplicate in the sense that
	// matters here: a retried publish whose first ack was lost. Correct
	// at-least-once behaviour, recorded, never a failure.
	for id := range seen {
		if _, ok := ackedSet[id]; !ok {
			v.Duplicates++
		}
	}

	var missing []string
	for _, id := range ackedIDs {
		if _, ok := seen[id]; !ok {
			missing = append(missing, id)
		}
	}
	sort.Strings(missing)
	v.Missing = len(missing)

	// Measured before the verdict switch, and deliberately not consulted by it.
	v.Ordering, v.Inversions = measureOrdering(ackedIDs, observedIDs)

	switch {
	case void != "":
		v.Outcome, v.Reason = Void, string(void)
	case claimedLoad && ackedLines == 0:
		v.Outcome = Void
		v.Reason = "scenario claimed load but nothing was acked — the instrument was pointed at nothing"
	case ackedLines > 0 && observedLines == 0:
		v.Outcome = Void
		v.Reason = fmt.Sprintf("read came back empty with %d acked — reader-shaped, not loss-shaped; retry only on a fresh channel", ackedLines)
	case len(missing) > 0:
		v.Outcome = Fail
		v.Reason = fmt.Sprintf("%d acked message(s) absent from the read", len(missing))
		// The verdict carries a readable sample; AllMissingIDs carries every id, and
		// Write() spills it to missing-ids.txt.
		//
		// TRUNCATION IS NOT A DISPLAY CHOICE, IT IS EVIDENCE DESTRUCTION. This list
		// is sorted ascending, so a head-truncation keeps the EARLIEST losses and
		// discards the latest — and "which teardown did this belong to" is answered
		// by the latest ones. A release-verification run lost 93 of 133 ids to
		// exactly this, and the question it needed to answer concerned the third pod
		// teardown, i.e. precisely the ids the truncation had removed. The check was
		// unanswerable by construction, not by bad luck.
		v.AllMissingIDs = missing
		v.MissingIDs = missing
		if len(v.MissingIDs) > 20 {
			v.MissingIDs = v.MissingIDs[:20]
		}
	default:
		v.Outcome = Pass
	}
	return v, nil
}

// WriteVoid records a VOID verdict and exits 2, for the case where the verifier
// cannot complete its verification read at all — the reader could not connect, the
// subscribe was refused, the stream never opened.
//
// Every verifier MUST use this rather than dying with a bare error, and the reason is
// concrete: a verifier that exits without writing verdict.json shows up to the
// scenario runner as ERROR, which reads as "the harness broke". But "I acked 1211
// messages and then could not read the channel" is not a broken harness — it is the
// textbook VOID, and the ledger already has a rule for it. Losing that distinction
// throws away the acked count, which is the one number that says how far the run got.
//
// Observed on 2026-08-01: a broker killed mid-scenario produced two ERROR rows that
// should have been VOID, and the acked totals behind them were discarded.
func WriteVoid(outDir, connector, scenario, reason string, ackedSoFar int) {
	v := &Verdict{
		Connector: connector, Scenario: scenario,
		Acked: ackedSoFar, Outcome: Void, Reason: reason,
	}
	if err := v.Write(filepath.Join(outDir, "verdict.json")); err != nil {
		fmt.Fprintf(os.Stderr, "ledger: could not write void verdict: %v\n", err)
	}
	fmt.Println(v.String())
	os.Exit(v.ExitCode())
}

// SetPublishWindow records when publishing ran and how fast, so a later reader can
// convert any message id to a timestamp exactly rather than by assumption.
func (v *Verdict) SetPublishWindow(start, end time.Time, acked int) {
	if start.IsZero() || end.IsZero() || acked <= 0 {
		return
	}
	v.PublishStart, v.PublishEnd = start.UTC(), end.UTC()
	if d := end.Sub(start).Seconds(); d > 0 {
		v.PublishRatePerSec = float64(acked) / d
	}
}

// Write renders the verdict to path as indented JSON, and spills the COMPLETE
// missing-id list alongside it as missing-ids.txt.
//
// The complete list is a separate file on purpose: the verdict stays readable at a
// glance, and the evidence needed to adjudicate the run months later survives
// regardless of how long the list is.
func (v *Verdict) Write(path string) error {
	if len(v.AllMissingIDs) > 0 {
		full := filepath.Join(filepath.Dir(path), "missing-ids.txt")
		if werr := os.WriteFile(full, []byte(strings.Join(v.AllMissingIDs, "\n")+"\n"), 0o644); werr != nil {
			fmt.Fprintf(os.Stderr, "ledger: could not write the full missing-id list: %v\n", werr)
		}
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

// String renders the one-line human summary printed at the end of every run.
func (v *Verdict) String() string {
	s := fmt.Sprintf("%s [%s/%s] acked=%d observed=%d missing=%d duplicates=%d",
		v.Outcome, v.Connector, v.Scenario, v.Acked, v.Observed, v.Missing, v.Duplicates)
	if v.Reason != "" {
		s += " — " + v.Reason
	}
	return s
}

// ExitCode maps the verdict onto a process exit code, so a shell scenario can branch
// on it without parsing JSON. Void is deliberately NOT 1: a caller that treats any
// non-zero as failure would turn "we learned nothing" into "the product broke".
func (v *Verdict) ExitCode() int {
	switch v.Outcome {
	case Pass:
		return 0
	case Fail:
		return 1
	default:
		return 2
	}
}
