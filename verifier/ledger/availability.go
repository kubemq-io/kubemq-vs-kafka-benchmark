package ledger

import (
	"fmt"
	"time"
)

// Publisher is the retry-and-measure wrapper every verifier's publish loop runs
// through.
//
// It exists because of a measurement taken on a real 3-node GKE cluster on
// 2026-08-01: one pod was deleted under load, and the STOMP publisher's connection
// died with "server shutting down" and never returned — 156 messages acked, then
// 2844 consecutive failures. Without retry, a chaos scenario does not measure the
// cluster's behaviour at all; it measures the moment the client gave up.
//
// But retry alone would only hide the gap. So the wrapper also MEASURES it: every
// failure, reconnect and recovery is timestamped, and what falls out is the
// PUBLISH-AVAILABILITY WINDOW — how long this connector's producer path was
// actually unusable through the disruption. That is a customer-facing number that
// has never been measured for these connectors, and it turns a harness necessity
// into the phase's most quotable result.
type Publisher struct {
	log      *ConnLog
	maxRetry int
	backoff  time.Duration

	// gap tracking
	inGap     bool
	gapStart  time.Time
	gaps      []Gap
	reconnect func() error
}

// Gap is one contiguous window in which publishing was failing.
type Gap struct {
	Start    time.Time     `json:"start"`
	End      time.Time     `json:"end"`
	Duration time.Duration `json:"duration_ns"`
	Attempts int           `json:"attempts"`
	LastErr  string        `json:"last_error,omitempty"`
}

// NewPublisher wires a publish loop to a state log and an optional reconnect hook.
// reconnect may be nil for clients that re-establish their own transport.
func NewPublisher(statePath string, reconnect func() error) *Publisher {
	return &Publisher{
		log:       NewConnLog(statePath),
		maxRetry:  12,
		backoff:   250 * time.Millisecond,
		reconnect: reconnect,
	}
}

// Send runs one publish through retry-with-backoff and returns whether it was
// ultimately ACKED. The caller supplies a closure that performs exactly one attempt
// and reports whether the connector's own acknowledgement arrived — the ack
// semantics stay with the connector, which is the whole point of the adapter seam.
//
// A message that never acks after every retry is NOT written to the ledger by the
// caller: an unacknowledged publish is not a promise, whatever the reason.
func (p *Publisher) Send(attempt func() (bool, error)) bool {
	delay := p.backoff
	var lastErr error
	for i := 0; i < p.maxRetry; i++ {
		ok, err := attempt()
		if ok {
			if p.inGap {
				// Recovery: close the gap and record how long the producer path was
				// unusable. This is the number the campaign reports.
				g := Gap{
					Start: p.gapStart, End: time.Now().UTC(),
					Duration: time.Since(p.gapStart), Attempts: i + 1,
				}
				if lastErr != nil {
					g.LastErr = lastErr.Error()
				}
				p.gaps = append(p.gaps, g)
				p.inGap = false
				p.log.Record("publish_recovered",
					fmt.Sprintf("unavailable for %s after %d attempts", g.Duration.Round(time.Millisecond), g.Attempts))
			}
			return true
		}
		lastErr = err
		if !p.inGap {
			p.inGap = true
			p.gapStart = time.Now().UTC()
			p.log.Record("publish_failing", errString(err))
		}
		if p.reconnect != nil {
			if rerr := p.reconnect(); rerr != nil {
				p.log.Record("reconnect_failed", errString(rerr))
			} else {
				p.log.Record("reconnected", "")
			}
		}
		time.Sleep(delay)
		if delay < 4*time.Second {
			delay *= 2
		}
	}
	return false
}

// Gaps returns the closed availability gaps observed so far.
func (p *Publisher) Gaps() []Gap { return p.gaps }

// Summary renders the availability result for the verdict, and states plainly when
// the producer path never came back — an open gap at the end of a run is not the
// same as a measured outage, and must not be reported as one.
func (p *Publisher) Summary() (totalUnavailable time.Duration, gapCount int, neverRecovered bool) {
	for _, g := range p.gaps {
		totalUnavailable += g.Duration
	}
	return totalUnavailable, len(p.gaps), p.inGap
}

// Close finalises the state log, recording an unterminated gap if the producer path
// was still failing when the run ended.
func (p *Publisher) Close() {
	if p.inGap {
		p.log.Record("publish_never_recovered",
			fmt.Sprintf("still failing after %s", time.Since(p.gapStart).Round(time.Millisecond)))
	}
	p.log.Close()
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// AttachAvailability folds the publish-availability measurement into a verdict.
// Like the reader-state mechanism, it reports and never rewrites the outcome.
func (v *Verdict) AttachAvailability(p *Publisher) {
	total, count, never := p.Summary()
	if count == 0 && !never {
		return
	}
	v.PublishGaps = count
	v.PublishUnavailableMs = total.Milliseconds()
	v.PublishNeverRecovered = never
}
