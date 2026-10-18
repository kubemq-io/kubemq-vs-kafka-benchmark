package ledger

import (
	"path/filepath"
	"testing"
	"time"
)

// write builds a ledger file from a list of ids so each case states its own input.
func write(t *testing.T, path string, ids []string) {
	t.Helper()
	w, err := NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter(%s): %v", path, err)
	}
	for _, id := range ids {
		if err := w.Append(Acked{Id: id, T: time.Now().UTC(), Ack: "test"}); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestCompute(t *testing.T) {
	cases := []struct {
		name        string
		acked       []string
		observed    []string
		claimedLoad bool
		void        VoidReason
		want        Outcome
		wantMissing int
		wantDups    int
	}{
		{
			name:        "every acked id observed is the only way to pass",
			acked:       []string{"a", "b", "c"},
			observed:    []string{"a", "b", "c"},
			claimedLoad: true,
			want:        Pass,
		},
		{
			name:        "one acked id absent fails, with no tolerance",
			acked:       []string{"a", "b", "c"},
			observed:    []string{"a", "c"},
			claimedLoad: true,
			want:        Fail,
			wantMissing: 1,
		},
		{
			// A retried publish whose first ack was lost. Correct at-least-once
			// behaviour: recorded, never a failure.
			name:        "the same id twice is a duplicate, not a failure",
			acked:       []string{"a", "b"},
			observed:    []string{"a", "a", "b"},
			claimedLoad: true,
			want:        Pass,
			wantDups:    1,
		},
		{
			name:        "an observed id that was never acked is counted, not failed",
			acked:       []string{"a"},
			observed:    []string{"a", "ghost"},
			claimedLoad: true,
			want:        Pass,
			wantDups:    1,
		},
		{
			// The failure mode that produced two false greens in this programme:
			// an instrument pointed at nothing agrees with everything.
			name:        "zero acked while claiming load is VOID, never PASS",
			acked:       nil,
			observed:    nil,
			claimedLoad: true,
			want:        Void,
		},
		{
			// Reader-shaped, not loss-shaped. The previous phase reported
			// "89 ACKED messages are gone" on a cluster that had lost nothing.
			name:        "empty read with acks is VOID, never FAIL",
			acked:       []string{"a", "b", "c"},
			observed:    nil,
			claimedLoad: true,
			want:        Void,
			wantMissing: 3,
		},
		{
			// Digest drift means the cluster was not the cluster we meant to test,
			// whatever the ids happen to say.
			name:        "a caller-supplied void reason wins over a clean pass",
			acked:       []string{"a"},
			observed:    []string{"a"},
			claimedLoad: true,
			void:        "image digests moved mid-scenario",
			want:        Void,
		},
		{
			name:        "a caller-supplied void reason wins over a fail",
			acked:       []string{"a", "b"},
			observed:    []string{"a"},
			claimedLoad: true,
			void:        "image digests moved mid-scenario",
			want:        Void,
			wantMissing: 1,
		},
		{
			// Without the claimed-load flag, an idle scenario is not evidence of
			// anything and must not be voided for being idle.
			name:        "zero acked without claimed load is not voided",
			acked:       nil,
			observed:    nil,
			claimedLoad: false,
			want:        Pass,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			ackedPath := filepath.Join(dir, "acked.jsonl")
			observedPath := filepath.Join(dir, "observed.jsonl")
			write(t, ackedPath, tc.acked)
			write(t, observedPath, tc.observed)

			v, err := Compute("test", tc.name, ackedPath, observedPath, tc.claimedLoad, tc.void)
			if err != nil {
				t.Fatalf("Compute: %v", err)
			}
			if v.Outcome != tc.want {
				t.Errorf("verdict = %s, want %s (reason: %s)", v.Outcome, tc.want, v.Reason)
			}
			if v.Missing != tc.wantMissing {
				t.Errorf("missing = %d, want %d", v.Missing, tc.wantMissing)
			}
			if v.Duplicates != tc.wantDups {
				t.Errorf("duplicates = %d, want %d", v.Duplicates, tc.wantDups)
			}
		})
	}
}

// A caller that treats any non-zero exit as failure would turn "we learned nothing"
// into "the product broke". Void must be distinguishable from fail at the shell.
func TestExitCodesDistinguishVoidFromFail(t *testing.T) {
	for _, tc := range []struct {
		outcome Outcome
		want    int
	}{
		{Pass, 0},
		{Fail, 1},
		{Void, 2},
	} {
		if got := (&Verdict{Outcome: tc.outcome}).ExitCode(); got != tc.want {
			t.Errorf("%s exit code = %d, want %d", tc.outcome, got, tc.want)
		}
	}
}

// Ordering is a reported property and must never move a verdict. A run that
// delivered everything out of order still passed: nothing was lost.
func TestOrderingIsReportedNeverJudged(t *testing.T) {
	dir := t.TempDir()
	ackedPath := filepath.Join(dir, "acked.jsonl")
	observedPath := filepath.Join(dir, "observed.jsonl")
	write(t, ackedPath, []string{"a", "b", "c", "d"})
	write(t, observedPath, []string{"d", "c", "b", "a"}) // fully reversed

	v, err := Compute("test", "reordered", ackedPath, observedPath, true, "")
	if err != nil {
		t.Fatalf("Compute: %v", err)
	}
	if v.Outcome != Pass {
		t.Errorf("verdict = %s, want PASS — reordering is not loss", v.Outcome)
	}
	if v.Ordering != "reordered" {
		t.Errorf("ordering = %q, want %q", v.Ordering, "reordered")
	}
	if v.Inversions != 3 {
		t.Errorf("inversions = %d, want 3", v.Inversions)
	}
}

func TestOrderingInOrder(t *testing.T) {
	dir := t.TempDir()
	ackedPath := filepath.Join(dir, "acked.jsonl")
	observedPath := filepath.Join(dir, "observed.jsonl")
	write(t, ackedPath, []string{"a", "b", "c"})
	write(t, observedPath, []string{"a", "b", "c"})

	v, err := Compute("test", "ordered", ackedPath, observedPath, true, "")
	if err != nil {
		t.Fatalf("Compute: %v", err)
	}
	if v.Ordering != "in-order" || v.Inversions != 0 {
		t.Errorf("ordering = %q/%d, want in-order/0", v.Ordering, v.Inversions)
	}
}
