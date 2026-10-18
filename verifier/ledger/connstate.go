package ledger

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// ConnLog records a reader's connection-state transitions alongside its ledger.
//
// It exists to make one specific verdict TRIABLE. Four of the ten connectors —
// MQTT, STOMP, AMQP 0-9-1 and AMQP 1.0 — have no replay: their readers attach
// before load and there is no second chance to read. If such a reader drops
// mid-scenario, "the reader died" and "the cluster lost messages" produce the SAME
// shape in the ledger: acked ids with no matching observation.
//
// The only thing that distinguishes them is whether the reader was attached when
// the missing messages should have arrived. So the reader records when it was
// attached, and the argument becomes a lookup instead of a hypothesis.
//
// This matters because the previous phase spent three wrong hypotheses on a verdict
// that turned out to be reader-shaped rather than loss-shaped. The fix then was a
// rule; the fix here is a record.
//
// RULE FOR ANY DURABLE-SUBSCRIBER-TYPE VERIFIER: if the connector has no replay,
// its verifier MUST carry a ConnLog. A no-replay verifier without one can produce
// a FAIL it cannot defend.
type ConnLog struct {
	w *Writer
}

// ConnEvent is one line of reader-state.jsonl.
type ConnEvent struct {
	T     time.Time `json:"t"`
	State string    `json:"state"`
	Note  string    `json:"note,omitempty"`
}

// NewConnLog opens the reader-state file. A failure to open it is reported and
// then tolerated: losing the triage record is bad, but aborting a scenario that is
// otherwise fine would be worse.
func NewConnLog(path string) *ConnLog {
	w, err := NewWriter(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "conn log: %v (continuing without triage record)\n", err)
		return &ConnLog{}
	}
	return &ConnLog{w: w}
}

// Record appends a transition. Typical states: connected, disconnected,
// resubscribed, resubscribe_failed.
func (c *ConnLog) Record(state, note string) {
	if c == nil || c.w == nil {
		return
	}
	_ = c.w.Append(ConnEvent{T: time.Now().UTC(), State: state, Note: note})
}

// Close flushes the record.
func (c *ConnLog) Close() {
	if c == nil || c.w == nil {
		return
	}
	_ = c.w.Close()
}

// ReadConnEvents returns the transitions recorded at path, oldest first. A missing
// file yields no events and no error: a verifier for a connector WITH replay has no
// reason to have written one.
func ReadConnEvents(path string) ([]ConnEvent, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("conn log: open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	var events []ConnEvent
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		if len(sc.Bytes()) == 0 {
			continue
		}
		var e ConnEvent
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			return nil, fmt.Errorf("conn log: %s: %w", path, err)
		}
		events = append(events, e)
	}
	return events, sc.Err()
}

// DroppedDuring reports whether the reader was disconnected at any point, and how
// many times. A scenario whose verdict is FAIL while this returns true is NOT a
// clean loss report — it is a candidate VOID, and the report must say so rather
// than quietly attributing the gap to the cluster.
func DroppedDuring(path string) (dropped bool, count int) {
	events, err := ReadConnEvents(path)
	if err != nil {
		return false, 0
	}
	for _, e := range events {
		if e.State == "disconnected" || e.State == "resubscribe_failed" {
			count++
		}
	}
	return count > 0, count
}
