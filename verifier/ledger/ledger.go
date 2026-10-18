// Package ledger is the shared acked-loss instrument for the connector-resiliency
// campaign. One format, one verdict rule, ten connector adapters that write into it.
//
// The asymmetry between the two files is the whole design:
//
//   - acked.jsonl holds only messages the SERVER promised to have taken. A publish
//     that returned an error is NEVER written, because it may or may not have landed
//     and must never be counted as owed.
//   - observed.jsonl holds what a reader actually saw. A message present that was
//     never acked is a retried publish whose first ack was lost — correct
//     at-least-once behaviour, recorded and never a failure.
//
// See ../../ledger-design.md for the per-connector ack semantics this encodes.
package ledger

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Acked is one line of acked.jsonl: a message the connector's own acknowledgement
// mechanism confirmed. Id is the harness's id, carried inside the payload — never a
// broker-assigned id, because a broker-assigned id is exactly what a failure can lose.
type Acked struct {
	Id   string         `json:"id"`
	T    time.Time      `json:"t"`
	Ack  string         `json:"ack"`
	Meta map[string]any `json:"meta,omitempty"`
}

// Observed is one line of observed.jsonl: a message a reader saw. Dup is set when the
// protocol itself told us this was a redelivery.
type Observed struct {
	Id   string         `json:"id"`
	T    time.Time      `json:"t"`
	Dup  bool           `json:"dup,omitempty"`
	Meta map[string]any `json:"meta,omitempty"`
}

// Writer appends to one ledger file. Safe for concurrent use: connector adapters
// publish from several goroutines and the ledger must not interleave partial lines.
type Writer struct {
	mu sync.Mutex
	f  *os.File
	w  *bufio.Writer
	n  int
}

// NewWriter creates or truncates path and returns a Writer for it.
func NewWriter(path string) (*Writer, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("ledger: mkdir for %s: %w", path, err)
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("ledger: create %s: %w", path, err)
	}
	return &Writer{f: f, w: bufio.NewWriter(f)}, nil
}

// Append writes one record as a JSON line and flushes it. Flushing per record is
// deliberate: a scenario kills pods, and a ledger that lost its tail to a buffer
// would understate what was acked and manufacture a false pass.
func (w *Writer) Append(rec any) error {
	b, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("ledger: marshal: %w", err)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.w.Write(append(b, '\n')); err != nil {
		return fmt.Errorf("ledger: write: %w", err)
	}
	w.n++
	return w.w.Flush()
}

// Count returns how many records have been appended.
func (w *Writer) Count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.n
}

// Close flushes and closes the underlying file.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.w.Flush(); err != nil {
		_ = w.f.Close()
		return err
	}
	return w.f.Close()
}

// ReadIDs returns the ids in a ledger file, in order, plus the total line count.
// The two differ when the same id appears twice — which is what makes a duplicate
// visible rather than silently deduplicated.
func ReadIDs(path string) (ids []string, lines int, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, fmt.Errorf("ledger: open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec struct {
			Id string `json:"id"`
		}
		if err := json.Unmarshal(line, &rec); err != nil {
			return nil, 0, fmt.Errorf("ledger: %s line %d: %w", path, lines+1, err)
		}
		lines++
		ids = append(ids, rec.Id)
	}
	if err := sc.Err(); err != nil {
		return nil, 0, fmt.Errorf("ledger: scan %s: %w", path, err)
	}
	return ids, lines, nil
}
