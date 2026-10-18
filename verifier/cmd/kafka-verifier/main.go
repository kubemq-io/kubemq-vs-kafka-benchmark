// kafka-verifier is the Kafka adapter of the connector-resiliency acked-loss
// instrument — connector #7.
//
// What counts as ACKED here: an acks=all produce response with no error and a
// server-assigned partition/offset. acks=all is not a tuning choice, it is the
// definition: acks=1 would let the instrument call a message safe that a leader
// failure could still lose, which is precisely the event under test.
//
// How loss is measured: a FRESH topic per run, consumed from the start. Kafka is
// the only connector of the ten whose server-side offsets are directly re-readable,
// so the read needs no durable subscriber established in advance — which is what
// makes it the cheapest verifier of the set.
//
// Usage:
//
//	kafka-verifier -addr host:9092 -out <dir> -count 200
//	kafka-verifier ... -drop-nth 50    # the RED gate
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/twmb/franz-go/pkg/kadm"
	"os"
	"sync"
	"time"

	"github.com/kubemq-io/kubemq-vs-kafka-benchmark/verifier/ledger"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	var (
		addr     = flag.String("addr", "localhost:9092", "Kafka bootstrap address host:port")
		out      = flag.String("out", "", "output directory for the ledger files (required)")
		topic    = flag.String("topic", "", "topic to use; default is a fresh time-stamped one")
		count    = flag.Int("count", 200, "messages to publish")
		scenario = flag.String("scenario", "standalone", "scenario name recorded in the verdict")
		dropNth  = flag.Int("drop-nth", 0, "RED GATE: silently drop every Nth message on the READ side. 0 disables")
		readWait = flag.Duration("read-wait", 15*time.Second, "how long the verification read collects before it stops")
		settle   = flag.Duration("settle", 2*time.Second, "pause after the last produce so in-flight acks land before the read")
		rate     = flag.Int("rate", 0, "steady-rate mode: messages per second, produced asynchronously for -duration (0 = one synchronous send at a time, -count messages)")
		duration = flag.Duration("duration", 0, "steady-rate mode: how long to produce")
		size     = flag.Int("size", 0, "payload bytes per record: the id followed by '|' and deterministic filler derived from the id, verified byte-for-byte on read (0 = id only)")
		corrupt  = flag.Int("corrupt-nth", 0, "RED GATE: flip one filler byte in every Nth produced record (needs -size). A read-side content check that still passes is decorative")
	)
	flag.Parse()
	payloadSize = *size
	corruptNth = *corrupt
	if corruptNth > 0 && payloadSize == 0 {
		fatal("-corrupt-nth needs -size")
	}

	if *out == "" {
		fatal("-out is required")
	}
	if *topic == "" {
		*topic = fmt.Sprintf("cr-kafka-%d", time.Now().UnixNano())
	}

	ctx := context.Background()
	ackedPath := *out + "/acked.jsonl"
	observedPath := *out + "/observed.jsonl"

	acked, err := ledger.NewWriter(ackedPath)
	if err != nil {
		fatal("%v", err)
	}
	observed, err := ledger.NewWriter(observedPath)
	if err != nil {
		fatal("%v", err)
	}

	fmt.Printf("kafka-verifier: %s topic=%s count=%d\n", *addr, *topic, *count)
	if *dropNth > 0 {
		fmt.Printf("  RED GATE ACTIVE: dropping every %dth message on the read side\n", *dropNth)
	}

	// ---- produce ---------------------------------------------------------
	prod, err := kgo.NewClient(
		kgo.SeedBrokers(*addr),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		// franz-go disables idempotency automatically for non-all acks; with
		// AllISRAcks it stays on, which is what we want — a retried produce should
		// dedupe server-side rather than inflate the observed count.
		kgo.ProducerBatchMaxBytes(1<<20),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		fatal("connect producer: %v", err)
	}

	// Retry-with-backoff, and MEASURE the producer outage window. Wired even though
	// C3 currently keeps Kafka off the Kubernetes rig: a rule that applies only to
	// the connectors currently in play is a rule that breaks the moment one comes
	// back. The harness self-test enforces it for exactly that reason — and caught
	// this file on its first run.
	pubState := ledger.NewPublisher(*out+"/publisher-state.jsonl", nil)
	defer pubState.Close()

	var produceErrs int
	if *rate > 0 {
		if *duration <= 0 {
			fatal("-rate needs -duration")
		}
		produceErrs = produceAtRate(ctx, prod, acked, *topic, *rate, *duration, *out)
	} else {
		for i := 0; i < *count; i++ {
			id := fmt.Sprintf("cr-kafka-%06d", i)
			var rec *kgo.Record
			ok := pubState.Send(func() (bool, error) {
				res := prod.ProduceSync(ctx, &kgo.Record{
					Topic: *topic,
					Value: payload(id, i),
				})
				r, perr := res.First()
				if perr != nil {
					return false, perr
				}
				rec = r
				// A produce response with no error carrying a real offset. Anything else
				// is an unknown, not an owed message.
				return r != nil, nil
			})
			if ok && rec != nil {
				if werr := acked.Append(ledger.Acked{
					Id: id, T: time.Now().UTC(), Ack: "acks_all_offset",
					Meta: map[string]any{"topic": *topic, "partition": rec.Partition, "offset": rec.Offset},
				}); werr != nil {
					fatal("%v", werr)
				}
			} else {
				produceErrs++
			}
		}
	}
	prod.Close()
	if err := acked.Close(); err != nil {
		fatal("%v", err)
	}
	fmt.Printf("  produced: %d acked, %d not acked\n", acked.Count(), produceErrs)

	time.Sleep(*settle)

	// ---- the single verification read ------------------------------------
	// A fresh topic consumed from the start. No consumer group: a group would add a
	// committed-offset dependency the verdict does not need and cannot verify here.
	cons, err := kgo.NewClient(
		kgo.SeedBrokers(*addr),
		kgo.ConsumeTopics(*topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		fatal("connect consumer: %v", err)
	}

	// Read until every partition reaches the end offset it had once producing stopped.
	// Stopping on a quiet stream instead would report a slow reader as "acked messages
	// missing" on a topic that holds every one of them: a reader that did not reach the
	// end has learned nothing, so it is VOID, never FAIL.
	ends := map[int32]int64{}
	{
		acl, aerr := kgo.NewClient(kgo.SeedBrokers(*addr))
		if aerr != nil {
			fatal("connect admin: %v", aerr)
		}
		lo, lerr := kadm.NewClient(acl).ListEndOffsets(ctx, *topic)
		acl.Close()
		if lerr != nil {
			fatal("list end offsets: %v", lerr)
		}
		lo.Each(func(o kadm.ListedOffset) {
			if o.Err == nil && o.Offset > 0 {
				ends[o.Partition] = o.Offset
			}
		})
	}
	next := map[int32]int64{}
	caughtUp := func() bool {
		for p, e := range ends {
			if next[p] < e {
				return false
			}
		}
		return true
	}

	var seen, dropped, corrupted int
	readCtx, cancel := context.WithTimeout(ctx, *readWait)
	defer cancel()

	for !caughtUp() {
		if readCtx.Err() != nil {
			break
		}
		pollCtx, pollCancel := context.WithTimeout(readCtx, 1500*time.Millisecond)
		fetches := cons.PollFetches(pollCtx)
		pollCancel()

		fetches.EachRecord(func(r *kgo.Record) {
			seen++
			if r.Offset+1 > next[r.Partition] {
				next[r.Partition] = r.Offset + 1
			}
			// RED gate on the read side: the same shape as a message the cluster
			// failed to return. If the verdict still passes with this on, the
			// instrument is decorative.
			if *dropNth > 0 && seen%*dropNth == 0 {
				dropped++
				return
			}
			id, ok := parsePayload(r.Value)
			if !ok {
				// Content mismatch: the record is counted as not observed (so it is owed and
				// fails the verdict) and reported separately as corrupt.
				corrupted++
				if corrupted <= 5 {
					fmt.Printf("  CORRUPT record at partition %d offset %d (%d bytes)\n", r.Partition, r.Offset, len(r.Value))
				}
				return
			}
			if werr := observed.Append(ledger.Observed{
				Id: id, T: time.Now().UTC(),
				Meta: map[string]any{"partition": r.Partition, "offset": r.Offset},
			}); werr != nil {
				fmt.Fprintf(os.Stderr, "observed ledger: %v\n", werr)
			}
		})
	}
	var void ledger.VoidReason
	if !caughtUp() {
		behind := 0
		for p, e := range ends {
			if next[p] < e {
				behind++
			}
		}
		void = ledger.VoidReason(fmt.Sprintf("reader did not reach the end offsets within -read-wait %s (%d of %d partitions behind)", *readWait, behind, len(ends)))
		fmt.Println("  " + string(void))
	}
	cons.Close()
	if err := observed.Close(); err != nil {
		fatal("%v", err)
	}
	if dropped > 0 {
		fmt.Printf("  RED GATE: swallowed %d message(s) on the read side\n", dropped)
	}
	if payloadSize > 0 {
		fmt.Printf("  content check: %d-byte payloads, %d corrupt record(s) on read%s\n", payloadSize, corrupted,
			map[bool]string{true: fmt.Sprintf(" (RED GATE: every %dth record was corrupted on write)", corruptNth), false: ""}[corruptNth > 0])
		_ = os.WriteFile(*out+"/content-check.txt", []byte(fmt.Sprintf("size=%d corrupt_on_read=%d corrupt_nth_planted=%d\n", payloadSize, corrupted, corruptNth)), 0o644)
	}

	// ---- verdict ---------------------------------------------------------
	v, err := ledger.Compute("kafka", *scenario, ackedPath, observedPath, true, void)
	if err != nil {
		fatal("%v", err)
	}
	if *rate > 0 {
		attachAckGaps(v, *out+"/acks-per-second.jsonl")
	} else {
		v.AttachAvailability(pubState)
	}
	if err := v.Write(*out + "/verdict.json"); err != nil {
		fatal("%v", err)
	}
	fmt.Println(v.String())
	os.Exit(v.ExitCode())
}

var (
	payloadSize int
	corruptNth  int
)

// payload builds the record value: the id, '|', then filler derived from the id by a
// xorshift generator seeded with the id's FNV hash, so the reader can regenerate and
// compare it without a ledger lookup. With -corrupt-nth, every Nth record (by produce
// sequence n) has one filler byte flipped: the planted fault the content check must catch.
func payload(id string, n int) []byte {
	if payloadSize <= len(id)+1 {
		return []byte(id)
	}
	b := make([]byte, payloadSize)
	copy(b, id)
	b[len(id)] = '|'
	fill(b[len(id)+1:], id)
	if corruptNth > 0 && n > 0 && n%corruptNth == 0 {
		b[len(id)+1+(n%len(b[len(id)+1:]))] ^= 0x5a
	}
	return b
}

func fill(dst []byte, id string) {
	var h uint64 = 14695981039346656037
	for i := 0; i < len(id); i++ {
		h ^= uint64(id[i])
		h *= 1099511628211
	}
	x := h | 1
	for i := range dst {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		dst[i] = byte(x)
	}
}

// parsePayload returns the id and whether the filler matches what payload() would have
// produced for it. Records without a '|' (size 0 runs) are returned as-is.
func parsePayload(v []byte) (string, bool) {
	i := bytes.IndexByte(v, '|')
	if i < 0 {
		return string(v), payloadSize == 0
	}
	id := string(v[:i])
	if payloadSize == 0 {
		return id, true
	}
	if len(v) != payloadSize {
		return id, false
	}
	want := make([]byte, len(v)-i-1)
	fill(want, id)
	return id, bytes.Equal(want, v[i+1:])
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "kafka-verifier: "+format+"\n", args...)
	os.Exit(2)
}

// produceAtRate paces asynchronous produces at rate msg/s for d. A record counts as acked
// only when its produce callback returns no error and a real offset. Records still
// retrying when the run ends are flushed (up to 60 s); anything that never succeeds is
// "not acked", which is not owed. Per-second ack counts go to acks-per-second.jsonl: the
// kill cells read the outage from the longest run of seconds with zero acks.
func produceAtRate(ctx context.Context, prod *kgo.Client, acked *ledger.Writer, topic string, rate int, d time.Duration, out string) int {
	var mu sync.Mutex
	perSec := map[int64]int{}
	var errs int
	start := time.Now()
	fmt.Printf("  steady-rate mode: %d msg/s for %s\n", rate, d)
	const tick = 10 * time.Millisecond
	perTick := float64(rate) * tick.Seconds()
	t := time.NewTicker(tick)
	defer t.Stop()
	var owed float64
	i := 0
	for time.Since(start) < d {
		<-t.C
		owed += perTick
		for ; owed >= 1; owed-- {
			id := fmt.Sprintf("cr-kafka-%09d", i)
			i++
			prod.Produce(ctx, &kgo.Record{Topic: topic, Value: payload(id, i-1)}, func(r *kgo.Record, err error) {
				now := time.Now().UTC()
				mu.Lock()
				defer mu.Unlock()
				if err != nil || r == nil {
					errs++
					return
				}
				perSec[now.Unix()]++
				if werr := acked.Append(ledger.Acked{
					Id: id, T: now, Ack: "acks_all_offset",
					Meta: map[string]any{"topic": topic, "partition": r.Partition, "offset": r.Offset},
				}); werr != nil {
					fmt.Fprintf(os.Stderr, "acked ledger: %v\n", werr)
				}
			})
		}
	}
	fctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	if err := prod.Flush(fctx); err != nil {
		fmt.Printf("  flush after the run ended: %v\n", err)
	}
	mu.Lock()
	defer mu.Unlock()
	w, err := ledger.NewWriter(out + "/acks-per-second.jsonl")
	if err == nil {
		for s := start.Unix(); s <= time.Now().Unix(); s++ {
			_ = w.Append(map[string]any{"t": time.Unix(s, 0).UTC(), "acked": perSec[s]})
		}
		_ = w.Close()
	}
	fmt.Printf("  sent %d records in %s\n", i, time.Since(start).Round(time.Second))
	return i - acked.Count() // sent but never acked (errors plus anything still unflushed)
}

// attachAckGaps sets the verdict's publish-availability fields from the per-second ack
// counts: gaps = runs of seconds with zero acks between the first and last acked second,
// unavailable = the longest such run.
func attachAckGaps(v *ledger.Verdict, path string) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()
	type row struct {
		T     time.Time `json:"t"`
		Acked int       `json:"acked"`
	}
	var rows []row
	dec := json.NewDecoder(f)
	for {
		var r row
		if dec.Decode(&r) != nil {
			break
		}
		rows = append(rows, r)
	}
	first, last := -1, -1
	for i, r := range rows {
		if r.Acked > 0 {
			if first < 0 {
				first = i
			}
			last = i
		}
	}
	if first < 0 {
		return
	}
	run, longest, gaps := 0, 0, 0
	for i := first; i <= last; i++ {
		if rows[i].Acked == 0 {
			if run == 0 {
				gaps++
			}
			run++
			if run > longest {
				longest = run
			}
		} else {
			run = 0
		}
	}
	v.PublishGaps = gaps
	v.PublishUnavailableMs = int64(longest) * 1000
	fmt.Printf("  publish availability: %d gap(s) with zero acked seconds, longest %d s\n", gaps, longest)
}
