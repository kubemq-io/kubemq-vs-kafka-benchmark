// kafka-compat checks one protocol behaviour the benchmark depends on and OMB never looks at:
// a record's producer-set CreateTime timestamp comes back unchanged. OMB's end-to-end latency
// is computed from that timestamp, so a broker that rewrote it would corrupt every
// end-to-end number without any error.
//
// It writes records with a known, distinctive timestamp to every partition of a fresh topic,
// reads them back from the start, and requires every one to carry the exact timestamp it was
// sent with and timestamp type CreateTime.
//
// -corrupt-ms N is the red gate: it adds N ms to the expected timestamp on the read side, so
// a healthy broker must FAIL. A check that has never failed has not been shown to work.
//
// Exit codes: 0 PASS, 1 FAIL, 2 could not run (setup error).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	addr := flag.String("addr", "localhost:9092", "bootstrap host:port")
	partitions := flag.Int("partitions", 16, "partitions of the fresh topic")
	perPartition := flag.Int("per-partition", 10, "records written to each partition")
	corruptMs := flag.Int64("corrupt-ms", 0, "RED GATE: expect timestamps shifted by N ms, so a correct broker fails")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	topic := fmt.Sprintf("cr-kafka-compat-%d", time.Now().UnixNano())

	cl, err := kgo.NewClient(kgo.SeedBrokers(*addr), kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		setup("client: %v", err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	if _, err := adm.CreateTopic(ctx, int32(*partitions), 3, nil, topic); err != nil {
		setup("create topic %s: %v", topic, err)
	}
	defer func() { _, _ = adm.DeleteTopics(context.Background(), topic) }()

	// A fixed, distinctive base far from "now", so a broker substituting its own clock
	// cannot pass by coincidence.
	base := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	want := map[string]int64{}
	for p := 0; p < *partitions; p++ {
		for i := 0; i < *perPartition; i++ {
			key := fmt.Sprintf("p%02d-%03d", p, i)
			ts := base.Add(time.Duration(p*1000+i) * time.Millisecond)
			want[key] = ts.UnixMilli()
			r := &kgo.Record{Topic: topic, Partition: int32(p), Key: []byte(key), Value: []byte(key), Timestamp: ts}
			if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
				setup("produce %s: %v", key, err)
			}
		}
	}

	rd, err := kgo.NewClient(kgo.SeedBrokers(*addr), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		setup("reader: %v", err)
	}
	defer rd.Close()

	bad, seen := 0, 0
	perPart := map[int32]int{}
	for seen < len(want) && ctx.Err() == nil {
		fs := rd.PollFetches(ctx)
		fs.EachRecord(func(r *kgo.Record) {
			seen++
			perPart[r.Partition]++
			exp, ok := want[string(r.Key)]
			if !ok {
				return
			}
			exp += *corruptMs
			got := r.Timestamp.UnixMilli()
			if got != exp || r.Attrs.TimestampType() != 0 {
				if bad < 5 {
					fmt.Printf("  MISMATCH %s partition %d: sent %d, got %d (type %d; 0=CreateTime)\n",
						r.Key, r.Partition, exp, got, r.Attrs.TimestampType())
				}
				bad++
			}
		})
	}
	fmt.Printf("kafka-compat: topic=%s partitions=%d records sent=%d read=%d partitions-read=%d\n",
		topic, *partitions, len(want), seen, len(perPart))
	if *corruptMs != 0 {
		fmt.Println("  RED GATE ACTIVE: expecting timestamps shifted by " + strconv.FormatInt(*corruptMs, 10) + " ms")
	}
	switch {
	case seen < len(want):
		fmt.Printf("FAIL: read %d of %d records\n", seen, len(want))
		os.Exit(1)
	case len(perPart) != *partitions:
		fmt.Printf("FAIL: records came back from %d of %d partitions\n", len(perPart), *partitions)
		os.Exit(1)
	case bad > 0:
		fmt.Printf("FAIL: %d of %d records came back with a different timestamp or type\n", bad, seen)
		os.Exit(1)
	}
	fmt.Println("PASS: every record came back with its exact CreateTime timestamp, from every partition")
}

func setup(f string, a ...any) {
	fmt.Fprintf(os.Stderr, "kafka-compat: "+f+"\n", a...)
	os.Exit(2)
}
