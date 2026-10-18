# Method

KubeMQ v3.6.22 (embedded Kafka protocol, 3-node Raft) against Apache Kafka 4.3.1 (KRaft, 3 brokers) on identical
GCP hardware, measured on what a Kafka user asks about:

1. the highest message rate each broker carries (1 KB messages),
2. latency versus load, from light load to the ceiling,
3. large-message bandwidth (100 KB messages),
4. recovery from a killed partition leader, with an acknowledged-loss verifier beside the load,
5. CPU, memory, disk and network each broker spends.

**Every number is one run.** No repeats and no spread are shown; gaps under about 10 % are a tie.

## 1. Rig

One GKE cluster per broker, identical except the broker. Created with `scripts/up.sh` (Terraform in `infra/terraform`).

| | Each cluster |
|---|---|
| Brokers | 3 × n2-standard-8, Intel Ice Lake, one broker per node, 7 CPU / 26 GiB Guaranteed QoS |
| Broker disk | 2 × 375 GB local NVMe as one RAID-0 (about 820 MB/s sequential write, about 12,000 synced writes/s) |
| Drivers | 2 × n2-standard-8, one OpenMessaging Benchmark (OMB) worker each |
| Monitoring | 1 × e2-standard-4: Prometheus and node-exporter; brokers scraped every second; Kafka JMX exporter |
| Kafka | 4.3.1 via Strimzi 1.2.0, KRaft combined broker+controller (not Strimzi's production recommendation; chosen for symmetry with KubeMQ's in-process Raft), 6 GiB heap, tuned configuration in `infra/kafka/kafka.yaml`, 3 replicas, `min.insync.replicas=2` |
| KubeMQ | kubemq-next v3.6.22 pinned by digest (`infra/kubemq/versions.env`), chart 3.5.0, shipped defaults (8 shards, leaders balanced, append-only log, default ack policy), `GOMEMLIMIT=20GiB` |
| Zone | `us-east1-b` |

**What an acknowledgement means.** Kafka with `acks=all`: the leader waits for every in-sync replica (normally 3 of 3)
to have the write in the OS; no per-write fsync. KubeMQ: acknowledged when 2 of 3 Raft members have the write in the
OS; fsync in the background. These are not identical.

Before any cell, every broker node is fingerprinted while its disks are empty (`scripts/baseline.sh`): fio 1 MiB
sequential write and 4 KiB synced write, iperf3 between brokers, `/proc/cpuinfo`, chrony offset. Clusters whose fio or
iperf results differ by more than 10 % are recreated. The results are in `results/<date>/infra/*/baseline/`.

## 2. Client

Stock OMB Kafka driver, Apache kafka-clients 3.6.1. Two client profiles; `run.sh --profile std|b512` picks
`omb/drivers/` or `omb/drivers-512k/` and refuses to run unless the KubeMQ and Kafka driver files differ only in
`bootstrap.servers`.

| Profile | batch.size | buffer.memory | Both profiles |
|---|---|---|---|
| `std` | 131072 (128 KB) | default | `acks=all`, `enable.idempotence=true`, `linger.ms=1`, `max.in.flight.requests.per.connection=5`, no compression, `enable.auto.commit=false`, `group.protocol=classic` |
| `b512` | 524288 (512 KB) | 128 MiB | same |

Consumers: `receive.buffer.bytes=-1`, so the kernel sizes the socket receive buffer. The kafka-clients default pins it
at 64 KB, which at these rates adds a stall of about 250 ms per consumer connection a few times a minute, a property of
the client and not of either broker. The published Kafka runs (2026-10-04) predate this setting and used the default;
their end-to-end latency tracks their publish latency at every rung, so it is set by the producer side, not by the
consumer buffer.

Workload: 1 topic, 16 partitions, 3 replicas, `min.insync.replicas=2`, 4 producers, 4 consumers (one subscription).

## 3. Cells

A 2-minute warm-up precedes every measured window. One discarded warm-up run follows every deployment. Each KubeMQ run
starts on a freshly reinstalled KubeMQ with empty volumes (`scripts/fresh-kubemq.sh`); each Kafka run starts on the
running cluster after the previous run's topic was deleted.

| Cell | What | Client | Rates | Measured |
|---|---|---|---|---|
| B | Light-load latency, 1 KB | `std` | 100k msg/s | 15 min |
| A1 | Rate ladder, 1 KB | `std` | from 300k in 100k steps (200k once if 300k is not reached) | 5 min per rung |
| A2 | Rate ladder, 1 KB | `b512` | from A1's highest reached rung, 100k steps | 5 min per rung |
| C | 100 KB messages | `b512` | 4,000, 6,000, 8,000 msg/s (≈ 410, 615, 820 MB/s) | 5 min per rung |
| D | Leader kill at 50k msg/s, 1 KB | `std` | SIGKILL at minute 5, verifier beside the load | 15 min |

With the 128 KB client a 100 KB record cannot share a batch, so cell C uses the 512 KB client. Rungs run past the
point where a side stopped are marked "extra".

## 4. Rules per rung

`scripts/gate.sh` computes, identically for both sides, from OMB's 10-second statistics over the measured window. A
ladder stops at the first rung failing any of:

1. carried publish rate ≥ 99 % of target over the window and over its last 60 s;
2. backlog not growing: mean backlog of the last minute minus that of the first minute ≤ 1 s of the target rate;
3. OMB publish-delay p99 < 100 ms (the producers kept schedule; a producer running seconds behind would otherwise
   still "carry" the rate on paper).

No pass/fail verdict is published; latency and resource figures are shown as measured at every rung.

The saturated resource per rung comes from the 1-second export: disk (≥ 90 % of fio), CPU (≥ 6.5 of 7 cores), network
(≥ 90 % of iperf), driver (worker CPU ≥ 6.5 cores), client window (publish-delay rising with none of those), or none.

A run with an infrastructure fault (driver restart, preemption, missing telemetry, chrony offset > 5 ms on a driver
node) is void and rerun once; anything else stands.

Percentiles reported: publish p50 / p99 / p99.9 and end-to-end p50 / p99 / p99.9 (cross-node clocks, millisecond
resolution, chrony offsets recorded), publish-delay p99.

## 5. Leader kill (cell D)

Seconds before the kill, `scripts/chaos-kill.sh` finds the broker leading the most partitions with a Metadata request
(on a tie, the lowest broker id). The broker process is killed with SIGKILL from the node; the container restarts on the
same volume. Proof: restart count +1, same volume and data-directory inode, same image digest, survivors untouched. A
separate verifier (`verifier/`) publishes 5,000 msg/s with an acknowledged-ID ledger on its own topic and reads every
acknowledged message back. Reported: acknowledged messages lost, longest interval with no acknowledgements, time until
the killed broker is Ready, time until all replicas are in sync.

## 6. Resources

From each run's 1-second Prometheus export, per broker: CPU cores, memory working set, disk write MB/s and busy %,
bytes written per payload byte, network MB/s; derived: cores per 100k msg/s and dollars per billion messages (on-demand
list price of the 3 broker nodes ÷ messages carried per hour).

## 7. Known limits

- One run per cell.
- Kafka's configuration was tuned and reviewed only by us, the vendor, not by an outside Kafka engineer.
- The two brokers acknowledge differently (section 1).
- The two sides ran on different days and cluster incarnations, same hardware shape, baselines within 10 %.
- Not tested: fan-out, soak, backlog drain, many partitions, strict fsync, repeats.
- Fixed-rate runs only: OMB's unthrottled mode understates one side.
