# Kafka tuning session — 2026-09-28 23:53 to 2026-09-29 01:27 UTC (1 h 33 min of a 2 h budget)

Rules (docs/METHOD.md): workload T1 only (peak, 16 partitions, 1 KB); knobs limited to
`num.io.threads`, `num.network.threads`, `num.replica.fetchers`, `socket.*.buffer.bytes`,
`log.segment.bytes`, `replica.fetch.max.bytes` (the tuning script refused anything else).
Each step: Strimzi rolling restart, wait until no partition is under-replicated, one discarded
warm-up, two T1 runs. A step is kept only if **both** runs beat the best run before it.

Cluster `kkbench-a`, Apache Kafka 4.3.1, 2 OMB workers (the driver-ceiling control showed 4 workers
do not raise the peak).

| Step | Settings added | T1 runs (msg/s) | Kept? |
|---|---|---|---|
| untuned | — | 325,823 · 352,419 · 336,189 · 336,131 | baseline, best 352,419 |
| A | `num.network.threads=8`, `num.io.threads=16` | 344,382 · 339,235 | no (neither beats 352,419) |
| B | A + `num.replica.fetchers=4`, `replica.fetch.max.bytes=10485760` | 379,242 · 358,193 | yes (both beat 352,419) |
| C | B + `socket.send.buffer.bytes=1048576`, `socket.receive.buffer.bytes=1048576` | 401,399 · 426,781 | yes (both beat 379,242) |

Result: step C is the Kafka configuration for the matrix (`infra/kafka/kafka.yaml`). Step A's
thread counts ride along inside B and C; on their own they made no measurable difference.
Median T1 moved from about 336k to about 414k msg/s (+23%) on two runs.

`log.segment.bytes` was not tried: T1 runs are 7 minutes and the default 1 GiB segment rolls a few
times per partition at these rates, which is not a plausible bottleneck; the budget went to the
replication and socket knobs instead.

The raw tuning runs are not part of this repository.
