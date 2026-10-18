# KubeMQ vs Apache Kafka — benchmark on identical GCP hardware

**This benchmark is run and published by KubeMQ, the vendor of one of the two systems.** Read it with that in mind.
Everything needed to rerun it is here: cluster creation, both deployments, the frozen workloads, the runner, the raw
results and the generated report. The KubeMQ half needs a licence key from KubeMQ (see [`PREREQS.md`](PREREQS.md)).

**Results page:** <https://kubemq-io.github.io/kubemq-vs-kafka-benchmark/>

**Question:** on two identical GKE clusters (3 × n2-standard-8 brokers on local NVMe, 2 driver nodes, the
OpenMessaging Benchmark client with Apache kafka-clients 3.6.1), how do KubeMQ v3.6.22 (embedded Kafka protocol,
3-node Raft, shipped defaults) and Apache Kafka 4.3.1 (KRaft, 3 brokers, tuned) compare on:

1. the highest message rate each carries (1 KB messages);
2. latency versus load, from 100k msg/s to the ceiling;
3. 100 KB message bandwidth;
4. recovery from a killed partition leader, with an acknowledged-loss verifier beside the load;
5. CPU, memory, disk and network per broker.

KubeMQ measured 2026-10-16, Kafka 2026-10-04, on the same hardware shape (baselines within 10 %).
**Every number is one run.** No repeats, no spread shown; gaps under about 10 % are a tie.

## Headline numbers

| Measure | KubeMQ v3.6.22 | Apache Kafka 4.3.1 |
|---|---|---|
| **Throughput, 1 KB messages, 16 partitions, 3 replicas** | | |
| Highest target rate reached, 128 KB client batches | **700k msg/s** | 300k msg/s |
| Highest target rate reached, 512 KB client batches | **700k msg/s** | 400k msg/s |
| Next rung after the top, 128 KB client | 800k target → 745k achieved; 93.1 % of target; producers 30.2 s behind schedule | 400k target → 400k achieved; publish-delay p99 838 ms |
| **End-to-end latency p50 / p99, 1 KB, 128 KB client batches** | | |
| at 100k msg/s (15 min) | **2 / 4 ms** | 2 / 114 ms |
| at 300k msg/s | **3 / 6 ms** | 3 / 274 ms |
| at 400k msg/s | 4 / 9 ms | 20 / 625 ms (publish-delay p99 838 ms) |
| at 500k msg/s | 4 / 15 ms | — |
| at 600k msg/s | 5 / 20 ms | — |
| **Publish latency p99 (producer ack), 1 KB, 128 KB client batches** | | |
| at 100k msg/s | **3 ms** | 114 ms |
| at 300k msg/s | **4 ms** | 275 ms |
| at 400k msg/s | **5 ms** | 631 ms |
| **Large messages, 100 KB, 512 KB client batches** | | |
| Highest target rate reached | **6,000 msg/s = 614 MB/s** | 4,000 msg/s = 410 MB/s |
| Achieved at 6,000 msg/s target (614 MB/s) | **6,005 msg/s = 615 MB/s** | 4,694 msg/s = 481 MB/s |
| Achieved at 8,000 msg/s target (819 MB/s) | **7,601 msg/s = 778 MB/s** | 4,738 msg/s = 485 MB/s (extra) |
| **Resilience: partition leader killed (SIGKILL) at 50k msg/s** | | |
| Acknowledged messages lost | 0 | 0 |
| Longest interval with no acknowledgements | **2 s** | 8 s |
| Killed server Ready again | **13 s** | 23 s |
| All replicas back in sync | **31 s** | 59 s |
| **Resources at 300k msg/s, 1 KB, 128 KB client (highest rung both reached)** | | |
| Broker CPU, cores of 7 (mean per broker) | 4.3 | **2.8** |
| Broker memory working set (cgroup, includes page cache) | **2.5 GiB** | 24.6 GiB |
| Disk written per broker | 334 MB/s | 314 MB/s |
| Bytes written per payload byte | 1.09 | 1.02 |
| Network per broker (in + out) | 649 MB/s | 635 MB/s |

Full tables, the latency-versus-load curves, per-rung resources and the kill timeline are on the
[results page](https://kubemq-io.github.io/kubemq-vs-kafka-benchmark/). Method: [`docs/METHOD.md`](docs/METHOD.md).

## Known weaknesses

- One run per cell.
- Kafka's broker configuration was tuned by us ([`tuning/kafka.md`](tuning/kafka.md)) and reviewed only by us, the
  vendor, not by an outside Kafka engineer.
- The two brokers acknowledge differently: Kafka `acks=all` waits for every in-sync replica to have the write in the
  OS, no per-write fsync; KubeMQ acknowledges when 2 of 3 Raft members have the write in the OS and syncs in the
  background.
- The two sides ran on different days and cluster incarnations, same hardware shape.
- KubeMQ consumers ran with `receive.buffer.bytes=-1` (kernel-sized socket receive buffer); the Kafka runs used the
  kafka-clients default. Kafka's end-to-end latency tracks its publish latency at every rung, so it is set by the
  producer side, not by the consumer buffer.
- Each KubeMQ run started on a freshly reinstalled KubeMQ with empty volumes; each Kafka run started on the running
  cluster after the previous run's topic was deleted.

## Reproduce

Prerequisites: [`PREREQS.md`](PREREQS.md). Then:

```bash
echo "$KUBEMQ_LICENSE_KEY" > .license.txt          # KubeMQ half only
terraform -chdir=infra/terraform init
scripts/build-images.sh                            # OMB and verifier images
scripts/up.sh kafka                                # cluster a: Kafka
scripts/up.sh kubemq                               # cluster b: KubeMQ
scripts/omb-deploy.sh --slot a && scripts/omb-deploy.sh --slot b
scripts/compat-check.sh --slot b
caffeinate -i scripts/matrix.sh --slot a           # terminal 1
caffeinate -i scripts/matrix.sh --slot b           # terminal 2
scripts/report.py results/<kubemq-date> results/<kafka-date>
scripts/down.sh --slot a && scripts/down.sh --slot b
```

`up.sh` schedules `down.sh` 12 hours after every start, so a forgotten cluster does not keep billing.

## Layout

| Path | What |
|---|---|
| `docs/METHOD.md` | rig, client, cells, stop rules, leader kill, resources, known limits |
| `infra/` | Terraform, Kafka (Strimzi), KubeMQ (chart and operator values, pinned image), monitoring, local SSD |
| `omb/` | OpenMessaging Benchmark image, driver files (128 KB and 512 KB client batches), workloads |
| `verifier/` | acknowledged-loss verifier and protocol checks used beside the leader kill |
| `scripts/` | cluster lifecycle, baseline, run, gate, matrix, leader kill, report |
| `tuning/kafka.md` | how the Kafka configuration was tuned |
| `results/2026-10-16/` | KubeMQ runs: OMB JSON, 1 s Prometheus exports, logs, verifier ledgers, baselines, digests |
| `results/2026-10-04/` | Kafka runs, same layout |
| `report/` | the generated results page (GitHub Pages) |

## License

Apache-2.0, see [`LICENSE`](LICENSE).
