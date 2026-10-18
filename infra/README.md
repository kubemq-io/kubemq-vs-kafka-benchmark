# Infrastructure

Two GKE clusters, `kkbench-a` and `kkbench-b`, identical except the broker. One Terraform
module (`terraform/`), one Terraform workspace per slot, state kept locally and gitignored.

```bash
export KUBEMQ_LICENSE_KEY=...          # KubeMQ half only; from KubeMQ sales
scripts/up.sh kafka                    # slot a
scripts/up.sh kubemq                   # slot b
scripts/baseline.sh compare results/<date>/infra/a-kafka-*/baseline results/<date>/infra/b-kubemq-*/baseline
scripts/down.sh --slot a && scripts/down.sh --slot b
```

## Pinned versions

| Component | Version | Pin |
|---|---|---|
| GKE control plane and nodes | 1.36.4-gke.1247000, REGULAR channel with a 30-day no-upgrades exclusion (GKE no longer allows new projects to opt out of channels) | `terraform/variables.tf` |
| Terraform Google provider | 8.4.0 | `terraform/.terraform.lock.hcl` |
| Strimzi | 1.2.0 (lists Kafka 4.3.1 as supported) | operator digest in `kafka/operator-values.yaml` |
| Apache Kafka | 4.3.1 | `quay.io/strimzi/kafka@sha256:e90a1a74…` in `kafka/kafka.yaml` |
| KubeMQ | v3.6.22, chart 3.5.0, operator v3.5.0 | tag + digest in `kubemq/*.yaml` |
| kube-prometheus-stack | 91.8.1 | `monitoring/versions.env` |
| local static provisioner | v2.9.0 | digest in `local-ssd/provisioner.yaml` |

## Choices worth knowing

- **Disk.** Each broker node has 2 × 375 GB NVMe Local SSD, attached as raw block devices
  (never ephemeral-storage mode, where a pod delete wipes data). `local-ssd/raid.yaml`
  stripes them into one RAID-0 array, ext4 with lazy init off, mounted at `/mnt/disks/raid0`;
  the local static provisioner publishes it as one PersistentVolume per node. Both brokers get
  one 600 GiB volume on that stripe: KubeMQ asks for exactly one volume per server, so Kafka
  gets a single-volume JBOD rather than two disks.
- **Placement.** The KubeMQ operator does not expose tolerations, so the broker pool carries no
  taint on either side; brokers are pinned by node label with required anti-affinity. The driver
  pool is tainted. Both operators run on the monitoring node (the KubeMQ chart has no operator
  nodeSelector, so `up.sh` patches one in). Whatever else lands on broker nodes is recorded per
  incarnation in `broker-node-pods.tsv`.
- **Google's agents.** Managed Prometheus and workload logging are off; only system-component
  logging and metrics remain. Our own Prometheus on the monitoring node is the metrics path.
- **TTL.** `scripts/ttl.sh` runs `down.sh` 10 hours after `up.sh` from a background process on
  the machine that ran `up.sh`. It polls the wall clock, so laptop sleep still counts, and on
  macOS it holds off idle sleep; it does not survive that machine being shut down. The deadline
  is also a cluster label (`ttl-deadline`), visible in `gcloud container clusters list`.
- **Zone.** The plan names `us-central1-a`. On 2026-09-28 every us-central1 zone refused
  `n2-standard-8` with the Ice Lake pin (stockout; without the pin, us-central1-a handed out
  Cascade Lake). Single-machine probes succeeded in us-east1-b/c, us-east4-a/c, us-west1-a/b and
  us-south1-a, so the default is now `us-east1-b`. Override with `GCP_ZONE`; both clusters must
  share a zone.
