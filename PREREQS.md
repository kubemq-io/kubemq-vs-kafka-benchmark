# Prerequisites for reproducing

- A GCP project with billing, and quota in one zone for both clusters at once: 80 N2 vCPUs
  (2 × 5 × `n2-standard-8`), 12 Local SSD partitions (2 per broker node × 3 × 2 clusters),
  2 × `e2-standard-4`. Check: `gcloud compute regions describe <region> --format='value(quotas)'`.
- A KubeMQ licence key. **The KubeMQ half cannot run without one; keys come from KubeMQ, not
  self-serve.** Put it in `.license.txt` at the repository root (gitignored) or export it as
  `KUBEMQ_LICENSE_KEY`; `scripts/up.sh` turns it into a Kubernetes Secret. Never commit it.
- Tools: `gcloud`, `kubectl`, `helm` ≥ 3.14, `terraform` ≥ 1.6, `docker` or `podman`, Go ≥ 1.24,
  `jq`, `python3`. `fio` and `iperf3` run as pods, not on your machine.
- A container registry you can push to (for the OpenMessaging Benchmark and verifier images
  `scripts/build-images.sh` builds).
- Time and money: both clusters cost about $6 per hour together; one day (bring-up, ~6–7 h of
  cells per side in parallel, report) is roughly $50. `up.sh` schedules a `down.sh` 12 hours after
  every start; set a billing budget alert before the first run. Start `matrix.sh` and keep the
  laptop awake (`caffeinate -i`), because the TTL poller runs on your machine.
