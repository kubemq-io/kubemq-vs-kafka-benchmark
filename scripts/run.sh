#!/usr/bin/env bash
#
# run.sh — run ONE workload once against the broker on one cluster, with every guard.
#
#   run.sh --slot a|b --cell <workload> [--repeat N | --warmup | --label NAME] [--clean] [--profile std|b512]
#   run.sh --check-drivers            only the driver-file diff guard (and its planted-fault test)
#
# <workload> is a file name in omb/workloads without .yaml (e.g. t1-peak-16p).
# --warmup marks the discarded first run after a deploy (recorded, never reported).
# --label NAME records a shakedown/control run under that name (never a matrix repeat).
# --clean deletes leftover benchmark topics instead of refusing on them.
# --profile std (default) uses omb/drivers/, b512 uses omb/drivers-512k/ (batch.size 512 KB, buffer.memory
#   128 MiB). The diff guard runs on whichever pair is chosen; the profile is recorded in the run
#   directory name (<run>-b512) and in driver.yaml.
#
# Refuses to run unless:
#   - omb/drivers/kafka.yaml and kubemq.yaml differ in the bootstrap.servers line ONLY
#   - no benchmark topic from an earlier run exists on this broker
#   - disk used on every broker node's local SSD is below 40%
# Records (results/<date>/runs/<broker>/<cell>/<rN|warmup-time>/):
#   OMB result JSON and log, driver and workload files used, digests before/after (the run
#   is VOID if they differ), disk before/after, chrony tracking before/after, topics after,
#   end offsets per partition (the run fails if any partition received nothing), worker and
#   broker logs for the run window (workers/, brokers/), and the restart evidence for every
#   broker pod (pods/, logs/, identity/, events.txt, operator.log, nodes/; see lib.sh
#   capture_broker_evidence), a continuous log stream per pod (logs/<p>.stream.log) and the
#   Prometheus range export for the window (prom/, with prom/MISSING.txt naming any family
#   that came back empty).
#
source "$(dirname "$0")/lib.sh"

DRIVERS_DIR="$REPO_ROOT/omb/drivers"
DISK_LIMIT_PCT=40

# --- driver-file guard --------------------------------------------------------
# Compare the two files with comments and blank lines dropped and the bootstrap value
# masked. Anything left over is a difference that could favour one side.
drivers_identical() {  # $1 $2 files
  norm() { grep -vE '^[[:space:]]*(#|$)' "$1" | sed -E 's/^([[:space:]]*bootstrap\.servers=).*/\1<masked>/'; }
  diff <(norm "$1") <(norm "$2")
}

check_drivers() {
  drivers_identical "$DRIVERS_DIR/kafka.yaml" "$DRIVERS_DIR/kubemq.yaml" >/dev/null ||
    { drivers_identical "$DRIVERS_DIR/kafka.yaml" "$DRIVERS_DIR/kubemq.yaml" >&2; die "driver files differ beyond bootstrap.servers. Refusing."; }
  # Show the guard failing on a planted one-line difference, every time it is used.
  local planted; planted="$(mktemp)"
  sed 's/^  linger.ms=1$/  linger.ms=5/' "$DRIVERS_DIR/kubemq.yaml" > "$planted"
  if drivers_identical "$DRIVERS_DIR/kafka.yaml" "$planted" >/dev/null; then
    rm -f "$planted"; die "the driver diff guard did NOT catch a planted difference; the guard is broken."
  fi
  rm -f "$planted"
  echo "driver files: identical except bootstrap.servers (guard caught a planted linger.ms change)"
}

if [ "${1:-}" = "--check-drivers" ]; then check_drivers; exit 0; fi

SLOT=""; CELL=""; REPEAT=""; WARMUP=0; CLEAN=0; LABEL=""; PROFILE="std"
while [ $# -gt 0 ]; do
  case "$1" in
    --slot) SLOT="$2"; shift 2 ;;
    --cell) CELL="$2"; shift 2 ;;
    --repeat) REPEAT="$2"; shift 2 ;;
    --warmup) WARMUP=1; shift ;;
    --label) LABEL="$2"; shift 2 ;;
    --clean) CLEAN=1; shift ;;
    --profile) PROFILE="$2"; shift 2 ;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) die "unknown arg: $1" ;;
  esac
done
check_slot "$SLOT"
case "$PROFILE" in std) DRIVERS_DIR="$REPO_ROOT/omb/drivers" ;; b512) DRIVERS_DIR="$REPO_ROOT/omb/drivers-512k" ;; rcvbuf) DRIVERS_DIR="$REPO_ROOT/omb/drivers-rcvbuf" ;; *) die "--profile must be std, b512 or rcvbuf" ;; esac
WORKLOAD="$REPO_ROOT/omb/workloads/$CELL.yaml"
[ -f "$WORKLOAD" ] || die "no workload omb/workloads/$CELL.yaml"
[ "$WARMUP" = 1 ] || [ -n "$REPEAT" ] || [ -n "$LABEL" ] || die "give --repeat N, --warmup or --label NAME"
NAME="$(cluster_name "$SLOT")"; guard_name "$NAME"
BROKER="$(cluster_labels "$NAME" | jq -r '.broker // empty')"
check_broker "$BROKER"
DRIVER="$DRIVERS_DIR/$BROKER.yaml"
BOOT="$(sed -nE 's/^[[:space:]]*bootstrap\.servers=(.*)$/\1/p' "$DRIVER")"

check_drivers

RUN_ID="r$REPEAT"; [ "$WARMUP" = 1 ] && RUN_ID="warmup-$(date -u +%H%M%S)"; [ -n "$LABEL" ] && RUN_ID="$LABEL"
[ "$PROFILE" = std ] || RUN_ID="$RUN_ID-$PROFILE"
OUT="$REPO_ROOT/results/${KKBENCH_RESULTS_DATE:-$(date -u +%Y-%m-%d)}/runs/$BROKER/$CELL/$RUN_ID"
[ ! -e "$OUT/result.json" ] || die "$OUT already has a result; refusing to overwrite."
mkdir -p "$OUT"
exec > >(tee -a "$OUT/run.log") 2>&1
note "$BROKER / $CELL / $RUN_ID on $NAME"

tools() { k -n omb exec deploy/kafka-tools -c tools -- "$@"; }
BROKER_SEL="$(broker_selector "$BROKER")"; BROKER_CONTAINER="$(broker_container "$BROKER")"

list_bench_topics() {
  tools /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOT" --list 2>/dev/null |
    grep -E '^(test-topic-|cr-kafka-)' || true
}

disk_report() {  # prints "node used% usedGB" per broker node; returns 1 if any >= limit
  local bad=0 pod node line pct
  while IFS='=' read -r pod node; do
    [ -n "$pod" ] || continue
    line="$(k -n local-ssd exec "$pod" -- nsenter -t 1 -m -- df -P -B1G /mnt/disks/raid0 | tail -1)"
    pct="$(awk '{gsub("%","",$5); print $5}' <<<"$line")"
    echo "$node ${pct}% $(awk '{print $3}' <<<"$line")GB"
    [ "$pct" -lt "$DISK_LIMIT_PCT" ] || bad=1
  done <<<"$(raid_pods)"
  return "$bad"
}

chrony_report() {
  local pod node
  while IFS='=' read -r pod node; do
    [ -n "$pod" ] || continue
    echo "== $node"; k -n local-ssd exec "$pod" -- nsenter -t 1 -m -- chronyc tracking 2>&1 | grep -E 'System time|Last offset|RMS offset'
  done <<<"$(raid_pods)"
}

# --- pre-run guards -------------------------------------------------------------
LEFT="$(list_bench_topics)"
# A listed topic that the server itself describes as non-existent is a phantom listing (2026-10-01,
# v3.6.6: a deleted topic flaps back into ListTopics for 30+ min after a kill). It cannot hold data
# or partitions, so it does not block the run; it is recorded in <run>/phantom-topics.txt.
if [ -n "$LEFT" ]; then
  REAL=""
  for t in $LEFT; do
    d="$(tools /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOT" --describe --topic "$t" 2>&1)"
    if grep -q 'PartitionCount' <<<"$d"; then REAL="$REAL $t"
    else echo "$(utc) $t listed but not describable (phantom): $(grep -oE 'Error while executing topic command : .*' <<<"$d" | head -1 | cut -c1-120)" >> "$OUT/phantom-topics.txt"; fi
  done
  [ -s "$OUT/phantom-topics.txt" ] && note "phantom topic listing ignored: $(grep -c . "$OUT/phantom-topics.txt") (see phantom-topics.txt)"
  LEFT="${REAL# }"
fi
if [ -n "$LEFT" ]; then
  if [ "$CLEAN" = 1 ]; then
    note "deleting leftover benchmark topics: $(tr '\n' ' ' <<<"$LEFT")"
    for t in $LEFT; do tools /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOT" --delete --topic "$t" >/dev/null; done
    sleep 10
    [ -z "$(list_bench_topics)" ] || die "topics still present after delete: $(list_bench_topics | tr '\n' ' ')"
  else
    die "benchmark topics from an earlier run still exist: $(tr '\n' ' ' <<<"$LEFT"). Re-run with --clean."
  fi
fi
echo "topics: no benchmark topics present"
disk_report > "$OUT/disk-before.txt" || { cat "$OUT/disk-before.txt"; die "a broker disk is at or above ${DISK_LIMIT_PCT}% used"; }
note "disk before: $(tr '\n' ';' < "$OUT/disk-before.txt")"
chrony_report > "$OUT/chrony-before.txt"
"$REPO_ROOT/scripts/record-digests.sh" --slot "$SLOT" --ns "$BROKER" --out "$OUT/digests-before.tsv" >/dev/null ||
  die "digest recording failed"
cp "$DRIVER" "$OUT/driver.yaml"; cp "$WORKLOAD" "$OUT/workload.yaml"

# --- run ------------------------------------------------------------------------
COORD="$(k -n omb get pods -l app=omb-coordinator -o jsonpath='{.items[0].metadata.name}')"
[ -n "$COORD" ] || die "no coordinator pod: run scripts/omb-deploy.sh --slot $SLOT"
WORKERS="$(k -n omb get pods -l app=omb-worker -o jsonpath='{range .items[*]}http://{.metadata.name}.omb-worker.omb.svc.cluster.local:8080,{end}')"
WORKERS="${WORKERS%,}"
[ -n "$WORKERS" ] || die "no OMB workers"
cx() { k -n omb exec "$COORD" -- "$@"; }
cx sh -c 'rm -rf /tmp/run && mkdir -p /tmp/run'
k -n omb exec -i "$COORD" -- sh -c 'cat > /tmp/run/driver.yaml' < "$DRIVER"
k -n omb exec -i "$COORD" -- sh -c 'cat > /tmp/run/workload.yaml' < "$WORKLOAD"
note "OMB start $(utc) — workers: $WORKERS"
date -u +%Y-%m-%dT%H:%M:%SZ > "$OUT/start.txt"
# Kernel TCP counters of every worker and broker pod, before and after (scripts/tcpstat.sh): the one layer the
# Prometheus export cannot see. Best effort; never fails the run.
"$REPO_ROOT/scripts/tcpstat.sh" --slot "$SLOT" --out "$OUT/tcp" --tag before 2>&1 | tail -1 || true
# Follow every broker pod's log from here: kubelet rotates at 10 MiB and a busy run loses the head.
start_log_streams "$BROKER" "$BROKER_SEL" "$BROKER_CONTAINER" "$OUT"
# Detached inside the pod: an exec stream through the API server can drop mid-run.
cx sh -c "cd /benchmark && (bin/benchmark --drivers /tmp/run/driver.yaml --workers $WORKERS -o /tmp/run/result.json /tmp/run/workload.yaml > /tmp/run/omb.log 2>&1; echo \$? > /tmp/run/exit) >/dev/null 2>&1 &"

# Warm-up + test duration + setup slack.
DUR_MIN="$(awk '/^testDurationMinutes:/ {t=$2} /^warmupDurationMinutes:/ {w=$2} END {print t+w}' "$WORKLOAD")"
DEADLINE=$(( $(date +%s) + DUR_MIN * 60 + 600 ))
while ! cx test -f /tmp/run/exit 2>/dev/null; do
  [ "$(date +%s)" -lt "$DEADLINE" ] || { cx sh -c 'pkill -f io.openmessaging.benchmark.Benchmark' || true; die "OMB did not finish in ${DUR_MIN} min + 10 min slack"; }
  sleep 20
done
date -u +%Y-%m-%dT%H:%M:%SZ > "$OUT/end.txt"
"$REPO_ROOT/scripts/tcpstat.sh" --slot "$SLOT" --out "$OUT/tcp" --tag after 2>&1 | tail -1 || true
python3 "$REPO_ROOT/scripts/tcpstat-diff.py" "$OUT" > "$OUT/tcp/deltas.txt" 2>&1 || true
RC="$(cx cat /tmp/run/exit)"
cx cat /tmp/run/omb.log > "$OUT/omb.log"
cx cat /tmp/run/result.json > "$OUT/result.json" 2>/dev/null || rm -f "$OUT/result.json"
note "OMB end $(utc), exit $RC"

# --- post-run --------------------------------------------------------------------
# Client-side evidence: every worker's log for the run window (commit failures, rebalances,
# retries). Without these the cause of duplicate deliveries on 2026-09-28 could not be shown.
mkdir -p "$OUT/workers"
RUN_SECS=$(( $(date +%s) - $(date -u -j -f %Y-%m-%dT%H:%M:%SZ "$(cat "$OUT/start.txt")" +%s 2>/dev/null || date -u -d "$(cat "$OUT/start.txt")" +%s) + 60 ))
for w in $(k -n omb get pods -l app=omb-worker -o name); do
  k -n omb logs "$w" --since="${RUN_SECS}s" > "$OUT/workers/${w#pod/}.log" 2>&1
done
gzip -f "$OUT"/workers/*.log 2>/dev/null
# Server-side evidence: every broker container's log for the same window.
mkdir -p "$OUT/brokers"
for b in $(k -n "$BROKER" get pods -o name); do
  k -n "$BROKER" logs "$b" --all-containers --since="${RUN_SECS}s" > "$OUT/brokers/${b#pod/}.log" 2>&1
done
gzip -f "$OUT"/brokers/*.log 2>/dev/null
# Restart evidence for every broker pod (previous-container log, termination state, applied
# probes, events, operator log, kernel OOM lines per node) and the process-identity banners.
capture_broker_evidence "$BROKER" "$BROKER_SEL" "$BROKER_CONTAINER" "$OUT" "$RUN_SECS"
stop_log_streams "$OUT"
gzip -f "$OUT"/logs/*.log 2>/dev/null
# Raw Prometheus series for the window, one minute of margin each side; the analysis reads these.
mkdir -p "$OUT/prom"
if prom_export "$OUT/prom" "$(from_epoch $(( $(to_epoch "$(cat "$OUT/start.txt")") - 60 )))" \
                           "$(from_epoch $(( $(to_epoch "$(cat "$OUT/end.txt")") + 60 )))" > "$OUT/prom/export.txt"; then
  cat "$OUT/prom/export.txt"
else
  cat "$OUT/prom/export.txt"; echo "prom export: families missing, see ${OUT#"$REPO_ROOT"/}/prom/MISSING.txt"
fi
gzip -f "$OUT"/prom/*.json 2>/dev/null   # ~15x smaller; readers use gunzip -c
"$REPO_ROOT/scripts/record-digests.sh" --slot "$SLOT" --ns "$BROKER" --out "$OUT/digests-after.tsv" >/dev/null
VOID=0
"$REPO_ROOT/scripts/record-digests.sh" --compare "$OUT/digests-before.tsv" "$OUT/digests-after.tsv" >/dev/null 2>&1 || VOID=1
disk_report > "$OUT/disk-after.txt" || true
chrony_report > "$OUT/chrony-after.txt"
list_bench_topics > "$OUT/topics-after.txt"
# Every partition of every OMB topic must have received records. Guards the trap where a
# producer caches a 1-partition metadata answer and writes everything to partition 0.
PARTS_WANT="$(awk '/^partitionsPerTopic:/ {print $2}' "$WORKLOAD")"
PART_FAIL=0
for t in $(grep '^test-topic-' "$OUT/topics-after.txt"); do
  tools /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server "$BOOT" --topic "$t" --time -1 2>/dev/null \
    | sort -t: -k2 -n > "$OUT/end-offsets-$t.txt"
  n="$(grep -c . "$OUT/end-offsets-$t.txt")"
  empty="$(awk -F: '$3 == 0' "$OUT/end-offsets-$t.txt" | wc -l | tr -d ' ')"
  echo "partitions: $t has $n partitions (want $PARTS_WANT), $empty with no records"
  { [ "$n" = "$PARTS_WANT" ] && [ "$empty" = 0 ]; } || PART_FAIL=1
done
grep -q '^test-topic-' "$OUT/topics-after.txt" || { echo "partitions: no OMB topic found after the run"; PART_FAIL=1; }
# Leave the broker clean for the next run; the guard above re-checks anyway. A refused delete is
# recorded and retried for up to 10 min (2026-10-01, v3.6.6: DeleteTopics answered "the coordinator
# is loading" for ~19 min after a leader kill), never hidden.
t0=$(date +%s); : > "$OUT/topic-delete.txt"
for t in $(cat "$OUT/topics-after.txt"); do
  until out="$(tools /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOT" --delete --topic "$t" 2>&1)"; do
    echo "$(utc) delete $t refused: $(grep -oE 'Error while executing topic command : .*' <<<"$out" | head -1 | cut -c1-160)" >> "$OUT/topic-delete.txt"
    grep -q 'does not exist' <<<"$out" && break
    [ $(( $(date +%s) - t0 )) -lt 600 ] || { echo "$(utc) delete $t GAVE UP after 10 min" >> "$OUT/topic-delete.txt"; break; }
    sleep 10
  done
  echo "$(utc) delete $t: done ($(( $(date +%s) - t0 )) s after the first attempt)" >> "$OUT/topic-delete.txt"
done
grep -q refused "$OUT/topic-delete.txt" && echo "topic delete: $(grep -c refused "$OUT/topic-delete.txt") refusals before success (see topic-delete.txt)"
# Brokers reclaim deleted-topic space asynchronously (Kafka after file.delete.delay.ms;
# KubeMQ through compaction). Wait up to 10 minutes for every node to drop back under
# RECLAIM_PCT and record how long it took; the next run's 40% guard catches the rest.
# With a fresh KubeMQ before every run (KKBENCH_FRESH=1, the default) the next run starts on wiped disks, so the
# wait buys nothing: record one snapshot and move on.
RECLAIM_PCT=10; t0=$(date +%s)
if [ "${KKBENCH_FRESH:-1}" = 1 ] && [ "$BROKER" = kubemq ]; then
  disk_report > "$OUT/disk-after-cleanup.txt" 2>/dev/null
  echo "reclaim: not waited (next run wipes KubeMQ): $(tr '\n' ';' < "$OUT/disk-after-cleanup.txt")" | tee "$OUT/reclaim.txt"
else
while :; do
  disk_report > "$OUT/disk-after-cleanup.txt" 2>/dev/null
  awk -v lim="$RECLAIM_PCT" '{gsub("%","",$2); if ($2+0 >= lim) bad=1} END {exit bad}' "$OUT/disk-after-cleanup.txt" && break
  [ $(( $(date +%s) - t0 )) -lt 600 ] || break
  sleep 15
done
echo "reclaim: $(( $(date +%s) - t0 )) s after topic delete: $(tr '\n' ';' < "$OUT/disk-after-cleanup.txt")" | tee "$OUT/reclaim.txt"
fi

[ "$VOID" = 0 ] || { echo "VOID" > "$OUT/VOID"; die "image digests changed during the run: VOID"; }
[ "$RC" = 0 ] && [ -s "$OUT/result.json" ] || die "OMB failed (exit $RC); see $OUT/omb.log"
[ "$PART_FAIL" = 0 ] || die "not every partition received records (see end-offsets-*.txt)"
jq -r '"publish rate avg: \(.publishRate | add / length | floor) msg/s   consume rate avg: \(.consumeRate | add / length | floor) msg/s   publish latency p99 (aggregated): \(.aggregatedPublishLatency99pct) ms"' "$OUT/result.json"
note "recorded ${OUT#"$REPO_ROOT"/}"
