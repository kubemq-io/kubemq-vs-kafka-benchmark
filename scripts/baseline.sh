#!/usr/bin/env bash
#
# baseline.sh — hardware fingerprint of one cluster incarnation, before any broker runs.
#
#   baseline.sh run --slot a|b --out DIR      measure and write DIR/{cpuinfo,fio,iperf,chrony}/ + summary.json
#   baseline.sh compare DIR_A DIR_B           twin check: PASS only if every metric is within 10%
#
# Per broker node: fio 1 MiB sequential write (direct I/O, queue depth 32, 60 s) and
# fio 4 KiB write with fdatasync after every write (60 s), both on the RAID-0 local SSD
# mount. iperf3 between broker nodes over the pod network (the path brokers use),
# ring order 0→1, 1→2, 2→0, 4 streams, 20 s, one pair at a time. /proc/cpuinfo model
# and `chronyc tracking` from every n2 node (brokers and drivers).
#
# Must run before the broker is deployed: fio writes to the same disk the broker's
# volume lives on, and its file is removed afterwards.
#
source "$(dirname "$0")/lib.sh"

TWIN_TOLERANCE_PCT=10

# kubectl exec streams through the API server and can drop on a flaky laptop link;
# measurements write to a file inside the pod and are fetched with a retried cat.
kx() {  # kx POD CMD... — exec with 3 attempts
  local pod="$1"; shift; local i
  for i in 1 2 3; do k -n kkbench-baseline exec "$pod" -- "$@" && return 0; echo "  (exec attempt $i failed; retrying)" >&2; sleep 5; done
  return 1
}

median() { sort -g | awk '{a[NR]=$1} END { if (NR%2) print a[(NR+1)/2]; else print (a[NR/2]+a[NR/2+1])/2 }'; }

run() {
  local out="$1"
  mkdir -p "$out"/{cpuinfo,fio,iperf,chrony}
  note "deploying measurement pods"
  k apply -f "$REPO_ROOT/infra/baseline/tools.yaml" >/dev/null
  k -n kkbench-baseline rollout status ds/tools-broker --timeout=600s >/dev/null || die "tools-broker not ready"
  k -n kkbench-baseline rollout status ds/tools-driver --timeout=600s >/dev/null || die "tools-driver not ready"

  local bpods dpods
  bpods=$(k -n kkbench-baseline get pods -l app=tools-broker -o jsonpath='{range .items[*]}{.metadata.name}={.spec.nodeName}={.status.podIP}{"\n"}{end}' | sort -t= -k2)
  dpods=$(k -n kkbench-baseline get pods -l app=tools-driver -o jsonpath='{range .items[*]}{.metadata.name}={.spec.nodeName}{"\n"}{end}' | sort -t= -k2)
  [ "$(grep -c . <<<"$bpods")" = "3" ] || die "expected 3 broker-node tools pods, got: $bpods"

  note "cpuinfo and chrony on every n2 node"
  while IFS='=' read -r pod node _; do
    [ -n "$pod" ] || continue
    kx "$pod" cat /proc/cpuinfo > "$out/cpuinfo/$node.txt"
    kx "$pod" nsenter -t 1 -m -- chronyc tracking > "$out/chrony/$node.txt" 2>/dev/null || true
  done <<<"$bpods"$'\n'"$dpods"

  note "fio on each broker node (about 2 min per node)"
  while IFS='=' read -r pod node _; do
    [ -n "$pod" ] || continue
    kx "$pod" fio --name=seqwrite --filename=/data/fio.tmp --rw=write --bs=1M \
      --size=50G --ioengine=libaio --direct=1 --iodepth=32 --runtime=60 --time_based \
      --output-format=json --output=/tmp/seqwrite.json || die "fio seqwrite failed on $node"
    kx "$pod" cat /tmp/seqwrite.json > "$out/fio/$node.seqwrite.json" || die "fetch failed on $node"
    kx "$pod" fio --name=syncwrite --filename=/data/fio.tmp --rw=write --bs=4k \
      --size=4G --ioengine=sync --fdatasync=1 --runtime=60 --time_based \
      --output-format=json --output=/tmp/syncwrite.json || die "fio syncwrite failed on $node"
    kx "$pod" cat /tmp/syncwrite.json > "$out/fio/$node.syncwrite.json" || die "fetch failed on $node"
    kx "$pod" rm -f /data/fio.tmp
    kx "$pod" fio --version > "$out/fio/fio-version.txt"
  done <<<"$bpods"

  note "iperf3 ring between broker nodes"
  local -a P N I; local i=0
  while IFS='=' read -r pod node ip; do P[i]=$pod; N[i]=$node; I[i]=$ip; i=$((i+1)); done <<<"$bpods"
  for s in 0 1 2; do
    local c=$(( (s+1) % 3 ))
    kx "${P[s]}" sh -c 'pkill iperf3; nohup iperf3 -s >/dev/null 2>&1 &'
    sleep 2
    # Detached: a saturated NIC starves the exec stream itself, so the client must not
    # depend on it. Poll with short execs once the 20 s run is over.
    kx "${P[c]}" sh -c "rm -f /tmp/iperf.json /tmp/iperf.done; (iperf3 -c ${I[s]} -P 4 -t 20 -J --logfile /tmp/iperf.json; echo \$? > /tmp/iperf.done) >/dev/null 2>&1 &"
    sleep 25
    local tries=0
    until kx "${P[c]}" test -f /tmp/iperf.done 2>/dev/null; do
      tries=$((tries+1)); [ "$tries" -lt 12 ] || die "iperf3 ${N[c]} -> ${N[s]} never finished"; sleep 5
    done
    [ "$(kx "${P[c]}" cat /tmp/iperf.done)" = "0" ] || die "iperf3 ${N[c]} -> ${N[s]} failed"
    kx "${P[c]}" cat /tmp/iperf.json > "$out/iperf/${N[c]}--to--${N[s]}.json" || die "fetch failed"
    kx "${P[c]}" rm -f /tmp/iperf.json /tmp/iperf.done
    kx "${P[s]}" pkill iperf3 || true
  done

  summarize "$out"
  note "removing measurement pods"
  k delete ns kkbench-baseline --wait=true >/dev/null
}

summarize() {
  local out="$1"
  {
    echo '{'
    echo '  "cpu_models": '"$(cat "$out"/cpuinfo/*.txt | grep -m1000 '^model name' | sort | uniq -c | awk '{$1=$1};1' | jq -R . | jq -s -c .)"','
    echo '  "fio_seqwrite_MBps": '"$(for f in "$out"/fio/*.seqwrite.json; do jq '.jobs[0].write.bw_bytes/1e6' "$f"; done | jq -s -c .)"','
    echo '  "fio_syncwrite_iops": '"$(for f in "$out"/fio/*.syncwrite.json; do jq '.jobs[0].write.iops' "$f"; done | jq -s -c .)"','
    echo '  "iperf_Gbps": '"$(for f in "$out"/iperf/*.json; do jq '.end.sum_received.bits_per_second/1e9' "$f"; done | jq -s -c .)"
    echo '}'
  } | jq . > "$out/summary.json"
  cat "$out/summary.json"
  # The CPU model name is masked inside GCE guests ("Intel(R) Xeon(R) CPU @ 2.60GHz");
  # the platform check lives in up.sh, from the Compute API's cpuPlatform field.
}

compare() {
  local a="$1/summary.json" b="$2/summary.json" fail=0
  [ -f "$a" ] && [ -f "$b" ] || die "need two baseline dirs with summary.json"
  for m in fio_seqwrite_MBps fio_syncwrite_iops iperf_Gbps; do
    local ma mb
    ma=$(jq -r ".${m}[]" "$a" | median); mb=$(jq -r ".${m}[]" "$b" | median)
    local pct
    pct=$(awk -v x="$ma" -v y="$mb" 'BEGIN { hi = (x>y)?x:y; lo = (x>y)?y:x; printf "%.1f", (hi-lo)/hi*100 }')
    local verdict=PASS
    awk -v p="$pct" -v t="$TWIN_TOLERANCE_PCT" 'BEGIN { exit !(p > t) }' && { verdict=FAIL; fail=1; }
    printf '  %-20s A median %-10s B median %-10s differ %5s%%  %s\n' "$m" "$ma" "$mb" "$pct" "$verdict"
  done
  [ "$fail" = "0" ] && echo "PASS: the two clusters are twins within ${TWIN_TOLERANCE_PCT}%." ||
    { echo "FAIL: recreate the slower cluster before use." >&2; exit 1; }
}

case "${1:-}" in
  run)
    shift; SLOT=""; OUT=""
    while [ $# -gt 0 ]; do case "$1" in --slot) SLOT="$2"; shift 2 ;; --out) OUT="$2"; shift 2 ;; *) die "unknown arg $1" ;; esac; done
    check_slot "$SLOT"; [ -n "$OUT" ] || die "--out required"
    run "$OUT" ;;
  summarize) summarize "$2" ;;
  compare) compare "$2" "$3" ;;
  *) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;;
esac
