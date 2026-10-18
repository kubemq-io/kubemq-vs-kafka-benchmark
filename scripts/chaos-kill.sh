#!/usr/bin/env bash
#
# chaos-kill.sh — cell K1: kill the partition leader 5 minutes into the measured window.
#
#   chaos-kill.sh --slot a|b (--repeat N | --label NAME)
#   chaos-kill.sh --selftest          show the locator refusing on a tie and on zero leaders
#
# Sequence:
#   1. starts run.sh for d-kill-leader-50k (50k msg/s, 2 min warm-up + 15 min measured)
#   2. when OMB logs the start of benchmark traffic, pre-creates the verifier topic
#      (16 partitions, 3 replicas) and starts the acked-loss verifier at 5k msg/s beside OMB
#   3. at measured minute 5: locates the broker leading the most partitions of the OMB topic
#      (Metadata, seconds before the kill). Refuses on zero or several equal leaders, and if the
#      kill would land outside the produce window. A refusal leaves the cell VOID.
#   4. kills it: SIGKILL to the broker container's init process, sent from the node through the
#      privileged raid0 pod. `kill -9 1` from inside the container does not work on either
#      side: the kernel drops SIGKILL sent to a PID namespace's init from inside that namespace
#      (Kafka's init is tini), and the KubeMQ image has no shell or kill binary. Delivered from
#      the host it is an immediate crash; the container restarts in place on the same volume.
#   5. proves the kill: restartCount +1, new container ID, same PV UID, same data-dir inode,
#      same image digest; records time to Ready and time until no partition is under-replicated.
#      Then proves the kill hit ONLY its target: every other broker pod keeps its restartCount,
#      pod UID, container ID and process-identity incarnation (the re-exec case no container
#      counter sees). Any moved field fails the proof and names the pod and the field.
#      Restart evidence for every pod (kill/pods, kill/logs incl. --previous, kill/identity,
#      kill/events.txt, kill/operator.log, kill/nodes, kill/diag heap profiles) is saved after
#      recovery.
#   6. collects the verifier verdict (PASS / FAIL / VOID) and its per-second ack timeline:
#      the outage is the longest run of seconds with zero acknowledged sends
#
source "$(dirname "$0")/lib.sh"

# Locator core, fed `kafka-topics --describe` text: prints the broker id leading the most
# partitions, or fails on zero leaders or a tie at the top.
top_leader() {
  local counts top n
  counts="$(grep -oE 'Leader: ?-?[0-9]+' | grep -oE '\-?[0-9]+$' | grep -v '^-1$' | sort | uniq -c | sort -rn)"
  [ -n "$counts" ] || { echo "no partition has a leader" >&2; return 1; }
  top="$(head -1 <<<"$counts" | awk '{print $1}')"
  n="$(awk -v t="$top" '$1==t' <<<"$counts" | wc -l | tr -d ' ')"
  [ "$n" = 1 ] || { echo "tie: $n brokers each lead $top partitions ($(tr '\n' ' ' <<<"$counts"))" >&2; return 1; }
  echo "$(head -1 <<<"$counts" | awk '{print $2}') $top $(tr '\n' ' ' <<<"$counts")"
}

if [ "${1:-}" = "--selftest" ]; then
  ok=1
  # Whole-cluster proof on canned pods_state output: killed pod +1 and new container/incarnation,
  # survivors identical -> PASS; a survivor whose incarnation moved -> FAIL naming it.
  T="$(mktemp -d)"
  printf 'bench-0 0 u0 containerd://a inc-a\nbench-1 0 u1 containerd://b inc-b\nbench-2 0 u2 containerd://c inc-c\n' > "$T/before"
  printf 'bench-0 0 u0 containerd://a inc-a\nbench-1 1 u1 containerd://bb inc-bb\nbench-2 0 u2 containerd://c inc-c\n' > "$T/pass"
  printf 'bench-0 0 u0 containerd://a inc-a\nbench-1 1 u1 containerd://bb inc-bb\nbench-2 0 u2 containerd://c inc-cc\n' > "$T/fail-inc"
  printf 'bench-0 1 u0 containerd://aa inc-aa\nbench-1 1 u1 containerd://bb inc-bb\nbench-2 0 u2 containerd://c inc-c\n' > "$T/fail-rc"
  printf 'bench-0 0 u0 containerd://a -\nbench-1 0 u1 containerd://b -\nbench-2 0 u2 containerd://c -\n' > "$T/before-nobanner"
  printf 'bench-0 0 u0 containerd://a -\nbench-1 1 u1 containerd://bb -\nbench-2 0 u2 containerd://c -\n' > "$T/pass-nobanner"
  proof_pods "$T/before" "$T/pass" bench-1 >/dev/null || { echo "selftest: clean kill wrongly FAILED"; ok=0; }
  # A survivor whose banner rotated out of its log (same container, same restart count) is unchanged
  # and named as such (2026-10-01: a 6-hour-old survivor read "-" after a double kill).
  proof_pods "$T/before" "$T/pass-nobanner" bench-1 > "$T/out" || { echo "selftest: survivor banner rotation wrongly FAILED"; ok=0; }
  grep -q 'banner no longer in the current log' "$T/out" || { echo "selftest: banner rotation not named"; ok=0; }
  printf 'bench-0 0 u0 containerd://a -\nbench-1 1 u1 containerd://bb -\nbench-2 0 u2 containerd://cc -\n' > "$T/fail-nobanner-cid"
  proof_pods "$T/before" "$T/fail-nobanner-cid" bench-1 >/dev/null 2>&1 && { echo "selftest: SURVIVOR CONTAINER CHANGE WITH LOST BANNER NOT REFUSED"; ok=0; }
  proof_pods "$T/before-nobanner" "$T/pass-nobanner" bench-1 >/dev/null || { echo "selftest: kill without banners wrongly FAILED"; ok=0; }
  proof_pods "$T/before" "$T/fail-inc" bench-1 > "$T/out" && { echo "selftest: SURVIVOR INCARNATION CHANGE NOT REFUSED"; ok=0; }
  grep -q '^bench-2: incarnation moved inc-c -> inc-cc$' "$T/out" || { echo "selftest: incarnation failure does not name bench-2/incarnation"; ok=0; }
  proof_pods "$T/before" "$T/fail-rc" bench-1 > "$T/out" && { echo "selftest: SURVIVOR RESTART NOT REFUSED"; ok=0; }
  grep -q '^bench-0: restartCount moved 0 -> 1$' "$T/out" || { echo "selftest: restart failure does not name bench-0/restartCount"; ok=0; }
  proof_pods "$T/before" "$T/before" bench-1 >/dev/null 2>&1 && { echo "selftest: NO-OP KILL NOT REFUSED"; ok=0; }
  printf '{"level":"info","msg":"process identity","incarnation":"7f3a","pod":"bench-1"}\n' | identity_lines | incarnation_of | grep -qx 7f3a ||
    { echo "selftest: incarnation not parsed from a JSON banner"; ok=0; }
  printf 'INFO process identity incarnation=7f3b pod=bench-1 pid=1\n' | identity_lines | incarnation_of | grep -qx 7f3b ||
    { echo "selftest: incarnation not parsed from a key=value banner"; ok=0; }
  printf 'INFO starting kubemq server\n' | identity_lines | incarnation_of | grep -qx -- - ||
    { echo "selftest: missing banner does not read as '-'"; ok=0; }
  rm -rf "$T"
  printf 'Partition: 0\tLeader: 1\nPartition: 1\tLeader: 1\nPartition: 2\tLeader: 0\n' | top_leader >/dev/null ||
    { echo "selftest: unique leader wrongly refused"; ok=0; }
  printf 'Partition: 0\tLeader: 1\nPartition: 1\tLeader: 0\n' | top_leader >/dev/null 2>&1 &&
    { echo "selftest: TIE NOT REFUSED"; ok=0; }
  printf 'Partition: 0\tLeader: -1\n' | top_leader >/dev/null 2>&1 &&
    { echo "selftest: ZERO LEADERS NOT REFUSED"; ok=0; }
  [ "$ok" = 1 ] && echo "selftest: locator picks a unique leader, refuses a tie, refuses zero leaders; whole-cluster proof passes a clean kill, accepts a rotated survivor banner, and names a moved survivor field" || exit 1
  exit 0
fi

SLOT=""; REPEAT=""; LABEL=""; TIEBREAK=0
while [ $# -gt 0 ]; do
  case "$1" in
    --slot) SLOT="$2"; shift 2 ;;
    --repeat) REPEAT="$2"; shift 2 ;;
    --label) LABEL="$2"; shift 2 ;;
    # Declared deviation for a structural tie (16 partitions over 3 brokers balance to 6/6/4 after a
    # restart, so no unique top leader exists): kill the LOWEST-ID broker among those leading the
    # most partitions. The target still leads the maximum; the choice is recorded in the timeline.
    --tie-break) TIEBREAK=1; shift ;;
    *) die "unknown arg: $1" ;;
  esac
done
check_slot "$SLOT"
NAME="$(cluster_name "$SLOT")"; guard_name "$NAME"
BROKER="$(cluster_labels "$NAME" | jq -r .broker)"; check_broker "$BROKER"
BOOT="$(sed -nE 's/^[[:space:]]*bootstrap\.servers=(.*)$/\1/p' "$REPO_ROOT/omb/drivers/$BROKER.yaml")"
CELL="${KKBENCH_KILL_CELL:-d-kill-leader-50k}"   # override to run a different kill workload
RUNARGS=(--label "$LABEL"); RUN_ID="$LABEL"
[ -n "$REPEAT" ] && { RUNARGS=(--repeat "$REPEAT"); RUN_ID="r$REPEAT"; }
[ -n "$RUN_ID" ] || die "give --repeat N or --label NAME"
OUT="$REPO_ROOT/results/${KKBENCH_RESULTS_DATE:-$(date -u +%Y-%m-%d)}/runs/$BROKER/$CELL/$RUN_ID"
KD="$OUT/kill"; mkdir -p "$KD"
NS="$BROKER"; CONTAINER="$(broker_container "$BROKER")"; SEL="$(broker_selector "$BROKER")"
tools() { k -n omb exec deploy/kafka-tools -c tools -- "$@"; }
ver()   { k -n omb exec deploy/kafka-tools -c verifier -- "$@"; }
log()   { echo "$(utc) $*" | tee -a "$KD/timeline.txt"; }

"$REPO_ROOT/scripts/run.sh" --slot "$SLOT" --cell "$CELL" "${RUNARGS[@]}" > "$KD/run.out" 2>&1 &
RUNPID=$!
log "run.sh started (pid $RUNPID)"

# --- wait for the measured window --------------------------------------------------
COORD="$(k -n omb get pods -l app=omb-coordinator -o jsonpath='{.items[0].metadata.name}')"
deadline=$(( $(date +%s) + 900 ))
# Only this run's log counts: the coordinator still holds the previous run's log until run.sh
# resets /tmp/run, so require this workload's name before the traffic marker.
in_window() { k -n omb exec "$COORD" -- sh -c "grep -q 'WORKLOAD : D kill the partition leader' /tmp/run/omb.log && grep -q 'Starting benchmark traffic' /tmp/run/omb.log" 2>/dev/null; }
until [ -s "$OUT/start.txt" ] && in_window; do
  kill -0 "$RUNPID" 2>/dev/null || die "run.sh exited before the measured window (see $KD/run.out)"
  [ "$(date +%s)" -lt "$deadline" ] || die "OMB never reached the measured window"
  sleep 5
done
T_BENCH=$(date +%s); log "OMB measured window started"

# --- verifier beside OMB --------------------------------------------------------------
VT="cr-kafka-k1-$(date +%s)"
tools /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOT" --create --topic "$VT" \
  --partitions 16 --replication-factor 3 --config min.insync.replicas=2 >/dev/null || die "verifier topic create failed"
ver sh -c "rm -rf /tmp/k1 && mkdir -p /tmp/k1 && (kafka-verifier -addr $BOOT -topic $VT -out /tmp/k1 -rate 5000 -duration ${KKBENCH_VERIFIER_DURATION:-13m} -read-wait 900s -scenario k1-$BROKER > /tmp/k1/verifier.log 2>&1; echo \$? > /tmp/k1/exit) >/dev/null 2>&1 &"
log "verifier started on $VT at 5000 msg/s for ${KKBENCH_VERIFIER_DURATION:-13m}"

# --- locate and kill at measured minute 5 ---------------------------------------------
sleep $(( T_BENCH + 300 - $(date +%s) ))
OMBTOPIC="$(tools /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOT" --list | grep '^test-topic-' | head -1)"
[ -n "$OMBTOPIC" ] || { echo VOID > "$KD/VOID"; die "no OMB topic found; cell VOID"; }
tools /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOT" --describe --topic "$OMBTOPIC" > "$KD/leaders-before.txt"
# The broker list can come back empty under load (request timeout); retry before voiding the cell.
for _ in 1 2 3 4 5; do
  tools /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server "$BOOT" 2>/dev/null | grep -E '\(id: [0-9]+' > "$KD/brokers.txt"
  [ "$(grep -c . "$KD/brokers.txt")" -ge 3 ] && break; sleep 3
done
if ! LOC="$(top_leader < "$KD/leaders-before.txt" 2>"$KD/locator-refusal.txt")"; then
  if [ "$TIEBREAK" = 1 ] && grep -q '^tie:' "$KD/locator-refusal.txt"; then
    counts="$(grep -oE 'Leader: ?-?[0-9]+' "$KD/leaders-before.txt" | grep -oE '\-?[0-9]+$' | grep -v '^-1$' | sort | uniq -c | sort -k1,1rn -k2,2n)"
    LOC="$(head -1 <<<"$counts" | awk '{print $2, $1}') $(tr '\n' ' ' <<<"$counts")"
    log "locator: $(cat "$KD/locator-refusal.txt"); --tie-break: lowest broker id among the tied top leaders (declared deviation, recorded)"
    echo "tie-break lowest-id" > "$KD/tie-break.txt"
  else
    echo VOID > "$KD/VOID"; log "locator refused: $(cat "$KD/locator-refusal.txt"); no kill; cell VOID"
    wait "$RUNPID"; exit 1
  fi
fi
LID="${LOC%% *}"
HOST="$(grep -E "\(id: $LID " "$KD/brokers.txt" | awk '{print $1}')"
POD="${HOST%%.*}"
if [ -z "$POD" ] && [ "$BROKER" = kubemq ]; then
  # The broker list can be empty under load; KubeMQ's broker id N is always pod bench-(N-1).
  POD="bench-$((LID - 1))"; HOST="$POD (mapped from broker id, broker list empty)"
fi
log "locator: broker id $LID ($POD) leads $(cut -d' ' -f2 <<<"$LOC") partitions of $OMBTOPIC; counts [$(cut -d' ' -f3- <<<"$LOC")]"
k -n "$NS" get pod "$POD" >/dev/null 2>&1 || { echo VOID > "$KD/VOID"; log "leader '$HOST' does not map to a pod; no kill; cell VOID"; wait "$RUNPID"; exit 1; }
# The kill must land inside the produce window with at least 5 min of it left.
[ $(( $(date +%s) - T_BENCH )) -lt 600 ] || { echo VOID > "$KD/VOID"; die "kill would land too late in the window; cell VOID"; }

raidpod_on() { k -n local-ssd get pods -l app=raid0 --field-selector spec.nodeName="$1" -o jsonpath='{.items[0].metadata.name}'; }
podstate() {  # restartCount containerID pvUID dataInode imageID node
  local j node pvc pv
  j="$(k -n "$NS" get pod "$POD" -o json)"
  node="$(jq -r .spec.nodeName <<<"$j")"
  pvc="$(jq -r '[.spec.volumes[] | select(.persistentVolumeClaim) | .persistentVolumeClaim.claimName][0]' <<<"$j")"
  pv="$(k -n "$NS" get pvc "$pvc" -o jsonpath='{.spec.volumeName}')"
  local raidpod
  raidpod="$(raidpod_on "$node")"
  echo "$(jq -r --arg c "$CONTAINER" '.status.containerStatuses[] | select(.name==$c) | "\(.restartCount) \(.containerID)"' <<<"$j") \
$(k get pv "$pv" -o jsonpath='{.metadata.uid}') \
$(k -n local-ssd exec "$raidpod" -- nsenter -t 1 -m -- stat -c %i /mnt/disks/raid0) \
$(jq -r --arg c "$CONTAINER" '.status.containerStatuses[] | select(.name==$c) | .imageID' <<<"$j") $node"
}
BEFORE="$(podstate)"; echo "$BEFORE" > "$KD/state-before.txt"
pods_state "$NS" "$SEL" "$CONTAINER" > "$KD/state-before-all.txt"   # every pod: restartCount uid containerID incarnation
read -r RC0 CID0 PVU0 INO0 IMG0 NODE <<<"$BEFORE"
RAIDPOD="$(raidpod_on "$NODE")"
HOSTPID="$(k -n local-ssd exec "$RAIDPOD" -- nsenter -t 1 -m -- /home/kubernetes/bin/crictl inspect "${CID0#containerd://}" | jq -r .info.pid)"
[ -n "$HOSTPID" ] && [ "$HOSTPID" != null ] || { echo VOID > "$KD/VOID"; die "could not resolve the container's host PID; cell VOID"; }
k -n local-ssd exec "$RAIDPOD" -- sh -c "cat /proc/$HOSTPID/cmdline | tr '\\0' ' '" > "$KD/killed-process.txt"
T_KILL=$(date +%s)
k -n local-ssd exec "$RAIDPOD" -- bash -c "kill -9 $HOSTPID" || die "kill failed"
log "KILL: SIGKILL host pid $HOSTPID ($(cut -c1-80 "$KD/killed-process.txt")) of $POD on $NODE, measured minute $(( (T_KILL - T_BENCH) / 60 ))"

# --- recovery -------------------------------------------------------------------------
# Time to Ready comes from the pod's Ready condition, which Kubernetes stamps when it flips.
# The container status's `ready` field is not used: right after a restart it can still read
# true with the new container ID, which once reported "Ready after 2 s" for a broker whose
# Ready condition actually returned 22 s after the kill.
ready_after_kill() {
  local c ts
  c="$(k -n "$NS" get pod "$POD" -o json | jq -r '.status.conditions[] | select(.type=="Ready") | "\(.status) \(.lastTransitionTime)"')"
  [ "${c%% *}" = True ] || return 1
  ts="$(date -u -j -f %Y-%m-%dT%H:%M:%SZ "${c#* }" +%s 2>/dev/null || date -u -d "${c#* }" +%s)"
  [ "$ts" -gt "$T_KILL" ] && echo "$ts"
}
until T_READY="$(ready_after_kill)"; do
  [ $(( $(date +%s) - T_KILL )) -lt 600 ] || { log "pod not Ready 10 min after the kill"; T_READY=$(date +%s); break; }
  sleep 2
done
log "$POD Ready again after $(( T_READY - T_KILL )) s"
AFTER="$(podstate)"; echo "$AFTER" > "$KD/state-after.txt"
read -r RC1 CID1 PVU1 INO1 IMG1 _ <<<"$AFTER"
PROOF=PASS
[ "$RC1" = $((RC0 + 1)) ] || { PROOF=FAIL; log "proof: restartCount $RC0 -> $RC1 (want +1)"; }
[ "$CID1" != "$CID0" ]    || { PROOF=FAIL; log "proof: container ID unchanged"; }
[ "$PVU1" = "$PVU0" ]     || { PROOF=FAIL; log "proof: PV UID changed $PVU0 -> $PVU1"; }
[ "$INO1" = "$INO0" ]     || { PROOF=FAIL; log "proof: data-dir inode changed $INO0 -> $INO1"; }
[ "$IMG1" = "$IMG0" ]     || { PROOF=FAIL; log "proof: image digest changed"; }
# The kill must have hit only its target: the survivors keep every identity field.
pods_state "$NS" "$SEL" "$CONTAINER" > "$KD/state-after-all.txt"
if ! proof_pods "$KD/state-before-all.txt" "$KD/state-after-all.txt" "$POD" > "$KD/proof-pods.txt"; then
  PROOF=FAIL; while read -r l; do log "proof: $l"; done < "$KD/proof-pods.txt"
fi
log "kill proof: $PROOF (restartCount $RC0 -> $RC1, container changed, PV and data-dir inode and image identical; $(( $(grep -c . "$KD/state-before-all.txt") - 1 )) survivors checked on restartCount/uid/containerID/incarnation)"
echo "$PROOF" > "$KD/proof.txt"

until [ -z "$(tools /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOT" --describe --topic "$OMBTOPIC" --under-replicated-partitions 2>/dev/null | grep -E 'Partition:')" ]; do
  [ $(( $(date +%s) - T_KILL )) -lt 900 ] || { log "partitions still under-replicated 15 min after the kill"; break; }
  sleep 2
done
log "no under-replicated partition of $OMBTOPIC $(( $(date +%s) - T_KILL )) s after the kill"
tools /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOT" --describe --topic "$OMBTOPIC" > "$KD/leaders-after.txt"
# Restart evidence for every pod, back to two minutes before the measured window began.
capture_broker_evidence "$NS" "$SEL" "$CONTAINER" "$KD" $(( $(date +%s) - T_BENCH + 120 ))
capture_boot_evidence "$NS" "$SEL" "$CONTAINER" "$KD" "$KD"/state-before-all.txt
log "boot evidence: $(tr '\n' ';' < "$KD"/boot/INDEX.txt)"
log "evidence: $(grep -c . "$KD/state-after-all.txt") pods captured (pods/ logs/ identity/ events.txt operator.log nodes/ diag/); heap profiles copied: $(grep -cE $'\t[^\t]+\\.pprof\t[0-9]' "$KD/diag/INDEX.txt" 2>/dev/null || echo 0)"
if [ "$BROKER" = kubemq ]; then
  # KubeMQ validates its licence at cold start; its duration belongs on the timeline.
  k -n kubemq logs "$POD" 2>/dev/null | grep -E 'starting kubemq server|license verified|lease adopted' | head -5 > "$KD/licence-at-start.log"
fi

# --- wrap up ---------------------------------------------------------------------------
wait "$RUNPID"; RUNRC=$?
until ver test -f /tmp/k1/exit 2>/dev/null; do sleep 10; done
for f in verdict.json verifier.log acks-per-second.jsonl missing-ids.txt; do
  ver cat "/tmp/k1/$f" > "$KD/verifier-$f" 2>/dev/null || rm -f "$KD/verifier-$f"
done
VRC="$(ver cat /tmp/k1/exit)"
tools /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOT" --delete --topic "$VT" >/dev/null 2>&1 || true
log "verifier exit $VRC: $(jq -r '"\(.verdict) acked=\(.acked) observed=\(.observed) missing=\(.missing) longest zero-ack run=\((.publish_unavailable_ms // 0)/1000) s"' "$KD/verifier-verdict.json" 2>/dev/null)"
log "run.sh exit $RUNRC"
[ "$PROOF" = PASS ] && [ "$VRC" = 0 ] && [ "$RUNRC" = 0 ] || exit 1
