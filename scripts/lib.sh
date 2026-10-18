# shellcheck shell=bash
# shellcheck disable=SC2034  # variables used by the scripts that source this file
# Shared settings and guards for up.sh, down.sh, ttl.sh, baseline.sh.
# Sourced, never executed.

set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="$REPO_ROOT/.state"          # gitignored: kubeconfigs, TTL pids, logs
TF_DIR="$REPO_ROOT/infra/terraform"
mkdir -p "$STATE_DIR"

PROJECT="${GCP_PROJECT:-$(gcloud config get-value project 2>/dev/null)}"
ZONE="${GCP_ZONE:-us-east1-b}"   # us-central1 had no Ice Lake n2 capacity on 2026-09-28
NAME_PREFIX="${KKBENCH_PREFIX:-kkbench}"
TTL_HOURS="${KKBENCH_TTL_HOURS:-12}"

# The one cluster in the reference project that no script may touch under any argument.
PROTECTED_CLUSTER="kubemq-production-cluster"

# Terraform uses the operator's gcloud login rather than application-default credentials.
# Terraform cannot refresh a token, so force a fresh one (about 60 minutes) before each run;
# `print-access-token` may hand back a cached token with minutes left.
tf_auth() {
  GOOGLE_OAUTH_ACCESS_TOKEN="$(gcloud config config-helper --force-auth-refresh \
    --format='value(credential.access_token)' 2>/dev/null)"
  [ -n "$GOOGLE_OAUTH_ACCESS_TOKEN" ] || die "gcloud is not logged in"
  export GOOGLE_OAUTH_ACCESS_TOKEN
}

# Block while GKE still has an operation running on the cluster (a delete that an
# interrupted terraform started keeps going server-side).
wait_cluster_ops() {
  local n="$1" deadline=$(( $(date +%s) + 1200 ))
  while [ -n "$(gcloud container operations list --zone "$ZONE" --project "$PROJECT" \
      --filter="status=RUNNING AND targetLink~/clusters/$n\$" --format='value(name)' 2>/dev/null)" ]; do
    [ "$(date +%s)" -lt "$deadline" ] || { echo "  operations on $n still running after 20 min" >&2; return 1; }
    echo "  waiting for a running GKE operation on $n..."; sleep 20
  done
}

die()  { echo "FAIL: $*" >&2; exit 1; }
note() { echo "==> $*"; }
utc()  { date -u +%Y-%m-%dT%H:%M:%SZ; }

cluster_name() { echo "${NAME_PREFIX}-$1"; }       # $1 = slot

# Every script calls this before acting on a cluster name.
guard_name() {
  local n="$1"
  [ "$n" != "$PROTECTED_CLUSTER" ] || die "'$n' is the production cluster. Refusing."
  case "$n" in
    *prod*|*live*) die "cluster name '$n' looks like a real environment. Refusing." ;;
    "${NAME_PREFIX}"-a|"${NAME_PREFIX}"-b) ;;
    *) die "cluster name '$n' is not one of ${NAME_PREFIX}-a / ${NAME_PREFIX}-b. Refusing." ;;
  esac
}

check_slot() { case "${1:-}" in a|b) ;; *) die "slot must be a or b (got '${1:-}')" ;; esac; }
# Local port-forward ports are per slot so both clusters can be driven from one laptop at once
# (slot a = base, slot b = base + 100).
slot_port() { [ "${SLOT:-a}" = b ] && echo $(($1 + 100)) || echo "$1"; }
check_broker() { case "${1:-}" in kafka|kubemq) ;; *) die "broker must be kafka or kubemq (got '${1:-}')" ;; esac; }

# A private kubeconfig per slot, so these scripts never change the operator's own
# kubectl context (which may well point at a production cluster).
kubeconfig_for() { echo "$STATE_DIR/kubeconfig-$1"; }
k() { KUBECONFIG="$(kubeconfig_for "$SLOT")" kubectl "$@"; }
h() { KUBECONFIG="$(kubeconfig_for "$SLOT")" helm "$@"; }

cluster_exists() {
  gcloud container clusters describe "$1" --zone "$ZONE" --project "$PROJECT" \
    --format='value(name)' >/dev/null 2>&1
}

cluster_labels() {
  gcloud container clusters describe "$1" --zone "$ZONE" --project "$PROJECT" \
    --format='json(resourceLabels)' 2>/dev/null | jq -c '.resourceLabels // {}'
}

# Wait until a command succeeds, or die after a timeout. $1 = seconds, $2 = what, rest = command.
wait_for() {
  local secs="$1" what="$2"; shift 2
  local deadline=$(( $(date +%s) + secs ))
  until "$@" >/dev/null 2>&1; do
    [ "$(date +%s)" -lt "$deadline" ] || die "timed out after ${secs}s waiting for: $what"
    sleep 10
  done
}

to_epoch()   { date -u -j -f %Y-%m-%dT%H:%M:%SZ "$1" +%s 2>/dev/null || date -u -d "$1" +%s; }  # ISO-8601 Z -> unix
from_epoch() { date -u -r "$1" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -d "@$1" +%Y-%m-%dT%H:%M:%SZ; }  # unix -> ISO-8601 Z

# --- broker pods and their identity ------------------------------------------------------
# Namespace, pod selector and container name per broker. Kept in one place so the evidence
# capture, the kill proof and up.sh select the same pods.
broker_selector()  { case "$1" in kafka) echo "strimzi.io/cluster=bench,strimzi.io/broker-role=true" ;; kubemq) echo "app=bench" ;; esac; }
broker_container() { case "$1" in kafka) echo kafka ;; kubemq) echo bench ;; esac; }
operator_deploy()  { case "$1" in kafka) echo strimzi-cluster-operator ;; kubemq) echo kubemq-operator-next ;; esac; }  # $1 = ns

broker_pods() { k -n "$1" get pods -l "$2" -o jsonpath='{.items[*].metadata.name}' | tr ' ' '\n' | sort; }  # $1 ns $2 selector
raid_pods()   { k -n local-ssd get pods -l app=raid0 -o jsonpath='{range .items[*]}{.metadata.name}={.spec.nodeName}{"\n"}{end}'; }

# The server logs one INFO line at start containing `process identity` with fields incarnation,
# pod, pod_uid, container_id, pid, start_time. The incarnation is the one identity that moves
# when the process re-execs inside a container the kubelet never restarted.
identity_lines() { grep -a 'process identity' | grep -aE 'incarnation' || true; }  # no banner is a valid, empty answer
incarnation_of() {  # stdin: identity lines; prints the LAST line's incarnation, or "-" when there is none
  local v
  v="$(tail -1 | grep -oE 'incarnation["'"'"'=: ]+[A-Za-z0-9._-]+' | sed -E 's/^incarnation["'"'"'=: ]+//' | head -1)"
  echo "${v:--}"
}

# One line per pod: <pod> <restartCount> <podUID> <containerID> <incarnation>. $1 ns $2 selector $3 container
pods_state() {
  local ns="$1" sel="$2" c="$3" p j inc
  for p in $(broker_pods "$ns" "$sel"); do
    j="$(k -n "$ns" get pod "$p" -o json)"
    inc="$(k -n "$ns" logs "$p" -c "$c" 2>/dev/null | identity_lines | incarnation_of)"
    echo "$p $(jq -r --arg c "$c" '"\(.status.containerStatuses[] | select(.name==$c) | .restartCount) \(.metadata.uid) \(.status.containerStatuses[] | select(.name==$c) | .containerID)"' <<<"$j") $inc"
  done
}

# Kill proof over every broker pod. $1 before-file, $2 after-file (pods_state output), $3 killed pod.
# Prints one line per moved field, naming the pod and the field. Returns 1 if anything moved.
# The killed pod must restart in place: count +1, new container, same pod object, new incarnation
# (when both banners exist). Every other pod must be byte-identical on all four fields.
proof_pods() {
  local before="$1" after="$2" killed="$3" bad=0 seen=0
  local pod rc0 uid0 cid0 inc0 rc1 uid1 cid1 inc1 line
  while read -r pod rc0 uid0 cid0 inc0; do
    [ -n "$pod" ] || continue
    line="$(awk -v p="$pod" '$1==p' "$after")"
    [ -n "$line" ] || { echo "$pod: absent after recovery"; bad=1; continue; }
    read -r _ rc1 uid1 cid1 inc1 <<<"$line"
    if [ "$pod" = "$killed" ]; then
      seen=1
      [ "$rc1" = "$((rc0 + 1))" ] || { echo "$pod (killed): restartCount $rc0 -> $rc1 (want +1)"; bad=1; }
      [ "$cid1" != "$cid0" ]      || { echo "$pod (killed): containerID unchanged"; bad=1; }
      [ "$uid1" = "$uid0" ]       || { echo "$pod (killed): uid changed $uid0 -> $uid1 (pod recreated, not restarted)"; bad=1; }
      if [ "$inc0" != - ] && [ "$inc1" != - ]; then
        [ "$inc1" != "$inc0" ] || { echo "$pod (killed): incarnation unchanged $inc0"; bad=1; }
      fi
    else
      [ "$rc1" = "$rc0" ]   || { echo "$pod: restartCount moved $rc0 -> $rc1"; bad=1; }
      [ "$uid1" = "$uid0" ] || { echo "$pod: uid moved $uid0 -> $uid1"; bad=1; }
      [ "$cid1" = "$cid0" ] || { echo "$pod: containerID moved ${cid0#containerd://} -> ${cid1#containerd://}"; bad=1; }
      # A survivor whose banner has rotated out of its (long-running) log reads "-": with the same
      # container ID and restart count that is "unchanged", not a moved incarnation.
      if [ "$inc1" = - ] && [ "$cid1" = "$cid0" ] && [ "$rc1" = "$rc0" ]; then echo "$pod: identity banner no longer in the current log (rotated); container and restart count unchanged"
      else [ "$inc1" = "$inc0" ] || { echo "$pod: incarnation moved $inc0 -> $inc1"; bad=1; }; fi
    fi
  done < "$before"
  [ "$seen" = 1 ] || { echo "$killed: not in the before-state"; bad=1; }
  while read -r pod _; do
    [ -z "$pod" ] || grep -q "^$pod " "$before" || { echo "$pod: new pod after recovery"; bad=1; }
  done < "$after"
  return "$bad"
}

# --- evidence capture -----------------------------------------------------------------------
# Everything needed to explain a restart after the fact, for EVERY broker pod, not just the one
# under suspicion. $1 ns $2 selector $3 container $4 out-dir $5 window seconds (how far back).
# Writes pods/<p>.json, logs/<p>.current.log, logs/<p>.previous.log, identity/<p>.txt,
# events.txt, operator.log, nodes/<node>.oom.txt, diag/<p>/*.pprof + diag/INDEX.txt. Every file
# that can be empty for a good reason carries a first line saying whether the capture succeeded.
capture_broker_evidence() {
  local ns="$1" sel="$2" c="$3" out="$4" since="$5" p pods
  mkdir -p "$out/pods" "$out/logs" "$out/identity" "$out/nodes"
  pods="$(broker_pods "$ns" "$sel")"
  for p in $pods; do
    k -n "$ns" get pod "$p" -o json > "$out/pods/$p.json" 2>&1
    k -n "$ns" logs "$p" -c "$c" --timestamps --since="${since}s" > "$out/logs/$p.current.log" 2>&1
    if ! k -n "$ns" logs "$p" -c "$c" --previous --timestamps > "$out/logs/$p.previous.log" 2>"$out/logs/$p.previous.err"; then
      # "previous terminated container ... not found" means the container never restarted.
      { echo "# no previous container: $(tr -d '\n' < "$out/logs/$p.previous.err")"; } > "$out/logs/$p.previous.log"
    fi
    rm -f "$out/logs/$p.previous.err"
    { echo "== current"; identity_lines < "$out/logs/$p.current.log"
      echo "== previous"; identity_lines < "$out/logs/$p.previous.log"
      echo "== terminated"; jq -c --arg c "$c" '.status.containerStatuses[] | select(.name==$c) | {restartCount, containerID, lastState, state}' "$out/pods/$p.json" 2>/dev/null
    } > "$out/identity/$p.txt"
  done
  { echo "## kubectl -n $ns get events --sort-by=.lastTimestamp -o wide"
    k -n "$ns" get events --sort-by=.lastTimestamp -o wide 2>&1
    echo; echo "## kubectl get events -A --field-selector reason=OOMKilling"
    k get events -A --field-selector reason=OOMKilling -o wide 2>&1
  } > "$out/events.txt"
  k -n "$ns" logs "deploy/$(operator_deploy "$ns")" --timestamps --since="${since}s" > "$out/operator.log" 2>&1
  node_oom_report "$out/nodes" "$since"
  collect_heap_profiles "$ns" "$sel" "$c" "$out/diag"
}

# Heap profiles the server's memory watchdog (v3.6.4+) writes to <store path>/diag/
# heap-<utc>-<pct>.pprof on the broker's data volume when the Go heap crosses 60 % and 85 % of
# GOMEMLIMIT. The data volume is a local PV on the node's RAID-0 mount, so the files are read
# from the host through the privileged raid0 pod on that node (the KubeMQ image has no shell,
# so `kubectl exec find` is not an option). When the PV's host path cannot be derived, falls
# back to `kubectl cp` from the pod, which needs `tar` in the image and may fail; INDEX.txt
# records which path was used and whether it worked. No files is a valid, empty result.
# $1 ns $2 selector $3 container $4 out-dir. Writes <out>/<pod>/*.pprof and <out>/INDEX.txt
# (pod, file, size bytes, mtime).
collect_heap_profiles() {
  local ns="$1" sel="$2" c="$3" out="$4" p j node pvc pv hostpath raidpod files f n mount vol
  mkdir -p "$out"
  { echo "# heap profiles from <store>/diag/*.pprof on each broker's data volume, $(utc)"
    echo "# pod	file	size_bytes	mtime	sha256 (local copy)"; } > "$out/INDEX.txt"
  for p in $(broker_pods "$ns" "$sel"); do
    mkdir -p "$out/$p"
    j="$(k -n "$ns" get pod "$p" -o json 2>/dev/null)" || { echo "$p	-	-	pod object unreadable" >> "$out/INDEX.txt"; continue; }
    node="$(jq -r '.spec.nodeName // ""' <<<"$j")"
    vol="$(jq -r '[.spec.volumes[] | select(.persistentVolumeClaim)][0].name // ""' <<<"$j")"
    pvc="$(jq -r '[.spec.volumes[] | select(.persistentVolumeClaim)][0].persistentVolumeClaim.claimName // ""' <<<"$j")"
    pv=""; [ -z "$pvc" ] || pv="$(k -n "$ns" get pvc "$pvc" -o jsonpath='{.spec.volumeName}' 2>/dev/null)"
    hostpath=""; [ -z "$pv" ] || hostpath="$(k get pv "$pv" -o json 2>/dev/null | jq -r '.spec.local.path // .spec.hostPath.path // ""')"
    raidpod=""; [ -z "$node" ] || raidpod="$(k -n local-ssd get pods -l app=raid0 --field-selector spec.nodeName="$node" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)"
    if [ -n "$hostpath" ] && [ -n "$raidpod" ]; then
      # Host path known: list, then copy each file byte-for-byte through the raid0 pod.
      if ! files="$(k -n local-ssd exec "$raidpod" -- nsenter -t 1 -m -- find "$hostpath" -maxdepth 6 -type f -path '*/diag/*.pprof' 2>/dev/null)"; then
        echo "$p	-	-	host read FAILED via $raidpod ($node:$hostpath)" >> "$out/INDEX.txt"; continue
      fi
      n=0
      for f in $files; do
        [ -n "$f" ] || continue
        # Bounded: a cat stalled on a troubled volume must not hold the whole evidence capture.
        if KUBECONFIG="$(kubeconfig_for "$SLOT")" timeout 60 kubectl -n local-ssd exec "$raidpod" -- nsenter -t 1 -m -- cat "$f" > "$out/$p/$(basename "$f")" 2>/dev/null; then
          echo "$p	$(basename "$f")	$(KUBECONFIG="$(kubeconfig_for "$SLOT")" timeout 20 kubectl -n local-ssd exec "$raidpod" -- nsenter -t 1 -m -- stat -c '%s	%y' "$f" 2>/dev/null || echo '-	-')	$(shasum -a 256 "$out/$p/$(basename "$f")" 2>/dev/null | cut -c1-64)" >> "$out/INDEX.txt"
        else
          echo "$p	$(basename "$f")	-	copy FAILED from $node:$f" >> "$out/INDEX.txt"; rm -f "$out/$p/$(basename "$f")"
        fi
        n=$(( n + 1 ))
      done
      [ "$n" -gt 0 ] || echo "$p	-	-	no profiles (host $node:$hostpath via $raidpod)" >> "$out/INDEX.txt"
    else
      # Host path not derivable: kubectl cp from the pod's data mount (needs tar in the image).
      mount=""; [ -z "$vol" ] || mount="$(jq -r --arg c "$c" --arg v "$vol" '[.spec.containers[] | select(.name==$c) | .volumeMounts[] | select(.name==$v)][0].mountPath // ""' <<<"$j")"
      if [ -z "$mount" ]; then
        echo "$p	-	-	no PV host path and no data mount found; nothing collected" >> "$out/INDEX.txt"; continue
      fi
      if k -n "$ns" cp -c "$c" "$p:$mount/diag" "$out/$p" >/dev/null 2>"$out/$p/.cp.err"; then
        n=0
        for f in "$out/$p"/*.pprof; do
          [ -f "$f" ] || continue
          echo "$p	$(basename "$f")	$(stat -f '%z	%Sm' -t '%Y-%m-%d %H:%M:%S %z' "$f" 2>/dev/null || stat -c '%s	%y' "$f")	(kubectl cp fallback)" >> "$out/INDEX.txt"
          n=$(( n + 1 ))
        done
        [ "$n" -gt 0 ] || echo "$p	-	-	no profiles (kubectl cp fallback from $mount/diag)" >> "$out/INDEX.txt"
      else
        echo "$p	-	-	kubectl cp fallback FAILED from $mount/diag: $(tr -d '\n' < "$out/$p/.cp.err" | cut -c1-200)" >> "$out/INDEX.txt"
      fi
      rm -f "$out/$p/.cp.err"
    fi
  done
}

# Boot evidence for every pod that restarted inside the window (v3.6.6+: restart replay and the
# Kafka start-up scan are the recovery-time suspects). $1 ns $2 selector $3 container $4 out-dir
# $5 before-state file (pods_state). Writes boot/<pod>.{metrics,log}.txt: the boot-phase metric
# families scraped from the pod, and the log lines for `server ready` (duration_ms), snapshot
# use at launch, and `logdb first entry` (the replay each shard had to do).
capture_boot_evidence() {
  local ns="$1" sel="$2" c="$3" out="$4" before="$5" p rc0 rc1 port=${EVIDENCE_LOCAL_PORT:-$(slot_port 18500)} pf
  mkdir -p "$out/boot"
  for p in $(broker_pods "$ns" "$sel"); do
    rc0="$(awk -v p="$p" '$1==p {print $2}' "$before")"
    rc1="$(k -n "$ns" get pod "$p" -o jsonpath="{.status.containerStatuses[?(@.name=='$c')].restartCount}" 2>/dev/null)"
    [ -n "$rc0" ] && [ "$rc1" != "$rc0" ] || continue
    lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1 && die "local port $port already in use (another port-forward?); refusing to read boot metrics from the wrong pod"
    KUBECONFIG="$(kubeconfig_for "$SLOT")" kubectl -n "$ns" port-forward "pod/$p" "$port:8080" >/dev/null 2>&1 & pf=$!; sleep 2
    curl -s --max-time 10 "http://127.0.0.1:$port/metrics" | grep -E '^kubemq_boot_phase_seconds|^kubemq_kafka_boot_phase_seconds|^kubemq_process_start_time' > "$out/boot/$p.metrics.txt"
    kill "$pf" 2>/dev/null; wait "$pf" 2>/dev/null; port=$((port+1))
    k -n "$ns" logs "$p" -c "$c" --timestamps 2>/dev/null | grep -aiE 'server ready|snapshot|logdb first entry|license verified|starting kubemq server' > "$out/boot/$p.log.txt"
    echo "$p: restartCount $rc0 -> $rc1; $(grep -c . "$out/boot/$p.metrics.txt") boot-phase samples; $(grep -ac . "$out/boot/$p.log.txt") boot log lines" >> "$out/boot/INDEX.txt"
  done
  [ -s "$out/boot/INDEX.txt" ] || echo "no pod restarted in this window" > "$out/boot/INDEX.txt"
}

# Kernel OOM evidence per broker node, through the privileged raid0 pod the scripts already use
# for chronyc and df (no ssh). $1 out-dir $2 window seconds. journalctl -k, dmesg as fallback.
node_oom_report() {
  local out="$1" since="$2" pod node f rc raw
  while IFS='=' read -r pod node; do
    [ -n "$pod" ] || continue
    f="$out/$node.oom.txt"
    if raw="$(k -n local-ssd exec "$pod" -- nsenter -t 1 -m -- journalctl -k --no-pager --since "@$(( $(date +%s) - since ))" 2>&1)"; then rc=0
    elif raw="$(k -n local-ssd exec "$pod" -- nsenter -t 1 -m -- dmesg -T 2>&1)"; then rc=0
    else rc=1; fi
    if [ "$rc" = 0 ]; then
      { echo "# capture ok ($node, last ${since}s): $(grep -ciE 'killed process|out of memory|oom' <<<"$raw") matching kernel lines"
        grep -iE 'killed process|out of memory|oom' <<<"$raw"; } > "$f"
    else
      { echo "# capture FAILED ($node)"; echo "$raw"; } > "$f"
    fi
  done <<<"$(raid_pods)"
}

# Continuous log capture, so a kubelet log rotation (10 MiB on GKE, not configurable from the
# rig) cannot take the head of a busy pod's log before run end. `kubectl logs -f` ends when the
# container it follows dies, which is exactly when the pre-restart log is at risk; what came
# after lives in logs/<p>.current.log and, for the dead container, logs/<p>.previous.log.
# $1 ns $2 selector $3 container $4 out-dir. Pids are kept in $4/.stream-pids for stop_log_streams.
start_log_streams() {
  local ns="$1" sel="$2" c="$3" out="$4" p
  mkdir -p "$out/logs"; : > "$out/.stream-pids"
  for p in $(broker_pods "$ns" "$sel"); do
    KUBECONFIG="$(kubeconfig_for "$SLOT")" kubectl -n "$ns" logs -f --timestamps "$p" -c "$c" > "$out/logs/$p.stream.log" 2>&1 &
    echo $! >> "$out/.stream-pids"
  done
}
stop_log_streams() {  # $1 out-dir
  local pid
  [ -f "$1/.stream-pids" ] || return 0
  while read -r pid; do [ -z "$pid" ] || { kill "$pid" 2>/dev/null; wait "$pid" 2>/dev/null; }; done < "$1/.stream-pids"
  rm -f "$1/.stream-pids"
}

# --- idle gap between runs -----------------------------------------------------------------
# idle_gap: wait until KKBENCH_IDLE_GAP_MIN minutes (default 16; 0 = off) after the newest end.txt under
# results/$KKBENCH_RESULTS_DATE*/runs/ — the matrix folder and any folder whose name starts with it (e.g. its
# -confirm sibling), any broker, any cell. Nothing there yet: start at once. Why 16: the KubeMQ raft log keeps
# a 15-minute retention floor; a run started sooner runs while the previous run's log is being trimmed.
idle_gap() {
  [ "${KKBENCH_FRESH:-1}" = 1 ] && return 0   # a wiped KubeMQ has no previous run's log to trim
  local gap="${KKBENCH_IDLE_GAP_MIN:-16}"
  [ "$gap" -gt 0 ] 2>/dev/null || return 0
  local last end_epoch wait_s
  # shellcheck disable=SC2012
  last="$(ls -t "$REPO_ROOT"/results/"$KKBENCH_RESULTS_DATE"*/runs/*/*/*/end.txt 2>/dev/null | head -1)"
  [ -n "$last" ] || return 0
  end_epoch="$(to_epoch "$(head -1 "$last" | tr -d '[:space:]')")" || return 0
  wait_s=$(( end_epoch + gap * 60 - $(date +%s) ))
  [ "$wait_s" -gt 0 ] || return 0
  echo "==> idle gap: waiting ${wait_s}s (${gap} min after the run that ended $(head -1 "$last"))" >&2
  sleep "$wait_s"
}

# --- fresh broker before a run (the default) ------------------------------------------------------
# fresh_broker BROKER RUN_DIR — with KKBENCH_FRESH=1 (default) and a KubeMQ cluster: wipe and reinstall KubeMQ
# (scripts/fresh-kubemq.sh) and record what was done in RUN_DIR/fresh.log after the run creates it (the caller
# moves the log in). KKBENCH_FRESH=0 restores the old same-servers method, which then waits for the idle gap.
fresh_broker() {
  local broker="$1" log="$STATE_DIR/fresh-last-$SLOT.log"
  [ "${KKBENCH_FRESH:-1}" = 1 ] && [ "$broker" = kubemq ] || { idle_gap; return 0; }
  : > "$log"
  "$REPO_ROOT/scripts/fresh-kubemq.sh" --slot "$SLOT" --out "$log" >/dev/null || die "fresh KubeMQ failed; see $log"
}
keep_fresh_log() { [ -f "$STATE_DIR/fresh-last-$SLOT.log" ] && [ -d "$1" ] && mv "$STATE_DIR/fresh-last-$SLOT.log" "$1/fresh.log"; return 0; }

# --- Prometheus range export ----------------------------------------------------------------
# Raw series for the run window, one JSON per metric name, so the analysis reads saved data and
# never the live dashboard. Reached by port-forward to the kube-prometheus-stack service (no
# other path exists on the rig). $1 out-dir $2 start ISO $3 end ISO. Writes prom/<name>.json,
# prom/index.tsv (name, series, samples) and prom/MISSING.txt naming every family with no samples.
PROM_SVC="svc/kps-kube-prometheus-stack-prometheus"
PROM_FAMILIES=(
  'kubemq_.*'
  'dragonboat_.*'
  'go_(sched_latencies_seconds|gc_pauses_seconds|goroutines|memstats_heap_inuse_bytes).*'
  # Enough to split a heap surge into heap objects / stacks / off-heap (runtime/metrics classes).
  'go_memory_classes_.*'
  'go_memstats_(heap_alloc_bytes|heap_objects|stack_inuse_bytes|sys_bytes)'
  'process_resident_memory_bytes'
  'process_cpu_seconds_total'
  # Kafka broker JMX (Strimzi metricsConfig): request-handler / network-processor idle, GC, segment rolls
  'kafka_server_.*'
  'kafka_network_.*'
  'kafka_log_.*'
  'jvm_gc_.*'
  'jvm_memory_.*'
  'kube_pod_container_status_restarts_total'
  'kube_pod_container_status_last_terminated_reason'
  'kube_pod_container_status_last_terminated_exitcode'
  'container_memory_working_set_bytes'
  'container_cpu_usage_seconds_total'
  'node_pressure_.*'
  # Kernel TCP counters (node exporter netstat collector): zero-window advertisements, receive-queue drops, loss probes.
  'node_netstat_(Tcp|TcpExt)_.*'
  'node_disk_(io_time_seconds_total|io_time_weighted_seconds_total|write_time_seconds_total|read_bytes_total|written_bytes_total|reads_completed_total|writes_completed_total)'
  'node_memory_(MemAvailable|MemFree|Cached|Dirty|Writeback)_bytes'
  'node_disk_read_time_seconds_total'
  'node_network_(transmit|receive)_bytes_total'
  'up'
  'scrape_duration_seconds'
  'scrape_samples_scraped'
)
PROM_CHUNK=10000   # steps per query_range call; Prometheus refuses more than 11,000 points

prom_export() {
  local out="$1" s e base pf fam names name n_series n_samples fam_samples i cs ce tmp missing=0
  s="$(to_epoch "$2")"; e="$(to_epoch "$3")"
  mkdir -p "$out"; : > "$out/index.tsv"; : > "$out/MISSING.txt"
  local PROM_LOCAL_PORT="${PROM_LOCAL_PORT:-$(slot_port 19090)}"
  base="http://127.0.0.1:$PROM_LOCAL_PORT"
  lsof -nP -iTCP:"$PROM_LOCAL_PORT" -sTCP:LISTEN >/dev/null 2>&1 && die "local port $PROM_LOCAL_PORT already in use (another port-forward?); refusing to export metrics from the wrong cluster"
  KUBECONFIG="$(kubeconfig_for "$SLOT")" kubectl -n monitoring port-forward "$PROM_SVC" "$PROM_LOCAL_PORT:9090" > "$out/.port-forward.log" 2>&1 &
  pf=$!
  for i in $(seq 1 20); do curl -sf --max-time 2 "$base/-/ready" >/dev/null 2>&1 && break; sleep 1; done
  if ! curl -sf --max-time 5 "$base/-/ready" >/dev/null 2>&1; then
    echo "prometheus unreachable through port-forward ($PROM_SVC)" | tee "$out/MISSING.txt"
    kill "$pf" 2>/dev/null; wait "$pf" 2>/dev/null; return 1
  fi
  echo "window $2 .. $3 ($((e - s)) s), step 1s, chunk $PROM_CHUNK" > "$out/window.txt"
  tmp="$(mktemp -d)"
  for fam in "${PROM_FAMILIES[@]}"; do
    names="$(curl -sG --max-time 60 "$base/api/v1/series" --data-urlencode "match[]={__name__=~\"$fam\"}" \
      --data-urlencode "start=$s" --data-urlencode "end=$e" | jq -r '.data[]?.__name__' | sort -u)"
    fam_samples=0
    for name in $names; do
      i=0; cs=$s
      while [ "$cs" -le "$e" ]; do
        ce=$(( cs + PROM_CHUNK - 1 )); [ "$ce" -le "$e" ] || ce=$e
        curl -sG --max-time 120 "$base/api/v1/query_range" --data-urlencode "query=$name" \
          --data-urlencode "start=$cs" --data-urlencode "end=$ce" --data-urlencode "step=1s" > "$tmp/$name.$i.json"
        cs=$(( ce + 1 )); i=$(( i + 1 ))
      done
      # Merge the chunks by label set, so one series stays one entry.
      jq -s '{status:"success", data:{resultType:"matrix", result:
               ([.[].data.result[]?] | group_by(.metric) | map({metric: .[0].metric, values: (map(.values) | add)}))}}' \
        "$tmp/$name".*.json > "$out/$name.json"
      rm -f "$tmp/$name".*.json
      n_series="$(jq '.data.result | length' "$out/$name.json")"
      n_samples="$(jq '[.data.result[].values | length] | add // 0' "$out/$name.json")"
      printf '%s\t%s\t%s\n' "$name" "$n_series" "$n_samples" >> "$out/index.tsv"
      fam_samples=$(( fam_samples + n_samples ))
    done
    [ "$fam_samples" -gt 0 ] || { echo "$fam" >> "$out/MISSING.txt"; missing=1; }
  done
  rmdir "$tmp" 2>/dev/null
  kill "$pf" 2>/dev/null; wait "$pf" 2>/dev/null; rm -f "$out/.port-forward.log"
  echo "prom export: $(wc -l < "$out/index.tsv" | tr -d ' ') metric names, $(awk -F'\t' '{s+=$3} END {print s+0}' "$out/index.tsv") samples, $(wc -l < "$out/MISSING.txt" | tr -d ' ') families missing"
  return "$missing"
}

# On-demand us-central1 list prices, 2026-09 (USD/hour). Only used for the printed plan.
PRICE_N2_STD_8=0.3885
PRICE_E2_STD_4=0.1340
PRICE_LOCAL_SSD_375=0.0411
PRICE_PD_BALANCED_100=0.0137
PRICE_CLUSTER_FEE=0.10

cost_per_hour() {  # $1 = driver node count
  awk -v d="$1" -v n2="$PRICE_N2_STD_8" -v e2="$PRICE_E2_STD_4" -v ssd="$PRICE_LOCAL_SSD_375" \
      -v pd="$PRICE_PD_BALANCED_100" -v fee="$PRICE_CLUSTER_FEE" \
      'BEGIN { printf "%.2f", (3+d)*n2 + e2 + 6*ssd + (4+d)*pd + fee }'
}
