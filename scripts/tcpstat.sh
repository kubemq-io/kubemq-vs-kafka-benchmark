#!/usr/bin/env bash
#
# tcpstat.sh --slot a|b --out DIR --tag before|after|<name> — snapshot the kernel's TCP counters for the
# network namespace of every OMB worker pod (the consumer and producer side) and of every KubeMQ server pod
# (the broker side), so transport-level events (retransmissions, loss probes, receive-queue drops) are recorded
# per run beside the broker metrics.
#
# One file per pod: DIR/tcp-<tag>-<pod>.txt holding /proc/net/netstat and /proc/net/snmp as the pod's own
# network namespace reports them. A pod that cannot be read gets a one-line file saying so; the run never
# fails because of this script. scripts/tcpstat-diff.py prints the deltas between two tags.
#
# How each side is read:
#   OMB workers   `kubectl exec` into the worker container (eclipse-temurin, has cat): its /proc/net is the pod's.
#   KubeMQ pods   the image has no shell, so the privileged, hostPID raid0 pod on the same node finds the server
#                 process in the host's pid table and enters its network namespace with nsenter -t <pid> -n.
#
source "$(dirname "$0")/lib.sh"
SLOT=""; OUT=""; TAG=""
while [ $# -gt 0 ]; do
  case "$1" in
    --slot) SLOT="$2"; shift 2 ;;
    --out) OUT="$2"; shift 2 ;;
    --tag) TAG="$2"; shift 2 ;;
    *) die "unknown arg: $1" ;;
  esac
done
check_slot "$SLOT"; [ -n "$OUT" ] && [ -n "$TAG" ] || die "--out DIR and --tag NAME are required"
mkdir -p "$OUT"
stamp="$(utc)"

# OMB workers: the consumer side.
for w in $(k -n omb get pods -l app=omb-worker -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
  f="$OUT/tcp-$TAG-$w.txt"
  if k -n omb exec "$w" -c worker -- cat /proc/net/netstat /proc/net/snmp > "$f.tmp" 2>/dev/null; then
    { echo "# $stamp $w (omb worker pod netns)"; cat "$f.tmp"; } > "$f"
  else
    echo "# $stamp $w: read FAILED" > "$f"
  fi
  rm -f "$f.tmp"
done

# KubeMQ servers: the broker side, through the raid0 pod on each broker node.
while IFS='=' read -r rpod node; do
  [ -n "$rpod" ] || continue
  bpod="$(k -n kubemq get pods -l app=bench --field-selector spec.nodeName="$node" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)"
  [ -n "$bpod" ] || continue
  f="$OUT/tcp-$TAG-$bpod.txt"
  # The server process: the host pid whose command line starts with the kubemq binary (one per broker node).
  if k -n local-ssd exec "$rpod" -- sh -c '
      for d in /proc/[0-9]*; do
        c="$(tr "\0" " " < "$d/cmdline" 2>/dev/null)"
        case "$c" in /kubemq*|kubemq*|*/kubemq\ *|*/kubemq) pid="${d#/proc/}"; break ;; esac
      done
      [ -n "${pid:-}" ] || { echo "no kubemq process found on this node" >&2; exit 1; }
      echo "# host pid $pid"; nsenter -t "$pid" -n -- cat /proc/net/netstat /proc/net/snmp' > "$f.tmp" 2>"$f.err"; then
    { echo "# $stamp $bpod on $node (server pod netns)"; cat "$f.tmp"; } > "$f"
  else
    { echo "# $stamp $bpod on $node: read FAILED: $(tr -d '\n' < "$f.err" | cut -c1-200)"; } > "$f"
  fi
  rm -f "$f.tmp" "$f.err"
done <<<"$(raid_pods)"
echo "tcpstat $TAG: $(ls "$OUT"/tcp-"$TAG"-*.txt 2>/dev/null | wc -l | tr -d ' ') snapshots in ${OUT#"$REPO_ROOT"/}"
