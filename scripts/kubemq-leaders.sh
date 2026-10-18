#!/usr/bin/env bash
#
# kubemq-leaders.sh --slot a|b [--out FILE] — which KubeMQ server leads which shard, from each
# server's own kubemq_shard_leader metric. Prints one line per server and a split such as
# "3/3/2". A server whose metrics port does not answer is reported as "no answer".
#
source "$(dirname "$0")/lib.sh"
SLOT=""; OUTF=""
while [ $# -gt 0 ]; do case "$1" in --slot) SLOT="$2"; shift 2 ;; --out) OUTF="$2"; shift 2 ;; *) die "unknown arg: $1" ;; esac; done
check_slot "$SLOT"
port=${LEADERS_LOCAL_PORT:-$(slot_port 18190)}; split=""; report=""
lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1 && die "local port $port already in use (another port-forward?); refusing to read leaders from the wrong pod"
for p in $(k -n kubemq get pods -l app=bench -o jsonpath='{.items[*].metadata.name}'); do
  m=""
  for _ in 1 2 3; do   # port-forward can take a few seconds to bind; retry rather than report "no answer"
    KUBECONFIG="$(kubeconfig_for "$SLOT")" kubectl -n kubemq port-forward "pod/$p" "$port:8080" >/dev/null 2>&1 & pf=$!
    sleep 4
    m="$(curl -s --max-time 5 "http://127.0.0.1:$port/metrics")"
    kill "$pf" 2>/dev/null; wait "$pf" 2>/dev/null
    [ -n "$m" ] && break; port=$((port+7))
  done
  if [ -z "$m" ]; then line="$p: no answer on the metrics port"; split="$split/?"
  else
    shards="$(grep -E '^kubemq_shard_leader' <<<"$m" | awk '$2==1' | grep -oE 'shard="[0-9]+"' | grep -oE '[0-9]+' | tr '\n' ' ')"
    n="$(wc -w <<<"$shards" | tr -d ' ')"
    line="$p: leads $n shard(s): ${shards:-none}"; split="$split/$n"
  fi
  report="$report$line"$'\n'; port=$((port+1))
done
report="${report}split: ${split#/}"
echo "$report"
[ -z "$OUTF" ] || { echo "$(utc)"; echo "$report"; } >> "$OUTF"
