#!/usr/bin/env bash
#
# ttl.sh — the hard time-to-live on a benchmark cluster. up.sh calls `ttl.sh start`
# BEFORE it creates anything, so even a failed create is destroyed on schedule;
# down.sh calls `ttl.sh cancel`.
#
#   ttl.sh start  <slot> [hours]   schedule down.sh for <slot> at now + hours (default 10)
#   ttl.sh cancel <slot>           cancel it
#   ttl.sh status [slot]           show what is scheduled, and whether a TTL ever fired
#
# How it works: a detached background process on THIS machine polls the wall clock
# every 30 s and runs `down.sh --slot <slot> --yes --ttl` at the deadline. It polls
# the clock rather than sleeping 10 h, so time the laptop spent asleep still counts.
# On macOS it runs under `caffeinate -i -s` to stop idle and (on mains power) system
# sleep. It cannot survive the machine being shut down or its lid closed on battery:
# the deadline is also written into the cluster's `ttl-deadline` label so any session
# can see an overdue cluster with `gcloud container clusters list`.
#
# A fired TTL leaves .state/ttl-<slot>.fired behind. The matrix exit gate requires
# that no such file exists: a TTL firing means a run was not finished and torn down
# by hand.
#
source "$(dirname "$0")/lib.sh"

cmd="${1:-}"; SLOT="${2:-}"

pidfile()   { echo "$STATE_DIR/ttl-$1.pid"; }
deadfile()  { echo "$STATE_DIR/ttl-$1.deadline"; }
logfile()   { echo "$STATE_DIR/ttl-$1.log"; }
firedfile() { echo "$STATE_DIR/ttl-$1.fired"; }

alive() { local p; p="$(cat "$(pidfile "$1")" 2>/dev/null)" && [ -n "$p" ] && kill -0 "$p" 2>/dev/null; }

case "$cmd" in
  start)
    check_slot "$SLOT"
    hours="${3:-$TTL_HOURS}"
    if alive "$SLOT"; then
      die "a TTL is already scheduled for slot $SLOT (pid $(cat "$(pidfile "$SLOT")"), deadline $(cat "$(deadfile "$SLOT")")). Cancel it first."
    fi
    deadline_epoch=$(( $(date +%s) + hours * 3600 ))
    deadline_utc="$(date -u -r "$deadline_epoch" +%Y%m%d-%H%M 2>/dev/null || date -u -d "@$deadline_epoch" +%Y%m%d-%H%M)"
    wrapper=()
    command -v caffeinate >/dev/null && wrapper=(caffeinate -i -s)
    nohup ${wrapper[@]+"${wrapper[@]}"} bash -c '
      while [ "$(date +%s)" -lt '"$deadline_epoch"' ]; do sleep 30; done
      echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) TTL FIRED for slot '"$SLOT"'" | tee "'"$(firedfile "$SLOT")"'"
      "'"$REPO_ROOT"'/scripts/down.sh" --slot '"$SLOT"' --yes --ttl
    ' >>"$(logfile "$SLOT")" 2>&1 </dev/null &
    echo $! > "$(pidfile "$SLOT")"
    echo "$deadline_utc" > "$(deadfile "$SLOT")"
    echo "$deadline_utc"
    ;;
  cancel)
    check_slot "$SLOT"
    if alive "$SLOT"; then
      p="$(cat "$(pidfile "$SLOT")")"
      # Kill the poller and its children (caffeinate wraps bash on macOS).
      pkill -P "$p" 2>/dev/null; kill "$p" 2>/dev/null
      sleep 1
      alive "$SLOT" && die "could not stop TTL process $p for slot $SLOT"
      echo "TTL for slot $SLOT cancelled (was due $(cat "$(deadfile "$SLOT")" 2>/dev/null))."
    else
      echo "no TTL running for slot $SLOT."
    fi
    rm -f "$(pidfile "$SLOT")" "$(deadfile "$SLOT")"
    ;;
  status)
    for s in ${SLOT:-a b}; do
      if alive "$s"; then echo "slot $s: TTL scheduled, pid $(cat "$(pidfile "$s")"), due $(cat "$(deadfile "$s")") UTC"
      else echo "slot $s: no TTL running"; fi
      [ -f "$(firedfile "$s")" ] && echo "slot $s: ⚠ a TTL FIRED: $(cat "$(firedfile "$s")")"
    done
    ;;
  *) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;;
esac
