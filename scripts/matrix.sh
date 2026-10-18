#!/usr/bin/env bash
#
# matrix.sh --slot a|b [--from warmup|B|A1|A2|C|D] [--until C] [--skip-warmup] — run every showcase cell on ONE cluster, in order,
# with the ladder stop rules of docs/METHOD.md. Resumable: a cell whose run directory already
# holds a result.json is skipped. Run one matrix.sh per slot, in two terminals, under caffeinate:
#
#   caffeinate -i scripts/matrix.sh --slot a      # Kafka
#   caffeinate -i scripts/matrix.sh --slot b      # KubeMQ
#
# Order (one run each, label "run"):
#   warm-up    b-rate-100k-15m  --warmup   (discarded; required after every deploy)
#   B          b-rate-100k-15m             15 min light-load run (also the 100k ladder rung)
#   A1 ladder  a-rate-300k … a-rate-1000k  std client, +100k per rung; stops at the first FAIL (gate.sh
#              stop criteria); if 300k fails, a-rate-200k is run once
#   A2 ladder  b512 client, starts at A1's last carried rung, +100k per rung; stops at the first FAIL
#   C ladder   c-100kb-rate-4000/6000/8000/10000, b512 client; stops at the first FAIL
#   D          d-kill-leader-50k via chaos-kill.sh (--tie-break on a leader tie, stated in the report)
# gate.sh is applied after every fixed-rate run; its verdict is in <run>/gate.json.
#
# Fresh KubeMQ (default, KKBENCH_FRESH=1): before every measured cell, the leader kill included, KubeMQ is
# uninstalled, its volumes wiped and the pinned build reinstalled (scripts/fresh-kubemq.sh, log in <run>/fresh.log),
# so no run inherits anything from the one before. The warm-up runs on the cluster as deployed.
# Idle gap (only with KKBENCH_FRESH=0): every cell starts at least KKBENCH_IDLE_GAP_MIN minutes (default 16) after
# the previous run's measured window ended (its end.txt). --until C stops before the leader kill.
#
source "$(dirname "$0")/lib.sh"
SLOT=""; FROM=""; UNTIL=""; SKIP_WARMUP=0
while [ $# -gt 0 ]; do
  case "$1" in
    --slot) SLOT="$2"; shift 2 ;;
    --from) FROM="$2"; shift 2 ;;
    --until) UNTIL="$2"; shift 2 ;;
    --skip-warmup) SKIP_WARMUP=1; shift ;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) die "unknown arg: $1" ;;
  esac
done
check_slot "$SLOT"
NAME="$(cluster_name "$SLOT")"; BROKER="$(cluster_labels "$NAME" | jq -r '.broker // empty')"; check_broker "$BROKER"
export KKBENCH_RESULTS_DATE="${KKBENCH_RESULTS_DATE:-$(date -u +%Y-%m-%d)}"
RES="$REPO_ROOT/results/$KKBENCH_RESULTS_DATE/runs/$BROKER"
LOG="$REPO_ROOT/results/$KKBENCH_RESULTS_DATE/matrix-$BROKER.log"; mkdir -p "$(dirname "$LOG")"
exec > >(tee -a "$LOG") 2>&1
note() { echo "==> $*" >&2; }   # stderr: run_cell/ladder return their verdict on stdout, notes must not mix in
note "matrix on $NAME ($BROKER), results under $RES"

run_dir() { # cell profile -> run directory
  local id="run"; [ "$2" = std ] || id="run-$2"; echo "$RES/$1/$id"
}

# run_cell CELL PROFILE -> prints the gate status (pass | carried-over-gate | fail | void); 0 unless void
run_cell() {
  local cell="$1" prof="$2" dir; dir="$(run_dir "$cell" "$prof")"
  if [ -f "$dir/result.json" ]; then note "$cell ($prof): result exists, skipping"
  else
    fresh_broker "$BROKER"
    note "$cell ($prof): running"
    if ! "$REPO_ROOT/scripts/run.sh" --slot "$SLOT" --cell "$cell" --label run --profile "$prof" --clean >&2; then
      note "$cell ($prof): run.sh failed (VOID: infrastructure fault; rerun once by hand after fixing)"
      keep_fresh_log "$dir"; echo void; return 1
    fi
    keep_fresh_log "$dir"
  fi
  "$REPO_ROOT/scripts/gate.sh" "$dir" >/dev/null 2>&1 || true
  jq -r '.status // "void"' "$dir/gate.json" 2>/dev/null || echo void
}

# ladder PROFILE CELL... -> runs rungs in order until the first fail; prints the last carried rung's cell
ladder() {
  local prof="$1"; shift; local last="" st
  for cell in "$@"; do
    st="$(run_cell "$cell" "$prof")" || { note "ladder ($prof) stopped on a VOID run at $cell"; break; }
    note "$cell ($prof): $st"
    case "$st" in pass|carried-over-gate) last="$cell" ;; *) break ;; esac
  done
  echo "$last"
}

# stage STAGE -> true when this stage should run (no --from, or --from names it; later stages then run too)
stage() { if [ -z "$FROM" ]; then return 0; elif [ "$FROM" = "$1" ]; then FROM=""; return 0; else return 1; fi; }

# --- warm-up (discarded) -------------------------------------------------------------------------
if stage warmup && [ "$SKIP_WARMUP" = 0 ] && ! ls "$RES"/*/warmup-* >/dev/null 2>&1; then
  note "discarded warm-up"; idle_gap
  "$REPO_ROOT/scripts/run.sh" --slot "$SLOT" --cell a-rate-300k --warmup --clean || die "warm-up failed"
fi

# --- B: light load, 15 min --------------------------------------------------------------------------
if stage B; then note "B: $(run_cell b-rate-100k-15m std)"; fi

# --- A1: ladder, std client ------------------------------------------------------------------------
LAST_STD=""
if stage A1; then
  LAST_STD="$(ladder std a-rate-300k a-rate-400k a-rate-500k a-rate-600k a-rate-700k a-rate-800k a-rate-900k a-rate-1000k | tail -1)"
  if [ -z "$LAST_STD" ]; then note "A1: 300k not carried; running 200k once"; st="$(run_cell a-rate-200k std)"; note "a-rate-200k (std): $st"
    case "$st" in pass|carried-over-gate) LAST_STD=a-rate-200k ;; esac
  fi
  note "A1 (std) last carried rung: ${LAST_STD:-none}"
fi

# --- A2: ladder, 512 KB client, from A1's last carried rung ---------------------------------------
if stage A2; then
  if [ -z "$LAST_STD" ]; then  # resumed: derive from existing gate files
    for c in a-rate-1000k a-rate-900k a-rate-800k a-rate-700k a-rate-600k a-rate-500k a-rate-400k a-rate-300k a-rate-200k; do
      s="$(jq -r .status "$RES/$c/run/gate.json" 2>/dev/null || true)"; case "$s" in pass|carried-over-gate) LAST_STD="$c"; break ;; esac
    done
  fi
  start="${LAST_STD:-a-rate-200k}"
  ALL=(a-rate-200k a-rate-300k a-rate-400k a-rate-500k a-rate-600k a-rate-700k a-rate-800k a-rate-900k a-rate-1000k); SEQ=(); on=0
  for c in "${ALL[@]}"; do [ "$c" = "$start" ] && on=1; [ "$on" = 1 ] && SEQ+=("$c"); done
  note "A2 (b512) from $start"
  LAST_B512="$(ladder b512 "${SEQ[@]}" | tail -1)"; note "A2 (b512) last carried rung: ${LAST_B512:-none}"
fi

# --- C: 100 KB, 512 KB client ------------------------------------------------------------------------
if stage C; then
  LAST_C="$(ladder b512 c-100kb-rate-4000 c-100kb-rate-6000 c-100kb-rate-8000 c-100kb-rate-10000 | tail -1)"; note "C last carried rung: ${LAST_C:-none}"
fi

# --- D: leader kill ----------------------------------------------------------------------------------
if [ "$UNTIL" != C ] && stage D; then
  if [ -f "$RES/d-kill-leader-50k/run/result.json" ]; then note "D: result exists, skipping"
  else note "D: leader kill"; fresh_broker "$BROKER"; "$REPO_ROOT/scripts/chaos-kill.sh" --slot "$SLOT" --label run --tie-break || note "D: chaos-kill.sh returned non-zero; see its log"
    keep_fresh_log "$RES/d-kill-leader-50k/run"; fi
fi

note "matrix done on $NAME. Next: scripts/report.py results/$KKBENCH_RESULTS_DATE"
