#!/usr/bin/env bash
#
# gate.sh RUN_DIR — the pre-registered rules for one fixed-rate run (docs/METHOD.md).
# sustained.sh (criteria 1–3) with the last-60 s check, the slope-based backlog test and two latency-gate
# lines added. Applied identically to both sides.
#
# Stop criteria (a failed one ends the ladder):
#   1. carried publish rate >= 99 % of producerRate over the window AND over the last 60 s
#   2. backlog not growing: mean of the last minute - mean of the first minute <= 1 s of target rate
#   3. OMB publish-delay p99 < 100 ms (producers kept schedule; coordinated-omission guard)
# Latency gate (reported, never stops the ladder):
#   4. publish latency p99 <= 50 ms
#   5. end-to-end latency p99 <= 100 ms
#
# Status: pass (1-5 hold) | carried-over-gate (1-3 hold, 4 or 5 fails) | fail (1, 2 or 3 fails)
# Writes RUN_DIR/gate.json and prints one line. Exit 0 unless status is fail (exit 1).
# OMB stats are 10 s samples, so "last minute" = last 6 samples.
#
set -uo pipefail
RUN="${1:?RUN_DIR}"; R="$RUN/result.json"; W="$RUN/workload.yaml"
[ -f "$R" ] && [ -f "$W" ] || { echo "gate: $RUN has no result.json / workload.yaml" >&2; exit 2; }
TARGET="$(awk '/^producerRate:/ {print $2}' "$W")"
[ "$TARGET" -gt 0 ] 2>/dev/null || { echo "gate: unthrottled workload, no gate" >&2; exit 2; }
jq -n --slurpfile r "$R" --argjson t "$TARGET" '
  $r[0] as $r |
  ($r.publishRate) as $pr | ($r.backlog // []) as $bl |
  ($pr | add / length) as $rate |
  ($pr | if length > 6 then .[-6:] else . end | add / length) as $rate_last60 |
  ($bl | if length > 6 then .[:6] else . end | if length > 0 then add / length else 0 end) as $b_first |
  ($bl | if length > 6 then .[-6:] else . end | if length > 0 then add / length else 0 end) as $b_last |
  ($r.aggregatedPublishDelayLatency99pct / 1000) as $pd99 |
  ($r.aggregatedPublishLatency99pct) as $p99 |
  ($r.aggregatedEndToEndLatency99pct) as $e99 |
  ($rate >= 0.99 * $t and $rate_last60 >= 0.99 * $t) as $c1 |
  (($b_last - $b_first) <= $t) as $c2 |
  ($pd99 < 100) as $c3 |
  ($p99 <= 50) as $c4 |
  ($e99 <= 100) as $c5 |
  {
    target: $t, carried: ($rate | floor), carried_pct: (($rate / $t * 1000 | floor) / 10),
    carried_last60_pct: (($rate_last60 / $t * 1000 | floor) / 10),
    backlog_first_min: ($b_first | floor), backlog_last_min: ($b_last | floor), backlog_delta: (($b_last - $b_first) | floor),
    publish_delay_p99_ms: (($pd99 * 100 | floor) / 100),
    publish_p99_ms: $p99, e2e_p99_ms: $e99,
    publish_p50_ms: $r.aggregatedPublishLatency50pct, publish_p999_ms: $r.aggregatedPublishLatency999pct,
    e2e_p50_ms: $r.aggregatedEndToEndLatency50pct, e2e_p999_ms: $r.aggregatedEndToEndLatency999pct,
    criteria: {rate: $c1, backlog: $c2, schedule: $c3, publish_p99: $c4, e2e_p99: $c5},
    stop_ok: ($c1 and $c2 and $c3), gate_ok: ($c4 and $c5),
    status: (if ($c1 and $c2 and $c3) then (if ($c4 and $c5) then "pass" else "carried-over-gate" end) else "fail" end),
    failed: ([ (if $c1 then empty else "rate" end), (if $c2 then empty else "backlog" end), (if $c3 then empty else "schedule" end),
               (if $c4 then empty else "publish-p99" end), (if $c5 then empty else "e2e-p99" end) ])
  }' > "$RUN/gate.json" || { echo "gate: jq failed" >&2; exit 2; }
jq -r '"\(.status | ascii_upcase)  target \(.target)  carried \(.carried) (\(.carried_pct)%, last 60 s \(.carried_last60_pct)%)  backlog Δ \(.backlog_delta)  pub-delay p99 \(.publish_delay_p99_ms) ms  publish p99 \(.publish_p99_ms) ms  e2e p99 \(.e2e_p99_ms) ms  failed: \(.failed | join(",") | if . == "" then "-" else . end)"' "$RUN/gate.json"
[ "$(jq -r .status "$RUN/gate.json")" != fail ]
