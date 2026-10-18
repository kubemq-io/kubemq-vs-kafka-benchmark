#!/usr/bin/env bash
#
# sustained.sh RESULT_JSON — the pre-registered sustain rule for a fixed-rate latency rung
# (docs/METHOD.md). Sustained when ALL of:
#   - achieved publish rate >= 99% of the workload's producerRate
#   - consumer backlog did not grow over the measured window (last sample <= first + 1000 msgs)
#   - publish-delay 99th percentile < 100 ms (OMB reports it in microseconds)
# Prints the verdict and each criterion; exit 0 sustained, 1 not.
#
set -uo pipefail
R="$1"
jq -e -r '
  (.workload // "") as $w |
  (.publishRate | add / length) as $rate |
  (.backlog[0] // 0) as $b0 | (.backlog[-1] // 0) as $b1 |
  (.aggregatedPublishDelayLatency99pct / 1000) as $pd99ms |
  input as $target |
  ($rate >= 0.99 * $target) as $ok1 | ($b1 <= $b0 + 1000) as $ok2 | ($pd99ms < 100) as $ok3 |
  "rate \($rate|floor)/\($target) (\(if $ok1 then "ok" else "FAIL" end)); backlog \($b0)->\($b1) (\(if $ok2 then "ok" else "FAIL" end)); publish-delay p99 \($pd99ms*100|floor/100) ms (\(if $ok3 then "ok" else "FAIL" end)) => \(if ($ok1 and $ok2 and $ok3) then "SUSTAINED" else "NOT SUSTAINED" end)",
  ($ok1 and $ok2 and $ok3)' "$R" <(awk '/^producerRate:/ {print $2}' "$(dirname "$R")/workload.yaml") | head -1
jq -e -s '.[0] as $r | .[1] as $t | (($r.publishRate|add/length) >= 0.99*$t) and (($r.backlog[-1]//0) <= ($r.backlog[0]//0)+1000) and (($r.aggregatedPublishDelayLatency99pct/1000) < 100)' \
  "$R" <(awk '/^producerRate:/ {print $2}' "$(dirname "$R")/workload.yaml") >/dev/null
