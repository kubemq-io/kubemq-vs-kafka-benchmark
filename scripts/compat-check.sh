#!/usr/bin/env bash
#
# compat-check.sh — protocol checks the benchmark depends on, run identically against either
# broker. Each check is also shown FAILING on a planted fault before its pass counts.
#
#   compat-check.sh --slot a|b
#
#   1. metadata      the cluster advertises 3 brokers at 3 distinct addresses
#   2. topic config  the driver's topicConfig carries nothing KubeMQ is known to reject, and a
#                    topic created with it (3 replicas, 16 partitions) reads back as created
#   3. CreateTime    producer-set timestamps come back unchanged from every partition
#                    (red: kafka-compat -corrupt-ms 1 must FAIL)
#   4. acked loss    the verifier passes on a clean run (red: -drop-nth 40 must FAIL)
#
# A failure against KubeMQ is filed as a product defect with the exact request and response;
# it is never worked around here.
#
source "$(dirname "$0")/lib.sh"
SLOT=""
while [ $# -gt 0 ]; do case "$1" in --slot) SLOT="$2"; shift 2 ;; *) die "unknown arg: $1" ;; esac; done
check_slot "$SLOT"
NAME="$(cluster_name "$SLOT")"; guard_name "$NAME"
BROKER="$(cluster_labels "$NAME" | jq -r '.broker // empty')"; check_broker "$BROKER"
DRIVER="$REPO_ROOT/omb/drivers/$BROKER.yaml"
BOOT="$(sed -nE 's/^[[:space:]]*bootstrap\.servers=(.*)$/\1/p' "$DRIVER")"
OUT="$REPO_ROOT/results/${KKBENCH_RESULTS_DATE:-$(date -u +%Y-%m-%d)}/compat/$BROKER-$(date -u +%H%M%S)"
mkdir -p "$OUT"
exec > >(tee -a "$OUT/compat.log") 2>&1
note "compatibility checks against $BROKER ($BOOT)"

tools() { k -n omb exec deploy/kafka-tools -c tools -- "$@"; }
ver()   { k -n omb exec deploy/kafka-tools -c verifier -- "$@"; }
FAILS=0
pass() { echo "PASS  $*"; }
fail() { echo "FAIL  $*"; FAILS=$((FAILS+1)); }

# 1. metadata ---------------------------------------------------------------------
tools /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server "$BOOT" 2>/dev/null \
  | grep -E '^[^ ].*\(id: [0-9]+' > "$OUT/brokers.txt" || true
IDS="$(sed -E 's/.*\(id: ([0-9]+).*/\1/' "$OUT/brokers.txt" | sort -u | wc -l | tr -d ' ')"
HOSTS="$(awk '{print $1}' "$OUT/brokers.txt" | sort -u | wc -l | tr -d ' ')"
if [ "$IDS" = 3 ] && [ "$HOSTS" = 3 ]; then pass "metadata: 3 brokers at 3 distinct addresses ($(awk '{print $1}' "$OUT/brokers.txt" | tr '\n' ' '))"
else fail "metadata: $IDS broker ids at $HOSTS distinct addresses (want 3 and 3): $(cat "$OUT/brokers.txt")"; fi

# 2. topic config ----------------------------------------------------------------------
TOPICCFG="$(awk '/^topicConfig:/ {f=1; next} f && /^[^ ]/ {f=0} f && NF {gsub(/^ +/,""); print}' "$DRIVER")"
if grep -qE '^retention\.bytes=|^message\.timestamp\.type=LogAppendTime' <<<"$TOPICCFG"; then
  fail "topic config: driver sets retention.bytes or LogAppendTime, which KubeMQ rejects"
else pass "topic config: driver topicConfig is '$(tr '\n' ' ' <<<"$TOPICCFG")' (nothing known to be rejected)"; fi
AT="cr-kafka-audit-$(date +%s)"
CFGARGS=(); while read -r kv; do [ -n "$kv" ] && CFGARGS+=(--config "$kv"); done <<<"$TOPICCFG"
if tools /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOT" --create --topic "$AT" \
     --partitions 16 --replication-factor 3 "${CFGARGS[@]}" > "$OUT/audit-create.txt" 2>&1; then
  sleep 3
  tools /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOT" --describe --topic "$AT" > "$OUT/audit-describe.txt" 2>&1
  tools /opt/kafka/bin/kafka-configs.sh --bootstrap-server "$BOOT" --describe --entity-type topics --entity-name "$AT" > "$OUT/audit-configs.txt" 2>&1
  P="$(grep -oE 'PartitionCount: ?[0-9]+' "$OUT/audit-describe.txt" | grep -oE '[0-9]+$')"
  R="$(grep -oE 'ReplicationFactor: ?[0-9]+' "$OUT/audit-describe.txt" | grep -oE '[0-9]+$')"
  MISR="$(grep -oE 'min\.insync\.replicas=[0-9]+' "$OUT/audit-configs.txt" | head -1)"
  if [ "$P" = 16 ] && [ "$R" = 3 ] && [ "$MISR" = "min.insync.replicas=2" ]; then
    pass "topic config: created topic reads back 16 partitions, 3 replicas, $MISR"
  else fail "topic config: created topic reads back partitions=$P replicas=$R ${MISR:-min.insync.replicas absent} (want 16, 3, 2)"; fi
  tools /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOT" --delete --topic "$AT" >/dev/null 2>&1
else fail "topic config: create with '$(tr '\n' ' ' <<<"$TOPICCFG")' was refused: $(tail -3 "$OUT/audit-create.txt")"; fi

# 3. CreateTime ------------------------------------------------------------------------
ver kafka-compat -addr "$BOOT" > "$OUT/createtime.txt" 2>&1; rc=$?
ver kafka-compat -addr "$BOOT" -corrupt-ms 1 > "$OUT/createtime-red.txt" 2>&1; rrc=$?
if [ "$rc" = 0 ] && [ "$rrc" = 1 ]; then pass "CreateTime: round-trips exactly from every partition (and the planted 1 ms shift FAILs)"
else fail "CreateTime: green exit $rc (want 0), red exit $rrc (want 1); see createtime*.txt"; fi

# 4. acked-loss verifier -----------------------------------------------------------------
ver sh -c "rm -rf /tmp/v && mkdir -p /tmp/v/green /tmp/v/red"
ver kafka-verifier -addr "$BOOT" -out /tmp/v/green -count 2000 -scenario compat-green > "$OUT/verifier-green.txt" 2>&1; rc=$?
ver kafka-verifier -addr "$BOOT" -out /tmp/v/red -count 2000 -drop-nth 40 -scenario compat-red > "$OUT/verifier-red.txt" 2>&1; rrc=$?
ver cat /tmp/v/green/verdict.json > "$OUT/verifier-green-verdict.json" 2>/dev/null
ver cat /tmp/v/red/verdict.json > "$OUT/verifier-red-verdict.json" 2>/dev/null
if [ "$rc" = 0 ] && [ "$rrc" = 1 ]; then pass "acked loss: 2000/2000 PASS clean; -drop-nth 40 FAILs as it must"
else fail "acked loss: green exit $rc (want 0), red exit $rrc (want 1); see verifier-*.txt"; fi
# The verifier's topics are its own; remove them so run.sh's leftover check stays meaningful.
for t in $(tools /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOT" --list 2>/dev/null | grep '^cr-kafka-'); do
  tools /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOT" --delete --topic "$t" >/dev/null 2>&1
done

echo
[ "$FAILS" = 0 ] && note "ALL PASS against $BROKER; recorded ${OUT#"$REPO_ROOT"/}" ||
  die "$FAILS check(s) failed against $BROKER; recorded ${OUT#"$REPO_ROOT"/}"
