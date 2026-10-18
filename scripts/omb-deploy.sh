#!/usr/bin/env bash
#
# omb-deploy.sh — deploy the benchmark workers, coordinator and tools pod onto one cluster.
#
#   omb-deploy.sh --slot a|b [--workers 2|4]
#
# Image references come from omb/IMAGE, verifier/IMAGE (scripts/build-images.sh) and the
# Kafka image pinned in infra/kafka/versions.env (Kafka's own CLI, used against both sides).
#
source "$(dirname "$0")/lib.sh"
SLOT=""; WORKERS=""
while [ $# -gt 0 ]; do
  case "$1" in
    --slot) SLOT="$2"; shift 2 ;;
    --workers) WORKERS="$2"; shift 2 ;;
    *) die "unknown arg: $1" ;;
  esac
done
check_slot "$SLOT"
# shellcheck disable=SC1091
source "$REPO_ROOT/infra/kafka/versions.env"
[ -s "$REPO_ROOT/omb/IMAGE" ] || die "omb/IMAGE missing: run scripts/build-images.sh"
[ -s "$REPO_ROOT/verifier/IMAGE" ] || die "verifier/IMAGE missing: run scripts/build-images.sh"
OMB_IMAGE="$(cat "$REPO_ROOT/omb/IMAGE")"
VERIFIER_IMAGE="$(cat "$REPO_ROOT/verifier/IMAGE")"
# One worker per driver node unless told otherwise.
[ -n "$WORKERS" ] || WORKERS="$(k get nodes -l kkbench/pool=driver --no-headers | wc -l | tr -d ' ')"
[ "$WORKERS" -ge 1 ] || die "no driver nodes"

note "deploying $WORKERS OMB workers, coordinator and tools ($OMB_IMAGE)"
sed -e "s#__OMB_IMAGE__#$OMB_IMAGE#g" -e "s#__KAFKA_TOOLS_IMAGE__#$KAFKA_IMAGE#g" \
    -e "s#__VERIFIER_IMAGE__#$VERIFIER_IMAGE#g" -e "s#__WORKERS__#$WORKERS#g" \
    "$REPO_ROOT/omb/k8s/omb.yaml" | k apply -f - >/dev/null || die "apply failed"
k -n omb rollout status statefulset/omb-worker --timeout=600s || die "workers not ready"
k -n omb rollout status deploy/omb-coordinator --timeout=300s || die "coordinator not ready"
k -n omb rollout status deploy/kafka-tools --timeout=300s || die "kafka-tools not ready"
k -n omb get pods -o wide
