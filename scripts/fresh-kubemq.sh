#!/usr/bin/env bash
#
# fresh-kubemq.sh --slot a|b [--out FILE] — give the next run a clean KubeMQ: uninstall the KubemqCluster release,
# delete its volumes (the local PV provisioner wipes the RAID-0 disks and re-offers them), reinstall the pinned
# build from infra/kubemq/*, and wait until it is ready to measure:
#   - 3 Ready servers, Guaranteed QoS, one per broker node, all on the pinned digest
#   - the Kafka cluster feature marker written (consumers then negotiate the same Fetch versions as in every
#     other run; before the marker a client would fall back to older request versions)
#
# Every measured KubeMQ run starts on an empty, freshly started KubeMQ, so nothing from an earlier run can carry
# into it. matrix.sh calls this before each run when KKBENCH_FRESH=1 (the default).
# The licence Secret, the operator and the PodMonitor stay installed.
#
source "$(dirname "$0")/lib.sh"
SLOT=""; OUTF=""
while [ $# -gt 0 ]; do
  case "$1" in
    --slot) SLOT="$2"; shift 2 ;;
    --out) OUTF="$2"; shift 2 ;;
    *) die "unknown arg: $1" ;;
  esac
done
check_slot "$SLOT"
NAME="$(cluster_name "$SLOT")"; guard_name "$NAME"
BROKER="$(cluster_labels "$NAME" | jq -r '.broker // empty')"; [ "$BROKER" = kubemq ] || die "$NAME is not a KubeMQ cluster"
# shellcheck disable=SC1091
source "$REPO_ROOT/infra/kubemq/versions.env"
# KKBENCH_KUBEMQ_IMAGE (repo:tag@sha256:…) overrides the pinned build, for side-by-side runs of two builds.
KUBEMQ_IMAGE="${KKBENCH_KUBEMQ_IMAGE:-$KUBEMQ_IMAGE}"
[[ "$KUBEMQ_IMAGE" == *@sha256:* ]] || die "image must be pinned by digest: $KUBEMQ_IMAGE"
[ -n "$OUTF" ] && { mkdir -p "$(dirname "$OUTF")"; exec > >(tee -a "$OUTF") 2>&1; }
T0=$(date +%s)
note "fresh KubeMQ on $NAME ($(utc))"

h -n kubemq status kubemq-bench >/dev/null 2>&1 && { h -n kubemq uninstall kubemq-bench --wait --timeout 10m >/dev/null || die "uninstall failed"; }
k -n kubemq delete kubemqcluster bench --ignore-not-found --wait=true --timeout=5m >/dev/null || die "KubemqCluster delete failed"
k -n kubemq delete statefulset bench --ignore-not-found --wait=true --timeout=5m >/dev/null || true
gone() { [ "$(k -n kubemq get pods -l app=bench -o name 2>/dev/null | wc -l | tr -d ' ')" = 0 ]; }
wait_for 600 "broker pods gone" gone
k -n kubemq delete pvc -l app=bench --wait=true --timeout=5m >/dev/null 2>&1 || true
k -n kubemq get pvc -o name | grep bench-vol | xargs -r -n1 env KUBECONFIG="$(kubeconfig_for "$SLOT")" kubectl -n kubemq delete --wait=true --timeout=5m >/dev/null
avail() { [ "$(k get pv -o json | jq '[.items[] | select(.spec.storageClassName=="local-ssd" and .status.phase=="Available")] | length')" = 3 ]; }
wait_for 600 "3 local PVs wiped and Available again" avail
note "volumes wiped ($(( $(date +%s) - T0 )) s)"

h upgrade --install kubemq-bench "$KUBEMQ_CHART" --version "$KUBEMQ_CHART_VERSION" -n kubemq \
  -f "$REPO_ROOT/infra/kubemq/cluster-values.yaml" --set image.image="$KUBEMQ_IMAGE" >/dev/null || die "install failed"
ready() { [ "$(k -n kubemq get pods -l app=bench -o json | jq '[.items[] | select(.status.conditions[]? | select(.type=="Ready" and .status=="True"))] | length')" = 3 ]; }
wait_for 1200 "3 Ready broker pods" ready

J="$(k -n kubemq get pods -l app=bench -o json)"
want="${KUBEMQ_IMAGE##*@}"
jq -e --arg w "$want" '[.items[] | .status.containerStatuses[0].imageID | endswith($w)] | all' <<<"$J" >/dev/null ||
  die "a server runs a digest other than $want: $(jq -r '[.items[] | .metadata.name+"="+.status.containerStatuses[0].imageID] | join(" ")' <<<"$J")"
jq -e '[.items[] | .status.qosClass == "Guaranteed"] | all' <<<"$J" >/dev/null || die "a broker pod is not Guaranteed QoS"
[ "$(jq -r '[.items[].spec.nodeName] | unique | length' <<<"$J")" = 3 ] || die "broker pods are not on 3 distinct nodes"
BROKER_NODES="$(k get nodes -l kkbench/pool=broker -o name | sed 's#node/##' | sort)"
[ "$(jq -r '.items[].spec.nodeName' <<<"$J" | sort)" = "$BROKER_NODES" ] || die "broker pods are not on the broker pool"

marker() { local p; for p in bench-0 bench-1 bench-2; do
  k -n kubemq logs "$p" -c bench 2>/dev/null | grep -aq 'cluster marker written' && return 0; done; return 1; }
wait_for 300 "Kafka cluster feature marker written" marker
LVL="$(for p in bench-0 bench-1 bench-2; do k -n kubemq logs "$p" -c bench 2>/dev/null | grep -a 'cluster marker written'; done | grep -oE '"feature_level":[0-9]+' | head -1)"
note "fresh KubeMQ ready: 3/3 on ${KUBEMQ_IMAGE%%@*} (${want:0:19}…), ${LVL:-feature level not logged}, $(( $(date +%s) - T0 )) s total"
