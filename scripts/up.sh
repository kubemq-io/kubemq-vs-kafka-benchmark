#!/usr/bin/env bash
#
# up.sh — create one benchmark cluster and deploy one broker on it.
#
#   up.sh kafka|kubemq [--slot a|b] [--drivers 2|4] [--yes] [--skip-baseline] [--resume]
#
# --resume continues on a cluster an earlier up.sh created (same slot and broker, TTL still
# running): it skips creation and re-runs every later step, all of which are idempotent.
# Records go to that earlier run's directory.
#
# Default slots: kafka → a, kubemq → b. Repeat 3 of the matrix swaps them
# (`up.sh kafka --slot b`, `up.sh kubemq --slot a`) so each broker sees two host sets.
#
# In order, each step refusing to continue on failure:
#   1. pre-flight: tools, project, name guard, cluster absent, licence key present (KubeMQ)
#   2. prints the plan and hourly cost, requires the cluster name typed back
#   3. schedules the TTL (down.sh at +12 h) BEFORE creating anything
#   4. terraform apply (workspace = slot): broker, driver and monitoring pools; pinned GKE
#      version with a 30-day NO_UPGRADES maintenance exclusion
#   5. waits for every node Ready; RAID-0 local SSD and the local PV provisioner;
#      waits for 3 local PVs
#   6. hardware baseline (cpuinfo, chrony, fio, iperf3) while the disks are still empty
#   7. monitoring stack on the monitoring pool
#   8. the broker; waits for 3 Ready broker pods with Guaranteed QoS
#   9. KubeMQ: licence pre-flight from the running cluster (status, expiry ≥ 14 days)
#  10. records image digests, node facts and what else landed on broker nodes
#
# Everything recorded goes to results/<date>/infra/<slot>-<broker>-<time>/.
# The licence key is read from $KUBEMQ_LICENSE_KEY (or .license.txt) and only ever written into a
# Kubernetes Secret (via stdin, never argv). It is never printed.
#
source "$(dirname "$0")/lib.sh"

BROKER="${1:-}"; shift || true
check_broker "$BROKER"
SLOT=""; DRIVERS=2; ASSUME_YES=0; SKIP_BASELINE=0; RESUME=0
while [ $# -gt 0 ]; do
  case "$1" in
    --slot) SLOT="$2"; shift 2 ;;
    --drivers) DRIVERS="$2"; shift 2 ;;
    --yes) ASSUME_YES=1; shift ;;
    --skip-baseline) SKIP_BASELINE=1; shift ;;
    --resume) RESUME=1; shift ;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) die "unknown arg: $1" ;;
  esac
done
[ -n "$SLOT" ] || { [ "$BROKER" = kafka ] && SLOT=a || SLOT=b; }
check_slot "$SLOT"
NAME="$(cluster_name "$SLOT")"
guard_name "$NAME"

# --- 1. pre-flight ------------------------------------------------------------
for t in gcloud kubectl helm terraform jq; do command -v "$t" >/dev/null || die "$t not found"; done
[ -n "$PROJECT" ] || die "no GCP project (set GCP_PROJECT or gcloud config set project)."
if [ "$RESUME" = "1" ]; then
  cluster_exists "$NAME" || die "--resume: cluster $NAME does not exist."
  L="$(cluster_labels "$NAME")"
  [ "$(jq -r .purpose <<<"$L")" = "kubemq-kafka-benchmark" ] && [ "$(jq -r .broker <<<"$L")" = "$BROKER" ] ||
    die "--resume: $NAME is not a $BROKER benchmark cluster ($L)."
  TTLST="$("$REPO_ROOT/scripts/ttl.sh" status "$SLOT")"   # not piped: grep -q + pipefail = false negative
  [[ "$TTLST" == *"TTL scheduled"* ]] || die "--resume: no TTL running for slot $SLOT."
else
  cluster_exists "$NAME" && die "cluster $NAME already exists. down.sh --slot $SLOT first (or --resume)."
fi
if [ "$BROKER" = kubemq ]; then
  # The key comes from $KUBEMQ_LICENSE_KEY or, failing that, from .license.txt at the repo root (gitignored).
  if [ -z "${KUBEMQ_LICENSE_KEY:-}" ] && [ -s "$REPO_ROOT/.license.txt" ]; then
    KUBEMQ_LICENSE_KEY="$(tr -d '[:space:]' < "$REPO_ROOT/.license.txt")"; export KUBEMQ_LICENSE_KEY
  fi
  [ -n "${KUBEMQ_LICENSE_KEY:-}" ] || die "no licence key: set KUBEMQ_LICENSE_KEY or put the key in .license.txt (keys come from KubeMQ)."
fi
# Orphans from an earlier incarnation would bill silently; refuse until swept.
ORPH="$(gcloud compute disks list --project "$PROJECT" --filter="zone:($ZONE) AND labels.goog-k8s-cluster-name=$NAME" --format='value(name)' 2>/dev/null)"
[ -z "$ORPH" ] || [ "$RESUME" = "1" ] || die "orphaned disks from an earlier $NAME exist ($ORPH). Run down.sh --slot $SLOT."

# shellcheck disable=SC1091
source "$REPO_ROOT/infra/kafka/versions.env"
# shellcheck disable=SC1091
source "$REPO_ROOT/infra/kubemq/versions.env"
# shellcheck disable=SC1091
source "$REPO_ROOT/infra/monitoring/versions.env"

COST="$(cost_per_hour "$DRIVERS")"
GKE_VERSION="$(sed -n '/variable "gke_version"/,/}/s/.*default *= *"\(.*\)".*/\1/p' "$TF_DIR/variables.tf")"
cat <<PLAN

  KubeMQ vs Kafka benchmark — cluster creation plan

    project      $PROJECT
    zone         $ZONE
    cluster      $NAME   (slot $SLOT)
    broker       $BROKER
    GKE          $GKE_VERSION pinned (REGULAR channel + 30-day no-upgrades exclusion), autorepair OFF, on-demand
    broker pool  3 × n2-standard-8 (Intel Ice Lake), 2 × 375 GB Local SSD each → RAID-0 local PV
    driver pool  $DRIVERS × n2-standard-8 (Intel Ice Lake), tainted for benchmark workers only
    monitoring   1 × e2-standard-4
    cost         about \$$COST per hour for this cluster (about \$$(awk -v c="$COST" 'BEGIN{printf "%.0f", c*24}') per day)
    TTL          down.sh runs automatically $TTL_HOURS h from now unless cancelled

PLAN
if [ "$ASSUME_YES" != "1" ] && [ "$RESUME" = "0" ]; then
  printf "Type the cluster name to confirm creation: "
  read -r reply
  [ "$reply" = "$NAME" ] || { echo "aborted."; exit 1; }
fi

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="$REPO_ROOT/results/${KKBENCH_RESULTS_DATE:-$(date -u +%Y-%m-%d)}/infra/${SLOT}-${BROKER}-${STAMP}"
if [ "$RESUME" = "1" ]; then
  # shellcheck disable=SC2012
  OUT="$(ls -dt "$REPO_ROOT"/results/*/infra/"${SLOT}-${BROKER}"-* 2>/dev/null | head -1)"
  [ -n "$OUT" ] || die "--resume: no earlier record directory for ${SLOT}-${BROKER}."
fi
mkdir -p "$OUT"
exec > >(tee -a "$OUT/up.log") 2>&1
note "recording to ${OUT#"$REPO_ROOT"/}"

if [ "$RESUME" = "1" ]; then
  DEADLINE="$(cat "$STATE_DIR/ttl-$SLOT.deadline")"
  note "resuming on $NAME; TTL still due $DEADLINE UTC"
else
# --- 3. TTL first -------------------------------------------------------------
DEADLINE="$("$REPO_ROOT/scripts/ttl.sh" start "$SLOT")" || die "could not schedule the TTL; nothing created."
note "TTL scheduled: down.sh --slot $SLOT at $DEADLINE UTC"

# --- 4. terraform -------------------------------------------------------------
note "terraform apply (workspace $SLOT)"
tf_auth
terraform -chdir="$TF_DIR" init -input=false >/dev/null || die "terraform init failed"
terraform -chdir="$TF_DIR" workspace list 2>/dev/null | grep -qw "$SLOT" ||
  terraform -chdir="$TF_DIR" workspace new "$SLOT" >/dev/null
TF_WORKSPACE="$SLOT" terraform -chdir="$TF_DIR" apply -auto-approve -input=false \
  -var "project=$PROJECT" -var "zone=$ZONE" -var "slot=$SLOT" -var "broker=$BROKER" \
  -var "name_prefix=$NAME_PREFIX" -var "driver_node_count=$DRIVERS" -var "ttl_deadline=$DEADLINE" \
  -var "no_upgrade_start=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  || die "terraform apply failed. The TTL will still destroy whatever was created; or run down.sh --slot $SLOT."

fi

KUBECONFIG="$(kubeconfig_for "$SLOT")" gcloud container clusters get-credentials "$NAME" \
  --zone "$ZONE" --project "$PROJECT" >/dev/null 2>&1 || die "get-credentials failed"

# --- 5. nodes, local SSD, PVs -------------------------------------------------
EXPECT_NODES=$((3 + DRIVERS + 1))
nodes_ready() { [ "$(k get nodes --no-headers 2>/dev/null | awk '$2=="Ready"' | wc -l | tr -d ' ')" = "$EXPECT_NODES" ]; }
wait_for 600 "$EXPECT_NODES Ready nodes" nodes_ready
note "$EXPECT_NODES/$EXPECT_NODES nodes Ready"

note "RAID-0 local SSD and local PV provisioner"
k apply -f "$REPO_ROOT/infra/local-ssd/raid.yaml" >/dev/null
k -n local-ssd rollout status ds/raid0 --timeout=600s || die "RAID-0 DaemonSet not ready; see: kubectl -n local-ssd logs ds/raid0"
k -n local-ssd logs -l app=raid0 --prefix --tail=5 > "$OUT/raid0.log" 2>&1
k apply -f "$REPO_ROOT/infra/local-ssd/provisioner.yaml" >/dev/null
k -n local-ssd rollout status ds/local-volume-provisioner --timeout=300s || die "local PV provisioner not ready"
pv_count() { [ "$(k get pv -o json | jq '[.items[] | select(.spec.storageClassName=="local-ssd")] | length')" -ge 3 ]; }
wait_for 300 "3 local-ssd PersistentVolumes" pv_count
k get pv -o wide > "$OUT/pv.txt"
note "local PVs: $(k get pv -o json | jq -r '[.items[] | select(.spec.storageClassName=="local-ssd") | .spec.capacity.storage] | join(", ")')"

# --- node facts ---------------------------------------------------------------
gcloud container clusters describe "$NAME" --zone "$ZONE" --project "$PROJECT" --format=json \
  | jq '{name, currentMasterVersion, currentNodeVersion, releaseChannel, resourceLabels,
         nodePools: [.nodePools[] | {name, version, initialNodeCount, management,
                     config: {machineType: .config.machineType, minCpuPlatform: .config.minCpuPlatform,
                              imageType: .config.imageType, spot: .config.spot,
                              localNvmeSsdBlockConfig: .config.localNvmeSsdBlockConfig}}]}' \
  > "$OUT/cluster.json"
k get nodes -o json | jq -r '.items[] | [.metadata.name, .metadata.labels["kkbench/pool"],
  .metadata.labels["node.kubernetes.io/instance-type"], .status.nodeInfo.osImage,
  .status.nodeInfo.kernelVersion, .status.nodeInfo.kubeletVersion, .status.allocatable.cpu,
  .status.allocatable.memory] | @tsv' | sort -k2 > "$OUT/nodes.tsv"
jq -e '[.nodePools[] | .management.autoRepair // false] | any | not' \
  "$OUT/cluster.json" >/dev/null || die "a node pool has autorepair ON"
gcloud container clusters describe "$NAME" --zone "$ZONE" --project "$PROJECT" --format=json \
  | jq '.maintenancePolicy.window.maintenanceExclusions' > "$OUT/maintenance-exclusions.json"
jq -e 'to_entries | map(.value.maintenanceExclusionOptions.scope // "NO_UPGRADES") | index("NO_UPGRADES")' \
  "$OUT/maintenance-exclusions.json" >/dev/null || die "no NO_UPGRADES maintenance exclusion on $NAME"

# The pinned platform, from the Compute API (the model name inside the guest is masked).
gcloud compute instances list --project "$PROJECT" --filter="zone:($ZONE) AND name~^gke-${NAME}-" \
  --format='value(name,machineType.basename(),cpuPlatform)' | sort > "$OUT/instances.tsv"
awk -F'\t' '$2 ~ /^n2-/ && $3 != "Intel Ice Lake" { bad=1; print "not Ice Lake: "$0 } END { exit bad }' \
  "$OUT/instances.tsv" || die "an n2 node is not on the pinned Intel Ice Lake platform"
note "platform: $(awk -F'\t' '$2 ~ /^n2-/' "$OUT/instances.tsv" | wc -l | tr -d ' ') n2 nodes on Intel Ice Lake"

# --- 6. hardware baseline ----------------------------------------------------
if [ "$SKIP_BASELINE" = "0" ]; then
  "$REPO_ROOT/scripts/baseline.sh" run --slot "$SLOT" --out "$OUT/baseline" || die "baseline failed"
fi

# --- 7. monitoring ------------------------------------------------------------
note "monitoring stack ($KPS_CHART $KPS_CHART_VERSION)"
h repo add prometheus-community "$KPS_CHART_REPO" >/dev/null 2>&1 || true
h repo update prometheus-community >/dev/null
h upgrade --install kps "$KPS_CHART" --version "$KPS_CHART_VERSION" -n monitoring --create-namespace \
  -f "$REPO_ROOT/infra/monitoring/values.yaml" --wait --timeout 10m >/dev/null || die "monitoring install failed"

# --- 8. broker ----------------------------------------------------------------
broker_ready() {  # $1 ns, $2 pod label selector
  local n
  n=$(k -n "$1" get pods -l "$2" -o json | jq '[.items[] | select(.status.conditions[]? | select(.type=="Ready" and .status=="True"))] | length')
  [ "$n" = "3" ]
}

if [ "$BROKER" = kafka ]; then
  note "Strimzi $STRIMZI_VERSION operator"
  h upgrade --install strimzi "$STRIMZI_CHART" --version "$STRIMZI_VERSION" -n kafka --create-namespace \
    -f "$REPO_ROOT/infra/kafka/operator-values.yaml" --wait --timeout 10m >/dev/null || die "Strimzi install failed"
  note "Kafka $KAFKA_VERSION (KRaft combined, 3 nodes)"
  k apply -f "$REPO_ROOT/infra/kafka/kafka.yaml" >/dev/null || die "Kafka CR apply failed"
  k apply -f "$REPO_ROOT/infra/monitoring/kafka-podmonitor.yaml" >/dev/null || die "Kafka PodMonitor apply failed"
  k -n kafka wait kafka/bench --for=condition=Ready --timeout=1200s || die "Kafka not Ready: kubectl -n kafka get kafka bench -o yaml"
  BROKER_NS=kafka; BROKER_SEL="strimzi.io/cluster=bench,strimzi.io/broker-role=true"
else
  note "KubeMQ operator (chart $KUBEMQ_CHART_VERSION)"
  h repo add kubemq-next "$KUBEMQ_CHART_REPO" >/dev/null 2>&1 || true
  h repo update kubemq-next >/dev/null
  k create namespace kubemq >/dev/null 2>&1 || true
  printf '%s' "$KUBEMQ_LICENSE_KEY" | k -n kubemq create secret generic kubemq-license \
    --from-file=licenseKey=/dev/stdin --dry-run=client -o yaml | k apply -f - >/dev/null || die "licence Secret failed"
  h upgrade --install kubemq-operator "$KUBEMQ_CHART" --version "$KUBEMQ_CHART_VERSION" -n kubemq \
    -f "$REPO_ROOT/infra/kubemq/operator-values.yaml" --wait --timeout 10m >/dev/null || die "KubeMQ operator install failed"
  # The chart has no operator nodeSelector; keep it off broker nodes like Strimzi's.
  k -n kubemq patch deploy kubemq-operator-next --type merge \
    -p '{"spec":{"template":{"spec":{"nodeSelector":{"kkbench/pool":"monitoring"}}}}}' >/dev/null
  k -n kubemq rollout status deploy/kubemq-operator-next --timeout=300s || die "KubeMQ operator not ready"
  note "KubeMQ cluster (3 servers), image $(sed -E 's#.*:(v[^@]+)@.*#\1#' <<<"$KUBEMQ_IMAGE")"
  h upgrade --install kubemq-bench "$KUBEMQ_CHART" --version "$KUBEMQ_CHART_VERSION" -n kubemq \
    -f "$REPO_ROOT/infra/kubemq/cluster-values.yaml" >/dev/null || die "KubemqCluster install failed"
  # 1 s scrape of the brokers' own /metrics; without it no kubemq_*/dragonboat_* series exist.
  k apply -f "$REPO_ROOT/infra/monitoring/kubemq-podmonitor.yaml" >/dev/null || die "KubeMQ PodMonitor apply failed"
  BROKER_NS=kubemq; BROKER_SEL="app=bench"
fi

wait_for 1200 "3 Ready broker pods ($BROKER_SEL)" broker_ready "$BROKER_NS" "$BROKER_SEL"
k -n "$BROKER_NS" get pods -o wide | tee "$OUT/broker-pods.txt"

# Guaranteed QoS and one broker per broker node.
k -n "$BROKER_NS" get pods -l "$BROKER_SEL" -o json > "$OUT/broker-pods.json"
jq -e '[.items[] | .status.qosClass == "Guaranteed"] | all' "$OUT/broker-pods.json" >/dev/null ||
  die "a broker pod is not Guaranteed QoS: $(jq -r '[.items[] | .metadata.name+"="+.status.qosClass] | join(" ")' "$OUT/broker-pods.json")"
[ "$(jq -r '[.items[].spec.nodeName] | unique | length' "$OUT/broker-pods.json")" = "3" ] || die "broker pods are not on 3 distinct nodes"
BROKER_NODES="$(k get nodes -l kkbench/pool=broker -o name | sed 's#node/##' | sort)"
[ "$(jq -r '.items[].spec.nodeName' "$OUT/broker-pods.json" | sort)" = "$BROKER_NODES" ] || die "broker pods are not on the broker pool"

# --- 9. KubeMQ licence pre-flight -------------------------------------------
if [ "$BROKER" = kubemq ]; then
  note "licence pre-flight (GET /api/v1/license on a running server)"
  POD="$(jq -r '.items[0].metadata.name' "$OUT/broker-pods.json")"
  PFLOG="$STATE_DIR/pf-$SLOT.log"
  KUBECONFIG="$(kubeconfig_for "$SLOT")" kubectl -n kubemq port-forward "pod/$POD" ${LICENSE_LOCAL_PORT:-$(slot_port 18080)}:8080 >"$PFLOG" 2>&1 &
  PF=$!; sleep 4
  LIC="$(curl -s --max-time 10 http://127.0.0.1:${LICENSE_LOCAL_PORT:-$(slot_port 18080)}/api/v1/license)"; kill "$PF" 2>/dev/null
  # Keep only non-secret fields; the lease credential is never requested.
  jq '(.data // .) | {plan, mode, issued_to, license_expires_at, lease_expires_at, run_until,
      max_instances, instances, over_cap, warnings, last_refresh}' <<<"$LIC" > "$OUT/license.json" 2>/dev/null ||
    die "licence endpoint did not return JSON: $(head -c 300 <<<"$LIC")"
  cat "$OUT/license.json"
  EXP="$(jq -r '.license_expires_at // empty' "$OUT/license.json")"
  [ -n "$EXP" ] || die "licence status has no expiry; record it by hand before continuing."
  EXP_EPOCH="$(date -u -j -f %Y-%m-%dT%H:%M:%SZ "${EXP%%.*}" +%s 2>/dev/null || date -u -d "$EXP" +%s)"
  DAYS=$(( (EXP_EPOCH - $(date +%s)) / 86400 ))
  echo "  licence expires $EXP ($DAYS days from now)"
  [ "$DAYS" -ge 14 ] || die "licence expires in $DAYS days (< 14). Renew before a campaign."
fi

# --- 10. digests, placement ---------------------------------------------------
"$REPO_ROOT/scripts/record-digests.sh" --slot "$SLOT" --ns "$BROKER_NS" --out "$OUT/digests.tsv" || die "digest recording failed"
# Everything that is not a DaemonSet or the broker itself, on broker nodes: recorded, both sides.
k get pods -A -o json | jq -r --argjson nodes "$(printf '%s\n' "$BROKER_NODES" | jq -R . | jq -s .)" '
  .items[] | select(.spec.nodeName as $n | $nodes | index($n)) |
  select(([.metadata.ownerReferences[]?.kind] | index("DaemonSet")) | not) |
  [.spec.nodeName, .metadata.namespace, .metadata.name,
   ([.spec.containers[].resources.requests.cpu // "0"] | join("+"))] | @tsv' > "$OUT/broker-node-pods.tsv"
note "pods on broker nodes (non-DaemonSet):"; column -t "$OUT/broker-node-pods.tsv"

cat <<DONE

  UP: $NAME ($BROKER), recorded in ${OUT#"$REPO_ROOT"/}
    kubeconfig   export KUBECONFIG=$(kubeconfig_for "$SLOT")
    TTL          down.sh --slot $SLOT fires at $DEADLINE UTC (cancel: scripts/ttl.sh cancel $SLOT)
    tear down    scripts/down.sh --slot $SLOT
DONE
