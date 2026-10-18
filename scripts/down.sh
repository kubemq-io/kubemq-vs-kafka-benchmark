#!/usr/bin/env bash
#
# down.sh — destroy one benchmark cluster and PROVE it is gone.
#
#   down.sh --slot a|b [--yes] [--ttl]
#
# 1. Refuses any cluster that is not kkbench-a / kkbench-b, and any cluster that does not
#    carry lifecycle=throwaway and purpose=kubemq-kafka-benchmark labels.
# 2. Destroys through Terraform (falls back to `gcloud container clusters delete` if the
#    Terraform state is missing, e.g. on another machine).
# 3. Re-queries: exits non-zero unless the cluster is really gone.
# 4. Sweeps persistent disks that still carry the cluster's label
#    (goog-k8s-cluster-name). Deleting a GKE cluster does NOT delete PVC-backed disks.
#    Only unattached disks are deleted; an attached one fails the run.
# 5. Reports firewall rules still naming the cluster (GKE leaves LoadBalancer rules).
# 6. Cancels the TTL and removes the private kubeconfig.
#
# --ttl is passed only by ttl.sh when the deadline fires; it implies --yes.
#
source "$(dirname "$0")/lib.sh"

SLOT=""; ASSUME_YES=0; FROM_TTL=0
while [ $# -gt 0 ]; do
  case "$1" in
    --slot) SLOT="$2"; shift 2 ;;
    --yes)  ASSUME_YES=1; shift ;;
    --ttl)  FROM_TTL=1; ASSUME_YES=1; shift ;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) die "unknown arg: $1" ;;
  esac
done
check_slot "$SLOT"
NAME="$(cluster_name "$SLOT")"
guard_name "$NAME"
[ -n "$PROJECT" ] || die "no GCP project (set GCP_PROJECT or gcloud config set project)."

finish_ttl() {
  if [ "$FROM_TTL" = "1" ]; then
    rm -f "$STATE_DIR/ttl-$SLOT.pid" "$STATE_DIR/ttl-$SLOT.deadline"
  else
    "$REPO_ROOT/scripts/ttl.sh" cancel "$SLOT"
  fi
}

if cluster_exists "$NAME"; then
  LABELS="$(cluster_labels "$NAME")"
  [ "$(jq -r '.lifecycle // ""' <<<"$LABELS")" = "throwaway" ] &&
  [ "$(jq -r '.purpose // ""' <<<"$LABELS")" = "kubemq-kafka-benchmark" ] ||
    die "cluster '$NAME' lacks lifecycle=throwaway / purpose=kubemq-kafka-benchmark labels ($LABELS). Not ours; refusing."
  BROKER="$(jq -r '.broker // "kafka"' <<<"$LABELS")"

  cat <<PLAN

  About to DELETE, permanently:
    project  $PROJECT
    zone     $ZONE
    cluster  $NAME   (broker: $BROKER)
    labels   $LABELS
  Everything on it goes, including broker data and any results not yet copied off.

PLAN
  if [ "$ASSUME_YES" != "1" ]; then
    printf "Type the cluster name to confirm deletion: "
    read -r reply
    [ "$reply" = "$NAME" ] || { echo "aborted."; exit 1; }
  fi

  destroyed=0
  if [ -f "$TF_DIR/terraform.tfstate.d/$SLOT/terraform.tfstate" ]; then
    note "terraform destroy (workspace $SLOT)"
    tf_auth
    if TF_WORKSPACE="$SLOT" terraform -chdir="$TF_DIR" destroy -auto-approve -input=false \
        -var "project=$PROJECT" -var "zone=$ZONE" -var "slot=$SLOT" -var "broker=$BROKER" \
        -var "name_prefix=$NAME_PREFIX"; then
      destroyed=1
    else
      echo "terraform destroy failed; falling back to gcloud." >&2
    fi
  fi
  [ "$destroyed" = "1" ] || wait_cluster_ops "$NAME"
  if [ "$destroyed" = "0" ] && cluster_exists "$NAME"; then
    note "gcloud container clusters delete $NAME"
    gcloud container clusters delete "$NAME" --zone "$ZONE" --project "$PROJECT" --quiet
  fi
else
  echo "cluster '$NAME' does not exist in $ZONE; checking for leftovers only."
fi

# --- prove it is gone ---------------------------------------------------------
wait_cluster_ops "$NAME"
if cluster_exists "$NAME"; then
  die "cluster '$NAME' STILL EXISTS after delete. It is still billing. Re-run, or delete in the console."
fi
echo "  cluster:  gone (re-queried $(utc))."
# The cluster is proven gone; if Terraform did not do the deleting its state still lists
# resources that no longer exist. Drop that state so the next up.sh starts clean.
if [ -s "$TF_DIR/terraform.tfstate.d/$SLOT/terraform.tfstate" ] &&
   [ -n "$(TF_WORKSPACE="$SLOT" terraform -chdir="$TF_DIR" state list 2>/dev/null)" ]; then
  rm -f "$TF_DIR/terraform.tfstate.d/$SLOT/terraform.tfstate" "$TF_DIR/terraform.tfstate.d/$SLOT/terraform.tfstate.backup"
  echo "  terraform: stale state for workspace $SLOT removed."
fi

# --- orphaned disks, by label -------------------------------------------------
# GKE names PVC disks pvc-<uuid>; only the label ties them to the cluster.
FAILED=0
DISKS="$(gcloud compute disks list --project "$PROJECT" \
  --filter="zone:($ZONE) AND labels.goog-k8s-cluster-name=$NAME" \
  --format='value(name,sizeGb,users.len())' 2>/dev/null || true)"
if [ -n "$DISKS" ]; then
  while read -r dname dsize dusers; do
    [ -n "$dname" ] || continue
    if [ -n "$dusers" ] && [ "$dusers" != "0" ]; then
      echo "  ⚠ disk $dname (${dsize} GB) is still ATTACHED to something; not deleting." >&2
      FAILED=1
    else
      echo "  deleting orphaned disk $dname (${dsize} GB)"
      gcloud compute disks delete "$dname" --zone "$ZONE" --project "$PROJECT" --quiet || FAILED=1
    fi
  done <<<"$DISKS"
  LEFT="$(gcloud compute disks list --project "$PROJECT" \
    --filter="zone:($ZONE) AND labels.goog-k8s-cluster-name=$NAME" --format='value(name)' 2>/dev/null)"
  [ -z "$LEFT" ] || { echo "  ⚠ disks still present: $LEFT" >&2; FAILED=1; }
fi
[ "$FAILED" = "0" ] && echo "  disks:    none left carrying goog-k8s-cluster-name=$NAME."

# --- firewall rules naming the cluster ----------------------------------------
FW="$(gcloud compute firewall-rules list --project "$PROJECT" --format='value(name)' 2>/dev/null \
  | grep -E "(^|-)${NAME}(-|$)" || true)"
if [ -n "$FW" ]; then
  echo "  ⚠ firewall rules still naming $NAME (GKE does not always remove these):" >&2
  printf '      %s\n' $FW >&2
  FAILED=1
else
  echo "  firewall: no rules naming $NAME."
fi

finish_ttl
rm -f "$(kubeconfig_for "$SLOT")"

[ "$FAILED" = "0" ] || die "cluster deleted, but leftovers above need attention."
echo "TORN DOWN and verified: $NAME."
