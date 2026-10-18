#!/usr/bin/env bash
#
# record-digests.sh — write down the image digest every broker-side container is
# ACTUALLY running, and compare two observations.
#
#   record-digests.sh --slot a|b --out FILE [--ns "kafka kubemq"]    record
#   record-digests.sh --compare BEFORE AFTER                         PASS, or VOID on drift
#
# A tag is not a version: a pod restarted during a run can pull a different image
# behind the same tag. Digests are recorded before and after every run; if they moved,
# the run is VOID (it measured something it did not intend to), not failed.
#
# Records every container (init containers excluded) as pod / container / image /
# imageID. The node is printed but not written, so a reschedule is not a version change.
#
source "$(dirname "$0")/lib.sh"

SLOT=""; OUT=""; NSS="kafka kubemq"; A=""; B=""
while [ $# -gt 0 ]; do
  case "$1" in
    --slot) SLOT="$2"; shift 2 ;;
    --out) OUT="$2"; shift 2 ;;
    --ns) NSS="$2"; shift 2 ;;
    --compare) A="$2"; B="$3"; shift 3 ;;
    *) die "unknown arg: $1" ;;
  esac
done

if [ -n "$A" ]; then
  [ -f "$A" ] && [ -f "$B" ] || die "need two readable files"
  if diff -u "$A" "$B"; then echo "PASS: no image drift."; exit 0; fi
  echo "VOID: an image changed between the two observations. Discard and repeat the run." >&2
  exit 1
fi

check_slot "$SLOT"; [ -n "$OUT" ] || die "--out required"
: > "$OUT.tmp"
for ns in $NSS; do
  k get ns "$ns" >/dev/null 2>&1 || continue
  # Image = what the pod spec asked for; imageID = repo@digest actually running.
  # (.status.containerStatuses[].image is a local image-config id on containerd, not a name.)
  k -n "$ns" get pods -o json | jq -r --arg ns "$ns" '
    .items[] | .metadata.name as $p | .spec.nodeName as $n | .spec.containers as $spec |
    (.status.containerStatuses // [])[] | .name as $c |
    [$ns, $p, $c, ($spec[] | select(.name==$c) | .image), .imageID, $n] | @tsv' >> "$OUT.tmp"
done
[ -s "$OUT.tmp" ] || die "no pods found in: $NSS"
sort "$OUT.tmp" | column -t -s $'\t'
cut -f1-5 "$OUT.tmp" | sort > "$OUT"
rm -f "$OUT.tmp"

# Mixed digests within one image repository means the cluster is not level.
MIXED="$(awk -F'\t' '{split($5,a,"@"); print a[1]"\t"a[2]}' "$OUT" | sort -u | cut -f1 | uniq -d)"
[ -z "$MIXED" ] || die "mixed digests for: $MIXED"
# Every running digest must be the one the spec pinned (when the spec pins one).
DRIFT="$(awk -F'\t' 'index($4,"@sha256:") { split($4,s,"@"); split($5,r,"@"); if (s[2]!=r[2]) print $2"/"$3 }' "$OUT")"
[ -z "$DRIFT" ] || die "running digest differs from the pinned one for: $DRIFT"
echo "recorded $OUT (uniform per image repository; pinned digests confirmed)"
