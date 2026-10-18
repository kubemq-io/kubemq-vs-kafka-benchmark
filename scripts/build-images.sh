#!/usr/bin/env bash
#
# build-images.sh — build the two images this repo owns and push them to YOUR registry, so
# a reproducer never needs access to ours:
#   omb       OpenMessaging Benchmark at the pinned commit (omb/VERSION), unmodified
#   verifier  the acked-loss verifier (verifier/)
#
#   OMB_REGISTRY=<region>-docker.pkg.dev/<project>/<repo> scripts/build-images.sh [omb|verifier]
#
# Each pushed reference (tag@digest) is written to omb/IMAGE and verifier/IMAGE, which
# omb-deploy.sh substitutes into the manifests. Uses docker if present, else podman.
#
source "$(dirname "$0")/lib.sh"
# shellcheck disable=SC1091
source "$REPO_ROOT/omb/VERSION"
[ -n "${OMB_REGISTRY:-}" ] || die "set OMB_REGISTRY (e.g. us-east1-docker.pkg.dev/$PROJECT/benchmark)"
ENGINE="$(command -v docker || command -v podman)" || die "need docker or podman"
REGHOST="${OMB_REGISTRY%%/*}"

login() {
  if [ "$(basename "$ENGINE")" = podman ]; then
    gcloud auth print-access-token | podman login -u oauth2accesstoken --password-stdin "$REGHOST" >/dev/null ||
      die "podman login to $REGHOST failed"
  else
    gcloud auth configure-docker "$REGHOST" --quiet >/dev/null 2>&1
  fi
}

build_push() {  # $1 context dir, $2 image name, $3 tag, $4 output file, rest = build args
  local ctx="$1" name="$2" tag="$3" outf="$4"; shift 4
  local ref="$OMB_REGISTRY/$name:$tag" digest
  note "building $ref (linux/amd64) with $(basename "$ENGINE")"
  "$ENGINE" build --platform linux/amd64 "$@" -t "$ref" "$ctx" || die "build of $name failed"
  login
  if [ "$(basename "$ENGINE")" = podman ]; then
    podman push --digestfile "$STATE_DIR/$name.digest" "$ref" || die "push of $name failed"
    digest="$(cat "$STATE_DIR/$name.digest")"
  else
    docker push "$ref" >/dev/null || die "push of $name failed"
    digest="$(docker inspect --format '{{index .RepoDigests 0}}' "$ref" | sed 's/.*@//')"
  fi
  echo "$ref@$digest" > "$outf"
  note "$(basename "$(dirname "$outf")")/IMAGE = $ref@$digest"
}

what="${1:-all}"
if [ "$what" = all ] || [ "$what" = omb ]; then
  build_push "$REPO_ROOT/omb" omb "${OMB_COMMIT:0:12}" "$REPO_ROOT/omb/IMAGE" --build-arg "OMB_COMMIT=$OMB_COMMIT"
fi
if [ "$what" = all ] || [ "$what" = verifier ]; then
  VTAG="$(git -C "$REPO_ROOT" log -1 --format=%h -- verifier)"
  build_push "$REPO_ROOT/verifier" kafka-verifier "${VTAG:-dev}" "$REPO_ROOT/verifier/IMAGE"
fi
