#!/usr/bin/env bash
# Build the controller image for linux/amd64 from this checkout, push it, and
# print the immutable digest to pin in a deployment's colors.yml as `image`.
#
#   scripts/image.sh <registry-host/registry-name> [sha]
#
# The push credential comes from DOCKER_CONFIG in the environment (the doks
# package's `registry` verb writes a short-lived one); nothing here reads or
# writes credentials. The tag and the OCI revision label are the git SHA the
# image was built from, so a running pod can be traced to a commit. Run
# `bb test` first: the image build does not execute Babashka.
set -euo pipefail
registry=${1:?usage: scripts/image.sh <registry-host/registry-name> [sha]}
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
sha=${2:-$(git -C "$root" rev-parse HEAD)}
[[ -n ${DOCKER_CONFIG:-} ]] || { echo 'image: DOCKER_CONFIG must point at a directory holding a push credential' >&2; exit 2; }
if [[ -z ${2:-} && -n $(git -C "$root" status --porcelain) ]]; then
  echo 'image: working tree is dirty; commit first so the revision label is true' >&2; exit 2
fi
ref="$registry/redis-operator"
# docker runs as root and writes buildx state beside the config it is given;
# give it a private root-owned copy so nothing root-owned lands in the
# deployment's .colors/ tree (the doks cleanup could not remove one).
config=$(mktemp -d)
trap 'sudo -n rm -rf "$config"' EXIT
cp "$DOCKER_CONFIG/config.json" "$config/config.json"
DOCKER_CONFIG=$config
sudo -n env DOCKER_CONFIG="$DOCKER_CONFIG" docker buildx build --platform linux/amd64 --push \
  -t "$ref:$sha" --label "org.opencontainers.image.revision=$sha" "$root"
digest=$(sudo -n env DOCKER_CONFIG="$DOCKER_CONFIG" docker buildx imagetools inspect "$ref:$sha" --format '{{json .Manifest.Digest}}' | tr -d '"')
echo "image: $ref@$digest"
