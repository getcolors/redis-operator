#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
for namespace in colors-redis colors-other; do
  for color in green red blue; do
    mkdir -p "$tmp/$color"
    sed -e "s#WORKDIR#$tmp/$color/output#" -e "s/namespace: colors-redis/namespace: $namespace/" "$root/test/fixtures/colors.yml" > "$tmp/$color/colors.yml"
    launcher="$root/skills/package-redis-operator-$color/$color"
    (cd "$tmp/$color" && REDIS_OPERATOR_LIB_ROOT="$root" "$launcher" build >/dev/null)
  done
  diff -ru "$tmp/green/output" "$tmp/red/output"
  diff -ru "$tmp/green/output" "$tmp/blue/output"
done
echo 'parity: manifests match in all three colours'
