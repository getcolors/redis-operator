#!/usr/bin/env bash
# The payload launcher is the one file the test suite cannot reach. Prove a
# standalone copy of it, pointed at this checkout, renders the fixture from a
# directory that carries nothing but colors.yml.
set -euo pipefail
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
launcher="$root/skills/package-redis-operator-green/green"
grep -q 'io.github.getcolors.redis-operator.workflow/workflow' "$launcher"
grep -q 'io.github.getcolors.redis-operator.workflow/allowed-events' "$launcher"
[[ -L "$root/green" ]] && [[ $(readlink "$root/green") == skills/package-redis-operator-green/green ]]
tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
cp "$launcher" "$tmp/green"; chmod +x "$tmp/green"
sed "s#WORKDIR#.colors#" "$root/test/fixtures/colors.yml" > "$tmp/colors.yml"
(cd "$tmp" && env -i PATH="$PATH" HOME="$HOME" REDIS_OPERATOR_LIB_ROOT="$root" ./green build >/dev/null)
[[ -f "$tmp/.colors/redis-operator-fixture/operator/manifests.json" ]]
[[ -f "$tmp/.colors/redis-operator-fixture/operator/redis-deployment.json" ]]
(cd "$tmp" && env -i PATH="$PATH" HOME="$HOME" REDIS_OPERATOR_LIB_ROOT="$root" ./green create --dry-run >/dev/null)
if (cd "$tmp" && env -i PATH="$PATH" HOME="$HOME" REDIS_OPERATOR_LIB_ROOT="$root" ./green delete --dry-run >/dev/null 2>&1); then
  echo 'launcher: delete --dry-run must refuse while compute-prevent-destroy is true' >&2; exit 1
fi
if (cd "$tmp" && env -i PATH="$PATH" HOME="$HOME" REDIS_OPERATOR_LIB_ROOT="$root" COLORS_PAR_PROFILE=x ./green build >/dev/null 2>&1); then
  echo 'launcher: COLORS_PAR_PROFILE must be refused' >&2; exit 1
fi
echo 'launcher: all checks passed'
for color in red blue; do
  cp "$root/skills/package-redis-operator-$color/$color" "$tmp/$color"
  (cd "$tmp" && env -i PATH="$PATH" HOME="$HOME" REDIS_OPERATOR_LIB_ROOT="$root" "./$color" build >/dev/null)
  (cd "$tmp" && env -i PATH="$PATH" HOME="$HOME" REDIS_OPERATOR_LIB_ROOT="$root" "./$color" create --dry-run >/dev/null)
  for event in delete drill; do
    if (cd "$tmp" && env -i PATH="$PATH" HOME="$HOME" REDIS_OPERATOR_LIB_ROOT="$root" "./$color" "$event" --dry-run >/dev/null 2>&1); then
      echo "$color: $event guard failed" >&2; exit 1
    fi
  done
  if (cd "$tmp" && env -i PATH="$PATH" HOME="$HOME" REDIS_OPERATOR_LIB_ROOT="$root" COLORS_PAR_PROFILE=x "./$color" build >/dev/null 2>&1); then
    echo "$color: profile guard failed" >&2; exit 1
  fi
done
