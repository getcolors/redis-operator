#!/usr/bin/env bash
# Render the fixture and compare the operator tree byte for byte with the
# committed golden. --accept replaces the golden after the secret guard.
set -euo pipefail
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
fixture="$tmp/colors.yml"
sed "s#WORKDIR#$tmp/work#" "$root/test/fixtures/colors.yml" > "$fixture"
env -i PATH="$PATH" HOME="$HOME" REDIS_OPERATOR_LIB_ROOT="$root" "$root/green" build -f "$fixture" >/dev/null
actual="$tmp/work/redis-operator-fixture"
golden="$root/test/resources/golden/redis-operator-fixture"
# No rendered artefact may carry a real secret into a committed golden. POSIX
# grep on purpose: a missing binary inside `if` is simply false, so the guard
# must not depend on one that may be absent.
if grep -rEq 'client-key-data|client-certificate-data|BEGIN (RSA |EC |OPENSSH |DSA )?PRIVATE KEY|github_pat_|ghp_|gho_|ghu_|ghs_|ghr_|dop_v1_|COLORS_PAR_[A-Z_]*(TOKEN|KEY)[A-Z_]*" *: *"[^"]' "$actual"; then
  echo 'golden: a credential-shaped value was rendered' >&2; exit 1
fi
if [[ ${1:-} == --accept ]]; then rm -rf "$golden"; mkdir -p "$(dirname "$golden")"; cp -a "$actual" "$golden"; echo 'golden: accepted'; exit 0; fi
[[ -d "$golden" ]] || { echo 'golden missing; inspect build then run bb golden:accept' >&2; exit 1; }
diff -ru "$golden" "$actual"
echo 'golden: fixture matches'
