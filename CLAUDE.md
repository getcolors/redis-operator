# CLAUDE.md

Guidance for agents working in this repository. Read
`~/code/getcolors/CLAUDE.md` first for the cross-repository conventions; this
file covers only what is specific to `redis-operator`.

## What this is

Two things under one pin. `src/colors/` is the **controller**: `colors.main`
starts a Green Kubernetes controller, `colors.redis` adapts the Redis package
(observe, converge, delete) and `colors.probe` is the in-pod probe the
operational verbs exec. The Dockerfile builds that into the controller image,
entry point `bb controller --in-cluster`. `src/io/github/getcolors/redis_operator/`
is the **Package Skill** side: `validate`, `tools` (rendering, kubectl, the
DigitalOcean API, the ownership predicate), `workflow` (preflight and the
`build`/`create`/`delete` graph) and `operator` (`check`, `rehearse`, `drill`,
`restart`). The CRD lives at
`src/resources/io/github/getcolors/redis_operator/crd.yml` so the pinned
library carries it; `build` reads it from the classpath, never from a working
tree. The first consumer is `../redis-operator-doks`.

Green only: `green.kubernetes` exists in no other colour, so there is no
parity suite. `bb test`, `bb golden` and `scripts/launcher.sh` are the nets.

## Commands

```sh
bb test                          # 8 controller tests + the package suites
bb golden                        # fixture render vs test/resources/golden
./scripts/launcher.sh            # standalone payload copy builds the fixture
./green build                    # renders .colors/<profile>/operator/*.json
./green create --dry-run
./green create                   # only with explicit authorization
./green check | rehearse | restart
COLORS_PAR_DRILL_DELETE_OWNED_DROPLET=true ./green drill   # deletes a Droplet
COLORS_PAR_COMPUTE_PREVENT_DESTROY=false ./green delete    # destructive
bb controller <context> <namespace>                         # the controller, locally
```

`env -i PATH=$PATH HOME=$HOME ./green build` and `./green create --dry-run`
must pass from a fresh checkout with no credentials; `./green delete --dry-run`
and `COLORS_PAR_PROFILE=x ./green build` must refuse with exit 2. Never run a
real `create`, `drill` or `delete` without explicit authorization. Never build
or push the image without authorization; `scripts/image.sh` is the only way
to build it and it prints the digest a deployment pins.

## Desired state and credentials

`colors.yml` is flat, kebab-case and non-secret; every key is listed in
`skills/package-redis-operator-green/references/configuration.md`. The keys
from `provider-compute` down become `spec.config` of the custom resource.
`kube-context` is required and every kubectl call passes `--context`;
`KUBECONFIG` comes from the environment (the deployment's `.envrc`). Build
and dry-run are credential-free. A real `create` reads the five
`COLORS_PAR_*` credentials from the process environment and writes them to
the `redis-credentials` Secret on `kubectl apply -f -` stdin with output
suppressed; nothing parses `.envrc.private`, nothing under `.colors/` holds a
secret, and every other kubectl failure carries its stderr into `:green/err`
so it can be diagnosed.

Never export `COLORS_PAR_PROFILE`. Keep `compute-prevent-destroy: true`
committed. `drill` deletes a live Droplet and runs only under
`COLORS_PAR_DRILL_DELETE_OWNED_DROPLET=true`, exactly.

## Protocol invariants

- `spec.suspend` is never rendered and never reset by an apply. `create`
  refuses a suspended resource with exit 1. `rehearse` suspends with a
  resourceVersion-tested patch, waits for the controller to acknowledge
  `Suspended` at the new generation, and resumes only when the resource is
  exactly as it left it and the remote outcome is certain; otherwise it
  prints "resource left suspended; verify no workflow runs before resuming"
  and exits non-zero. A failed re-read never masks the original error.
- `drill` fails closed: recorded provider ID == live Droplet ID, name ==
  profile == probe name, not a node-pool Droplet (with `doks-cluster-id`)
  and no `k8s:` tag, recorded IP among the public v4 addresses, marker
  round-trip, then one DELETE of exactly that ID. Recovery means a different
  provider ID, Ready at unchanged UID and generation, old Droplet 404, and a
  fresh authenticated write. JSON decode errors during the wait are retried.
- `restart` proves a reconcile that happened after the restart:
  `status.lastReconcileTime` must advance past the value read before scaling
  down. A stale Ready status proves nothing.
- `delete` never removes the controller before the finalizer completes, and
  removes the CRD only when no RedisDeployment remains in any namespace.
- The adapter logs one line per observe/converge/delete outcome with the
  reason and provider ID; never secrets or raw workflow output. A failed
  converge or delete is retained as
  `/data/work/<profile>/failures/<UTC timestamp>-<step>.log` (0600 in 0700,
  20 newest kept) with every `COLORS_PAR_*` value masked to `***`; the log
  line names the file. `check` prints `failures retained: N`; read one with
  `kubectl exec -n <namespace> deployment/colors-redis-operator -- cat
  /data/work/<profile>/failures/<file>`.
- Ready is polled through transient `Reconciling` passes (5 s, 180 s) by
  `check` and by every precondition; `Failed`, `Invalid`, `Blocked`,
  suspension and deletion are errors, except that the create and recovery
  waits tolerate `Failed` because the controller retries. A single read of
  the phase proves nothing with a short reconcile interval.

## Package and deployment coupling

The deployment launcher is a copy, not a symlink. The pin in
`skills/package-redis-operator-green/green` is stamped only by `bb pin` after
a clean pushed commit; never invent or hand-edit `redis-operator-sha`. After
repinning, update consumers:

```sh
npx skills update -p -y
cp .agents/skills/package-redis-operator-green/green green
```

The image is pinned separately, by digest in the deployment's `colors.yml`.
A change to `src/colors/` needs a new image; a change to the package side
needs a new pin; a change to the CRD needs both.

For local cross-boundary development use `REDIS_OPERATOR_LIB_ROOT`,
`GREEN_LIB_ROOT`, `REDIS_LIB_ROOT` or `COLORS_COMPUTE_LIB_ROOT` rather than
editing a SHA in `deps.edn`. The deps.edn pins are green 215e298, redis
ec260f5 (`:deps/root green`) and colors-compute 7e1c234 (the reviewed
repair for interrupted operations).

`bb golden` protects the rendered manifests (grace period, security context,
pull secret, RBAC) and the custom resource. Read every golden diff; never run
`bb golden:accept` merely to make the check pass.

## Git

Work on the current branch. Do not commit or push unless explicitly asked.
