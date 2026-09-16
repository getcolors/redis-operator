# Redis operator

A Green-only Package Skill in two halves that share one repository and one
pin:

- **The controller image.** A Green Kubernetes controller that reconciles
  `RedisDeployment` custom resources by running the Redis package's Clojure
  workflow. It provisions one Redis 7.2 Droplet on DigitalOcean per resource;
  Redis does not run in the controller Pod. `bb controller --in-cluster` is
  the image entry point.
- **The package.** `skills/package-redis-operator-green` installs that
  controller into an existing cluster from a non-secret `colors.yml`, applies
  one resource, and carries the operational verbs the deployment needs:
  `check`, `rehearse`, `drill`, `restart`, `delete`.

```sh
./green build
./green create --dry-run
./green create
./green check
./green rehearse
./green restart
COLORS_PAR_DRILL_DELETE_OWNED_DROPLET=true ./green drill
COLORS_PAR_COMPUTE_PREVENT_DESTROY=false ./green delete
```

Install with `npx skills add getcolors/redis-operator`, then copy
`.agents/skills/package-redis-operator-green/green` to the deployment root.
Credentials are `COLORS_PAR_*` exports in `.envrc.private`; never set
`COLORS_PAR_PROFILE`. See
`skills/package-redis-operator-green/references/configuration.md` for every key.
The worked deployment is `getcolors/redis-operator-doks`, which runs against a
DOKS cluster the `doks` package created; its `.envrc` exports `KUBECONFIG`
pointing at that deployment's rendered kubeconfig, and `kube-context` in
`colors.yml` names the context inside it.

## Contract

Each namespaced `RedisDeployment` owns one profile. `spec.config` contains the
Redis package's DigitalOcean/R2 configuration. The operator fixes the workdir to
`/data/work`, provider to DigitalOcean, backend to R2, and keeps package
`compute-prevent-destroy` enabled while converging. Do not pass secrets in the CR.

`state: running` is the only supported state in this release. `suspend: true`
pauses new reconciliation. Changes to configuration, periodic checks, and a new
`colors.getcolors.ai/reconcile-request` annotation value request reconciliation.
The package never sets or resets `spec.suspend` on apply; `create` refuses a
suspended resource, and `rehearse` is the only verb that suspends and resumes.

`deletionPolicy` defaults to `Retain`. `Destroy` explicitly enables the package's
deletion workflow. When a Droplet was externally deleted, destruction skips the
unreachable application cleanup and still removes owned compute shared resources.
A deployment whose compute state is `partial` (an interrupted first create)
cannot be destroyed through the controller: compute inspection refuses it, and
the only exit is Retain, which orphans whatever the partial state declares.
Finish or repair such a create before selecting Destroy. R2 state and backup
buckets are externally managed and remain after deletion.

The state backend endpoint, bucket, and resolved profile identify the deployment.
They are immutable. Two resources must not declare conflicting desired states for
the same profile, even though their workflow executions are serialized.

## Observation and recovery

The adapter reads owned compute state, then queries the DigitalOcean API for that
exact Droplet ID and checks its recorded name. Only a confirmed HTTP 404 proves
provider absence — and only relative to the token in use: a Droplet in another
team also answers 404, so rotating the Secret to a token from a different team
would make the operator recreate the Droplet there while the original keeps
running. Rotate tokens within one team. Authentication, provider, ownership,
and state-read errors are retryable failures, not permission to recreate
infrastructure. Partial initial compute state can resume through the library's
ownership protocol on the next create; it cannot be destroyed (see above).

A healthy observation checks provider region, size, image, recorded public IP,
and authenticated Redis PING over SSH. The image and size checks compare slugs
as DigitalOcean reports them: a retired base image (slug nulled by the provider)
or an external resize makes every observation mismatch, and each reconcile
interval then reruns the create workflow, whose smoke test restarts Redis.
Correct `spec.config` to the live values rather than leaving that loop running.
A successful configuration hash and Droplet ID are persisted on the controller
volume. Healthy periodic checks do not rerun the Redis create workflow.
Unhealthy Redis or changed configuration runs the package workflow again.
This is not an exhaustive audit of every Redis setting or provider firewall rule.

OpenTofu refreshes the owned resource state before planning: a missing Droplet
is recreated. The library refuses destructive replacement plans. Recreating a
Droplet regenerates its Redis password and initializes an empty Redis data volume.
Existing backup objects survive, but automatic restore is **not implemented**.
A successful self-healing drill proves service recovery, not data recovery.

The adapter prints one line per observe, converge and delete outcome on the
controller's stdout (`kubectl logs`): the profile, the decision and its reason,
and the provider ID. It never prints secrets, the opts map or raw workflow
output.

## Verbs

| Verb | Side effects | Guard |
|---|---|---|
| `build` | Writes `.colors/<profile>/operator/manifests.json` and `redis-deployment.json`. No cluster contact, no credentials. | — |
| `create` | Applies the Namespace, the `redis-credentials` Secret (stdin), waits for the pull Secret if named, applies the manifests, waits for the CRD to be Established and the controller to roll out, applies the resource, waits up to 45 min for `Ready` at the current generation. `--dry-run` skips every side effect. | Five credentials present; refuses (exit 1) a suspended or deleting resource. |
| `check` | Reads the resource and runs the health probe in the controller pod. | Exit 1 unless Ready at the current generation, unsuspended, not deleting, and PING answers. |
| `rehearse` | Patches `spec.suspend=true` (resourceVersion-tested), waits for the acknowledged `Suspended` phase, runs the package's backup rehearsal through the probe, resumes, waits for Ready. Evidence: `.colors/<profile>/evidence/backup-rehearsal.json`. | Leaves the resource suspended and exits non-zero whenever the remote outcome is uncertain, the resource changed, or the re-read failed; prints "resource left suspended; verify no workflow runs before resuming". |
| `drill` | Proves ownership (ID, name, profile, no DOKS worker ID, no `k8s:` tag, recorded IP), round-trips a marker, deletes exactly that Droplet, waits up to 40 min for a different provider ID, Ready at unchanged UID and generation, old Droplet 404, and a fresh authenticated write. Evidence: `self-healing.json`. | `COLORS_PAR_DRILL_DELETE_OWNED_DROPLET=true` exactly, plus `COLORS_PAR_DO_TOKEN`; exit 2 otherwise. |
| `restart` | Scales the controller to 0, waits for the old pod to stop (never forces), scales to 1, waits for `status.lastReconcileTime` to advance past the restart, then verifies same UID, generation, provider ID, health and convergence record. Evidence: `controller-restart.json`. | Exactly one replica. |
| `delete` | Patches `deletionPolicy=Destroy`, deletes the resource, waits up to 30 min for the finalizer, deletes the Namespace (15 min), deletes the CRD only if no RedisDeployment remains in any namespace. The controller is never removed before the finalizer completes. | `compute-prevent-destroy` must be false (`COLORS_PAR_COMPUTE_PREVENT_DESTROY=false`); exit 2 otherwise, `--dry-run` included. Refuses a suspended resource. |

## Run and test

```bash
bb test                     # controller adapter, probe, and package suites
bb golden                   # rendered fixture matches test/resources/golden
./scripts/launcher.sh       # a standalone payload copy renders the fixture
bb controller kind-colors-dev colors-redis
# Inside Kubernetes:
bb controller --in-cluster colors-redis
```

The controller requires `kubectl`, OpenTofu, Ansible, SSH, Redis CLI and AWS CLI.
The Dockerfile installs this toolchain. Run `bb test` on the build host before
building; the image build does not execute Babashka under QEMU, and CI proves
`colors.main` and `colors.probe` load from a clean checkout. The controller
resolves its pinned source dependencies on first startup, requiring outbound
access to GitHub and the Maven repositories. Git and Java are included for that
resolution. Validate the resulting image on a native worker of its target
architecture.

## Image

```bash
DOCKER_CONFIG=/path/to/push-config scripts/image.sh registry.digitalocean.com/<registry>
```

The script builds `linux/amd64` from the checked-out commit, pushes
`<registry>/redis-operator:<sha>` with the `org.opencontainers.image.revision`
label, and prints the immutable `@sha256:` digest to pin in the deployment's
`colors.yml` as `image`. The `doks` package's `registry` verb writes the
short-lived push config the script expects in `DOCKER_CONFIG`; the pull side
is the Secret DOKS injects into every namespace, named by `image-pull-secret`.
The AWS CLI archive has no URL-addressable checksum; the image digest pins the
toolchain that resulted.

Inject only these five environment variables from a Kubernetes Secret:

- `COLORS_PAR_DO_TOKEN`
- `COLORS_PAR_R2_ACCESS_KEY_ID`
- `COLORS_PAR_R2_SECRET_ACCESS_KEY`
- `COLORS_PAR_REDIS_BACKUP_R2_ACCESS_KEY_ID`
- `COLORS_PAR_REDIS_BACKUP_R2_SECRET_ACCESS_KEY`

Startup rejects other `COLORS_PAR_*` overrides, ensuring configuration comes
from the custom resource. Generated results contain configuration hashes and
provider IDs, never credentials or workflow opts.

## Execution boundary

Use one controller replica with the Recreate strategy and a persistent volume
mounted at `/data` and `/root/.ssh`. Persisting private SSH keys is essential:
remote infrastructure state does not contain those keys. The controller serializes
whole workflows; compute additionally protects its own infrastructure stages
with a remote coordination journal, and the pinned colors-compute carries the
reviewed repair for an operation interrupted mid-stage.

The controller runs as root because the PVC subPath at `/root/.ssh` is owned
by the user the workflows run as. Its container security context still drops
every capability, forbids privilege escalation and uses the runtime seccomp
profile; nothing in the toolchain is setuid or needs a capability.

This is a development controller. Neither Kubernetes Recreate nor a PVC fences
a disconnected old controller. Do not force-delete an uncertain controller or
steal its journal lock. Confirm its processes stopped before resuming. Graceful
shutdown waits for active workflows: the Deployment's termination grace period
is 10800 s, above the sum of the package's Ansible (7200 s) and OpenTofu plan
(1800 s) caps, so a rolling restart cannot SIGKILL a workflow mid-stage. Human
or CI package executions must not overlap operator management of the same
profile; `rehearse` is the sanctioned way to run the package against a managed
profile, and suspension is a one-shot acknowledgement, not a lease.
