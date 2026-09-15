# Redis operator

A Green controller running in Kubernetes that provisions Redis on a separate
DigitalOcean Droplet. It imports the existing Redis package's Clojure workflow;
Redis does not run in the controller Pod.

## Contract

Each namespaced `RedisDeployment` owns one profile. `spec.config` contains the
Redis package's DigitalOcean/R2 configuration. The operator fixes the workdir to
`/data/work`, provider to DigitalOcean, backend to R2, and keeps package
`compute-prevent-destroy` enabled while converging. Do not pass secrets in the CR.

`state: running` is the only supported state in this release. `suspend: true`
pauses new reconciliation. Changes to configuration, periodic checks, and a new
`colors.getcolors.ai/reconcile-request` annotation value request reconciliation.

`deletionPolicy` defaults to `Retain`. `Destroy` explicitly enables the package's
deletion workflow. When a Droplet was externally deleted, destruction skips the
unreachable application cleanup and still removes owned compute shared resources.
R2 state and backup buckets are externally managed and remain after deletion.

The state backend endpoint, bucket, and resolved profile identify the deployment.
They are immutable. Two resources must not declare conflicting desired states for
the same profile, even though their workflow executions are serialized.

## Observation and recovery

The adapter reads owned compute state, then queries the DigitalOcean API for that
exact Droplet ID and checks its recorded name. Only a confirmed HTTP 404 proves
provider absence. Authentication, provider, ownership, and state-read errors are
retryable failures, not permission to recreate infrastructure. Partial initial
compute state can resume through the library's ownership protocol.

A healthy observation checks provider region, size, image, recorded public IP,
and authenticated Redis PING over SSH. A successful configuration hash and
Droplet ID are persisted on the controller volume. Healthy periodic checks do not
rerun the Redis create workflow, whose smoke test intentionally restarts Redis.
Unhealthy Redis or changed configuration runs the package workflow again.
This is not an exhaustive audit of every Redis setting or provider firewall rule.

OpenTofu refreshes the owned resource state before planning: a missing Droplet
is recreated. The library refuses destructive replacement plans. Recreating a
Droplet regenerates its Redis password and initializes an empty Redis data volume.
Existing backup objects survive, but automatic restore is **not implemented**.
A successful self-healing test proves service recovery, not data recovery.

## Run and test

```bash
bb test
bb controller kind-colors-dev colors-redis
# Inside Kubernetes:
bb controller --in-cluster colors-redis
```

The controller requires `kubectl`, OpenTofu, Ansible, SSH, Redis CLI and AWS CLI.
The Dockerfile installs this toolchain. Run `bb test` on the build host before
building; the image build does not execute Babashka under QEMU. The controller
resolves its pinned source dependencies on first startup, requiring outbound
access to GitHub and the Maven repositories. Git and Java are included for that
resolution. Validate the resulting image on a native worker of its target
architecture.
Build and publish an image for your worker architecture, then deploy by image
digest. The AWS CLI installer resolves at build time; the image digest pins the
resulting toolchain. `manifests/crd.yml` registers the resource schema. The installation renderer emits
namespace, CRD, namespaced RBAC, PVC, and controller Deployment:

```bash
bb -m colors.install colors-redis registry.example/redis-operator@sha256:<digest>
# Once redis-credentials and registry-credentials Secrets exist in colors-redis:
bb install <kube-context> colors-redis registry.example/redis-operator@sha256:<digest>
```

`bb install` invokes a Green workflow that applies the installation and waits for
the controller Deployment. Create RedisDeployment instances only after the CRD
reports Established. Operator installation never deletes existing resources. The live deployment's
`redis-doks/scripts/install.py` uses the renderer and `kubectl` directly while
handling private credential and registry Secret setup. `bb install` is the
separate reusable Green installation workflow for callers that prepared those
Secrets themselves.

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
with a remote coordination journal.

This is a development controller. Neither Kubernetes Recreate nor a PVC fences
a disconnected old controller. Do not force-delete an uncertain controller or
steal its journal lock. Confirm its processes stopped before resuming. Graceful
shutdown waits for active workflows; configure enough termination time. Human
or CI package executions must not overlap operator management of the same profile.

The deployment configuration, live tests, and operational handoff live in the
separate `getcolors/redis-doks` repository.
