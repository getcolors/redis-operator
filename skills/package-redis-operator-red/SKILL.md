---
name: package-redis-operator-red
description: Install and operate the RedisDeployment controller in an existing Kubernetes cluster with Red; it provisions one Redis Droplet on DigitalOcean from a custom resource.
---

# Redis operator Package Skill

Use the bundled `red` launcher against a non-secret `colors.yml`, with
`KUBECONFIG` pointing at the target cluster and `kube-context` naming its
context.

```sh
./red build              # render manifests and the RedisDeployment under .colors/
./red create --dry-run   # walk the create graph without contacting the cluster
./red create             # install the controller, apply the resource, wait for Ready
./red check              # Ready at the current generation and an authenticated PING
./red rehearse           # suspend, run the backup rehearsal in the controller, resume
./red restart            # restart the controller and prove the Droplet survives
COLORS_PAR_DRILL_DELETE_OWNED_DROPLET=true ./red drill   # delete the Droplet, prove recovery
COLORS_PAR_COMPUTE_PREVENT_DESTROY=false ./red delete    # Destroy through the finalizer
```

Read `references/configuration.md` before editing desired state. Put the five
credentials only in the ignored `.envrc.private` as `COLORS_PAR_*`; `create`
copies them into the `redis-credentials` Secret. Never export
`COLORS_PAR_PROFILE`, edit `.colors/`, weaken `compute-prevent-destroy`, reset
`spec.suspend` on a resource by hand, or run a real `create`, `drill`, or
`delete` without authorization. `drill` deletes a live Droplet; `rehearse`
leaves the resource suspended when it cannot prove the remote workflow
stopped, and says so.
