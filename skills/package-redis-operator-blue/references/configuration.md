# Configuration

`colors.yml` is a flat, kebab-case, non-secret map. The in-repo example is the
root `colors.yml`; the worked deployment is `redis-operator-doks`.

## Environment

- `KUBECONFIG` — the kubeconfig kubectl reads. The deployment's `.envrc`
  exports it, pointing at the `doks` deployment's rendered
  `.colors/<profile>/kubeconfig`. The package never passes `--kubeconfig`.
- `COLORS_PAR_DO_TOKEN`, `COLORS_PAR_R2_ACCESS_KEY_ID`,
  `COLORS_PAR_R2_SECRET_ACCESS_KEY`, `COLORS_PAR_REDIS_BACKUP_R2_ACCESS_KEY_ID`,
  `COLORS_PAR_REDIS_BACKUP_R2_SECRET_ACCESS_KEY` — required by a real `create`,
  which copies exactly these five into the `redis-credentials` Secret on
  `kubectl apply -f -` stdin. `drill` needs `COLORS_PAR_DO_TOKEN` for the
  DigitalOcean API. Nothing parses `.envrc.private`; the values come from the
  process environment.
- `COLORS_PAR_COMPUTE_PREVENT_DESTROY=false` — lifts the delete guard for one run.
- `COLORS_PAR_DRILL_DELETE_OWNED_DROPLET=true` — exact acknowledgement that
  `drill` may delete the owned Droplet; anything else exits 2.
- `COLORS_PAR_PROFILE` must never be set; the package refuses to run.

## Keys

| Key | Default | Meaning |
|---|---|---|
| `profile` | required | Names the work directory, the custom resource (unless `resource-name` is set) and the Redis package profile, which keys its R2 state and names its Droplet. `[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}`; immutable once created. |
| `kube-context` | required | The kubectl context every call passes as `--context`. |
| `namespace` | `colors-redis` | Namespace for the controller, the Secret and the resource. DNS label. |
| `resource-name` | `profile` | Name of the RedisDeployment. DNS label. |
| `image` | required | Controller image, `<repository>@sha256:<64 hex>`. Built by `scripts/image.sh`. |
| `image-pull-secret` | absent | Name of an existing pull Secret in the namespace (the one DOKS registry integration injects). Absent renders no `imagePullSecrets`. `create` waits up to 120 s for it. |
| `reconcile-interval` | `60s` | `spec.reconcileInterval`; positive duration in `ms`, `s`, `m` or `h`. |
| `deletion-policy` | `Retain` | `spec.deletionPolicy`: `Retain` or `Destroy`. `delete` patches it to `Destroy` regardless, because reaching `delete` means the guard was lifted. |
| `compute-prevent-destroy` | `true` | Committed guard; `delete` exits 2 unless it is false. |
| `doks-cluster-id` | absent | Optional DOKS cluster UUID. When set, `drill` fetches the node pools' Droplet IDs and refuses any of them; when absent, workers are excluded by their `k8s:` tags and by name only. |
| `provider-compute` | `digitalocean` | Fixed by the CRD. |
| `provider-backend` | `r2` | Fixed by the CRD. |
| `redis-image` | required | Redis image pinned `tag@sha256:...`. |
| `redis-port` | required | 1–65535. |
| `redis-backup-r2-bucket` | required | Bucket for RDB backup sets. |
| `redis-backup-r2-endpoint` | required | `https://` endpoint of that bucket. |
| `redis-backup-r2-region` | required | Usually `auto`. |
| `redis-backup-oncalendar` | required | systemd `OnCalendar` expression for the backup timer. |
| `redis-backup-retention-days` | required | Positive integer. |
| `redis-backup-max-age-hours` | required | Positive integer; the monitor's freshness bound. |
| `digitalocean-region` | required | Droplet region slug. |
| `digitalocean-size` | required | Droplet size slug. |
| `digitalocean-image` | required | Droplet image slug. |
| `digitalocean-ssh-sources` | required | Non-empty list of public IPv4 `/32` networks admitted to SSH: every DOKS worker (the controller's egress) and every developer. Private, loopback, link-local, shared, documentation and reserved ranges are rejected. |
| `r2-bucket` | required | Bucket for the Redis package's compute state. Immutable. |
| `r2-endpoint` | required | `https://` endpoint of that bucket. Immutable. |

The keys from `provider-compute` down form `spec.config` of the custom
resource, with `profile` added. They are validated twice: by this package
(shape, providers, SSH sources) and by the pinned Redis package's own
validators, so the controller will accept what `build` renders. All errors are
reported together with exit 2.

## Rendered files

`build` writes `.colors/<profile>/operator/manifests.json` (a `v1/List` of
Namespace, CRD, ServiceAccount, Role, RoleBinding, PVC and the controller
Deployment) and `.colors/<profile>/operator/redis-deployment.json` (the
RedisDeployment). Neither contains a secret; the Secret is applied directly by
`create` and never rendered. The operational verbs write evidence to
`.colors/<profile>/evidence/backup-rehearsal.json`, `self-healing.json` and
`controller-restart.json`.
