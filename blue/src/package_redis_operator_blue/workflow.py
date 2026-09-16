"""Package lifecycle graph. Build and dry-run need no credentials."""

import ipaddress, os, re
from blue import dry_run, progress
from blue.cli import read_pars
from blue.lifecycle import preflight
from blue.workflow import workflow as make_workflow, failed
from package_redis_blue.validate import state_errors as redis_errors
from package_redis_blue.workflow import DEFAULTS as REDIS_DEFAULTS
from . import tools as t, operator

DEFAULTS = {
    "namespace": "colors-redis",
    "reconcile-interval": "60s",
    "deletion-policy": "Retain",
    "compute-prevent-destroy": True,
    "provider-compute": "digitalocean",
    "provider-backend": "r2",
    "workdir": ".colors",
}
EVENTS = ["build", "create", "check", "rehearse", "drill", "restart", "delete"]


def state_errors(opts):
    required = [
        "profile",
        "kube-context",
        "namespace",
        "resource-name",
        "image",
        "reconcile-interval",
        "deletion-policy",
        "compute-prevent-destroy",
        *t.CONFIG_KEYS,
    ]
    errors = [
        k + " is required" for k in required if opts.get(k) is None or opts[k] == ""
    ]
    patterns = {
        "profile": r"[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}",
        "kube-context": r"[A-Za-z0-9][A-Za-z0-9._:@/-]{0,252}",
        "namespace": r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?",
        "resource-name": r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?",
        "image": r"[^\s]+@sha256:[a-f0-9]{64}",
        "image-pull-secret": r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?",
        "reconcile-interval": r"[1-9][0-9]*(ms|s|m|h)",
        "doks-cluster-id": r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
    }
    for k, p in patterns.items():
        if k in opts and not re.fullmatch(p, str(opts[k])):
            errors.append(k + " has invalid format")
    for k, allowed in [
        ("deletion-policy", ["Retain", "Destroy"]),
        ("provider-compute", ["digitalocean"]),
        ("provider-backend", ["r2"]),
    ]:
        if opts.get(k) not in allowed:
            errors.append(k + " must be " + " or ".join(allowed))
    if type(opts.get("compute-prevent-destroy")) is not bool:
        errors.append("compute-prevent-destroy must be true or false")
    sources = opts.get("digitalocean-ssh-sources")
    if not isinstance(sources, list) or not sources:
        errors.append("digitalocean-ssh-sources must list at least one public IPv4 /32")
    else:
        for source in sources:
            try:
                address = ipaddress.ip_network(source)
                valid = (
                    isinstance(source, str)
                    and source.endswith("/32")
                    and address.version == 4
                    and address.prefixlen == 32
                    and address.network_address.is_global
                    and not address.network_address.is_multicast
                )
            except (ValueError, TypeError):
                valid = False
            if not valid:
                errors.append(
                    "digitalocean-ssh-sources entries must be public IPv4 /32 networks"
                )
    shape = (
        {
            **REDIS_DEFAULTS,
            **t.resource(opts)["spec"]["config"],
            "workdir": "/data/work",
            "redis-storage-managed": False,
            "compute-prevent-destroy": True,
            "provider-compute": "digitalocean",
            "provider-backend": "r2",
        }
        if all(
            k in opts
            for k in (
                "profile",
                "namespace",
                "resource-name",
                "deletion-policy",
                "reconcile-interval",
            )
        )
        else None
    )
    if shape:
        errors.extend(e for e in redis_errors(shape) if not e.endswith("is required"))
    return errors


async def start_step(opts, env=None):
    opts = {**opts, "resource-name": opts.get("resource-name") or opts.get("profile")}
    return await preflight(
        opts,
        defaults=DEFAULTS,
        overlay=read_pars,
        env=env,
        validators=[
            lambda o, e, c: (
                ["COLORS_PAR_PROFILE is set; profile must come from colors.yml only"]
                if e.get("COLORS_PAR_PROFILE")
                else []
            ),
            lambda o, e, c: state_errors(o),
            lambda o, e, c: (
                [k + " is required" for k in t.CREDENTIALS if not e.get(k, "").strip()]
                if c["event"] == "create" and c["real"]
                else []
            ),
            lambda o, e, c: (
                [
                    "drill deletes the owned Redis Droplet; set COLORS_PAR_DRILL_DELETE_OWNED_DROPLET=true"
                ]
                if c["event"] == "drill"
                and e.get("COLORS_PAR_DRILL_DELETE_OWNED_DROPLET") != "true"
                else []
            ),
            lambda o, e, c: (
                ["COLORS_PAR_DO_TOKEN is required"]
                if c["event"] == "drill"
                and c["real"]
                and not e.get("COLORS_PAR_DO_TOKEN")
                else []
            ),
            lambda o, e, c: (
                [
                    "compute destruction is protected; set COLORS_PAR_COMPUTE_PREVENT_DESTROY=false to delete"
                ]
                if c["event"] == "delete"
                and o.get("compute-prevent-destroy") is not False
                else []
            ),
        ],
    )


async def render_step(opts):
    t.render(opts)
    return {**opts, "blue/exit": 0}


async def create_step(opts):
    await t.apply(opts, t.manifests(opts)["items"][0])
    await t.apply(
        opts,
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": "redis-credentials", "namespace": opts["namespace"]},
            "type": "Opaque",
            "stringData": {k: os.environ[k] for k in t.CREDENTIALS},
        },
        quiet=True,
    )
    if opts.get("image-pull-secret"):

        async def present():
            try:
                return await t.kubectl(
                    opts,
                    [
                        "get",
                        "secret",
                        opts["image-pull-secret"],
                        "-n",
                        opts["namespace"],
                        "-o",
                        "name",
                    ],
                    not_found=None,
                )
            except Exception as exc:
                raise t.Fatal(str(exc)) from exc

        await t.wait_for("pull secret", present, 120)
    await t.apply(opts, t.manifests(opts))
    await t.kubectl(
        opts,
        ["wait", "--for=condition=Established", "crd/" + t.RESOURCE, "--timeout=60s"],
        request_timeout=False,
        timeout=90,
    )
    await t.kubectl(
        opts,
        [
            "rollout",
            "status",
            "deployment/" + t.CONTROLLER,
            "-n",
            opts["namespace"],
            "--timeout=600s",
        ],
        request_timeout=False,
        timeout=630,
    )
    current = await t.get_resource(opts)
    if current:
        t.active(current)
    await t.apply(opts, t.resource(opts))
    await t.wait_ready(opts, 2700, True)
    return {**opts, "blue/exit": 0}


async def delete_step(opts):
    current = await t.get_resource(opts)
    if current:
        if t.suspended(current):
            raise t.Fatal("Resource suspended; verify no workflow runs before delete")
        if current["spec"].get("deletionPolicy") != "Destroy":
            await t.patch(
                opts,
                current,
                [{"op": "add", "path": "/spec/deletionPolicy", "value": "Destroy"}],
            )
        if not t.deleting(current):
            await t.kubectl(
                opts,
                [
                    "delete",
                    t.RESOURCE,
                    opts["resource-name"],
                    "-n",
                    opts["namespace"],
                    "--wait=false",
                    "--ignore-not-found",
                ],
            )

        async def gone():
            return await t.get_resource(opts) is None

        await t.wait_for("RedisDeployment finalizer", gone, 1800, 15)
    await t.kubectl(
        opts,
        [
            "delete",
            "namespace",
            opts["namespace"],
            "--ignore-not-found",
            "--timeout=900s",
        ],
        request_timeout=False,
        timeout=930,
    )
    remaining = await t.kubectl(
        opts,
        ["get", t.RESOURCE, "-A", "-o", "json"],
        parse=True,
        not_found={"items": []},
    )
    if not remaining["items"]:
        await t.kubectl(opts, ["delete", "crd", t.RESOURCE, "--ignore-not-found"])
    return {**opts, "blue/exit": 0}


async def operation_step(opts):
    await getattr(operator, opts["blue/event"])(opts)
    return {**opts, "blue/exit": 0}


async def quiet(fn, opts):
    try:
        return await fn(opts)
    except Exception as exc:
        return {**opts, "blue/exit": 1, "blue/err": str(exc)}


def wire_fn(step, opts):
    event = opts["blue/event"]
    if step == "redis-operator/start":
        return [
            start_step,
            "redis-operator/render"
            if event in ("build", "create")
            else "redis-operator/" + event,
        ]
    if step == "redis-operator/render":
        return [render_step, *(["redis-operator/create"] if event == "create" else [])]
    fn = (
        create_step
        if event == "create"
        else delete_step
        if event == "delete"
        else operation_step
    )
    return [lambda o: quiet(fn, o)]


WORKFLOW = make_workflow(
    start="redis-operator/start",
    wire_fn=wire_fn,
    next_fn=lambda s, successors, o: (
        [] if failed(o) else [(n, o) for n in (successors or [])]
    ),
)
WORKFLOW = progress.advise(WORKFLOW)
WORKFLOW = dry_run.advise(
    WORKFLOW, ["redis-operator/" + e for e in EVENTS if e != "build"]
)
