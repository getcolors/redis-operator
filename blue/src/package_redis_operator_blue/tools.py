"""Manifest rendering and bounded transports used by the package."""

from __future__ import annotations
import asyncio, copy, json, os, re, signal, time, urllib.request, urllib.error
from pathlib import Path
from datetime import datetime, timezone
import yaml

CONTROLLER = "colors-redis-operator"
RESOURCE = "redisdeployments.colors.getcolors.ai"
CREDENTIALS = [
    "COLORS_PAR_DO_TOKEN",
    "COLORS_PAR_R2_ACCESS_KEY_ID",
    "COLORS_PAR_R2_SECRET_ACCESS_KEY",
    "COLORS_PAR_REDIS_BACKUP_R2_ACCESS_KEY_ID",
    "COLORS_PAR_REDIS_BACKUP_R2_SECRET_ACCESS_KEY",
]
CONFIG_KEYS = "provider-compute provider-backend redis-image redis-port redis-backup-r2-bucket redis-backup-r2-endpoint redis-backup-r2-region redis-backup-oncalendar redis-backup-retention-days redis-backup-max-age-hours digitalocean-region digitalocean-size digitalocean-image digitalocean-ssh-sources r2-bucket r2-endpoint".split()


class Fatal(RuntimeError):
    pass


def now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def ready(cr):
    m, s = cr.get("metadata", {}), cr.get("status", {})
    return (
        m.get("generation") is not None
        and m.get("generation") == s.get("observedGeneration")
        and s.get("phase") == "Ready"
        and any(
            c.get("type") == "Ready" and c.get("status") == "True"
            for c in s.get("conditions", [])
        )
    )


def suspended(cr):
    return cr.get("spec", {}).get("suspend") is True


def deleting(cr):
    return cr.get("metadata", {}).get("deletionTimestamp") is not None


def acknowledged(cr):
    return (
        suspended(cr)
        and not deleting(cr)
        and cr.get("status", {}).get("phase") == "Suspended"
        and cr["metadata"]["generation"] == cr["status"].get("observedGeneration")
    )


def active(cr):
    if not cr or suspended(cr) or deleting(cr):
        raise Fatal("RedisDeployment is absent, suspended or being deleted")
    return cr


def manifests(opts):
    directory = Path(__file__).parent / "resources"
    document = json.loads(
        (directory / "manifests.json")
        .read_text()
        .replace("{{namespace}}", opts["namespace"])
        .replace("{{image}}", opts["image"])
    )
    document["items"].insert(1, yaml.safe_load((directory / "crd.yml").read_text()))
    if opts.get("image-pull-secret"):
        document["items"][-1]["spec"]["template"]["spec"]["imagePullSecrets"] = [
            {"name": opts["image-pull-secret"]}
        ]
    return document


def resource(opts):
    return {
        "apiVersion": "colors.getcolors.ai/v1alpha1",
        "kind": "RedisDeployment",
        "metadata": {"name": opts["resource-name"], "namespace": opts["namespace"]},
        "spec": {
            "state": "running",
            "deletionPolicy": opts["deletion-policy"],
            "reconcileInterval": opts["reconcile-interval"],
            "config": {
                "profile": opts["profile"],
                **{k: opts[k] for k in CONFIG_KEYS if k in opts},
            },
        },
    }


def write_file(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(_pretty(json.loads(json.dumps(data, sort_keys=True))) + "\n")
    temp.replace(path)
    return str(path)


def render(opts):
    directory = Path(opts["workdir"]) / opts["profile"] / "operator"
    return [
        write_file(directory / "manifests.json", manifests(opts)),
        write_file(directory / "redis-deployment.json", resource(opts)),
    ]


def evidence(opts, name, data):
    return write_file(
        Path(opts["workdir"]) / opts["profile"] / "evidence" / (name + ".json"), data
    )


async def command(argv, input="", timeout=120):
    proc = await asyncio.create_subprocess_exec(
        *map(str, argv),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    try:
        out, err = await asyncio.wait_for(proc.communicate(input.encode()), timeout)
    except asyncio.TimeoutError:
        os.killpg(proc.pid, signal.SIGKILL)
        await proc.communicate()
        return {"exit": 124, "out": "", "err": f"command timed out after {timeout}s"}
    return {"exit": proc.returncode, "out": out.decode(), "err": err.decode()}


_MISSING = object()


async def kubectl(
    opts,
    args,
    *,
    input="",
    timeout=120,
    quiet=False,
    parse=False,
    request_timeout=True,
    not_found=_MISSING,
):
    result = await command(
        [
            "kubectl",
            "--context",
            opts["kube-context"],
            *(["--request-timeout=30s"] if request_timeout else []),
            *args,
        ],
        input,
        timeout,
    )
    if result["exit"] == 0:
        return (
            (json.loads(result["out"]) if result["out"].strip() else None)
            if parse
            else result["out"]
        )
    if not_found is not _MISSING and re.search(r"\bnot ?found\b", result["err"], re.I):
        return not_found
    raise RuntimeError(
        f"kubectl {args[0]} failed (exit {result['exit']}): "
        + ("output suppressed" if quiet else result["err"].strip())
    )


async def apply(opts, data, quiet=False):
    return await kubectl(
        opts, ["apply", "-f", "-"], input=json.dumps(data), quiet=quiet
    )


async def get_resource(opts):
    return await kubectl(
        opts,
        [
            "get",
            RESOURCE,
            opts["resource-name"],
            "-n",
            opts["namespace"],
            "--ignore-not-found",
            "-o",
            "json",
        ],
        parse=True,
    )


async def wait_for(label, callback, timeout=180, interval=5):
    end = time.monotonic() + timeout
    error = None
    while True:
        try:
            result = await callback()
            if result:
                return result
        except Fatal:
            raise
        except Exception as exc:
            error = exc
        if time.monotonic() >= end:
            raise RuntimeError(
                f"Timed out waiting for {label}" + (f": {error}" if error else "")
            )
        await asyncio.sleep(interval)


async def wait_ready(opts, timeout=180, transient_failure=False):
    async def poll():
        cr = active(await get_resource(opts))
        phase = cr.get("status", {}).get("phase")
        if (
            phase in ("Invalid", "Blocked")
            or phase == "Failed"
            and not transient_failure
        ):
            raise Fatal(f"RedisDeployment reports {phase}")
        return cr if ready(cr) else None

    return await wait_for("Ready at current generation", poll, timeout)


def same_desired(a, b):
    return a.get("spec") == b.get("spec") and all(
        a.get("metadata", {}).get(k) == b.get("metadata", {}).get(k)
        for k in ("uid", "deletionTimestamp")
    )


async def patch(opts, current, changes):
    original = copy.deepcopy(current)
    for attempt in range(5):
        version = current["metadata"]["resourceVersion"]
        try:
            return await kubectl(
                opts,
                [
                    "patch",
                    RESOURCE,
                    opts["resource-name"],
                    "-n",
                    opts["namespace"],
                    "--type=json",
                    "-o",
                    "json",
                    "-p",
                    json.dumps(
                        [
                            {
                                "op": "test",
                                "path": "/metadata/resourceVersion",
                                "value": version,
                            },
                            *changes,
                        ]
                    ),
                ],
                parse=True,
            )
        except RuntimeError as exc:
            if (
                "the server rejected our request due to an error in our request"
                not in str(exc)
            ):
                raise
            fresh = await get_resource(opts)
            if not fresh or not same_desired(original, fresh):
                raise Fatal(
                    "RedisDeployment changed concurrently; patch not applied"
                ) from exc
            if fresh["metadata"]["resourceVersion"] == version or attempt == 4:
                raise
            current = fresh
            await asyncio.sleep(2)


async def controller_pod(opts):
    result = await kubectl(
        opts,
        [
            "get",
            "pods",
            "-n",
            opts["namespace"],
            "-l",
            "app=" + CONTROLLER,
            "-o",
            "json",
        ],
        parse=True,
    )
    pods = [p for p in result.get("items", []) if not deleting(p)]
    return (
        pods[0]
        if len(pods) == 1 and pods[0].get("status", {}).get("phase") == "Running"
        else None
    )


async def controller_up(opts):
    async def poll():
        pod = await controller_pod(opts)
        if not pod:
            return None
        start = pod.get("status", {}).get("startTime")
        if not start:
            return None
        logs = await kubectl(
            opts,
            [
                "logs",
                "pod/" + pod["metadata"]["name"],
                "-n",
                opts["namespace"],
                "-c",
                "controller",
                "--since-time=" + start,
            ],
            request_timeout=False,
            timeout=60,
        )
        return pod if "RedisDeployment controller running" in logs else None

    return await wait_for("controller startup", poll, 600)


async def probe(opts, operation, *args):
    await controller_up(opts)
    # Every controller image supplies the same executable regardless of runtime.
    output = await kubectl(
        opts,
        [
            "exec",
            "deployment/" + CONTROLLER,
            "-n",
            opts["namespace"],
            "--",
            "sh",
            "-c",
            'if [ -d /app/blue ]; then exec /app/blue/.venv/bin/python -m package_redis_operator_blue.probe "$@"; elif [ -d /app/red ]; then exec bun /app/red/src/probe.ts "$@"; else exec bb -m colors.probe "$@"; fi',
            "probe",
            opts["resource-name"],
            opts["namespace"],
            operation,
            *args,
        ],
        request_timeout=False,
        timeout=7800 if operation == "rehearse" else 180,
    )
    return json.loads(output)


async def healthy_probe(opts):
    value = await probe(opts, "health")
    if value.get("healthy") is not True:
        raise Fatal("Redis must be healthy before the operation")
    return value


async def digitalocean(token, method, path):
    def request():
        req = urllib.request.Request(
            "https://api.digitalocean.com/v2/" + path,
            method=method.upper(),
            headers={"Authorization": "Bearer " + token, "Accept": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=45) as response:
                body = response.read()
                decoded = json.loads(body) if body else None
                if method.lower() == "get" and (
                    not isinstance(decoded, dict) or not decoded
                ):
                    raise RuntimeError(
                        "DigitalOcean GET returned an invalid object; absence requires HTTP 404"
                    )
                return decoded
        except urllib.error.HTTPError as exc:
            if exc.code == 404 and method.lower() == "get":
                return None
            raise RuntimeError(
                f"DigitalOcean {method.upper()} failed: HTTP {exc.code}"
            ) from None

    return await asyncio.to_thread(request)


def owned_droplet(droplet, observed, profile, workers):
    provider = str(observed.get("providerId", ""))
    if (
        not re.fullmatch(r"[0-9]+", provider)
        or not droplet
        or provider != str(droplet.get("id"))
    ):
        raise Fatal("Recorded provider ID does not match live Droplet")
    if any(
        x != profile
        for x in (observed.get("profile"), observed.get("name"), droplet.get("name"))
    ):
        raise Fatal("Droplet does not belong to exact deployment profile")
    if provider in set(map(str, workers)) or any(
        str(t).startswith("k8s:") for t in droplet.get("tags", [])
    ):
        raise Fatal("Refusing a Kubernetes worker")
    if observed.get("ip") not in {
        a["ip_address"]
        for a in droplet.get("networks", {}).get("v4", [])
        if a.get("type") == "public"
    }:
        raise Fatal("Recorded address differs from live Droplet")
    return True


def _pretty(value, indent: int = 0) -> str:
    """Cheshire's pretty JSON, byte for byte, in insertion order."""
    if isinstance(value, (list, tuple)):
        if not value:
            return "[ ]"
        return "[ " + ", ".join(_pretty(item, indent) for item in value) + " ]"
    if isinstance(value, dict):
        if not value:
            return "{ }"
        pad = " " * (indent + 2)
        body = ",\n".join(
            f"{pad}{json.dumps(str(k), ensure_ascii=False)} : {_pretty(v, indent + 2)}"
            for k, v in value.items()
        )
        return "{\n" + body + "\n" + " " * indent + "}"
    return json.dumps(value, ensure_ascii=False)
