"""Redis adapter. Unreadable ownership never authorizes recreation."""

import hashlib, json, os, re
from pathlib import Path
from datetime import datetime, timezone
from blue.workflow import run, failed
from package_redis_blue import (
    workflow as redis,
    tools as redis_tools,
    compute,
    validate as validation,
)
from colors_compute.inspection import read_deployment
from . import tools as t

CREDENTIAL_VARS = {
    key: key.removeprefix("COLORS_PAR_").lower().replace("_", "-")
    for key in t.CREDENTIALS
}
HEALTH_COMMAND = "cd /opt/redis && docker compose exec -T redis sh -c 'export REDISCLI_AUTH=\"$(sed -n '\"'\"'s/^requirepass //p'\"'\"' /etc/redis/redis.conf)\"; redis-cli --no-auth-warning PING'"


def check_environment(env):
    if any(k.startswith("COLORS_PAR_") and k not in CREDENTIAL_VARS for k in env):
        raise RuntimeError(
            "Unexpected COLORS_PAR environment override; desired configuration must come from resource"
        )
    if any(not env.get(k, "").strip() for k in CREDENTIAL_VARS):
        raise RuntimeError("Required operator credentials are missing")


def options(config):
    return {
        **redis.DEFAULTS,
        **{k: v for k, v in config.items() if k != "blue.kubernetes/resource"},
        **{k: os.environ.get(e) for e, k in CREDENTIAL_VARS.items()},
        "workdir": os.environ.get("COLORS_WORKDIR", "/data/work"),
        "compute-prevent-destroy": True,
        "redis-storage-managed": False,
        "provider-compute": "digitalocean",
        "provider-backend": "r2",
    }


def identity(config):
    return [
        "r2",
        config.get("r2-endpoint"),
        config.get("r2-bucket"),
        config.get("profile"),
    ]


def validate(config):
    errors = list(validation.state_errors(options(config)))
    if not re.fullmatch(
        r"[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}", str(config.get("profile", ""))
    ):
        errors.append("Invalid profile")
    if (
        config.get("blue.kubernetes/resource", {})
        .get("spec", {})
        .get("state", "running")
        != "running"
    ):
        errors.append("This release supports running state only")
    if "compute-prevent-destroy" in config:
        errors.append("Use spec.deletionPolicy")
    return errors


def config_hash(config):
    return hashlib.sha256(
        json.dumps(
            {
                k: v
                for k, v in config.items()
                if k not in ("blue/event", "blue.kubernetes/resource")
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()


def marker_path(config):
    return (
        Path(options(config)["workdir"])
        / config["profile"]
        / "operator-success-blue.json"
    )


def read_marker(config):
    path = marker_path(config)
    return json.loads(path.read_text()) if path.exists() else None


def write_marker(config, node):
    t.write_file(
        marker_path(config),
        {"config-hash": config_hash(config), "provider-id": str(node["provider_id"])},
    )


def mask(text, env):
    for value in sorted(
        {v for k, v in env.items() if k.startswith("COLORS_PAR_") and v.strip()},
        key=len,
        reverse=True,
    ):
        text = text.replace(value, "***")
    return text


def retain_failure(config, result, env=None):
    env = os.environ if env is None else env
    directory = Path(options(config)["workdir"]) / config["profile"] / "failures"
    directory.mkdir(parents=True, exist_ok=True)
    directory.chmod(0o700)
    name = (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        + "-"
        + re.sub(r"[^A-Za-z0-9._-]", "-", str(result.get("blue/step", "unknown")))
        + ".log"
    )
    path = directory / name
    tmp = directory / (name + ".tmp")
    report = mask(
        json.dumps(
            {
                "profile": config["profile"],
                "event": config.get("blue/event", "create"),
                "step": result.get("blue/step"),
                "exit": result.get("blue/exit"),
                "err": result.get("blue/err"),
                "recap": result.get("ansible/recap"),
                "trace": result.get("blue/trace"),
            },
            default=str,
            indent=2,
        ),
        env,
    )
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as stream:
        stream.write(report)
    tmp.replace(path)
    for old in sorted(directory.glob("*.log"), reverse=True)[20:]:
        old.unlink()
    return name


async def inspect(opts):
    return await read_deployment(
        opts, redis_tools.environment(opts), {}, compute.requirements(opts)
    )


async def provider_get(opts, node):
    provider = str(node.get("provider_id", ""))
    if not re.fullmatch(r"[0-9]+", provider):
        raise RuntimeError("Invalid recorded Droplet ID")
    result = await t.digitalocean(opts["do-token"], "get", "droplets/" + provider)
    if result is None:
        return None
    droplet = result["droplet"]
    if str(droplet["id"]) != provider or droplet["name"] != node["name"]:
        raise RuntimeError("Provider identity does not match owned state")
    return droplet


async def remote(node, command):
    return await t.command(
        [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=10",
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
            "-i",
            node["ssh_identity_file"],
            node["user"] + "@" + node["ip"],
            command,
        ],
        timeout=30,
    )


async def service_health(opts, node):
    result = await remote(node, HEALTH_COMMAND)
    return result["exit"] == 0 and result["out"].strip() == "PONG"


def provider_matches(opts, node, droplet):
    return (
        node["name"] == droplet.get("name")
        and opts["digitalocean-region"] == droplet.get("region", {}).get("slug")
        and opts["digitalocean-size"] == droplet.get("size_slug")
        and opts["digitalocean-image"] == droplet.get("image", {}).get("slug")
        and any(
            a.get("type") == "public" and a.get("ip_address") == node["ip"]
            for a in droplet.get("networks", {}).get("v4", [])
        )
    )


async def observe(config, deps=None):
    deps = deps or {}
    opts = options(config)
    state = await deps.get("inspect", inspect)(opts)
    status = state["status"]
    provider = None
    if status in ("absent", "destroyed"):
        exists, matches, ready, reason = False, False, False, status
    elif status == "partial":
        exists, matches, ready, reason = True, False, False, "partial"
    elif status == "present":
        node = state["cluster"]["nodes"][0]
        provider = str(node["provider_id"])
        droplet = await deps.get("provider_get", provider_get)(opts, node)
        if droplet is None:
            exists, matches, ready, reason = (
                config.get("blue/event") == "delete",
                False,
                False,
                "droplet-absent",
            )
        else:
            healthy = droplet.get("status") == "active" and await deps.get(
                "service_health", service_health
            )(opts, node)
            marker = deps.get("read_marker", read_marker)(config) or {}
            drift = provider_matches(opts, node, droplet)
            exists = True
            ready = bool(healthy)
            matches = (
                healthy
                and marker.get("config-hash") == config_hash(config)
                and marker.get("provider-id") == provider
                and drift
            )
            reason = (
                "converged"
                if matches
                else "unhealthy"
                if not healthy
                else "provider-drift"
                if not drift
                else "new-provider-id"
                if marker.get("provider-id") != provider
                else "config-changed"
            )
    else:
        raise RuntimeError("Owned infrastructure state could not be read")
    print(
        f"redis profile={config['profile']} observe exists={exists} matches={matches} ready={ready} reason={reason} provider-id={provider or 'none'}",
        flush=True,
    )
    return {"exists": exists, "matches": bool(matches), "ready": ready}


async def converge(config):
    result = await run(
        redis.redis_workflow, {**options(config), "blue/event": "create"}
    )
    if failed(result):
        print(
            "redis converge outcome=failed retained=" + retain_failure(config, result),
            flush=True,
        )
    else:
        state = await inspect(options(config))
        nodes = state.get("cluster", {}).get("nodes", [])
        if (
            state.get("status") != "present"
            or not nodes
            or not nodes[0].get("provider_id")
        ):
            raise RuntimeError("Convergence finished without readable owned state")
        write_marker(config, nodes[0])
        print(
            "redis converge outcome=converged provider-id="
            + str(nodes[0]["provider_id"]),
            flush=True,
        )
    return {"blue/exit": 1 if failed(result) else 0}


async def delete(config):
    if (
        config.get("blue.kubernetes/resource", {}).get("spec", {}).get("deletionPolicy")
        != "Destroy"
    ):
        raise RuntimeError("Destroy must be explicitly selected")
    opts = {**options(config), "blue/event": "delete", "compute-prevent-destroy": False}
    state = await inspect(opts)
    missing = (
        state.get("status") == "present"
        and await provider_get(opts, state["cluster"]["nodes"][0]) is None
    )
    if missing:
        result = await redis_tools.ansible_local_step(
            {**opts, "colors-compute/cluster": state["cluster"]}
        )
        if not failed(result):
            result = await redis_tools.infrastructure_step(result)
    else:
        result = await run(redis.redis_workflow, opts)
    print(
        "redis delete outcome="
        + (
            "failed retained="
            + retain_failure({**config, "blue/event": "delete"}, result)
            if failed(result)
            else "destroyed"
        ),
        flush=True,
    )
    return {"blue/exit": 1 if failed(result) else 0}


def package():
    return {
        "resource": {
            "group": "colors.getcolors.ai",
            "version": "v1alpha1",
            "plural": "redisdeployments",
            "kind": "RedisDeployment",
        },
        "validate": validate,
        "identity": identity,
        "observe": observe,
        "converge": converge,
        "delete": delete,
    }
