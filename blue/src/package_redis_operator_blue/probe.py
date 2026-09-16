"""Authenticated in-pod probes. Captured errors never print secrets."""

import asyncio, contextlib, io, json, re, sys
from blue.workflow import run, failed
from . import adapter as a, tools as t


def safe_token(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9:_-]{1,160}", value):
        raise ValueError("Invalid probe token")
    return value


async def redis_command(node, command):
    result = await a.remote(
        node, a.HEALTH_COMMAND.replace("PING'", "--raw " + command + "'")
    )
    if result["exit"]:
        raise RuntimeError("Remote probe failed")
    return result["out"].strip()


async def probe(resource, namespace, operation, key=None, value=None):
    safe_token(resource)
    safe_token(namespace)
    result = await t.command(
        ["kubectl", "get", "redisdeployment", resource, "-n", namespace, "-o", "json"],
        timeout=30,
    )
    if result["exit"]:
        raise RuntimeError("Resource read failed")
    cr = json.loads(result["out"])
    config = {
        **cr["spec"]["config"],
        "profile": cr["spec"]["config"].get("profile")
        or cr.get("status", {}).get("profile")
        or namespace + "--" + resource,
    }
    opts = a.options(config)
    state = await a.inspect(opts)
    if state["status"] != "present":
        raise RuntimeError("State not ready")
    node = state["cluster"]["nodes"][0]
    path = a.marker_path(config)
    evidence = {
        "providerId": str(node["provider_id"]),
        "name": node["name"],
        "ip": node["ip"],
        "profile": opts["profile"],
        "convergenceRecordModifiedMs": int(path.stat().st_mtime * 1000)
        if path.exists()
        else 0,
    }
    if operation == "inspect":
        pass
    elif operation == "health":
        evidence["healthy"] = await redis_command(node, "PING") == "PONG"
    elif operation == "set-marker":
        safe_token(key)
        safe_token(value)
        if await redis_command(node, "SET " + key + " " + value) != "OK":
            raise RuntimeError("Marker write failed")
        evidence["marker"] = await redis_command(node, "GET " + key)
    elif operation == "get-marker":
        evidence["marker"] = await redis_command(node, "GET " + safe_token(key))
    elif operation == "rehearse":
        if not t.acknowledged(cr):
            raise RuntimeError(
                "Backup rehearsal requires acknowledged controller suspension"
            )
        with (
            contextlib.redirect_stdout(io.StringIO()),
            contextlib.redirect_stderr(io.StringIO()),
        ):
            result = await run(
                a.redis.redis_workflow, {**opts, "blue/event": "rehearse"}
            )
        evidence["rehearsalPassed"] = not failed(result)
    else:
        raise ValueError("Unknown probe operation")
    return evidence


def main():
    try:
        print(json.dumps(asyncio.run(probe(*sys.argv[1:]))))
    except Exception:
        print("Probe failed; captured output suppressed", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
