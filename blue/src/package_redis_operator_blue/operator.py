"""Operational checks with guarded suspension and disruption."""

from . import tools as t
from uuid import uuid4
from datetime import datetime


def unchanged(cr, e, key):
    return (
        cr is not None
        and cr["metadata"].get("uid") == e["resourceUID"]
        and cr["metadata"].get("generation") == e[key]
    )


def require_unchanged(cr, e, key):
    if not unchanged(cr, e, key):
        raise t.Fatal("Desired state changed during operation")
    return cr


async def check(opts):
    await t.wait_ready(opts)
    observed = await t.healthy_probe(opts)
    print("check", observed)
    directory = "/data/work/" + opts["profile"] + "/failures"
    files = await t.kubectl(
        opts,
        [
            "exec",
            "deployment/" + t.CONTROLLER,
            "-n",
            opts["namespace"],
            "--",
            "sh",
            "-c",
            f"ls -1 {directory} 2>/dev/null || true",
        ],
        request_timeout=False,
        timeout=60,
    )
    print("failures retained:", len(files.splitlines()))


async def rehearse(opts):
    original = await t.wait_ready(opts)
    evidence = {
        "startedAt": t.now(),
        "resourceUID": original["metadata"]["uid"],
        "passed": False,
    }

    def save():
        return t.evidence(opts, "backup-rehearsal", evidence)

    save()
    suspended = await t.patch(
        opts, original, [{"op": "add", "path": "/spec/suspend", "value": True}]
    )
    evidence["suspendedGeneration"] = suspended["metadata"]["generation"]
    save()
    failure = None
    uncertain = False
    try:

        async def acknowledged():
            current = require_unchanged(
                await t.get_resource(opts), evidence, "suspendedGeneration"
            )
            return current if t.acknowledged(current) else None

        await t.wait_for("acknowledged suspension", acknowledged, 7800, 3)
        evidence["suspensionAcknowledgedAt"] = t.now()
        save()
        try:
            result = await t.probe(opts, "rehearse")
        except Exception:
            uncertain = True
            raise
        evidence["result"] = result
        if result.get("rehearsalPassed") is not True:
            raise RuntimeError("Backup rehearsal failed")
        evidence["passed"] = True
    except Exception as exc:
        failure = exc
    try:
        current = await t.get_resource(opts)
        if uncertain:
            evidence["resumeBlocked"] = "Remote execution completion is uncertain"
        elif not unchanged(current, evidence, "suspendedGeneration"):
            evidence["resumeBlocked"] = "Resource changed during rehearsal"
        else:
            resumed = await t.patch(
                opts, current, [{"op": "add", "path": "/spec/suspend", "value": False}]
            )
            evidence.update(
                resumedGeneration=resumed["metadata"]["generation"], resumedAt=t.now()
            )
    except Exception as exc:
        evidence["resumeBlocked"] = "Could not re-read or resume resource: " + str(exc)
    path = save()
    blocked = evidence.get("resumeBlocked")
    if failure or blocked:
        raise RuntimeError(
            str(failure or blocked)
            + (
                (
                    "; "
                    + blocked
                    + "; resource left suspended; verify no workflow runs before resuming"
                )
                if blocked
                else ""
            )
            + "; evidence: "
            + path
        )
    await t.wait_ready(opts, 2700)


async def drill(opts):
    cr = await t.wait_ready(opts)
    before = await t.healthy_probe(opts)
    token = opts["do-token"]
    cluster = opts.get("doks-cluster-id")
    workers = set()
    if cluster:
        result = await t.digitalocean(token, "get", "kubernetes/clusters/" + cluster)
        workers = {
            str(n["droplet_id"])
            for p in result["kubernetes_cluster"]["node_pools"]
            for n in p["nodes"]
        }
    droplet = await t.digitalocean(token, "get", "droplets/" + before["providerId"])
    t.owned_droplet((droplet or {}).get("droplet"), before, opts["profile"], workers)
    key = "colors:self-heal:" + uuid4().hex
    value = uuid4().hex
    if (await t.probe(opts, "set-marker", key, value)).get("marker") != value:
        raise t.Fatal("Authenticated SET/GET failed before disruption")
    evidence = {
        "startedAt": t.now(),
        "clusterId": cluster,
        "resourceUID": cr["metadata"]["uid"],
        "generation": cr["metadata"]["generation"],
        "before": before,
        "excludedWorkerIds": sorted(workers),
        "markerKey": key,
        "markerValue": value,
        "expectedDataRecovery": False,
        "passed": False,
    }

    def save():
        return t.evidence(opts, "self-healing", evidence)

    save()
    await t.digitalocean(token, "delete", "droplets/" + before["providerId"])
    evidence["deleteAcceptedAt"] = t.now()
    save()

    async def recovered():
        current = require_unchanged(await t.get_resource(opts), evidence, "generation")
        if t.suspended(current) or t.deleting(current):
            raise t.Fatal("Resource suspended or deleting during recovery")
        after = await t.probe(opts, "health")
        if (
            after.get("providerId") == before["providerId"]
            or after.get("healthy") is not True
            or not t.ready(current)
        ):
            return None
        live = await t.digitalocean(token, "get", "droplets/" + after["providerId"])
        t.owned_droplet((live or {}).get("droplet"), after, opts["profile"], workers)
        if (
            await t.digitalocean(token, "get", "droplets/" + before["providerId"])
            is not None
        ):
            raise t.Fatal("Original Droplet still exists")
        survived = (await t.probe(opts, "get-marker", key)).get("marker") == value
        fresh = uuid4().hex
        written = (await t.probe(opts, "set-marker", key, fresh)).get("marker") == fresh
        evidence.update(
            recoveredAt=t.now(),
            after=after,
            priorMarkerSurvived=survived,
            authenticatedWriteReadPassed=written,
        )
        if not written:
            raise t.Fatal("Replacement write/read failed")
        return after

    try:
        await t.wait_for("service recovery", recovered, 2400, 15)
    except Exception as exc:
        evidence.update(
            timedOutAt=t.now(),
            lastErrorType="fatal" if isinstance(exc, t.Fatal) else "timeout",
            lastError=str(exc),
        )
        save()
        raise
    evidence["passed"] = True
    save()


async def restart(opts):
    cr = await t.wait_ready(opts)
    before = await t.healthy_probe(opts)
    namespace = opts["namespace"]
    deployment = "deployment/" + t.CONTROLLER
    doc = await t.kubectl(
        opts,
        ["get", "deployment", t.CONTROLLER, "-n", namespace, "-o", "json"],
        parse=True,
    )
    if doc["spec"].get("replicas") != 1:
        raise t.Fatal("Restart requires exactly one controller replica")
    old = await t.controller_pod(opts)
    evidence = {
        "startedAt": t.now(),
        "before": before,
        "resourceUID": cr["metadata"]["uid"],
        "generation": cr["metadata"]["generation"],
        "lastReconcileTimeBefore": cr.get("status", {}).get("lastReconcileTime"),
        "oldPod": (old or {}).get("metadata", {}).get("name"),
        "passed": False,
    }

    def save():
        return t.evidence(opts, "controller-restart", evidence)

    save()
    await t.kubectl(opts, ["scale", deployment, "-n", namespace, "--replicas=0"])

    async def stopped():
        return not (
            await t.kubectl(
                opts,
                [
                    "get",
                    "pods",
                    "-n",
                    namespace,
                    "-l",
                    "app=" + t.CONTROLLER,
                    "-o",
                    "json",
                ],
                parse=True,
            )
        ).get("items")

    try:
        await t.wait_for("old controller to stop", stopped, 7800)
    except Exception:
        evidence["failure"] = (
            "Old controller did not stop; intentionally leaving replicas at zero"
        )
        save()
        raise
    evidence["stoppedAt"] = t.now()
    save()
    await t.kubectl(opts, ["scale", deployment, "-n", namespace, "--replicas=1"])
    await t.kubectl(
        opts,
        ["rollout", "status", deployment, "-n", namespace, "--timeout=600s"],
        request_timeout=False,
        timeout=630,
    )
    pod = await t.controller_up(opts)
    start = pod["status"].get("startTime")
    if not start or old and old["metadata"]["uid"] == pod["metadata"]["uid"]:
        raise t.Fatal("Controller pod did not change or has no start time")
    evidence.update(newPod=pod["metadata"]["name"], newPodStartTime=start)
    save()

    async def reconciled():
        current = t.active(await t.get_resource(opts))
        timestamp = current.get("status", {}).get("lastReconcileTime")
        return (
            current
            if t.ready(current)
            and timestamp
            and datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            > datetime.fromisoformat(start.replace("Z", "+00:00"))
            else None
        )

    current = await t.wait_for("reconcile by new controller", reconciled, 1800, 10)
    after = await t.probe(opts, "health")
    if after.get("healthy") is not True or before["providerId"] != after.get(
        "providerId"
    ):
        raise t.Fatal("Restart did not preserve healthy infrastructure")
    if before.get("convergenceRecordModifiedMs") is None or before[
        "convergenceRecordModifiedMs"
    ] != after.get("convergenceRecordModifiedMs"):
        raise t.Fatal("Restart unexpectedly ran convergence")
    require_unchanged(current, evidence, "generation")
    evidence.update(
        passed=True,
        after=after,
        completedAt=t.now(),
        lastReconcileTimeAfter=current["status"]["lastReconcileTime"],
    )
    save()
