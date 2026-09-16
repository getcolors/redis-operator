import * as t from "./tools.ts";
import type { Obj } from "./tools.ts";
export const unchanged = (cr: Obj | null, e: Obj, key: string) =>
  !!cr &&
  cr.metadata?.uid === e.resourceUID &&
  cr.metadata?.generation === e[key];
export function requireUnchanged(cr: Obj | null, e: Obj, key: string): Obj {
  if (!unchanged(cr, e, key))
    throw new t.Fatal("Desired state changed during operation");
  return cr!;
}
export async function check(o: Obj) {
  await t.waitReady(o);
  console.log("check", await t.healthyProbe(o));
  const directory = "/data/work/" + o.profile + "/failures";
  const files = await t.kubectl(
    o,
    [
      "exec",
      "deployment/" + t.CONTROLLER,
      "-n",
      o.namespace,
      "--",
      "sh",
      "-c",
      `ls -1 ${directory} 2>/dev/null || true`,
    ],
    { requestTimeout: false, timeout: 60 },
  );
  console.log(
    "failures retained:",
    files.trim() ? files.trim().split("\n").length : 0,
  );
}
export async function rehearse(o: Obj) {
  const original = await t.waitReady(o),
    e: Obj = {
      startedAt: t.now(),
      resourceUID: original.metadata.uid,
      passed: false,
    };
  const save = () => t.evidence(o, "backup-rehearsal", e);
  save();
  const suspended = await t.patch(o, original, [
    { op: "add", path: "/spec/suspend", value: true },
  ]);
  e.suspendedGeneration = suspended.metadata.generation;
  save();
  let failure: any,
    uncertain = false;
  try {
    await t.waitFor(
      "acknowledged suspension",
      async () => {
        const cr = requireUnchanged(
          await t.getResource(o),
          e,
          "suspendedGeneration",
        );
        return t.acknowledged(cr) ? cr : null;
      },
      7800,
      3,
    );
    e.suspensionAcknowledgedAt = t.now();
    save();
    let result: Obj;
    try {
      result = await t.probe(o, "rehearse");
    } catch (error) {
      uncertain = true;
      throw error;
    }
    e.result = result;
    if (result.rehearsalPassed !== true)
      throw new Error("Backup rehearsal failed");
    e.passed = true;
  } catch (error) {
    failure = error;
  }
  try {
    const current = await t.getResource(o);
    if (uncertain) e.resumeBlocked = "Remote execution completion is uncertain";
    else if (!unchanged(current, e, "suspendedGeneration"))
      e.resumeBlocked = "Resource changed during rehearsal";
    else {
      const resumed = await t.patch(o, current, [
        { op: "add", path: "/spec/suspend", value: false },
      ]);
      e.resumedGeneration = resumed.metadata.generation;
      e.resumedAt = t.now();
    }
  } catch (error) {
    e.resumeBlocked = "Could not re-read or resume resource: " + String(error);
  }
  const path = save();
  if (failure || e.resumeBlocked)
    throw new Error(
      String(failure ?? e.resumeBlocked) +
        (e.resumeBlocked
          ? "; " +
            e.resumeBlocked +
            "; resource left suspended; verify no workflow runs before resuming"
          : "") +
        "; evidence: " +
        path,
    );
  await t.waitReady(o, 2700);
}
const uuid = () => crypto.randomUUID().replaceAll("-", "");
export async function drill(o: Obj) {
  const cr = await t.waitReady(o),
    before = await t.healthyProbe(o),
    token = o["do-token"],
    cluster = o["doks-cluster-id"],
    workers = new Set<string>();
  if (cluster) {
    const result = await t.digitalocean(
      token,
      "get",
      "kubernetes/clusters/" + cluster,
    );
    for (const pool of result!.kubernetes_cluster.node_pools)
      for (const node of pool.nodes) workers.add(String(node.droplet_id));
  }
  const droplet = await t.digitalocean(
    token,
    "get",
    "droplets/" + before.providerId,
  );
  t.ownedDroplet(droplet?.droplet, before, o.profile, workers);
  const key = "colors:self-heal:" + uuid(),
    value = uuid();
  if ((await t.probe(o, "set-marker", key, value)).marker !== value)
    throw new t.Fatal("Authenticated SET/GET failed before disruption");
  const e: Obj = {
    startedAt: t.now(),
    clusterId: cluster ?? null,
    resourceUID: cr.metadata.uid,
    generation: cr.metadata.generation,
    before,
    excludedWorkerIds: [...workers].sort(),
    markerKey: key,
    markerValue: value,
    expectedDataRecovery: false,
    passed: false,
  };
  const save = () => t.evidence(o, "self-healing", e);
  save();
  await t.digitalocean(token, "delete", "droplets/" + before.providerId);
  e.deleteAcceptedAt = t.now();
  save();
  try {
    await t.waitFor(
      "service recovery",
      async () => {
        const current = requireUnchanged(
          await t.getResource(o),
          e,
          "generation",
        );
        if (t.suspended(current) || t.deleting(current))
          throw new t.Fatal("Resource suspended or deleting during recovery");
        const after = await t.probe(o, "health");
        if (
          after.providerId === before.providerId ||
          after.healthy !== true ||
          !t.ready(current)
        )
          return null;
        const live = await t.digitalocean(
          token,
          "get",
          "droplets/" + after.providerId,
        );
        t.ownedDroplet(live?.droplet, after, o.profile, workers);
        if (
          (await t.digitalocean(
            token,
            "get",
            "droplets/" + before.providerId,
          )) !== null
        )
          throw new t.Fatal("Original Droplet still exists");
        const survived = (await t.probe(o, "get-marker", key)).marker === value,
          fresh = uuid(),
          written =
            (await t.probe(o, "set-marker", key, fresh)).marker === fresh;
        Object.assign(e, {
          recoveredAt: t.now(),
          after,
          priorMarkerSurvived: survived,
          authenticatedWriteReadPassed: written,
        });
        if (!written) throw new t.Fatal("Replacement write/read failed");
        return after;
      },
      2400,
      15,
    );
  } catch (error) {
    Object.assign(e, {
      timedOutAt: t.now(),
      lastErrorType: error instanceof t.Fatal ? "fatal" : "timeout",
      lastError: String(error),
    });
    save();
    throw error;
  }
  e.passed = true;
  save();
}
export async function restart(o: Obj) {
  const cr = await t.waitReady(o),
    before = await t.healthyProbe(o),
    namespace = o.namespace,
    deployment = "deployment/" + t.CONTROLLER;
  const doc = await t.kubectl(
    o,
    ["get", "deployment", t.CONTROLLER, "-n", namespace, "-o", "json"],
    { parse: true },
  );
  if (doc.spec.replicas !== 1)
    throw new t.Fatal("Restart requires exactly one controller replica");
  const old = await t.controllerPod(o);
  const e: Obj = {
    startedAt: t.now(),
    before,
    resourceUID: cr.metadata.uid,
    generation: cr.metadata.generation,
    lastReconcileTimeBefore: cr.status?.lastReconcileTime,
    oldPod: old?.metadata.name,
    passed: false,
  };
  const save = () => t.evidence(o, "controller-restart", e);
  save();
  await t.kubectl(o, ["scale", deployment, "-n", namespace, "--replicas=0"]);
  try {
    await t.waitFor(
      "old controller to stop",
      async () =>
        !(
          await t.kubectl(
            o,
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
            { parse: true },
          )
        ).items.length,
      7800,
    );
  } catch (error) {
    e.failure =
      "Old controller did not stop; intentionally leaving replicas at zero";
    save();
    throw error;
  }
  e.stoppedAt = t.now();
  save();
  await t.kubectl(o, ["scale", deployment, "-n", namespace, "--replicas=1"]);
  await t.kubectl(
    o,
    ["rollout", "status", deployment, "-n", namespace, "--timeout=600s"],
    { requestTimeout: false, timeout: 630 },
  );
  const pod = await t.controllerUp(o),
    start = pod.status?.startTime;
  if (!start || (old && old.metadata.uid === pod.metadata.uid))
    throw new t.Fatal("Controller pod did not change or has no start time");
  e.newPod = pod.metadata.name;
  e.newPodStartTime = start;
  save();
  const current = await t.waitFor(
      "reconcile by new controller",
      async () => {
        const current = t.active(await t.getResource(o)),
          timestamp = current.status?.lastReconcileTime;
        return t.ready(current) &&
          timestamp &&
          Date.parse(timestamp) > Date.parse(start)
          ? current
          : null;
      },
      1800,
      10,
    ),
    after = await t.probe(o, "health");
  if (after.healthy !== true || before.providerId !== after.providerId)
    throw new t.Fatal("Restart did not preserve healthy infrastructure");
  if (
    before.convergenceRecordModifiedMs == null ||
    before.convergenceRecordModifiedMs !== after.convergenceRecordModifiedMs
  )
    throw new t.Fatal("Restart unexpectedly ran convergence");
  requireUnchanged(current, e, "generation");
  Object.assign(e, {
    passed: true,
    after,
    completedAt: t.now(),
    lastReconcileTimeAfter: current.status.lastReconcileTime,
  });
  save();
}
