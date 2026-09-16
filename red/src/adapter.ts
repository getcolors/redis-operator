import { createHash } from "node:crypto";
import {
  readFileSync,
  existsSync,
  mkdirSync,
  chmodSync,
  openSync,
  writeFileSync,
  closeSync,
  renameSync,
  readdirSync,
  unlinkSync,
} from "node:fs";
import { join } from "node:path";
import { run, failed, type Opts } from "red/workflow";
import {
  defaults,
  redisWorkflow,
  tools as redisTools,
  compute,
  validate as validation,
} from "package-redis-red";
import { read_deployment } from "colors-compute-red";
import * as t from "./tools.ts";
import type { Obj } from "./tools.ts";
export const credentialVars = Object.fromEntries(
  t.CREDENTIALS.map((k) => [
    k,
    k.replace("COLORS_PAR_", "").toLowerCase().replaceAll("_", "-"),
  ]),
);
export const healthCommand = `cd /opt/redis && docker compose exec -T redis sh -c 'export REDISCLI_AUTH="$(sed -n '\"'\"'s/^requirepass //p'\"'\"' /etc/redis/redis.conf)"; redis-cli --no-auth-warning PING'`;
export function checkEnvironment(env: Record<string, string | undefined>) {
  if (
    Object.keys(env).some(
      (k) => k.startsWith("COLORS_PAR_") && !(k in credentialVars),
    )
  )
    throw new Error(
      "Unexpected COLORS_PAR environment override; desired configuration must come from resource",
    );
  if (Object.keys(credentialVars).some((k) => !env[k]?.trim()))
    throw new Error("Required operator credentials are missing");
}
export function options(config: Obj): Opts {
  const desired = { ...config };
  delete desired["red.kubernetes/resource"];
  return {
    ...defaults,
    ...desired,
    ...Object.fromEntries(
      Object.entries(credentialVars).map(([e, k]) => [k, process.env[e]]),
    ),
    workdir: process.env.COLORS_WORKDIR ?? "/data/work",
    "compute-prevent-destroy": true,
    "redis-storage-managed": false,
    "provider-compute": "digitalocean",
    "provider-backend": "r2",
  };
}
export const identity = (c: Obj) => [
  "r2",
  c["r2-endpoint"],
  c["r2-bucket"],
  c.profile,
];
export function validate(config: Obj): string[] {
  const errors = validation.stateErrors(options(config));
  if (!/^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$/.test(config.profile ?? ""))
    errors.push("Invalid profile");
  if (
    (config["red.kubernetes/resource"]?.spec?.state ?? "running") !== "running"
  )
    errors.push("This release supports running state only");
  if ("compute-prevent-destroy" in config)
    errors.push("Use spec.deletionPolicy");
  return errors;
}
function sorted(v: any): any {
  return Array.isArray(v)
    ? v.map(sorted)
    : v && typeof v === "object"
      ? Object.fromEntries(
          Object.keys(v)
            .sort()
            .map((k) => [k, sorted(v[k])]),
        )
      : v;
}
export function configHash(config: Obj): string {
  return createHash("sha256")
    .update(
      JSON.stringify(
        sorted(
          Object.fromEntries(
            Object.entries(config).filter(
              ([k]) => !["red/event", "red.kubernetes/resource"].includes(k),
            ),
          ),
        ),
      ),
    )
    .digest("hex");
}
export const markerPath = (c: Obj) =>
  join(options(c).workdir as string, c.profile, "operator-success-red.json");
export const readMarker = (c: Obj): Obj | null =>
  existsSync(markerPath(c))
    ? JSON.parse(readFileSync(markerPath(c), "utf8"))
    : null;
export const writeMarker = (c: Obj, node: Obj) =>
  t.writeFile(markerPath(c), {
    "config-hash": configHash(c),
    "provider-id": String(node.provider_id),
  });
export function mask(
  text: string,
  env: Record<string, string | undefined>,
): string {
  for (const value of [
    ...new Set(
      Object.entries(env)
        .filter(([k, v]) => k.startsWith("COLORS_PAR_") && v?.trim())
        .map(([, v]) => v!),
    ),
  ].sort((a, b) => b.length - a.length))
    text = text.replaceAll(value, "***");
  return text;
}
export function retainFailure(c: Obj, result: Obj, env = process.env): string {
  const directory = join(options(c).workdir as string, c.profile, "failures");
  mkdirSync(directory, { recursive: true });
  chmodSync(directory, 0o700);
  const name =
      new Date().toISOString().replaceAll(/[-:.]/g, "") +
      "-" +
      String(result["red/step"] ?? "unknown").replaceAll(
        /[^A-Za-z0-9._-]/g,
        "-",
      ) +
      ".log",
    path = join(directory, name),
    tmp = path + ".tmp";
  const report = mask(
      JSON.stringify(
        {
          profile: c.profile,
          event: c["red/event"] ?? "create",
          step: result["red/step"],
          exit: result["red/exit"],
          err: result["red/err"],
          recap: result["ansible/recap"],
          trace: result["red/trace"],
        },
        null,
        2,
      ),
      env,
    ),
    fd = openSync(tmp, "wx", 0o600);
  try {
    writeFileSync(fd, report);
  } finally {
    closeSync(fd);
  }
  renameSync(tmp, path);
  for (const old of readdirSync(directory)
    .filter((p) => p.endsWith(".log"))
    .sort()
    .reverse()
    .slice(20))
    unlinkSync(join(directory, old));
  return name;
}
export const inspect = (o: Opts) =>
  read_deployment(o, redisTools.environment(o), {}, compute.requirements(o));
export async function providerGet(o: Opts, node: Obj): Promise<Obj | null> {
  const provider = String(node.provider_id ?? "");
  if (!/^[0-9]+$/.test(provider))
    throw new Error("Invalid recorded Droplet ID");
  const result = await t.digitalocean(
    o["do-token"] as string,
    "get",
    "droplets/" + provider,
  );
  if (!result) return null;
  const droplet = result.droplet;
  if (String(droplet.id) !== provider || droplet.name !== node.name)
    throw new Error("Provider identity does not match owned state");
  return droplet;
}
export const remote = (node: Obj, command: string) =>
  t.command(
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
      node.ssh_identity_file,
      node.user + "@" + node.ip,
      command,
    ],
    "",
    30,
  );
export async function serviceHealth(_o: Opts, node: Obj): Promise<boolean> {
  const result = await remote(node, healthCommand);
  return result.exit === 0 && result.out.trim() === "PONG";
}
export function providerMatches(o: Opts, node: Obj, d: Obj): boolean {
  return (
    node.name === d.name &&
    o["digitalocean-region"] === d.region?.slug &&
    o["digitalocean-size"] === d.size_slug &&
    o["digitalocean-image"] === d.image?.slug &&
    (d.networks?.v4 ?? []).some(
      (a: Obj) => a.type === "public" && a.ip_address === node.ip,
    )
  );
}
export async function observe(
  c: Obj,
  deps: Obj = {},
): Promise<{ exists: boolean; matches: boolean; ready: boolean }> {
  const o = options(c),
    state = await (deps.inspect ?? inspect)(o),
    status = state.status;
  let exists: boolean,
    matches: boolean,
    ready: boolean,
    reason: string,
    provider: string | undefined;
  if (["absent", "destroyed"].includes(status)) {
    exists = matches = ready = false;
    reason = status;
  } else if (status === "partial") {
    exists = true;
    matches = ready = false;
    reason = "partial";
  } else if (status === "present") {
    const node = state.cluster!.nodes[0];
    provider = String(node.provider_id);
    const droplet = await (deps.providerGet ?? providerGet)(o, node);
    if (!droplet) {
      exists = c["red/event"] === "delete";
      matches = ready = false;
      reason = "droplet-absent";
    } else {
      const healthy =
          droplet.status === "active" &&
          (await (deps.serviceHealth ?? serviceHealth)(o, node)),
        marker = (deps.readMarker ?? readMarker)(c) ?? {},
        drift = providerMatches(o, node, droplet);
      exists = true;
      ready = !!healthy;
      matches = !!(
        healthy &&
        marker["config-hash"] === configHash(c) &&
        marker["provider-id"] === provider &&
        drift
      );
      reason = matches
        ? "converged"
        : !healthy
          ? "unhealthy"
          : !drift
            ? "provider-drift"
            : marker["provider-id"] !== provider
              ? "new-provider-id"
              : "config-changed";
    }
  } else throw new Error("Owned infrastructure state could not be read");
  console.log(
    `redis profile=${c.profile} observe exists=${exists} matches=${matches} ready=${ready} reason=${reason} provider-id=${provider ?? "none"}`,
  );
  return { exists, matches, ready };
}
export async function converge(c: Obj): Promise<Obj> {
  const result = await run(redisWorkflow, {
    ...options(c),
    "red/event": "create",
  });
  if (failed(result))
    console.log(
      "redis converge outcome=failed retained=" + retainFailure(c, result),
    );
  else {
    const state = await inspect(options(c)),
      nodes = state.cluster?.nodes ?? [];
    if (state.status !== "present" || !nodes[0]?.provider_id)
      throw new Error("Convergence finished without readable owned state");
    writeMarker(c, nodes[0]);
    console.log(
      "redis converge outcome=converged provider-id=" + nodes[0].provider_id,
    );
  }
  return { "red/exit": failed(result) ? 1 : 0 };
}
export async function deleteResource(c: Obj): Promise<Obj> {
  if (c["red.kubernetes/resource"]?.spec?.deletionPolicy !== "Destroy")
    throw new Error("Destroy must be explicitly selected");
  const o = {
      ...options(c),
      "red/event": "delete",
      "compute-prevent-destroy": false,
    },
    state = await inspect(o),
    missing =
      state.status === "present" &&
      (await providerGet(o, state.cluster!.nodes[0])) === null;
  let result: Opts;
  if (missing) {
    result = await redisTools.ansibleLocalStep({
      ...o,
      "colors-compute/cluster": state.cluster,
    });
    if (!failed(result)) result = await redisTools.infrastructureStep(result);
  } else result = await run(redisWorkflow, o);
  console.log(
    "redis delete outcome=" +
      (failed(result)
        ? "failed retained=" +
          retainFailure({ ...c, "red/event": "delete" }, result)
        : "destroyed"),
  );
  return { "red/exit": failed(result) ? 1 : 0 };
}
export function redisPackage() {
  return {
    resource: {
      group: "colors.getcolors.ai",
      version: "v1alpha1",
      plural: "redisdeployments",
      kind: "RedisDeployment",
    },
    validate,
    identity,
    observe,
    converge,
    delete: deleteResource,
  };
}
