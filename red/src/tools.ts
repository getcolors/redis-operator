import { readFileSync, mkdirSync, writeFileSync, renameSync } from "node:fs";
import { dirname, join } from "node:path";
export type Obj = Record<string, any>;
export const CONTROLLER = "colors-redis-operator",
  RESOURCE = "redisdeployments.colors.getcolors.ai";
export const CREDENTIALS = [
  "COLORS_PAR_DO_TOKEN",
  "COLORS_PAR_R2_ACCESS_KEY_ID",
  "COLORS_PAR_R2_SECRET_ACCESS_KEY",
  "COLORS_PAR_REDIS_BACKUP_R2_ACCESS_KEY_ID",
  "COLORS_PAR_REDIS_BACKUP_R2_SECRET_ACCESS_KEY",
];
export const CONFIG_KEYS =
  "provider-compute provider-backend redis-image redis-port redis-backup-r2-bucket redis-backup-r2-endpoint redis-backup-r2-region redis-backup-oncalendar redis-backup-retention-days redis-backup-max-age-hours digitalocean-region digitalocean-size digitalocean-image digitalocean-ssh-sources r2-bucket r2-endpoint".split(
    " ",
  );
export class Fatal extends Error {}
export const now = () => new Date().toISOString();
export function ready(cr: Obj): boolean {
  const m = cr.metadata ?? {},
    s = cr.status ?? {};
  return (
    m.generation != null &&
    m.generation === s.observedGeneration &&
    s.phase === "Ready" &&
    (s.conditions ?? []).some(
      (c: Obj) => c.type === "Ready" && c.status === "True",
    )
  );
}
export const suspended = (cr: Obj) => cr.spec?.suspend === true;
export const deleting = (cr: Obj) => cr.metadata?.deletionTimestamp != null;
export const acknowledged = (cr: Obj) =>
  suspended(cr) &&
  !deleting(cr) &&
  cr.status?.phase === "Suspended" &&
  cr.metadata?.generation === cr.status?.observedGeneration;
export function active(cr: Obj | null): Obj {
  if (!cr || suspended(cr) || deleting(cr))
    throw new Fatal("RedisDeployment is absent, suspended or being deleted");
  return cr;
}
export function manifests(opts: Obj): Obj {
  const directory = join(import.meta.dir, "../../resources");
  const doc = JSON.parse(
    readFileSync(join(directory, "manifests.json"), "utf8")
      .replaceAll("{{namespace}}", opts.namespace)
      .replaceAll("{{image}}", opts.image),
  );
  doc.items.splice(
    1,
    0,
    Bun.YAML.parse(readFileSync(join(directory, "crd.yml"), "utf8")),
  );
  if (opts["image-pull-secret"])
    doc.items.at(-1).spec.template.spec.imagePullSecrets = [
      { name: opts["image-pull-secret"] },
    ];
  return doc;
}
export function resource(o: Obj): Obj {
  return {
    apiVersion: "colors.getcolors.ai/v1alpha1",
    kind: "RedisDeployment",
    metadata: { name: o["resource-name"], namespace: o.namespace },
    spec: {
      state: "running",
      deletionPolicy: o["deletion-policy"],
      reconcileInterval: o["reconcile-interval"],
      config: {
        profile: o.profile,
        ...Object.fromEntries(
          CONFIG_KEYS.filter((k) => k in o).map((k) => [k, o[k]]),
        ),
      },
    },
  };
}
function sorted(value: any): any {
  return Array.isArray(value)
    ? value.map(sorted)
    : value && typeof value === "object"
      ? Object.fromEntries(
          Object.keys(value)
            .sort()
            .map((k) => [k, sorted(value[k])]),
        )
      : value;
}
export function writeFile(path: string, data: any): string {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path + ".tmp", pretty(sorted(data)) + "\n");
  renameSync(path + ".tmp", path);
  return path;
}
export function render(o: Obj) {
  const d = join(o.workdir, o.profile, "operator");
  return [
    writeFile(join(d, "manifests.json"), manifests(o)),
    writeFile(join(d, "redis-deployment.json"), resource(o)),
  ];
}
export const evidence = (o: Obj, name: string, data: any) =>
  writeFile(join(o.workdir, o.profile, "evidence", name + ".json"), data);
export async function command(
  argv: string[],
  input = "",
  timeout = 120,
): Promise<Obj> {
  const proc = Bun.spawn(argv, {
    stdin: new Blob([input]),
    stdout: "pipe",
    stderr: "pipe",
    detached: true,
  });
  let timed = false;
  const timer = setTimeout(() => {
    timed = true;
    try {
      process.kill(-proc.pid, "SIGKILL");
    } catch {
      proc.kill("SIGKILL");
    }
  }, timeout * 1000);
  try {
    const [exit, out, err] = await Promise.all([
      proc.exited,
      new Response(proc.stdout).text(),
      new Response(proc.stderr).text(),
    ]);
    return timed
      ? { exit: 124, out: "", err: `command timed out after ${timeout}s` }
      : { exit, out, err };
  } finally {
    clearTimeout(timer);
  }
}
export async function kubectl(
  o: Obj,
  args: string[],
  options: Obj = {},
): Promise<any> {
  const r = await command(
    [
      "kubectl",
      "--context",
      o["kube-context"],
      ...(options.requestTimeout === false ? [] : ["--request-timeout=30s"]),
      ...args,
    ],
    options.input ?? "",
    options.timeout ?? 120,
  );
  if (r.exit === 0)
    return options.parse ? (r.out.trim() ? JSON.parse(r.out) : null) : r.out;
  if ("notFound" in options && /\bnot ?found\b/i.test(r.err))
    return options.notFound;
  throw new Error(
    `kubectl ${args[0]} failed (exit ${r.exit}): ${options.quiet ? "output suppressed" : r.err.trim()}`,
  );
}
export const apply = (o: Obj, doc: Obj, quiet = false) =>
  kubectl(o, ["apply", "-f", "-"], { input: JSON.stringify(doc), quiet });
export const getResource = (o: Obj) =>
  kubectl(
    o,
    [
      "get",
      RESOURCE,
      o["resource-name"],
      "-n",
      o.namespace,
      "--ignore-not-found",
      "-o",
      "json",
    ],
    { parse: true },
  );
export async function waitFor(
  label: string,
  callback: () => Promise<any>,
  timeout = 180,
  interval = 5,
): Promise<any> {
  const end = Date.now() + timeout * 1000;
  let error: any;
  for (;;) {
    try {
      const result = await callback();
      if (result) return result;
    } catch (e) {
      if (e instanceof Fatal) throw e;
      error = e;
    }
    if (Date.now() >= end)
      throw new Error(
        `Timed out waiting for ${label}${error ? ": " + String(error) : ""}`,
      );
    await Bun.sleep(interval * 1000);
  }
}
export async function waitReady(
  o: Obj,
  timeout = 180,
  transientFailure = false,
): Promise<Obj> {
  return waitFor(
    "Ready at current generation",
    async () => {
      const cr = active(await getResource(o)),
        phase = cr.status?.phase;
      if (
        ["Invalid", "Blocked"].includes(phase) ||
        (phase === "Failed" && !transientFailure)
      )
        throw new Fatal(`RedisDeployment reports ${phase}`);
      return ready(cr) ? cr : null;
    },
    timeout,
  );
}
export const sameDesired = (a: Obj, b: Obj) =>
  JSON.stringify(sorted(a.spec)) === JSON.stringify(sorted(b.spec)) &&
  ["uid", "deletionTimestamp"].every(
    (k) => a.metadata?.[k] === b.metadata?.[k],
  );
export async function patch(
  o: Obj,
  current: Obj,
  changes: Obj[],
): Promise<Obj> {
  const original = structuredClone(current);
  for (let attempt = 0; attempt < 5; attempt++) {
    const version = current.metadata.resourceVersion;
    try {
      return await kubectl(
        o,
        [
          "patch",
          RESOURCE,
          o["resource-name"],
          "-n",
          o.namespace,
          "--type=json",
          "-o",
          "json",
          "-p",
          JSON.stringify([
            { op: "test", path: "/metadata/resourceVersion", value: version },
            ...changes,
          ]),
        ],
        { parse: true },
      );
    } catch (e) {
      if (
        !String(e).includes(
          "the server rejected our request due to an error in our request",
        )
      )
        throw e;
      const fresh = await getResource(o);
      if (!fresh || !sameDesired(original, fresh))
        throw new Fatal(
          "RedisDeployment changed concurrently; patch not applied",
        );
      if (fresh.metadata.resourceVersion === version || attempt === 4) throw e;
      current = fresh;
      await Bun.sleep(2000);
    }
  }
  throw new Error("Patch retries exhausted");
}
export async function controllerPod(o: Obj): Promise<Obj | null> {
  const result = await kubectl(
      o,
      [
        "get",
        "pods",
        "-n",
        o.namespace,
        "-l",
        "app=" + CONTROLLER,
        "-o",
        "json",
      ],
      { parse: true },
    ),
    pods = result.items.filter((p: Obj) => !deleting(p));
  return pods.length === 1 && pods[0].status?.phase === "Running"
    ? pods[0]
    : null;
}
export async function controllerUp(o: Obj): Promise<Obj> {
  return waitFor(
    "controller startup",
    async () => {
      const pod = await controllerPod(o);
      if (!pod?.status?.startTime) return null;
      const logs = await kubectl(
        o,
        [
          "logs",
          "pod/" + pod.metadata.name,
          "-n",
          o.namespace,
          "-c",
          "controller",
          "--since-time=" + pod.status.startTime,
        ],
        { requestTimeout: false, timeout: 60 },
      );
      return logs.includes("RedisDeployment controller running") ? pod : null;
    },
    600,
  );
}
export async function probe(
  o: Obj,
  operation: string,
  ...args: string[]
): Promise<Obj> {
  await controllerUp(o);
  const script =
    'if [ -d /app/blue ]; then exec /app/blue/.venv/bin/python -m package_redis_operator_blue.probe "$@"; elif [ -d /app/red ]; then exec bun /app/red/src/probe.ts "$@"; else exec bb -m colors.probe "$@"; fi';
  return JSON.parse(
    await kubectl(
      o,
      [
        "exec",
        "deployment/" + CONTROLLER,
        "-n",
        o.namespace,
        "--",
        "sh",
        "-c",
        script,
        "probe",
        o["resource-name"],
        o.namespace,
        operation,
        ...args,
      ],
      { requestTimeout: false, timeout: operation === "rehearse" ? 7800 : 180 },
    ),
  );
}
export async function healthyProbe(o: Obj): Promise<Obj> {
  const result = await probe(o, "health");
  if (result.healthy !== true)
    throw new Fatal("Redis must be healthy before the operation");
  return result;
}
export async function digitalocean(
  token: string,
  method: string,
  path: string,
): Promise<Obj | null> {
  const response = await fetch("https://api.digitalocean.com/v2/" + path, {
    method: method.toUpperCase(),
    headers: { Authorization: "Bearer " + token, Accept: "application/json" },
    signal: AbortSignal.timeout(45000),
  });
  if (response.status === 404 && method.toLowerCase() === "get") return null;
  if (!response.ok)
    throw new Error(
      `DigitalOcean ${method.toUpperCase()} failed: HTTP ${response.status}`,
    );
  const body = await response.text();
  return body ? JSON.parse(body) : null;
}
export function ownedDroplet(
  droplet: Obj | null,
  observed: Obj,
  profile: string,
  workers: Iterable<string>,
): boolean {
  const provider = String(observed.providerId ?? "");
  if (!/^[0-9]+$/.test(provider) || !droplet || provider !== String(droplet.id))
    throw new Fatal("Recorded provider ID does not match live Droplet");
  if (
    [observed.profile, observed.name, droplet.name].some((x) => x !== profile)
  )
    throw new Fatal("Droplet does not belong to exact deployment profile");
  if (
    new Set([...workers].map(String)).has(provider) ||
    (droplet.tags ?? []).some((v: any) => String(v).startsWith("k8s:"))
  )
    throw new Fatal("Refusing a Kubernetes worker");
  if (
    !(droplet.networks?.v4 ?? []).some(
      (a: Obj) => a.type === "public" && a.ip_address === observed.ip,
    )
  )
    throw new Fatal("Recorded address differs from live Droplet");
  return true;
}

export function pretty(value: unknown, indent = 0): string {
  if (Array.isArray(value)) {
    if (value.length === 0) return "[ ]";
    return `[ ${value.map((item) => pretty(item, indent)).join(", ")} ]`;
  }
  if (value !== null && typeof value === "object") {
    const entries = Object.entries(value as Obj);
    if (entries.length === 0) return "{ }";
    const pad = " ".repeat(indent + 2);
    return `{\n${entries
      .map(
        ([key, nested]) =>
          `${pad}${JSON.stringify(key)} : ${pretty(nested, indent + 2)}`,
      )
      .join(",\n")}\n${" ".repeat(indent)}}`;
  }
  return JSON.stringify(value ?? null);
}
