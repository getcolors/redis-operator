import { readPars } from "red/cli";
import { preflight } from "red/lifecycle";
import * as dryRun from "red/dry-run";
import * as progress from "red/progress";
import {
  workflow as makeWorkflow,
  failed,
  type Opts,
  type WireDecl,
} from "red/workflow";
import {
  validate as redisValidate,
  defaults as redisDefaults,
} from "package-redis-red";
import * as t from "./tools.ts";
import * as operator from "./operator.ts";
export const defaults = {
  namespace: "colors-redis",
  "reconcile-interval": "60s",
  "deletion-policy": "Retain",
  "compute-prevent-destroy": true,
  "provider-compute": "digitalocean",
  "provider-backend": "r2",
  workdir: ".colors",
};
export const events = [
  "build",
  "create",
  "check",
  "rehearse",
  "drill",
  "restart",
  "delete",
];
const blocks: [[number, number, number, number], number][] = [
  [[0, 0, 0, 0], 8],
  [[10, 0, 0, 0], 8],
  [[100, 64, 0, 0], 10],
  [[127, 0, 0, 0], 8],
  [[169, 254, 0, 0], 16],
  [[172, 16, 0, 0], 12],
  [[192, 0, 0, 0], 24],
  [[192, 0, 2, 0], 24],
  [[192, 168, 0, 0], 16],
  [[198, 18, 0, 0], 15],
  [[198, 51, 100, 0], 24],
  [[203, 0, 113, 0], 24],
  [[224, 0, 0, 0], 4],
  [[240, 0, 0, 0], 4],
];
export function publicIpv4Host(value: any): boolean {
  if (typeof value !== "string" || !/^([0-9]+\.){3}[0-9]+\/32$/.test(value))
    return false;
  const parts = value.slice(0, -3).split(".");
  if (parts.some((p) => !/^(0|[1-9][0-9]{0,2})$/.test(p) || Number(p) > 255))
    return false;
  const ip = parts.reduce((n, p) => (n << 8) | Number(p), 0);
  return !blocks.some(([a, b]) => {
    const base = a.reduce((n, p) => (n << 8) | p, 0),
      mask = -1 << (32 - b);
    return (ip & mask) === (base & mask);
  });
}
export function stateErrors(o: Opts): string[] {
  const required = [
      "profile",
      "kube-context",
      "namespace",
      "resource-name",
      "image",
      "reconcile-interval",
      "deletion-policy",
      "compute-prevent-destroy",
      ...t.CONFIG_KEYS,
    ],
    errors = required
      .filter((k) => o[k] == null || o[k] === "")
      .map((k) => k + " is required");
  const patterns: Record<string, RegExp> = {
    profile: /^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$/,
    "kube-context": /^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,252}$/,
    namespace: /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/,
    "resource-name": /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/,
    image: /^[^\s]+@sha256:[a-f0-9]{64}$/,
    "image-pull-secret": /^[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?$/,
    "reconcile-interval": /^[1-9][0-9]*(ms|s|m|h)$/,
    "doks-cluster-id":
      /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/,
  };
  for (const [k, p] of Object.entries(patterns))
    if (k in o && !p.test(String(o[k]))) errors.push(k + " has invalid format");
  for (const [k, allowed] of [
    ["deletion-policy", ["Retain", "Destroy"]],
    ["provider-compute", ["digitalocean"]],
    ["provider-backend", ["r2"]],
  ] as const)
    if (!(allowed as readonly any[]).includes(o[k]))
      errors.push(k + " must be " + allowed.join(" or "));
  if (typeof o["compute-prevent-destroy"] !== "boolean")
    errors.push("compute-prevent-destroy must be true or false");
  const sources = o["digitalocean-ssh-sources"];
  if (
    !Array.isArray(sources) ||
    !sources.length ||
    !sources.every(publicIpv4Host)
  )
    errors.push("digitalocean-ssh-sources must list public IPv4 /32 networks");
  const shape = {
    ...redisDefaults,
    ...t.resource(o).spec.config,
    workdir: "/data/work",
    "redis-storage-managed": false,
    "compute-prevent-destroy": true,
    "provider-compute": "digitalocean",
    "provider-backend": "r2",
  };
  errors.push(
    ...redisValidate
      .stateErrors(shape)
      .filter((e: string) => !e.endsWith("is required")),
  );
  return errors;
}
export async function startStep(opts: Opts, env = process.env): Promise<Opts> {
  return preflight(
    { ...opts, "resource-name": opts["resource-name"] || opts.profile },
    {
      defaults,
      overlay: readPars,
      validators: [
        (_o, e) =>
          e.COLORS_PAR_PROFILE
            ? [
                "COLORS_PAR_PROFILE is set; profile must come from colors.yml only",
              ]
            : [],
        (o) => stateErrors(o),
        (_o, e, c) =>
          c.event === "create" && c.real
            ? t.CREDENTIALS.filter((k) => !e[k]?.trim()).map(
                (k) => k + " is required",
              )
            : [],
        (_o, e, c) =>
          c.event === "drill" &&
          e.COLORS_PAR_DRILL_DELETE_OWNED_DROPLET !== "true"
            ? [
                "drill deletes the owned Redis Droplet; set COLORS_PAR_DRILL_DELETE_OWNED_DROPLET=true",
              ]
            : [],
        (_o, e, c) =>
          c.event === "drill" && c.real && !e.COLORS_PAR_DO_TOKEN
            ? ["COLORS_PAR_DO_TOKEN is required"]
            : [],
        (o, _e, c) =>
          c.event === "delete" && o["compute-prevent-destroy"] !== false
            ? [
                "compute destruction is protected; set COLORS_PAR_COMPUTE_PREVENT_DESTROY=false to delete",
              ]
            : [],
      ],
    },
    env,
  );
}
export async function renderStep(o: Opts): Promise<Opts> {
  t.render(o);
  return { ...o, "red/exit": 0 };
}
export async function createStep(o: Opts): Promise<Opts> {
  await t.apply(o, t.manifests(o).items[0]);
  await t.apply(
    o,
    {
      apiVersion: "v1",
      kind: "Secret",
      metadata: { name: "redis-credentials", namespace: o.namespace },
      type: "Opaque",
      stringData: Object.fromEntries(
        t.CREDENTIALS.map((k) => [k, process.env[k]]),
      ),
    },
    true,
  );
  if (o["image-pull-secret"])
    await t.waitFor(
      "pull secret",
      async () => {
        try {
          return await t.kubectl(
            o,
            [
              "get",
              "secret",
              o["image-pull-secret"] as string,
              "-n",
              o.namespace as string,
              "-o",
              "name",
            ],
            { notFound: null },
          );
        } catch (e) {
          throw new t.Fatal(String(e));
        }
      },
      120,
    );
  await t.apply(o, t.manifests(o));
  await t.kubectl(
    o,
    [
      "wait",
      "--for=condition=Established",
      "crd/" + t.RESOURCE,
      "--timeout=60s",
    ],
    { requestTimeout: false, timeout: 90 },
  );
  await t.kubectl(
    o,
    [
      "rollout",
      "status",
      "deployment/" + t.CONTROLLER,
      "-n",
      o.namespace as string,
      "--timeout=600s",
    ],
    { requestTimeout: false, timeout: 630 },
  );
  const current = await t.getResource(o);
  if (current) t.active(current);
  await t.apply(o, t.resource(o));
  await t.waitReady(o, 2700, true);
  return { ...o, "red/exit": 0 };
}
export async function deleteStep(o: Opts): Promise<Opts> {
  const current = await t.getResource(o);
  if (current) {
    if (t.suspended(current))
      throw new t.Fatal(
        "Resource suspended; verify no workflow runs before delete",
      );
    if (current.spec.deletionPolicy !== "Destroy")
      await t.patch(o, current, [
        { op: "add", path: "/spec/deletionPolicy", value: "Destroy" },
      ]);
    if (!t.deleting(current))
      await t.kubectl(o, [
        "delete",
        t.RESOURCE,
        o["resource-name"] as string,
        "-n",
        o.namespace as string,
        "--wait=false",
        "--ignore-not-found",
      ]);
    await t.waitFor(
      "RedisDeployment finalizer",
      async () => (await t.getResource(o)) === null,
      1800,
      15,
    );
  }
  await t.kubectl(
    o,
    [
      "delete",
      "namespace",
      o.namespace as string,
      "--ignore-not-found",
      "--timeout=900s",
    ],
    { requestTimeout: false, timeout: 930 },
  );
  const remaining = await t.kubectl(
    o,
    ["get", t.RESOURCE, "-A", "-o", "json"],
    { parse: true, notFound: { items: [] } },
  );
  if (!remaining.items.length)
    await t.kubectl(o, ["delete", "crd", t.RESOURCE, "--ignore-not-found"]);
  return { ...o, "red/exit": 0 };
}
export async function operationStep(o: Opts): Promise<Opts> {
  await (
    {
      check: operator.check,
      rehearse: operator.rehearse,
      drill: operator.drill,
      restart: operator.restart,
    } as Record<string, (o: Opts) => Promise<void>>
  )[o["red/event"] as string]!(o);
  return { ...o, "red/exit": 0 };
}
export async function quiet(
  fn: (o: Opts) => Promise<Opts>,
  o: Opts,
): Promise<Opts> {
  try {
    return await fn(o);
  } catch (e) {
    return { ...o, "red/exit": 1, "red/err": String(e) };
  }
}
export function wireFn(step: string, o: Opts): WireDecl {
  const event = o["red/event"];
  if (step === "redis-operator/start")
    return [
      startStep,
      ["build", "create"].includes(event as string)
        ? "redis-operator/render"
        : "redis-operator/" + event,
    ];
  if (step === "redis-operator/render")
    return [
      renderStep,
      ...(event === "create" ? ["redis-operator/create"] : []),
    ];
  const fn =
    event === "create"
      ? createStep
      : event === "delete"
        ? deleteStep
        : operationStep;
  return [(o: Opts) => quiet(fn, o)];
}
export const operatorWorkflow = dryRun.advise(
  progress.advise(
    makeWorkflow({
      start: "redis-operator/start",
      wireFn,
      nextFn: (_s, successors, o) =>
        failed(o) ? [] : (successors ?? []).map((n) => [n, o]),
    }),
  ),
  events.filter((e) => e !== "build").map((e) => "redis-operator/" + e),
);
