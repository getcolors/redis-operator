import { statSync, existsSync } from "node:fs";
import { run, failed } from "red/workflow";
import { redisWorkflow } from "package-redis-red";
import * as a from "./adapter.ts";
import * as t from "./tools.ts";
export function safeToken(v: any): string {
  if (typeof v !== "string" || !/^[A-Za-z0-9:_-]{1,160}$/.test(v))
    throw new Error("Invalid probe token");
  return v;
}
export async function redisCommand(
  node: t.Obj,
  command: string,
): Promise<string> {
  const result = await a.remote(
    node,
    a.healthCommand.replace("PING'", "--raw " + command + "'"),
  );
  if (result.exit) throw new Error("Remote probe failed");
  return result.out.trim();
}
export async function probe(
  resource: string,
  namespace: string,
  operation: string,
  key?: string,
  value?: string,
): Promise<t.Obj> {
  safeToken(resource);
  safeToken(namespace);
  const result = await t.command(
    [
      "kubectl",
      "get",
      "redisdeployment",
      resource,
      "-n",
      namespace,
      "-o",
      "json",
    ],
    "",
    30,
  );
  if (result.exit) throw new Error("Resource read failed");
  const cr = JSON.parse(result.out),
    config = {
      ...cr.spec.config,
      profile:
        cr.spec.config.profile ??
        cr.status?.profile ??
        namespace + "--" + resource,
    },
    o = a.options(config),
    state = await a.inspect(o);
  if (state.status !== "present") throw new Error("State not ready");
  const node = state.cluster!.nodes[0],
    path = a.markerPath(config),
    e: t.Obj = {
      providerId: String(node.provider_id),
      name: node.name,
      ip: node.ip,
      profile: o.profile,
      convergenceRecordModifiedMs: existsSync(path)
        ? Math.floor(statSync(path).mtimeMs)
        : 0,
    };
  switch (operation) {
    case "inspect":
      break;
    case "health":
      e.healthy = (await redisCommand(node, "PING")) === "PONG";
      break;
    case "set-marker":
      safeToken(key);
      safeToken(value);
      if ((await redisCommand(node, "SET " + key + " " + value)) !== "OK")
        throw new Error("Marker write failed");
      e.marker = await redisCommand(node, "GET " + key);
      break;
    case "get-marker":
      e.marker = await redisCommand(node, "GET " + safeToken(key));
      break;
    case "rehearse": {
      if (!t.acknowledged(cr))
        throw new Error(
          "Backup rehearsal requires acknowledged controller suspension",
        );
      const log = console.log,
        error = console.error;
      try {
        console.log = () => {};
        console.error = () => {};
        e.rehearsalPassed = !failed(
          await run(redisWorkflow, { ...o, "red/event": "rehearse" }),
        );
      } finally {
        console.log = log;
        console.error = error;
      }
      break;
    }
    default:
      throw new Error("Unknown probe operation");
  }
  return e;
}
if (import.meta.main) {
  try {
    const [resource, namespace, operation, key, value] = process.argv.slice(2);
    console.log(
      JSON.stringify(
        await probe(resource!, namespace!, operation!, key, value),
      ),
    );
  } catch {
    console.error("Probe failed; captured output suppressed");
    process.exit(1);
  }
}
