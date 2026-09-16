import { test, expect } from "bun:test";
import { readFileSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import * as t from "../src/tools.ts";
import * as w from "../src/workflow.ts";
import * as a from "../src/adapter.ts";
import { run } from "red/workflow";
const root = join(import.meta.dir, "../..");
const fixture = {
  ...w.defaults,
  ...(Bun.YAML.parse(
    readFileSync(join(root, "test/fixtures/colors.yml"), "utf8"),
  ) as t.Obj),
  "resource-name": "redis-operator-fixture",
};
test("render matches Green and never resets suspension", () => {
  for (const [name, value] of [
    ["manifests.json", t.manifests(fixture)],
    ["redis-deployment.json", t.resource(fixture)],
  ] as const)
    expect(value).toEqual(
      JSON.parse(
        readFileSync(
          join(
            root,
            "test/resources/golden/redis-operator-fixture/operator",
            name,
          ),
          "utf8",
        ),
      ),
    );
  expect(t.resource(fixture).spec).not.toHaveProperty("suspend");
});
test("build and dry run need no credentials", async () => {
  const workdir = mkdtempSync(join(tmpdir(), "operator-"));
  try {
    for (const event of ["build", "create"])
      expect(
        (
          await run(w.operatorWorkflow, {
            ...fixture,
            workdir,
            "red/event": event,
            "red/dry-run": true,
          })
        )["red/exit"],
      ).toBe(0);
  } finally {
    rmSync(workdir, { recursive: true });
  }
});
test("destruction and profile guards hold", async () => {
  for (const event of ["delete", "drill"])
    expect(
      (
        await w.startStep(
          { ...fixture, "red/event": event, "red/dry-run": true },
          {},
        )
      )["red/exit"],
    ).toBe(2);
  expect(
    (
      await w.startStep(
        { ...fixture, "red/event": "build" },
        { COLORS_PAR_PROFILE: "other" },
      )
    )["red/exit"],
  ).toBe(2);
  expect(
    (await w.startStep({ ...fixture, "red/event": "create" }, {}))["red/exit"],
  ).toBe(2);
});
test("SSH sources require public IPv4 /32", () => {
  for (const source of [
    "127.0.0.1/32",
    "10.0.0.1/32",
    "100.64.0.1/32",
    "192.0.2.1/32",
    "224.0.0.1/32",
    "8.8.8.8/24",
    "8.8.8.8",
  ])
    expect(
      w.stateErrors({ ...fixture, "digitalocean-ssh-sources": [source] })
        .length,
    ).toBeGreaterThan(0);
  expect(w.publicIpv4Host("8.8.8.8/32")).toBe(true);
});
test("ownership refuses workers and changed identities", () => {
  const observed = {
      providerId: "12",
      profile: "fixture",
      name: "fixture",
      ip: "8.8.8.8",
    },
    droplet = {
      id: 12,
      name: "fixture",
      tags: [],
      networks: { v4: [{ type: "public", ip_address: "8.8.8.8" }] },
    };
  expect(t.ownedDroplet(droplet, observed, "fixture", [])).toBe(true);
  for (const bad of [
    { ...droplet, id: 13 },
    { ...droplet, name: "other" },
    { ...droplet, tags: ["k8s:worker"] },
    { ...droplet, networks: { v4: [] } },
  ])
    expect(() => t.ownedDroplet(bad, observed, "fixture", [])).toThrow();
  expect(() => t.ownedDroplet(droplet, observed, "fixture", ["12"])).toThrow();
});
test("unreadable state never means absence; deletion retains ownership of absent droplet", async () => {
  await expect(
    a.observe(
      { profile: "fixture" },
      { inspect: async () => ({ status: "error" }) },
    ),
  ).rejects.toThrow("could not be read");
  const deps = {
    inspect: async () => ({
      status: "present",
      cluster: { nodes: [{ provider_id: "12" }] },
    }),
    providerGet: async () => null,
  };
  expect(
    await a.observe({ profile: "fixture", "red/event": "delete" }, deps),
  ).toEqual({ exists: true, matches: false, ready: false });
  expect(await a.observe({ profile: "fixture" }, deps)).toEqual({
    exists: false,
    matches: false,
    ready: false,
  });
});

test("uncertain rehearsal remains suspended, confirmed failure resumes", async () => {
  const { spyOn } = await import("bun:test");
  const { rehearse } = await import("../src/operator.ts");
  const directory = mkdtempSync(join(tmpdir(), "operator-rehearsal-"));
  const cr = {
    metadata: { uid: "uid", generation: 2, resourceVersion: "10" },
    spec: {},
    status: {},
  };
  const suspended = {
    metadata: { uid: "uid", generation: 3, resourceVersion: "11" },
    spec: { suspend: true },
    status: { phase: "Suspended", observedGeneration: 3 },
  };
  const ready = spyOn(t, "waitReady").mockResolvedValue(cr);
  const patch = spyOn(t, "patch").mockResolvedValue(suspended);
  const get = spyOn(t, "getResource").mockResolvedValue(suspended);
  const probe = spyOn(t, "probe").mockRejectedValue(
    new Error("transport timeout"),
  );
  try {
    await expect(rehearse({ ...fixture, workdir: directory })).rejects.toThrow(
      "resource left suspended",
    );
    expect(patch).toHaveBeenCalledTimes(1);
    patch.mockClear();
    probe.mockResolvedValue({ rehearsalPassed: false });
    await expect(rehearse({ ...fixture, workdir: directory })).rejects.toThrow(
      "Backup rehearsal failed",
    );
    expect(patch).toHaveBeenCalledTimes(2);
    expect(patch.mock.calls[1]![2][0]!.value).toBe(false);
  } finally {
    ready.mockRestore();
    patch.mockRestore();
    get.mockRestore();
    probe.mockRestore();
    rmSync(directory, { recursive: true });
  }
});

test("blocked finalizer prevents controller deletion", async () => {
  const { spyOn } = await import("bun:test");
  const get = spyOn(t, "getResource").mockResolvedValue({
    metadata: {},
    spec: { deletionPolicy: "Destroy" },
  });
  const kubectl = spyOn(t, "kubectl").mockResolvedValue("");
  const wait = spyOn(t, "waitFor").mockRejectedValue(
    new Error("finalizer blocked"),
  );
  try {
    await expect(w.deleteStep(fixture)).rejects.toThrow("finalizer blocked");
    expect(
      kubectl.mock.calls.some(
        (call) => call[1][0] === "delete" && call[1][1] === "namespace",
      ),
    ).toBe(false);
  } finally {
    get.mockRestore();
    kubectl.mockRestore();
    wait.mockRestore();
  }
});
