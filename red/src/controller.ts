import { controller, start, stop } from "red/kubernetes";
import { kubectlClient } from "red/kubernetes/client";
import { checkEnvironment, redisPackage } from "./adapter.ts";
export async function main(args = process.argv.slice(2)) {
  const [context, namespace = "colors-dev"] = args;
  if (!context)
    throw new Error("Usage: controller <context|--in-cluster> [namespace]");
  checkEnvironment(process.env);
  const client = kubectlClient(
    context === "--in-cluster" ? { inCluster: true } : { context },
  );
  const runtime = start(
    controller({
      packages: [redisPackage()],
      client,
      namespace,
      workers: 1,
      pollMs: 2000,
    }),
  );
  console.log("RedisDeployment controller running");
  await new Promise<void>((resolve) => {
    const done = () => resolve();
    process.once("SIGINT", done);
    process.once("SIGTERM", done);
  });
  await stop(runtime);
}
if (import.meta.main) await main();
