import { existsSync, readFileSync } from "node:fs";
import { resolve, dirname, join } from "node:path";
import { run } from "red/workflow";
import { events, operatorWorkflow } from "./workflow.ts";
export async function main(argv = process.argv.slice(2)): Promise<number> {
  let event = "",
    file = "",
    dry = false;
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i]!;
    if (arg === "--file") file = argv[++i] ?? "";
    else if (arg === "--dry-run") dry = true;
    else if (!event && !arg.startsWith("-")) event = arg;
    else {
      console.error("Unknown argument: " + arg);
      return 2;
    }
  }
  if (!events.includes(event)) {
    console.error(
      "Usage: redis-operator <" +
        events.join("|") +
        "> [--file colors.yml] [--dry-run]",
    );
    return 2;
  }
  if (!file) {
    let directory = process.cwd();
    for (;;) {
      if (existsSync(join(directory, "colors.yml"))) {
        file = join(directory, "colors.yml");
        break;
      }
      const parent = dirname(directory);
      if (parent === directory) break;
      directory = parent;
    }
  }
  if (!file) {
    console.error("colors.yml not found");
    return 2;
  }
  try {
    file = resolve(file);
    const opts = Bun.YAML.parse(readFileSync(file, "utf8")) as Record<
      string,
      any
    >;
    process.chdir(dirname(file));
    const result = await run(operatorWorkflow, {
      ...opts,
      "red/event": event,
      "red/dry-run": dry,
    });
    if (result["red/err"]) console.error(result["red/err"]);
    return Number(result["red/exit"] ?? 0);
  } catch (error) {
    console.error(String(error));
    return 2;
  }
}
if (import.meta.main) process.exit(await main());
