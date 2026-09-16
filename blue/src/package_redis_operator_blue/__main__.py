import argparse, asyncio, os, sys
from pathlib import Path
import yaml
from blue.workflow import run
from .workflow import WORKFLOW, EVENTS


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("event", choices=EVENTS)
    parser.add_argument("--file")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    path = (
        Path(args.file).resolve()
        if args.file
        else next(
            (
                p / "colors.yml"
                for p in [Path.cwd(), *Path.cwd().parents]
                if (p / "colors.yml").exists()
            ),
            None,
        )
    )
    if path is None:
        parser.error("colors.yml not found")
    try:
        opts = yaml.safe_load(path.read_text())
        os.chdir(path.parent)
        result = asyncio.run(
            run(
                WORKFLOW,
                {**opts, "blue/event": args.event, "blue/dry-run": args.dry_run},
            )
        )
        if result.get("blue/err"):
            print(result["blue/err"], file=sys.stderr)
        raise SystemExit(result.get("blue/exit", 0))
    except (ValueError, OSError, yaml.YAMLError) as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(2)


if __name__ == "__main__":
    main()
