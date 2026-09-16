import asyncio, signal, sys, os
from blue.kubernetes import controller, start, stop
from blue.kubernetes_client import kubectl_client
from .adapter import package, check_environment


async def main(args=None):
    args = sys.argv[1:] if args is None else args
    if not args:
        raise ValueError("Usage: controller <context|--in-cluster> [namespace]")
    check_environment(os.environ)
    client = kubectl_client(
        {"in_cluster": True} if args[0] == "--in-cluster" else {"context": args[0]}
    )
    runtime = start(
        controller(
            {
                "packages": [package()],
                "client": client,
                "namespace": args[1] if len(args) > 1 else "colors-dev",
                "workers": 1,
                "poll_ms": 2000,
            }
        )
    )
    done = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, done.set)
    print("RedisDeployment controller running", flush=True)
    await done.wait()
    await stop(runtime)


if __name__ == "__main__":
    asyncio.run(main())
