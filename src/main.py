from __future__ import annotations

import asyncio
import logging
import signal
import sys

import uvicorn

from .api.app import app, set_route_manager
from .config.schema import load_config
from .router.mc_router import MCRouter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


async def run_api_server(config):
    uvi_config = uvicorn.Config(
        app,
        host=config.api_listen_host,
        port=config.api_listen_port,
        log_level="info",
    )
    server = uvicorn.Server(uvi_config)
    await server.serve()


async def main() -> None:
    config = load_config()
    logger.info(f"Loaded config: root_domain={config.root_domain}")

    mc_router = MCRouter()
    set_route_manager(mc_router.route_manager)

    loop = asyncio.get_event_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown(mc_router)))

    await mc_router.start()
    await run_api_server(config)


async def shutdown(mc_router: MCRouter) -> None:
    logger.info("Shutting down...")
    await mc_router.stop()
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    asyncio.get_event_loop().stop()


def cli() -> None:
    asyncio.run(main())


if __name__ == "__main__":
    cli()
