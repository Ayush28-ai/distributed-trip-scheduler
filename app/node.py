"""Scheduler node entrypoint: python -m app.node   (NODE_ID=node1 ...)"""
import asyncio
import logging

from prometheus_client import start_http_server

from app.config import settings
from app.db import create_pool
from app.handlers import build_handlers
from app.log_config import log_event, setup_logging
from app.scheduler.dispatcher import Dispatcher
from app.scheduler.executor import Executor
from app.scheduler.materializer import Materializer
from app.scheduler.membership import Membership

log = logging.getLogger("node")


async def main() -> None:
    setup_logging(settings.log_level)
    pool = None
    for attempt in range(30):  # wait for Postgres
        try:
            pool = await create_pool(settings.database_url, min_size=2, max_size=20)
            break
        except Exception as e:
            log_event(log, "db_not_ready", attempt=attempt, error=str(e))
            await asyncio.sleep(2)
    if pool is None:
        raise SystemExit("database unavailable")

    start_http_server(settings.metrics_port)
    membership = Membership(pool, settings)
    executor = Executor(pool, settings, build_handlers(settings.notification_url))
    dispatcher = Dispatcher(pool, settings, membership, executor)
    materializer = Materializer(pool, settings, membership)
    log_event(log, "node_started", node=settings.node_id, metrics_port=settings.metrics_port)
    await membership.tick()
    await asyncio.gather(membership.run(), dispatcher.run(), materializer.run(), executor.renew_loop())


if __name__ == "__main__":
    asyncio.run(main())
