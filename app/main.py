import asyncio
import logging
import contextlib
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse, JSONResponse
from sqlalchemy import select

from .logging import setup_logging
from .settings import get_settings
from .services.orderbooks import OrderbookOrchestrator
from .services.universe import UniverseService
from .services.events import EventService
from .adapters.telegram.bot import TelegramBot
from .web.api import api_router
from .web.views import view_router
from .storage.db import init_db, close_db
from .storage.db import async_session_factory
from .services.metrics import get_metrics

setup_logging()
logger = logging.getLogger(__name__)
settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    universe = UniverseService(settings)
    event_service = EventService(settings)
    telegram = TelegramBot(settings)
    orchestrator = OrderbookOrchestrator(settings, universe, event_service, telegram)

    worker_task: asyncio.Task | None = None
    if settings.app_env == "mock":
        worker_task = asyncio.create_task(orchestrator.run_mock_stream())
    else:
        worker_task = asyncio.create_task(orchestrator.run_prod_stream())

    try:
        yield
    finally:
        if worker_task:
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker_task
        with contextlib.suppress(Exception):
            await telegram.close()
        with contextlib.suppress(Exception):
            await close_db()


def create_app() -> FastAPI:
    app = FastAPI(lifespan=lifespan)
    app.include_router(api_router, prefix="/api")
    app.include_router(view_router)

    @app.get("/health")
    async def health():
        db_ok = True
        try:
            session_factory = async_session_factory()
            async with session_factory() as session:
                await session.execute(select(1))
        except Exception:
            logger.exception("DB health check failed")
            db_ok = False

        metrics = get_metrics()
        last_update = metrics.last_update_ts
        if last_update and last_update.tzinfo is None:
            last_update = last_update.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        stale = (
            last_update is None
            or (now - last_update).total_seconds() > settings.liveness_max_stale_seconds
        )

        ok = db_ok and not stale
        status_code = 200 if ok else 503
        return JSONResponse(
            content={
                "status": "ok" if ok else "fail",
                "db_ok": db_ok,
                "last_update_ts": last_update.isoformat() if last_update else None,
                "stale_seconds": (now - last_update).total_seconds() if last_update else None,
            },
            status_code=status_code,
        )

    @app.get("/metrics", response_class=PlainTextResponse)
    async def metrics():
        return PlainTextResponse(get_metrics().render())

    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
