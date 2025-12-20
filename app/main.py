import asyncio
import logging
import contextlib
from contextlib import asynccontextmanager
from fastapi import FastAPI

from .logging import setup_logging
from .settings import get_settings
from .services.orderbooks import OrderbookOrchestrator
from .services.universe import UniverseService
from .services.events import EventService
from .adapters.telegram.bot import TelegramBot
from .web.api import api_router
from .web.views import view_router
from .storage.db import init_db

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


def create_app() -> FastAPI:
    app = FastAPI(lifespan=lifespan)
    app.include_router(api_router, prefix="/api")
    app.include_router(view_router)

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
