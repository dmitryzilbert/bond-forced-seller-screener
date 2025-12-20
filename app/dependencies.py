from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from .settings import get_settings
from .storage.db import async_session_factory


@asynccontextmanager
async def get_session() -> AsyncIterator:
    async_session = async_session_factory()
    async with async_session() as session:
        yield session


def get_app_env() -> str:
    return get_settings().app_env
