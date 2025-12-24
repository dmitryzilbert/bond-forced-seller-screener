from __future__ import annotations

import asyncio
import logging
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import declarative_base

from ..settings import get_settings

logger = logging.getLogger(__name__)

Base = declarative_base()
_engine = None
_session_factory = None


def get_engine():
    global _engine
    if _engine is None:
        settings = get_settings()
        _engine = create_async_engine(settings.database_url, echo=False, future=True)
    return _engine


def async_session_factory() -> async_sessionmaker[AsyncSession]:
    global _session_factory
    if _session_factory is None:
        _session_factory = async_sessionmaker(bind=get_engine(), expire_on_commit=False)
    return _session_factory


async def init_db():
    engine = get_engine()
    from .schema import InstrumentORM, OrderbookSnapshotORM, EventORM

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("DB initialized at %s", engine.url)


async def close_db():
    global _engine
    global _session_factory
    if _engine is not None:
        await _engine.dispose()
        logger.info("DB engine disposed")
        _engine = None
        _session_factory = None
