from __future__ import annotations

import datetime
from typing import List
from sqlalchemy import select, desc
from sqlalchemy.ext.asyncio import AsyncSession

from .db import async_session_factory
from .schema import EventORM, OrderbookSnapshotORM
from ..domain.models import Event
from ..settings import Settings


class EventRepository:
    def __init__(self, settings: Settings):
        self.session_factory = async_session_factory()

    async def add_event(self, event: Event):
        async with self.session_factory() as session:
            orm = EventORM(
                isin=event.isin,
                ts=event.ts,
                ytm_mid=event.ytm_mid,
                ytm_event=event.ytm_event,
                delta_ytm_bps=event.delta_ytm_bps,
                ask_lots_window=event.ask_lots_window,
                ask_notional_window=event.ask_notional_window,
                spread_ytm_bps=event.spread_ytm_bps,
                score=event.score,
                stress_flag=event.stress_flag,
                near_maturity_flag=event.near_maturity_flag,
                payload_json=event.payload,
            )
            session.add(orm)
            await session.commit()

    async def list_recent(self, limit: int = 30) -> List[Event]:
        async with self.session_factory() as session:
            stmt = select(EventORM).order_by(desc(EventORM.ts)).limit(limit)
            rows = (await session.execute(stmt)).scalars().all()
            return [self._to_model(r) for r in rows]

    async def list_by_isin(self, isin: str, limit: int = 50) -> List[Event]:
        async with self.session_factory() as session:
            stmt = select(EventORM).where(EventORM.isin == isin).order_by(desc(EventORM.ts)).limit(limit)
            rows = (await session.execute(stmt)).scalars().all()
            return [self._to_model(r) for r in rows]

    def _to_model(self, orm: EventORM) -> Event:
        return Event(
            isin=orm.isin,
            ts=orm.ts,
            ytm_mid=orm.ytm_mid,
            ytm_event=orm.ytm_event,
            delta_ytm_bps=orm.delta_ytm_bps,
            ask_lots_window=orm.ask_lots_window,
            ask_notional_window=orm.ask_notional_window,
            spread_ytm_bps=orm.spread_ytm_bps,
            score=orm.score,
            stress_flag=orm.stress_flag,
            near_maturity_flag=orm.near_maturity_flag,
            payload=orm.payload_json,
        )


class SnapshotRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def add_snapshot(self, snapshot: OrderbookSnapshotORM):
        self.session.add(snapshot)
        await self.session.commit()

    async def list_recent(self, isin: str, limit: int = 100):
        stmt = select(OrderbookSnapshotORM).where(OrderbookSnapshotORM.isin == isin).order_by(desc(OrderbookSnapshotORM.ts)).limit(limit)
        rows = (await self.session.execute(stmt)).scalars().all()
        return rows
