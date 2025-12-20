from __future__ import annotations

import datetime
from typing import List, Dict, Optional
from sqlalchemy import select, desc, update, func
from sqlalchemy.ext.asyncio import AsyncSession

from .db import async_session_factory
from .schema import EventORM, OrderbookSnapshotORM, InstrumentORM
from ..domain.models import Event, Instrument, OrderBookLevel, OrderBookSnapshot
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

    async def list_filtered(
        self,
        *,
        since: datetime.datetime,
        limit: int,
        sort_by: str,
        order: str,
        stress_flag: Optional[bool] = None,
        near_maturity_flag: Optional[bool] = None,
        min_notional: Optional[float] = None,
        min_delta_bps: Optional[float] = None,
        isin: Optional[str] = None,
    ) -> List[Event]:
        async with self.session_factory() as session:
            stmt = select(EventORM).where(EventORM.ts >= since)

            if stress_flag is not None:
                stmt = stmt.where(EventORM.stress_flag.is_(stress_flag))

            if near_maturity_flag is not None:
                stmt = stmt.where(EventORM.near_maturity_flag.is_(near_maturity_flag))

            if min_notional is not None:
                stmt = stmt.where(EventORM.ask_notional_window >= float(min_notional))

            if min_delta_bps is not None:
                stmt = stmt.where(func.abs(EventORM.delta_ytm_bps) >= float(min_delta_bps))

            if isin:
                stmt = stmt.where(EventORM.isin == isin)

            sort_column = EventORM.score
            if sort_by == "ytm_event":
                sort_column = EventORM.ytm_event
            elif sort_by == "ts":
                sort_column = EventORM.ts

            stmt = stmt.order_by(desc(sort_column) if order == "desc" else sort_column)
            stmt = stmt.limit(limit)

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


class InstrumentRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def upsert_many(self, instruments: list[Instrument]):
        for instrument in instruments:
            existing = await self.session.get(InstrumentORM, instrument.isin)
            if existing is None:
                existing = InstrumentORM(isin=instrument.isin)
                self.session.add(existing)
            existing.figi = instrument.figi
            existing.name = instrument.name
            existing.issuer = instrument.issuer
            existing.nominal = instrument.nominal
            existing.maturity_date = instrument.maturity_date
            existing.segment = instrument.segment
            existing.updated_at = instrument.eligibility_checked_at
            existing.amortization_flag = instrument.amortization_flag
            existing.has_call_offer = instrument.has_call_offer
            existing.eligible = instrument.eligible
            existing.eligible_reason = instrument.eligible_reason
            existing.eligibility_checked_at = instrument.eligibility_checked_at
            existing.is_shortlisted = instrument.is_shortlisted
        await self.session.commit()

    async def list_all(self) -> list[Instrument]:
        stmt = select(InstrumentORM)
        rows = (await self.session.execute(stmt)).scalars().all()
        return [self._to_model(r) for r in rows]

    async def list_shortlisted(self) -> list[Instrument]:
        stmt = select(InstrumentORM).where(InstrumentORM.is_shortlisted.is_(True))
        rows = (await self.session.execute(stmt)).scalars().all()
        return [self._to_model(r) for r in rows]

    async def index_by_isin(self) -> Dict[str, InstrumentORM]:
        stmt = select(InstrumentORM)
        rows = (await self.session.execute(stmt)).scalars().all()
        return {row.isin: row for row in rows}

    async def set_shortlist(self, shortlisted_isins: set[str]):
        stmt_true = update(InstrumentORM).values(is_shortlisted=False)
        await self.session.execute(stmt_true)
        if shortlisted_isins:
            stmt_mark = (
                update(InstrumentORM)
                .where(InstrumentORM.isin.in_(list(shortlisted_isins)))
                .values(is_shortlisted=True)
            )
            await self.session.execute(stmt_mark)
        await self.session.commit()

    def _to_model(self, orm: InstrumentORM) -> Instrument:
        return Instrument(
            isin=orm.isin,
            figi=orm.figi,
            name=orm.name,
            issuer=orm.issuer,
            nominal=orm.nominal,
            maturity_date=orm.maturity_date,
            segment=orm.segment,
            amortization_flag=orm.amortization_flag,
            has_call_offer=orm.has_call_offer,
            eligible=orm.eligible,
            eligible_reason=orm.eligible_reason,
            eligibility_checked_at=orm.eligibility_checked_at,
            is_shortlisted=orm.is_shortlisted,
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

    async def latest(self, isin: str) -> OrderBookSnapshot | None:
        stmt = (
            select(OrderbookSnapshotORM)
            .where(OrderbookSnapshotORM.isin == isin)
            .order_by(desc(OrderbookSnapshotORM.ts))
            .limit(1)
        )
        row = (await self.session.execute(stmt)).scalar_one_or_none()
        if row is None:
            return None
        return self._to_snapshot(row)

    async def list_all(self) -> list[OrderBookSnapshot]:
        stmt = select(OrderbookSnapshotORM)
        rows = (await self.session.execute(stmt)).scalars().all()
        return [self._to_snapshot(row) for row in rows]

    def _to_snapshot(self, orm: OrderbookSnapshotORM) -> OrderBookSnapshot:
        bids = [OrderBookLevel(**level) for level in orm.bids_json or []]
        asks = [OrderBookLevel(**level) for level in orm.asks_json or []]
        return OrderBookSnapshot(
            isin=orm.isin,
            ts=orm.ts,
            bids=bids,
            asks=asks,
            nominal=orm.nominal or 1000.0,
        )
