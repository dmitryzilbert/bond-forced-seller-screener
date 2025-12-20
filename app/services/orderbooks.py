from __future__ import annotations

import asyncio
import json
from pathlib import Path
import logging
from datetime import datetime

from ..domain.models import OrderBookSnapshot
from ..domain.detector import History, detect_event
from ..adapters.tinvest.client import TInvestClient
from ..adapters.tinvest.mapping import map_orderbook_payload
from ..settings import Settings
from ..services.events import EventService
from ..services.universe import UniverseService
from ..adapters.telegram.bot import TelegramBot

logger = logging.getLogger(__name__)


class OrderbookOrchestrator:
    def __init__(self, settings: Settings, universe: UniverseService, events: EventService, telegram: TelegramBot):
        self.settings = settings
        self.universe = universe
        self.events = events
        self.telegram = telegram
        self.history = History(
            max_points=self.settings.ask_window_history_size,
            flush_interval_seconds=self.settings.ask_window_flush_seconds,
        )
        self.client = TInvestClient(settings.tinvest_token, settings.tinvest_account_id, depth=settings.orderbook_depth)
        self._start_time = datetime.utcnow()
        self._last_metrics_log = datetime.utcnow()
        self._updates_count = 0
        self._dropped_updates = 0
        self._last_snapshot_ts: datetime | None = None
        self._active_subscriptions = 0

    async def run_mock_stream(self):
        instruments = self._filter_shortlist(await self.universe.shortlist())
        self._reset_metrics(len(instruments))
        instrument_map = {i.isin: i for i in instruments}
        payloads = Path("fixtures/orderbooks.ndjson").read_text().splitlines()
        for line in payloads:
            data = json.loads(line)
            snapshot = map_orderbook_payload(data)
            await self._handle_snapshot(snapshot, instrument_map, persist=False)
            await asyncio.sleep(0.05)

    async def run_prod_stream(self):
        instruments = self._filter_shortlist(await self.universe.shortlist())
        self._reset_metrics(len(instruments))
        instrument_map = {i.isin: i for i in instruments}
        async for snapshot in self.client.stream_orderbooks(instruments):
            await self._handle_snapshot(snapshot, instrument_map, persist=True)

    async def _persist_snapshot(self, snapshot: OrderBookSnapshot):
        from ..storage.repo import SnapshotRepository
        from ..storage.schema import OrderbookSnapshotORM
        from ..storage.db import async_session_factory

        async with async_session_factory() as session:
            repo = SnapshotRepository(session)
            orm = OrderbookSnapshotORM(
                isin=snapshot.isin,
                ts=snapshot.ts,
                bids_json=[level.model_dump() for level in snapshot.bids],
                asks_json=[level.model_dump() for level in snapshot.asks],
                best_bid=snapshot.best_bid,
                best_ask=snapshot.best_ask,
            )
            await repo.add_snapshot(orm)

    def _filter_shortlist(self, instruments: list) -> list:
        return [i for i in instruments if getattr(i, "is_shortlisted", False) and getattr(i, "eligible", False)]

    async def _handle_snapshot(self, snapshot: OrderBookSnapshot, instrument_map: dict, *, persist: bool):
        if not snapshot or (not snapshot.bids and not snapshot.asks):
            self._dropped_updates += 1
            return

        instrument = instrument_map.get(snapshot.isin)
        if not instrument:
            self._dropped_updates += 1
            return

        if persist:
            await self._persist_snapshot(snapshot)

        event = detect_event(
            snapshot,
            instrument,
            self.history,
            delta_ytm_max_bps=self.settings.delta_ytm_max_bps,
            ask_window_min_lots=self.settings.ask_window_min_lots,
            ask_window_min_notional=self.settings.ask_window_min_notional,
            ask_window_kvol=self.settings.ask_window_kvol,
            novelty_window_updates=self.settings.novelty_window_updates,
            novelty_window_seconds=self.settings.novelty_window_seconds,
            alert_hold_updates=self.settings.alert_hold_updates,
            spread_ytm_max_bps=self.settings.spread_ytm_max_bps,
            near_maturity_days=self.settings.near_maturity_days,
            stress_params={
                "stress_ytm_high_pct": self.settings.stress_ytm_high_pct,
                "stress_price_low_pct": self.settings.stress_price_low_pct,
                "stress_spread_ytm_bps": self.settings.stress_spread_ytm_bps,
                "stress_dev_peer_bps": self.settings.stress_dev_peer_bps,
            },
        )

        if event and event.alert:
            await self.events.save_event(event)
            await self.telegram.send_event(event, instrument)

        self._updates_count += 1
        self._last_snapshot_ts = snapshot.ts
        self._maybe_log_metrics()

    def _reset_metrics(self, active_subscriptions: int):
        self._start_time = datetime.utcnow()
        self._last_metrics_log = datetime.utcnow()
        self._updates_count = 0
        self._dropped_updates = 0
        self._last_snapshot_ts = None
        self._active_subscriptions = active_subscriptions
        logger.info(
            "Orderbook stream starting: %s active subscriptions", self._active_subscriptions
        )

    def _maybe_log_metrics(self):
        now = datetime.utcnow()
        if (now - self._last_metrics_log).total_seconds() < 60:
            return
        elapsed_minutes = max((now - self._start_time).total_seconds() / 60, 1 / 60)
        updates_per_min = self._updates_count / elapsed_minutes
        lag_seconds = (now - self._last_snapshot_ts).total_seconds() if self._last_snapshot_ts else None
        reconnects = getattr(self.client.stream, "reconnect_count", 0)
        logger.info(
            "Orderbook stream metrics | active=%s updates/min=%.2f lag_sec=%s dropped=%s reconnects=%s",
            self._active_subscriptions,
            updates_per_min,
            round(lag_seconds, 3) if lag_seconds is not None else None,
            self._dropped_updates,
            reconnects,
        )
        self._last_metrics_log = now
