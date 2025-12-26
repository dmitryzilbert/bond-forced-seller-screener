from __future__ import annotations

from datetime import datetime, timedelta, timezone

from ..adapters.tinvest.client import TInvestClient
from ..domain.models import Instrument, OrderBookSnapshot
from ..domain.ytm import ytm_from_price
from ..services.orderbooks import OrderbookService
from ..services.pricing import price_from_snapshot
from ..settings import Settings
from ..storage.db import async_session_factory
from ..storage.repo import InstrumentRepository, SnapshotRepository
from ..storage.schema import OrderbookSnapshotORM


class InstrumentSummaryService:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.session_factory = async_session_factory()
        self.orderbook_service = OrderbookService(settings)
        self.client = TInvestClient(
            settings.tinvest_token,
            settings.tinvest_account_id,
            depth=settings.orderbook_depth,
            app_env=settings.app_env,
            grpc_target_prod=settings.tinvest_grpc_target_prod,
            grpc_target_sandbox=settings.tinvest_grpc_target_sandbox,
            ssl_ca_bundle=settings.tinvest_ssl_ca_bundle,
            stream_heartbeat_interval_s=settings.stream_heartbeat_interval_s,
        )

    async def get_summary(self, isin: str) -> dict:
        instrument = await self._load_instrument(isin)
        snapshot = await self.orderbook_service.latest_snapshot(isin)
        snapshot_summary = self._snapshot_summary(snapshot)
        snapshots_last_1h = await self._snapshots_last_hours(isin, hours=1)
        ytm_mid = self._ytm_mid(snapshot_summary, instrument)
        diagnostics = self._build_diagnostics(
            snapshot=snapshot,
            snapshots_last_1h=snapshots_last_1h,
            instrument=instrument,
        )
        return {
            "instrument": instrument.model_dump() if instrument else None,
            "latest_snapshot": snapshot_summary,
            "ytm_mid": ytm_mid,
            "diagnostics": diagnostics,
        }

    async def refresh_snapshot(self, isin: str, *, depth: int = 10) -> dict:
        instrument = await self._load_instrument(isin)
        if instrument is None:
            raise ValueError("instrument_not_found")
        snapshot = await self.client.fetch_orderbook_snapshot(
            instrument,
            depth=depth,
            timeout=self.settings.orderbook_bootstrap_timeout_s,
        )
        if snapshot is not None:
            snapshot.isin = instrument.isin
            await self._persist_snapshot(snapshot)
        return await self.get_summary(isin)

    async def _load_instrument(self, isin: str) -> Instrument | None:
        async with self.session_factory() as session:
            repo = InstrumentRepository(session)
            return await repo.get_by_isin(isin)

    async def _snapshots_last_hours(self, isin: str, *, hours: int) -> int:
        since = datetime.now(timezone.utc) - timedelta(hours=hours)
        async with self.session_factory() as session:
            repo = SnapshotRepository(session)
            return await repo.count_since(isin, since)

    def _snapshot_summary(self, snapshot: OrderBookSnapshot | None) -> dict | None:
        if snapshot is None:
            return None
        prices = price_from_snapshot(snapshot)
        best_bid = prices["best_bid"]
        best_ask = prices["best_ask"]
        spread = None
        if best_bid is not None and best_ask is not None:
            spread = best_ask - best_bid
        return {
            "ts": snapshot.ts.isoformat(),
            "best_bid": best_bid,
            "best_ask": best_ask,
            "mid": prices["mid"],
            "has_bids": bool(snapshot.bids),
            "has_asks": bool(snapshot.asks),
            "spread": spread,
        }

    def _ytm_mid(self, snapshot_summary: dict | None, instrument: Instrument | None) -> float | None:
        if snapshot_summary is None or instrument is None:
            return None
        mid = snapshot_summary.get("mid")
        if mid is None or instrument.nominal is None or instrument.maturity_date is None:
            return None
        return ytm_from_price(
            mid,
            instrument.nominal,
            instrument.maturity_date,
            eligible=instrument.eligible,
        )

    def _build_diagnostics(
        self,
        *,
        snapshot: OrderBookSnapshot | None,
        snapshots_last_1h: int,
        instrument: Instrument | None,
    ) -> dict:
        reason = "unknown"
        now = datetime.now(timezone.utc)
        if snapshot is None:
            if self._market_closed(now):
                reason = "market_closed"
            else:
                reason = "no_snapshots"
        elif not snapshot.asks:
            reason = "no_asks"
        elif snapshots_last_1h < self.settings.novelty_window_updates:
            reason = "not_enough_updates"
        return {
            "snapshots_last_1h_count": snapshots_last_1h,
            "reason_event_unavailable": reason,
            "nominal_missing": not bool(getattr(instrument, "nominal", None)),
        }

    def _market_closed(self, now: datetime) -> bool:
        start = self.settings.alert_night_silent_start
        end = self.settings.alert_night_silent_end
        if start < end:
            return start <= now.hour < end
        return now.hour >= start or now.hour < end

    async def _persist_snapshot(self, snapshot: OrderBookSnapshot) -> None:
        async with self.session_factory() as session:
            repo = SnapshotRepository(session)
            orm = OrderbookSnapshotORM(
                isin=snapshot.isin,
                ts=snapshot.ts,
                bids_json=[level.model_dump() for level in snapshot.bids],
                asks_json=[level.model_dump() for level in snapshot.asks],
                best_bid=snapshot.best_bid,
                best_ask=snapshot.best_ask,
                nominal=snapshot.nominal,
            )
            await repo.add_snapshot(orm)
