from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

from ..adapters.tinvest.client import TInvestClient
from ..adapters.tinvest.mapping import map_orderbook_payload
from ..domain.models import Instrument, OrderBookSnapshot
from ..settings import Settings
from ..storage.db import async_session_factory
from ..storage.repo import InstrumentRepository, SnapshotRepository
from .metrics import get_metrics

logger = logging.getLogger(__name__)


@dataclass
class LivenessMetrics:
    updates_per_hour: float
    max_notional: float


@dataclass
class ShortlistSummary:
    universe_size: int
    eligible_size: int
    shortlisted_size: int
    exclusion_reasons: dict[str, int]


class UniverseService:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.client = TInvestClient(
            settings.tinvest_token, settings.tinvest_account_id, depth=settings.orderbook_depth
        )
        self.session_factory = async_session_factory()
        self.instruments: list[Instrument] = []

    async def load_source_instruments(self) -> List[Instrument]:
        if self.settings.app_env == "mock":
            path = Path("fixtures/instruments.json")
            raw = json.loads(path.read_text())
            parsed = []
            for item in raw:
                maturity = item.get("maturity_date")
                if isinstance(maturity, str):
                    item = {**item, "maturity_date": datetime.fromisoformat(maturity).date()}
                parsed.append(Instrument(**item))
            data = parsed
            return [self._with_defaults(item) for item in data]
        instruments = await self.client.list_bonds()
        return [self._with_defaults(i) for i in instruments]

    async def load(self) -> List[Instrument]:
        async with self.session_factory() as session:
            repo = InstrumentRepository(session)
            instruments = await repo.list_all()
        self.instruments = instruments
        return instruments

    async def shortlist(self) -> List[Instrument]:
        async with self.session_factory() as session:
            repo = InstrumentRepository(session)
            shortlisted = await repo.list_shortlisted()
        if shortlisted:
            self.instruments = shortlisted
            return shortlisted
        if self.settings.app_env == "mock":
            await self.rebuild_shortlist()
            async with self.session_factory() as session:
                repo = InstrumentRepository(session)
                shortlisted = await repo.list_shortlisted()
                self.instruments = shortlisted
                return shortlisted
        self.instruments = []
        return []

    async def rebuild_shortlist(self) -> ShortlistSummary:
        instruments = await self.load_source_instruments()
        checked = await self._apply_eligibility(instruments)
        exclusion_reasons: dict[str, int] = {}
        for inst in checked:
            if not inst.eligible:
                key = inst.eligible_reason or "missing_data"
                exclusion_reasons[key] = exclusion_reasons.get(key, 0) + 1

        shortlisted, shortlist_reasons = await self._apply_shortlist(checked)
        for key, value in shortlist_reasons.items():
            exclusion_reasons[key] = exclusion_reasons.get(key, 0) + value
        return ShortlistSummary(
            universe_size=len(instruments),
            eligible_size=len([i for i in checked if i.eligible]),
            shortlisted_size=len(shortlisted),
            exclusion_reasons=exclusion_reasons,
        )

    async def _apply_eligibility(self, instruments: list[Instrument]) -> list[Instrument]:
        now = datetime.utcnow()
        async with self.session_factory() as session:
            repo = InstrumentRepository(session)
            cache = await repo.index_by_isin()

        result: list[Instrument] = []
        for instrument in instruments:
            cached = cache.get(instrument.isin)
            has_call_offer = await self._resolve_call_offer(instrument, cached)
            amortization_flag = instrument.amortization_flag

            eligible_reason = "missing_data"
            eligible = False
            if amortization_flag is True:
                eligible_reason = "amortization"
            elif has_call_offer is True:
                eligible_reason = "call_offer"
            elif amortization_flag is False and has_call_offer is False:
                eligible_reason = "ok"
                eligible = True

            updated = self._copy_instrument(
                instrument,
                {
                    "has_call_offer": has_call_offer,
                    "amortization_flag": amortization_flag,
                    "eligible": eligible,
                    "eligible_reason": eligible_reason,
                    "eligibility_checked_at": now if not (cached and self._should_use_cache(cached, instrument)) else cached.eligibility_checked_at,
                    "is_shortlisted": False,
                },
            )
            result.append(updated)

        async with self.session_factory() as session:
            repo = InstrumentRepository(session)
            await repo.upsert_many(result)
        return result

    async def _apply_shortlist(self, instruments: list[Instrument]):
        eligible = [i for i in instruments if i.eligible]
        liveness_metrics = await self._collect_liveness_metrics()
        shortlisted: list[Instrument] = []
        exclusion_reasons: dict[str, int] = {}

        def track(reason: str):
            exclusion_reasons[reason] = exclusion_reasons.get(reason, 0) + 1

        candidates: list[tuple[Instrument, LivenessMetrics | None]] = []
        for inst in eligible:
            candidates.append((inst, liveness_metrics.get(inst.isin)))

        filtered: list[Instrument] = []
        for inst, metrics in candidates:
            if metrics is None:
                track("missing_data")
                continue
            if metrics.max_notional < self.settings.shortlist_min_notional:
                track("not_enough_notional")
                continue
            if metrics.updates_per_hour < self.settings.shortlist_min_updates_per_hour:
                track("not_enough_updates")
                continue
            filtered.append(inst)

        filtered.sort(key=lambda item: (liveness_metrics[item.isin].updates_per_hour, liveness_metrics[item.isin].max_notional), reverse=True)
        shortlisted = filtered[: self.settings.shortlist_max]
        shortlisted_isins = {i.isin for i in shortlisted}

        for inst in instruments:
            inst.is_shortlisted = inst.isin in shortlisted_isins

        async with self.session_factory() as session:
            repo = InstrumentRepository(session)
            await repo.set_shortlist(shortlisted_isins)

        metrics_tracker = get_metrics()
        metrics_tracker.set_instrument_totals(
            eligible=len(eligible), shortlisted=len(shortlisted)
        )

        self.instruments = shortlisted
        return shortlisted, exclusion_reasons

    async def _collect_liveness_metrics(self) -> dict[str, LivenessMetrics]:
        snapshots = await self._load_orderbook_snapshots()
        grouped: dict[str, list[OrderBookSnapshot]] = {}
        for snap in snapshots:
            grouped.setdefault(snap.isin, []).append(snap)

        metrics: dict[str, LivenessMetrics] = {}
        for isin, snaps in grouped.items():
            snaps_sorted = sorted(snaps, key=lambda s: s.ts)
            duration_hours = (snaps_sorted[-1].ts - snaps_sorted[0].ts).total_seconds() / 3600
            duration_hours = max(duration_hours, 1.0)
            updates_per_hour = len(snaps_sorted) / duration_hours
            max_notional = max(self._snapshot_notional(s) for s in snaps_sorted)
            metrics[isin] = LivenessMetrics(updates_per_hour=updates_per_hour, max_notional=max_notional)
        return metrics

    async def _load_orderbook_snapshots(self) -> list[OrderBookSnapshot]:
        if self.settings.app_env == "mock":
            payloads = Path("fixtures/orderbooks.ndjson").read_text().splitlines()
            return [map_orderbook_payload(json.loads(line)) for line in payloads if line]
        async with self.session_factory() as session:
            repo = SnapshotRepository(session)
            return await repo.list_all()

    def _snapshot_notional(self, snap: OrderBookSnapshot) -> float:
        candidates = []
        if snap.bids:
            candidates.append(snap.bids[0].price * snap.bids[0].lots * snap.nominal)
        if snap.asks:
            candidates.append(snap.asks[0].price * snap.asks[0].lots * snap.nominal)
        return max(candidates) if candidates else 0.0

    async def _resolve_call_offer(self, instrument: Instrument, cached=None) -> bool | None:
        if self._should_use_cache(cached, instrument):
            return cached.has_call_offer
        if self.settings.app_env == "mock":
            return instrument.has_call_offer
        try:
            return await self.client.has_future_call_offer(instrument)
        except Exception:
            logger.exception("Failed to fetch bond events for %s", instrument.isin)
            return None

    def _should_use_cache(self, cached, instrument: Instrument) -> bool:
        if not cached or not cached.eligibility_checked_at:
            return False
        if cached.maturity_date != instrument.maturity_date:
            return False
        return cached.eligibility_checked_at >= datetime.utcnow() - timedelta(days=1)

    def _with_defaults(self, instrument: Instrument) -> Instrument:
        amortization_flag = instrument.amortization_flag
        if amortization_flag is None:
            amortization_flag = False
        has_call_offer = instrument.has_call_offer
        if has_call_offer is None and self.settings.app_env == "mock":
            has_call_offer = False
        return self._copy_instrument(
            instrument,
            {
                "amortization_flag": amortization_flag,
                "has_call_offer": has_call_offer,
            },
        )

    def _copy_instrument(self, instrument: Instrument, updates: dict) -> Instrument:
        data = instrument.__dict__.copy()
        data.update(updates)
        return Instrument(**data)
