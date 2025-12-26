from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List

import grpc
import httpx

from ..adapters.tinvest.client import TInvestClient
from ..adapters.tinvest.mapping import map_orderbook_payload
from ..domain.models import Instrument, OrderBookSnapshot
from ..services.pricing import price_from_snapshot
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
    missing_reasons: dict[str, int] = field(default_factory=dict)
    missing_examples: list[dict] = field(default_factory=list)
    failed_bond_events: int = 0
    price_enriched: int = 0


class UniverseService:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.client = TInvestClient(
            settings.tinvest_token,
            settings.tinvest_account_id,
            depth=settings.orderbook_depth,
            app_env=settings.app_env,
            grpc_target_prod=settings.tinvest_grpc_target_prod,
            grpc_target_sandbox=settings.tinvest_grpc_target_sandbox,
            ssl_ca_bundle=settings.tinvest_ssl_ca_bundle,
        )
        self.session_factory = async_session_factory()
        self.instruments: list[Instrument] = []
        self._shortlist_flags: dict[str, dict] = {}
        self.failed_bond_events = 0

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
            shortlisted = [self._apply_cached_flags(i) for i in shortlisted]
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

        allow_unknown_call_offer = bool(
            getattr(self.settings, "allow_missing_call_offer", False)
            or getattr(self.settings, "allow_unknown_call_offer", False)
            or getattr(self.settings, "shortlist_allow_missing_call_offer", False)
            or getattr(self.settings, "shortlist_allow_unknown_call_offer", False)
            or not getattr(self.settings, "exclude_call_offer_unknown", True)
        )
        shortlist_candidates: list[Instrument] = []
        for inst in checked:
            if inst.eligible and inst.has_call_offer is None and not allow_unknown_call_offer:
                exclusion_reasons["call_offer_unknown"] = exclusion_reasons.get("call_offer_unknown", 0) + 1
                continue
            shortlist_candidates.append(inst)

        (
            shortlisted,
            shortlist_reasons,
            missing_reasons,
            missing_examples,
            price_enriched,
        ) = await self._apply_shortlist(shortlist_candidates)
        for key, value in shortlist_reasons.items():
            exclusion_reasons[key] = exclusion_reasons.get(key, 0) + value

        self._cache_flags(checked)
        return ShortlistSummary(
            universe_size=len(instruments),
            eligible_size=len([i for i in checked if i.eligible]),
            shortlisted_size=len(shortlisted),
            exclusion_reasons=exclusion_reasons,
            missing_reasons=missing_reasons,
            missing_examples=missing_examples,
            failed_bond_events=self.failed_bond_events,
            price_enriched=price_enriched,
        )

    async def _apply_eligibility(self, instruments: list[Instrument]) -> list[Instrument]:
        now = datetime.now(timezone.utc)
        async with self.session_factory() as session:
            repo = InstrumentRepository(session)
            cache = await repo.index_by_isin()

        result: list[Instrument] = []
        for instrument in instruments:
            cached = cache.get(instrument.isin)
            amortization_flag = instrument.amortization_flag

            eligible_reason = "missing_data"
            eligible = False
            offer_unknown = False
            resolved_has_call_offer = instrument.has_call_offer
            if self.settings.exclude_non_ru_isin and instrument.isin and not instrument.isin.startswith("RU"):
                eligible_reason = "non_ru_isin"
            elif self.settings.exclude_floating_coupon and instrument.floating_coupon_flag:
                eligible_reason = "floating_coupon"
            elif instrument.maturity_date is None:
                eligible_reason = "missing_maturity_date"
            elif amortization_flag is True:
                eligible_reason = "amortization"
            else:
                resolved_has_call_offer = await self._resolve_call_offer(instrument, cached)
                if resolved_has_call_offer is True:
                    eligible_reason = "call_offer"
                elif resolved_has_call_offer is None:
                    offer_unknown = True
                    if self.settings.exclude_call_offer_unknown:
                        eligible_reason = "call_offer_unknown"
                    else:
                        eligible_reason = "ok"
                        eligible = True
                elif amortization_flag is False and resolved_has_call_offer is False:
                    eligible_reason = "ok"
                    eligible = True

            updated = self._copy_instrument(
                instrument,
                {
                    "has_call_offer": (
                        cached.has_call_offer
                        if cached and self._should_use_cache(cached, instrument)
                        else resolved_has_call_offer
                    ),
                    "amortization_flag": amortization_flag,
                    "offer_unknown": offer_unknown,
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
        liveness_metrics, latest_snapshots = await self._collect_liveness_metrics()
        shortlisted: list[Instrument] = []
        exclusion_reasons: dict[str, int] = {}
        missing_reasons: dict[str, int] = {}
        missing_examples: list[dict] = []
        now = datetime.utcnow()
        max_age = timedelta(seconds=self.settings.price_max_age_s)
        price_enriched = 0
        id_enrich_errors: dict[str, str] = {}

        def track_exclusion(reason: str):
            exclusion_reasons[reason] = exclusion_reasons.get(reason, 0) + 1

        def track_missing(inst: Instrument, reasons: list[str]):
            for reason in reasons:
                missing_reasons[reason] = missing_reasons.get(reason, 0) + 1
            if len(missing_examples) < 5:
                missing_examples.append(
                    {
                        "isin": inst.isin,
                        "figi": inst.figi,
                        "missing": list(reasons),
                    }
                )

        def shortlist_score(inst: Instrument):
            metrics = liveness_metrics.get(inst.isin)
            updates = metrics.updates_per_hour if metrics else 0.0
            notional = metrics.max_notional if metrics else 0.0
            enrichment_rank = 1 if not inst.needs_enrichment else 0
            return (enrichment_rank, updates, notional)

        if self.settings.universe_enrich_ids_on_rebuild and self.client.rest.enabled:
            _, id_enrich_errors = await self._enrich_ids(eligible)

        missing_price_by_isin: dict[str, list[str]] = {}
        for inst in eligible:
            reasons = self._snapshot_missing_reasons(
                latest_snapshots.get(inst.isin), now=now, max_age=max_age
            )
            if reasons:
                missing_price_by_isin[inst.isin] = reasons

        missing_price_isins = list(missing_price_by_isin.keys())
        if missing_price_isins and self.settings.universe_enrich_prices_on_rebuild:
            price_enriched, enriched_snapshots, price_enrich_errors = await self._enrich_prices(
                [inst for inst in eligible if inst.isin in missing_price_isins]
            )
            latest_snapshots.update(enriched_snapshots)
            for isin, reason in price_enrich_errors.items():
                missing_price_by_isin.setdefault(isin, []).append(reason)

        filtered: list[Instrument] = []
        for inst in eligible:
            metrics = liveness_metrics.get(inst.isin)
            missing = self._instrument_missing_reasons(inst)
            latest_snapshot = latest_snapshots.get(inst.isin)
            missing_price_reasons = self._snapshot_missing_reasons(
                latest_snapshot, now=now, max_age=max_age
            )
            if missing_price_reasons:
                missing.extend(missing_price_reasons)
                if not self._has_instrument_id(inst):
                    missing.append("missing_price_instrument_id_missing")
                if inst.isin in id_enrich_errors:
                    missing.append("missing_price_id_enrich_failed")

            if missing:
                inst.missing_reasons = missing
                inst.needs_enrichment = True
                track_missing(inst, missing)
                if not self.settings.allow_missing_data_to_shortlist:
                    track_exclusion("missing_data")
                    continue
            else:
                inst.missing_reasons = []
                inst.needs_enrichment = False

            if metrics is None:
                filtered.append(inst)
                continue

            if metrics.max_notional < self.settings.shortlist_min_notional:
                track_exclusion("not_enough_notional")
                continue
            if metrics.updates_per_hour < self.settings.shortlist_min_updates_per_hour:
                track_exclusion("not_enough_updates")
                continue
            filtered.append(inst)

        filtered.sort(key=shortlist_score, reverse=True)
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
        return shortlisted, exclusion_reasons, missing_reasons, missing_examples, price_enriched

    async def _collect_liveness_metrics(self) -> tuple[dict[str, LivenessMetrics], dict[str, OrderBookSnapshot]]:
        snapshots = await self._load_orderbook_snapshots()
        grouped: dict[str, list[OrderBookSnapshot]] = {}
        latest: dict[str, OrderBookSnapshot] = {}
        for snap in snapshots:
            grouped.setdefault(snap.isin, []).append(snap)
            current = latest.get(snap.isin)
            if current is None or snap.ts > current.ts:
                latest[snap.isin] = snap

        metrics: dict[str, LivenessMetrics] = {}
        for isin, snaps in grouped.items():
            snaps_sorted = sorted(snaps, key=lambda s: s.ts)
            if len(snaps_sorted) == 1:
                duration_hours = 24.0
            else:
                duration_hours = (snaps_sorted[-1].ts - snaps_sorted[0].ts).total_seconds() / 3600
                duration_hours = max(duration_hours, 1e-6)
            updates_per_hour = len(snaps_sorted) / duration_hours
            max_notional = max(self._snapshot_notional(s) for s in snaps_sorted)
            metrics[isin] = LivenessMetrics(updates_per_hour=updates_per_hour, max_notional=max_notional)
        return metrics, latest

    def _instrument_missing_reasons(self, instrument: Instrument) -> list[str]:
        reasons: list[str] = []
        if not instrument.isin:
            reasons.append("missing_isin")
        if not instrument.figi:
            reasons.append("missing_figi")
        if instrument.has_call_offer is None:
            reasons.append("missing_call_offer")
        if instrument.maturity_date is None:
            reasons.append("missing_maturity_date")
        return reasons

    def _cache_flags(self, instruments: list[Instrument]) -> None:
        for inst in instruments:
            self._shortlist_flags[inst.isin] = {
                "needs_enrichment": getattr(inst, "needs_enrichment", False),
                "missing_reasons": list(getattr(inst, "missing_reasons", [])),
                "offer_unknown": getattr(inst, "offer_unknown", False),
            }

    def _apply_cached_flags(self, instrument: Instrument) -> Instrument:
        cached = self._shortlist_flags.get(instrument.isin, {})
        return self._copy_instrument(
            instrument,
            {
                "needs_enrichment": cached.get("needs_enrichment", False),
                "missing_reasons": list(cached.get("missing_reasons", [])),
                "offer_unknown": cached.get("offer_unknown", getattr(instrument, "offer_unknown", False)),
            },
        )

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

    def _snapshot_price(self, snap: OrderBookSnapshot) -> float | None:
        return price_from_snapshot(snap)["mid"]

    def _snapshot_missing_reasons(
        self,
        snapshot: OrderBookSnapshot | None,
        *,
        now: datetime,
        max_age: timedelta,
    ) -> list[str]:
        if snapshot is None:
            return ["missing_price_no_snapshot"]
        snapshot_ts = snapshot.ts
        if snapshot_ts.tzinfo is None:
            snapshot_ts = snapshot_ts.replace(tzinfo=timezone.utc)
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        if now - snapshot_ts > max_age:
            return ["missing_price_snapshot_stale"]
        if not snapshot.bids and not snapshot.asks:
            return ["missing_price_snapshot_empty"]
        if self._snapshot_price(snapshot) is None:
            return ["missing_price_snapshot_empty"]
        return []

    def _has_instrument_id(self, instrument: Instrument) -> bool:
        return bool(
            instrument.instrument_uid
            or instrument.figi
            or (instrument.ticker and instrument.class_code)
        )

    def _needs_id_enrichment(self, instrument: Instrument) -> bool:
        if instrument.instrument_uid is None:
            return True
        if not instrument.figi:
            return True
        if self._is_suspect_figi(instrument.figi):
            return True
        return False

    def _is_suspect_figi(self, figi: str | None) -> bool:
        if not figi:
            return True
        upper = figi.upper()
        if upper.startswith("TCS"):
            return True
        return False

    async def _enrich_prices(
        self,
        instruments: list[Instrument],
    ) -> tuple[int, dict[str, OrderBookSnapshot], dict[str, str]]:
        limit = max(0, self.settings.universe_enrich_prices_limit)
        if limit == 0:
            return 0, {}, {}
        candidates = [inst for inst in instruments if inst.isin][:limit]
        if not candidates:
            return 0, {}, {}

        concurrency = self.settings.universe_enrich_prices_concurrency
        if concurrency <= 0:
            concurrency = self.settings.orderbook_bootstrap_concurrency
        concurrency = max(1, concurrency)

        rps = self.settings.universe_enrich_prices_rps
        if rps <= 0:
            rps = self.settings.orderbook_bootstrap_rps
        rate_limit = self._build_rate_limiter(rps=rps)

        timeout_s = self.settings.universe_enrich_prices_timeout_s
        if timeout_s <= 0:
            timeout_s = self.settings.orderbook_bootstrap_timeout_s

        semaphore = asyncio.Semaphore(concurrency)
        counter_lock = asyncio.Lock()
        price_enriched = 0
        snapshots: dict[str, OrderBookSnapshot] = {}
        errors: dict[str, str] = {}
        metrics = get_metrics()

        async def fetch_and_persist(inst: Instrument) -> None:
            nonlocal price_enriched
            async with semaphore:
                await rate_limit()
                metrics.record_universe_price_enrich_attempt()
                try:
                    if not self._has_instrument_id(inst):
                        metrics.record_universe_price_enrich_error("missing_instrument_id")
                        async with counter_lock:
                            errors[inst.isin] = "missing_price_instrument_id_missing"
                        return
                    snapshot = await self.client.fetch_orderbook_snapshot(
                        inst,
                        depth=1,
                        timeout=timeout_s,
                    )
                    if snapshot is None:
                        metrics.record_universe_price_enrich_error("empty_snapshot")
                        async with counter_lock:
                            errors[inst.isin] = "missing_price_unary_empty"
                        return
                    snapshot.isin = inst.isin
                    await self._persist_snapshot(snapshot)
                    async with counter_lock:
                        snapshots[inst.isin] = snapshot
                        price_enriched += 1
                    metrics.record_universe_price_enrich_success()
                except Exception as exc:
                    reason = type(exc).__name__ or "unknown"
                    error_reason = "missing_price_unary_error"
                    if isinstance(exc, grpc.aio.AioRpcError):
                        code = exc.code()
                        reason = code.name if code else reason
                        if code == grpc.StatusCode.DEADLINE_EXCEEDED:
                            error_reason = "missing_price_unary_timeout"
                        elif code == grpc.StatusCode.NOT_FOUND:
                            error_reason = "missing_price_unary_not_found"
                    elif isinstance(exc, asyncio.TimeoutError):
                        error_reason = "missing_price_unary_timeout"
                    metrics.record_universe_price_enrich_error(reason)
                    async with counter_lock:
                        errors[inst.isin] = error_reason

        await asyncio.gather(*(fetch_and_persist(inst) for inst in candidates))
        return price_enriched, snapshots, errors

    async def _enrich_ids(
        self,
        instruments: list[Instrument],
    ) -> tuple[int, dict[str, str]]:
        limit = max(0, self.settings.universe_enrich_ids_limit)
        if limit == 0:
            return 0, {}
        candidates = [inst for inst in instruments if self._needs_id_enrichment(inst)][:limit]
        if not candidates:
            return 0, {}

        concurrency = max(1, self.settings.universe_enrich_ids_concurrency)
        rate_limit = self._build_rate_limiter(rps=self.settings.universe_enrich_ids_rps)
        timeout_s = self.settings.universe_enrich_ids_timeout_s

        semaphore = asyncio.Semaphore(concurrency)
        counter_lock = asyncio.Lock()
        updated = 0
        errors: dict[str, str] = {}
        metrics = get_metrics()

        async def fetch_and_update(inst: Instrument) -> None:
            nonlocal updated
            async with semaphore:
                await rate_limit()
                metrics.record_universe_id_enrich_attempt()
                try:
                    query = inst.isin or inst.ticker
                    if not query:
                        metrics.record_universe_id_enrich_error("missing_query")
                        async with counter_lock:
                            errors[inst.isin] = "missing_price_instrument_id_missing"
                        return
                    enriched = await self.client.find_instrument(
                        query=query,
                        instrument_kind="BOND",
                        timeout=timeout_s,
                    )
                    if enriched is None and inst.ticker and inst.ticker != inst.isin:
                        enriched = await self.client.find_instrument(
                            query=inst.ticker,
                            instrument_kind="BOND",
                            timeout=timeout_s,
                        )
                    if enriched is None:
                        metrics.record_universe_id_enrich_error("not_found")
                        async with counter_lock:
                            errors[inst.isin] = "missing_price_unary_not_found"
                        return
                    updates = {
                        "instrument_uid": enriched.get("instrument_uid"),
                        "figi": enriched.get("figi"),
                        "ticker": enriched.get("ticker") or inst.ticker,
                        "class_code": enriched.get("class_code") or inst.class_code,
                        "floating_coupon_flag": (
                            enriched.get("floating_coupon_flag")
                            if enriched.get("floating_coupon_flag") is not None
                            else inst.floating_coupon_flag
                        ),
                    }
                    updated_inst = self._copy_instrument(inst, updates)
                    async with self.session_factory() as session:
                        repo = InstrumentRepository(session)
                        await repo.update_ids(updated_inst)
                    async with counter_lock:
                        inst.instrument_uid = updated_inst.instrument_uid
                        inst.figi = updated_inst.figi
                        inst.ticker = updated_inst.ticker
                        inst.class_code = updated_inst.class_code
                        inst.floating_coupon_flag = updated_inst.floating_coupon_flag
                        updated += 1
                    metrics.record_universe_id_enrich_success()
                except Exception as exc:
                    reason = type(exc).__name__ or "unknown"
                    error_reason = "missing_price_id_enrich_failed"
                    if isinstance(exc, (asyncio.TimeoutError, httpx.TimeoutException)):
                        error_reason = "missing_price_unary_timeout"
                    metrics.record_universe_id_enrich_error(reason)
                    async with counter_lock:
                        errors[inst.isin] = error_reason

        await asyncio.gather(*(fetch_and_update(inst) for inst in candidates))
        return updated, errors

    def _build_rate_limiter(self, *, rps: float):
        rate = max(rps, 0.1)
        min_interval = 1.0 / rate
        rate_lock = asyncio.Lock()
        last_sent = {"ts": 0.0}

        async def rate_limit():
            async with rate_lock:
                now = asyncio.get_running_loop().time()
                sleep_for = min_interval - (now - last_sent["ts"])
                if sleep_for > 0:
                    await asyncio.sleep(sleep_for)
                last_sent["ts"] = asyncio.get_running_loop().time()

        return rate_limit

    async def _persist_snapshot(self, snapshot: OrderBookSnapshot) -> None:
        from ..storage.schema import OrderbookSnapshotORM

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

    async def _resolve_call_offer(self, instrument: Instrument, cached=None) -> bool | None:
        if self._should_use_cache(cached, instrument):
            return cached.has_call_offer
        if self.settings.app_env == "mock":
            return instrument.has_call_offer
        try:
            return await self.client.has_future_call_offer(instrument)
        except Exception as exc:
            self.failed_bond_events += 1
            logger.warning("Failed to fetch bond events for %s: %s", instrument.isin, exc)
            return None

    def _should_use_cache(self, cached, instrument: Instrument) -> bool:
        if not cached or not cached.eligibility_checked_at:
            return False
        if cached.maturity_date != instrument.maturity_date:
            return False
        return cached.eligibility_checked_at >= datetime.utcnow() - timedelta(days=7)

    def _with_defaults(self, instrument: Instrument) -> Instrument:
        amortization_flag = instrument.amortization_flag
        if amortization_flag is None:
            amortization_flag = False
        has_call_offer = instrument.has_call_offer
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
