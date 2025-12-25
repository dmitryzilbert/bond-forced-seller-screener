from __future__ import annotations

import asyncio
import contextlib
import json
from pathlib import Path
import logging
from datetime import datetime, timezone

import grpc

from ..domain.models import OrderBookSnapshot
from ..domain.detector import History, detect_event
from ..adapters.tinvest.client import TInvestClient
from ..adapters.tinvest.mapping import map_orderbook_payload
from ..tinvest.ids import api_instrument_id
from ..settings import Settings
from ..services.events import EventService
from ..services.universe import UniverseService
from ..adapters.telegram.bot import TelegramBot
from ..storage.db import async_session_factory
from ..storage.repo import SnapshotRepository
from .metrics import get_metrics

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
        self._start_time = datetime.now(timezone.utc)
        self._last_metrics_log = datetime.now(timezone.utc)
        self._updates_count = 0
        self._dropped_updates = 0
        self._last_snapshot_ts: datetime | None = None
        self._active_subscriptions = 0
        self.metrics = get_metrics()

    async def run_mock_stream(self):
        instruments = self._filter_shortlist(await self.universe.shortlist())
        self._reset_metrics(len(instruments))
        instrument_map = {i.isin: i for i in instruments}
        payloads = Path("fixtures/orderbooks.ndjson").read_text().splitlines()
        for line in payloads:
            if asyncio.current_task() and asyncio.current_task().cancelled():
                logger.info("Mock orderbook stream cancelled")
                break

            data = json.loads(line)
            snapshot = map_orderbook_payload(data)
            await self._handle_snapshot(snapshot, instrument_map, persist=False)
            try:
                await asyncio.sleep(0.05)
            except asyncio.CancelledError:
                logger.info("Mock orderbook stream cancelled during sleep")
                break

    async def run_prod_stream(self):
        instruments = self._filter_shortlist(await self.universe.shortlist())
        self._reset_metrics(len(instruments))
        instrument_map = {i.isin: i for i in instruments}
        bootstrap_task: asyncio.Task | None = None
        poll_task: asyncio.Task | None = None

        async def on_subscribed(ok_instruments: list) -> None:
            nonlocal instrument_map, bootstrap_task, poll_task
            if not ok_instruments:
                return
            instrument_map = {i.isin: i for i in ok_instruments}
            bootstrap_task = asyncio.create_task(
                self._bootstrap_snapshots_safe(ok_instruments, instrument_map)
            )
            poll_task = asyncio.create_task(
                self._poll_snapshots(ok_instruments, instrument_map)
            )

        try:
            async for snapshot in self.client.stream_orderbooks(
                instruments,
                on_subscribed=on_subscribed,
            ):
                await self._handle_snapshot(snapshot, instrument_map, persist=True)
        finally:
            if poll_task is not None:
                poll_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await poll_task
            if bootstrap_task is not None:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await bootstrap_task

    async def _persist_snapshot(self, snapshot: OrderBookSnapshot):
        from ..storage.repo import SnapshotRepository
        from ..storage.schema import OrderbookSnapshotORM
        from ..storage.db import async_session_factory

        session_factory = async_session_factory()
        async with session_factory() as session:
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

    async def _bootstrap_snapshots(
        self,
        instruments: list,
        instrument_map: dict,
    ) -> None:
        if not instruments or not self.settings.orderbook_bootstrap_enabled:
            return

        concurrency = max(1, self.settings.orderbook_bootstrap_concurrency)
        rate_limit = self._build_rate_limiter()
        semaphore = asyncio.Semaphore(concurrency)
        counter_lock = asyncio.Lock()
        fetch_ok_count = 0
        persist_ok_count = 0
        persist_error_count = 0
        dropped_count = 0

        logger.info("bootstrap starting %s", len(instruments))
        self.metrics.record_orderbook_bootstrap_attempt(len(instruments))

        async def fetch_and_ingest(inst):
            nonlocal fetch_ok_count, persist_ok_count, persist_error_count, dropped_count
            async with semaphore:
                await rate_limit()
                instrument_id = None
                try:
                    instrument_id = api_instrument_id(inst)
                except ValueError:
                    instrument_id = None
                figi = getattr(inst, "figi", None)
                uid = getattr(inst, "instrument_uid", None)
                instrument_isin = getattr(inst, "isin", None)
                if not instrument_isin:
                    logger.warning(
                        "Orderbook bootstrap dropped snapshot: missing_isin instrument_id=%s figi=%s uid=%s "
                        "instrument_isin=%s",
                        instrument_id,
                        figi,
                        uid,
                        instrument_isin,
                    )
                    async with counter_lock:
                        dropped_count += 1
                    self.metrics.record_snapshot_dropped("missing_isin")
                    return
                try:
                    if hasattr(self.client, "fetch_orderbook_snapshot"):
                        snapshot = await self.client.fetch_orderbook_snapshot(
                            inst,
                            depth=self.settings.orderbook_depth,
                            timeout=self.settings.orderbook_bootstrap_timeout_s,
                        )
                    else:
                        response = await self.client.fetch_orderbook_response(
                            inst,
                            depth=self.settings.orderbook_depth,
                            timeout=self.settings.orderbook_bootstrap_timeout_s,
                        )
                        if response is None:
                            logger.info("Orderbook bootstrap empty response for %s", instrument_id)
                            self.metrics.record_orderbook_bootstrap_error("empty_response")
                            return
                        snapshot = self.client.build_orderbook_snapshot(response, inst)
                    if snapshot is None:
                        self.metrics.record_orderbook_bootstrap_error("empty_snapshot")
                        return
                    async with counter_lock:
                        fetch_ok_count += 1
                    self.metrics.record_orderbook_bootstrap_fetch_ok()
                    try:
                        await self._persist_snapshot(snapshot)
                    except Exception as exc:
                        logger.exception(
                            "Orderbook bootstrap persist failed: instrument_id=%s figi=%s uid=%s isin=%s",
                            instrument_id,
                            figi,
                            uid,
                            instrument_isin,
                        )
                        async with counter_lock:
                            persist_error_count += 1
                        self.metrics.record_orderbook_bootstrap_persist_error(type(exc).__name__)
                        return
                    async with counter_lock:
                        persist_ok_count += 1
                    self.metrics.record_orderbook_bootstrap_persist_ok()
                    await self._handle_snapshot(snapshot, instrument_map, persist=False)
                except Exception as exc:
                    reason = type(exc).__name__
                    if isinstance(exc, grpc.aio.AioRpcError):
                        reason = exc.code().name if exc.code() else reason
                        logger.info(
                            "Orderbook bootstrap error: instrument_id=%s code=%s details=%s",
                            instrument_id,
                            exc.code(),
                            exc.details(),
                        )
                    else:
                        logger.info(
                            "Orderbook bootstrap error: instrument_id=%s error=%s",
                            instrument_id,
                            exc,
                        )
                    self.metrics.record_orderbook_bootstrap_error(reason)
                    return

        await asyncio.gather(*(fetch_and_ingest(inst) for inst in instruments))
        logger.info(
            "bootstrap done fetch_ok=%s persist_ok=%s persist_err=%s dropped=%s",
            fetch_ok_count,
            persist_ok_count,
            persist_error_count,
            dropped_count,
        )

    async def _bootstrap_snapshots_safe(
        self,
        instruments: list,
        instrument_map: dict,
    ) -> None:
        try:
            await self._bootstrap_snapshots(instruments, instrument_map)
        except Exception as exc:
            logger.exception("Orderbook bootstrap failed (ignored)")
            reason = type(exc).__name__ or "unexpected"
            self.metrics.record_orderbook_bootstrap_error(reason)

    async def _poll_snapshots(
        self,
        instruments: list,
        instrument_map: dict,
    ) -> None:
        if not instruments or not self.settings.orderbook_poll_enabled:
            return

        concurrency = max(1, self.settings.orderbook_bootstrap_concurrency)
        rate_limit = self._build_rate_limiter()
        semaphore = asyncio.Semaphore(concurrency)
        chunk_size = max(1, concurrency)
        order = list(instruments)
        cursor = 0

        logger.info(
            "Orderbook polling enabled: instruments=%s interval_s=%s concurrency=%s rps=%.2f",
            len(instruments),
            self.settings.orderbook_poll_interval_s,
            concurrency,
            self.settings.orderbook_bootstrap_rps,
        )

        async def fetch_and_ingest(inst):
            async with semaphore:
                await rate_limit()
                try:
                    snapshot = await self.client.fetch_orderbook_snapshot(
                        inst,
                        depth=self.settings.orderbook_depth,
                        timeout=self.settings.orderbook_bootstrap_timeout_s,
                    )
                except Exception as exc:
                    logger.warning("Orderbook poll failed for %s: %s", getattr(inst, "isin", None), exc)
                    return
                if snapshot is None:
                    return
                await self._handle_snapshot(snapshot, instrument_map, persist=True)

        while True:
            if not order:
                await asyncio.sleep(self.settings.orderbook_poll_interval_s)
                continue
            batch = order[cursor:] + order[:cursor]
            for idx in range(0, len(batch), chunk_size):
                chunk = batch[idx : idx + chunk_size]
                await asyncio.gather(*(fetch_and_ingest(inst) for inst in chunk))
            cursor = (cursor + chunk_size) % len(order)
            await asyncio.sleep(self.settings.orderbook_poll_interval_s)

    def _build_rate_limiter(self):
        rps = max(self.settings.orderbook_bootstrap_rps, 0.1)
        min_interval = 1.0 / rps
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

    def _filter_shortlist(self, instruments: list) -> list:
        filtered = [
            i for i in instruments if getattr(i, "is_shortlisted", False) and getattr(i, "eligible", False)
        ]
        requested = len(filtered)
        self.metrics.set_orderbook_subscriptions_requested(requested)
        cap = self.settings.orderbook_max_subscriptions_per_stream
        if requested > cap:
            def sort_key(inst):
                ytm_mid = getattr(inst, "ytm_mid", None)
                if ytm_mid is not None:
                    return (0, -ytm_mid, inst.isin)
                return (1, 0, inst.isin)

            filtered = sorted(filtered, key=sort_key)
            filtered = filtered[:cap]
            logger.info(
                "requested=%s capped_to=%s due_to_stream_limit",
                requested,
                cap,
            )
            self.metrics.record_orderbook_subscriptions_capped()
        return filtered

    async def _handle_snapshot(self, snapshot: OrderBookSnapshot, instrument_map: dict, *, persist: bool):
        if not snapshot or (not snapshot.bids and not snapshot.asks):
            self._dropped_updates += 1
            return

        if not snapshot.isin:
            self._dropped_updates += 1
            self.metrics.record_snapshot_dropped("missing_isin")
            logger.warning("Orderbook snapshot dropped: missing_isin snapshot_isin=%s", snapshot.isin)
            return

        instrument = instrument_map.get(snapshot.isin)
        if not instrument:
            self._dropped_updates += 1
            return

        if persist:
            try:
                await self._persist_snapshot(snapshot)
            except Exception:
                logger.exception("Orderbook snapshot persist failed: isin=%s", snapshot.isin)
                raise

        self.metrics.record_snapshot()

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

        if event is None:
            return

        suppression_reason = None
        if event.alert:
            suppression_reason = self._alert_suppression_reason(instrument)

        event.payload = {
            **(event.payload or {}),
            "needs_enrichment": getattr(instrument, "needs_enrichment", False),
            "missing_reasons": getattr(instrument, "missing_reasons", []),
            "offer_unknown": getattr(instrument, "offer_unknown", False),
            "candidate": bool(getattr(event, "candidate", False)),
            "alert": bool(getattr(event, "alert", False)),
        }
        if suppression_reason:
            event.payload = {
                **(event.payload or {}),
                "alert_suppressed_reason": suppression_reason,
            }

        self.metrics.record_candidate()

        if getattr(event, "candidate", False):
            await self.events.save_event(event, persist=persist)

        if event.alert:
            if suppression_reason:
                logger.info("[ALERT SUPPRESSED] %s reason=%s", instrument.isin, suppression_reason)
            elif event.stress_flag:
                logger.info("[STRESS ONLY] TG muted for %s", event.isin)
            else:
                await self.telegram.send_event(event, instrument)
            self.metrics.record_alert()

        self._updates_count += 1
        now = datetime.now(timezone.utc)
        self._last_snapshot_ts = snapshot.ts
        self.metrics.record_worker_heartbeat(ts=now)
        self.metrics.record_snapshot(ts=now)
        self._maybe_log_metrics()

    def _alert_suppression_reason(self, instrument) -> str | None:
        if getattr(instrument, "needs_enrichment", False) and self.settings.suppress_alerts_when_missing_data:
            return "missing_data"
        if getattr(instrument, "offer_unknown", False) and self.settings.suppress_alerts_when_offer_unknown:
            return "offer_unknown"
        return None

    def _reset_metrics(self, active_subscriptions: int):
        self._start_time = datetime.now(timezone.utc)
        self._last_metrics_log = datetime.now(timezone.utc)
        self._updates_count = 0
        self._dropped_updates = 0
        self._last_snapshot_ts = None
        self._active_subscriptions = active_subscriptions
        self.metrics.set_stream_reconnects(0)
        self.metrics.set_orderbook_subscriptions_requested(active_subscriptions)
        logger.info(
            "Orderbook stream starting: %s active subscriptions", self._active_subscriptions
        )

    def _maybe_log_metrics(self):
        now = datetime.now(timezone.utc)
        if (now - self._last_metrics_log).total_seconds() < 60:
            return
        elapsed_minutes = max((now - self._start_time).total_seconds() / 60, 1 / 60)
        updates_per_min = self._updates_count / elapsed_minutes
        lag_seconds = (now - self._last_snapshot_ts).total_seconds() if self._last_snapshot_ts else None
        reconnects = getattr(self.client.stream, "reconnect_count", 0)
        self.metrics.set_stream_reconnects(reconnects)
        logger.info(
            "Orderbook stream metrics | active=%s updates/min=%.2f lag_sec=%s dropped=%s reconnects=%s",
            self._active_subscriptions,
            updates_per_min,
            round(lag_seconds, 3) if lag_seconds is not None else None,
            self._dropped_updates,
            reconnects,
        )
        asyncio.create_task(self._maybe_send_liveness_alert(now))
        self._last_metrics_log = now

    async def _maybe_send_liveness_alert(self, now: datetime):
        if self.settings.app_env != "prod":
            return

        if not self.metrics.should_send_liveness_alert(
            now=now,
            cooldown_minutes=self.settings.liveness_alert_cooldown_minutes,
            threshold_minutes=self.settings.liveness_alert_minutes,
        ):
            return

        message = "[ALERT] No orderbook updates received."
        await self.telegram.send_text(message)
        self.metrics.mark_liveness_alert_sent(now=now)


class OrderbookService:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.session_factory = async_session_factory()

    async def latest_snapshot(self, isin: str) -> OrderBookSnapshot | None:
        async with self.session_factory() as session:
            repo = SnapshotRepository(session)
            result = await repo.latest(isin)
            return result

    async def latest_snapshots(self, limit: int = 50) -> list[dict]:
        async with self.session_factory() as session:
            repo = SnapshotRepository(session)
            rows = await repo.list_latest(limit)
            return [self._snapshot_payload(row) for row in rows]

    @staticmethod
    def _snapshot_payload(row) -> dict:
        bids = list(row.bids_json or [])[:3]
        asks = list(row.asks_json or [])[:3]
        best_bid = row.best_bid if row.best_bid is not None else (bids[0].get("price") if bids else None)
        best_ask = row.best_ask if row.best_ask is not None else (asks[0].get("price") if asks else None)
        mid = OrderbookService._mid_price(best_bid, best_ask)
        return {
            "isin": row.isin,
            "ts": row.ts,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "mid": mid,
            "bids": bids,
            "asks": asks,
        }

    @staticmethod
    def _mid_price(best_bid: float | None, best_ask: float | None) -> float | None:
        if best_bid is not None and best_ask is not None:
            return (best_bid + best_ask) / 2
        if best_bid is not None:
            return best_bid
        if best_ask is not None:
            return best_ask
        return None
