from __future__ import annotations

import asyncio
import json
import logging
import random
from datetime import datetime, timezone
from typing import Iterable, Callable, Awaitable

try:  # Optional dependency for environments without network
    import websockets
except ImportError:  # pragma: no cover - handled in __init__
    websockets = None

from ...domain.models import Instrument, OrderBookLevel, OrderBookSnapshot

logger = logging.getLogger(__name__)


class TInvestStream:
    WS_URL = "wss://invest-public-api.tinkoff.ru/ws/invest-public-api-v1/marketdata/stream"

    def __init__(self, token: str | None, depth: int = 10, *, connector: Callable[..., Awaitable] | None = None, dry_run: bool = False, max_reconnect_attempts: int = 20):
        self.token = token
        self.depth = depth
        self.enabled = bool(token)
        if connector is not None:
            self.connector = connector
        elif self.enabled:
            if websockets is None:
                raise RuntimeError("websockets package is required for TInvest stream")
            self.connector = websockets.connect
        else:
            self.connector = None
        self.dry_run = dry_run
        self.reconnect_count = 0
        self._last_active_instruments: list[Instrument] = []
        self.max_reconnect_attempts = max_reconnect_attempts

    async def subscribe(self, instruments: Iterable[Instrument]):
        if not self.enabled:
            return

        base_instruments = list(instruments)
        if not base_instruments:
            return

        backoff = 1
        max_backoff = 30
        overload_level = 0

        headers = {"Authorization": f"Bearer {self.token}"}
        while True:
            try:
                current_instruments = self._select_instruments(base_instruments, overload_level)
                if not current_instruments:
                    logger.info("No eligible shortlisted instruments to subscribe")
                    break
                async with self.connector(self.WS_URL, extra_headers=headers, ping_interval=20, ping_timeout=20) as ws:
                    await self._send_subscribe_batches(ws, current_instruments)
                    self._last_active_instruments = current_instruments
                    backoff = 1
                    overload_level = 0
                    async for message in ws:
                        snapshot = self._parse_message(message, current_instruments)
                        if snapshot:
                            yield snapshot
                    if self.dry_run:
                        break
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - network errors are expected in prod
                logger.warning("TInvest stream disconnected: %s", exc)
                self.reconnect_count += 1
                if self.dry_run:
                    break
                if self.reconnect_count >= self.max_reconnect_attempts:
                    logger.error("TInvest stream reached max reconnect attempts (%s)", self.max_reconnect_attempts)
                    break
                overload_level = min(overload_level + 1, 3)
                sleep_for = min(backoff, max_backoff) + random.uniform(0, backoff)
                await asyncio.sleep(sleep_for)
                backoff = min(backoff * 2, max_backoff)

    async def _send_subscribe_batches(self, ws, instruments: list[Instrument]):
        batch_size = max(10, 50 // (1 + len(instruments) // 200))
        for batch in self._chunked(instruments, batch_size):
            payload = {
                "subscribeOrderBookRequest": {
                    "subscriptionAction": "SUBSCRIPTION_ACTION_SUBSCRIBE",
                    "instruments": [
                        {"instrumentId": instrument.figi or instrument.isin, "depth": self.depth}
                        for instrument in batch
                    ],
                    "waitingClose": False,
                }
            }
            await ws.send(json.dumps(payload))

    def _select_instruments(self, instruments: Iterable[Instrument], overload_level: int) -> list[Instrument]:
        filtered = [i for i in instruments if i.is_shortlisted and i.eligible]
        if not filtered:
            return []
        prioritized = sorted(filtered, key=lambda inst: (inst.nominal, inst.isin), reverse=True)
        limit_factor = 2 ** overload_level
        allowed = max(10, len(prioritized) // limit_factor)
        return prioritized[:allowed]

    def _chunked(self, items: list[Instrument], size: int):
        for idx in range(0, len(items), size):
            yield items[idx : idx + size]

    @property
    def active_subscription_count(self) -> int:
        return len(self._last_active_instruments)

    def _parse_message(self, message: str | bytes, instruments: list[Instrument]) -> OrderBookSnapshot | None:
        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            logger.debug("Skip non-JSON marketdata message: %s", message)
            return None

        orderbook = payload.get("orderbook")
        if not orderbook:
            return None

        instrument_id = orderbook.get("figi") or orderbook.get("instrumentUid") or orderbook.get("instrumentId")
        instrument = next((i for i in instruments if i.figi == instrument_id or i.isin == instrument_id), None)
        if not instrument:
            return None

        ts = self._parse_datetime(orderbook.get("time"))
        bids = []
        for item in orderbook.get("bids", []):
            price = self._parse_quotation(item.get("price"))
            lots = self._parse_quantity(item.get("quantity", 0))
            if price and lots > 0:
                bids.append(OrderBookLevel(price=price, lots=lots))

        asks = []
        for item in orderbook.get("asks", []):
            price = self._parse_quotation(item.get("price"))
            lots = self._parse_quantity(item.get("quantity", 0))
            if price and lots > 0:
                asks.append(OrderBookLevel(price=price, lots=lots))

        if not bids and not asks:
            logger.debug("Skip empty orderbook update for %s", instrument.isin)
            return None

        return OrderBookSnapshot(
            isin=instrument.isin,
            ts=ts or datetime.now(timezone.utc),
            bids=bids,
            asks=asks,
            nominal=instrument.nominal,
        )

    def _parse_datetime(self, value: str | None) -> datetime | None:
        if not value:
            return None
        ts = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts.astimezone(timezone.utc)

    def _parse_quotation(self, value) -> float:
        if not value:
            return 0.0
        units = int(value.get("units", 0))
        nano = int(value.get("nano", 0))
        return units + nano / 1e9

    def _parse_quantity(self, value) -> int:
        try:
            quantity = int(value)
        except (TypeError, ValueError):
            return 0
        return max(quantity, 0)
