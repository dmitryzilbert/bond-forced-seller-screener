from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime
from typing import Iterable, Callable, Awaitable

try:  # Optional dependency for environments without network
    import websockets
except ImportError:  # pragma: no cover - handled in __init__
    websockets = None

from ...domain.models import Instrument, OrderBookLevel, OrderBookSnapshot

logger = logging.getLogger(__name__)


class TInvestStream:
    WS_URL = "wss://invest-public-api.tinkoff.ru/ws/invest-public-api-v1/marketdata/stream"

    def __init__(self, token: str | None, depth: int = 10, *, connector: Callable[..., Awaitable] | None = None, dry_run: bool = False):
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

    async def subscribe(self, instruments: Iterable[Instrument]):
        if not self.enabled:
            return

        instruments = list(instruments)
        if not instruments:
            return

        headers = {"Authorization": f"Bearer {self.token}"}
        backoff = 1
        max_backoff = 30
        while True:
            try:
                async with self.connector(self.WS_URL, extra_headers=headers, ping_interval=20, ping_timeout=20) as ws:
                    await self._send_subscribe(ws, instruments)
                    backoff = 1
                    async for message in ws:
                        snapshot = self._parse_message(message, instruments)
                        if snapshot:
                            yield snapshot
                    if self.dry_run:
                        break
            except Exception as exc:  # pragma: no cover - network errors are expected in prod
                logger.warning("TInvest stream disconnected: %s", exc)
                if self.dry_run:
                    break
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)

    async def _send_subscribe(self, ws, instruments: list[Instrument]):
        payload = {
            "subscribeOrderBookRequest": {
                "subscriptionAction": "SUBSCRIPTION_ACTION_SUBSCRIBE",
                "instruments": [
                    {"instrumentId": instrument.figi or instrument.isin, "depth": self.depth}
                    for instrument in instruments
                ],
                "waitingClose": False,
            }
        }
        await ws.send(json.dumps(payload))

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
        bids = [
            OrderBookLevel(price=self._parse_quotation(item.get("price")), lots=item.get("quantity", 0))
            for item in orderbook.get("bids", [])
            if item.get("price")
        ]
        asks = [
            OrderBookLevel(price=self._parse_quotation(item.get("price")), lots=item.get("quantity", 0))
            for item in orderbook.get("asks", [])
            if item.get("price")
        ]

        if not bids and not asks:
            logger.debug("Skip empty orderbook update for %s", instrument.isin)
            return None

        return OrderBookSnapshot(
            isin=instrument.isin,
            ts=ts or datetime.utcnow(),
            bids=bids,
            asks=asks,
            nominal=instrument.nominal,
        )

    def _parse_datetime(self, value: str | None) -> datetime | None:
        if not value:
            return None
        return datetime.fromisoformat(value.replace("Z", "+00:00"))

    def _parse_quotation(self, value) -> float:
        if not value:
            return 0.0
        units = int(value.get("units", 0))
        nano = int(value.get("nano", 0))
        return units + nano / 1e9
