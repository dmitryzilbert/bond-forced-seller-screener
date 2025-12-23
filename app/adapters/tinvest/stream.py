from __future__ import annotations

import asyncio
import inspect
import json
import logging
import random
import ssl
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Iterable, Callable, Awaitable, Protocol

import certifi

try:  # Optional dependency for environments without network
    import websockets
    from websockets.exceptions import InvalidHandshake, InvalidStatusCode
except ImportError:  # pragma: no cover - handled in __init__
    websockets = None
    InvalidHandshake = InvalidStatusCode = None

from ...domain.models import Instrument, OrderBookLevel, OrderBookSnapshot

logger = logging.getLogger(__name__)


class _SslSettings(Protocol):
    tinvest_ssl_ca_bundle: str | None
    tinvest_ssl_insecure: bool


def build_ssl_context(settings: _SslSettings) -> tuple[ssl.SSLContext, str]:
    if settings.tinvest_ssl_insecure:
        return ssl._create_unverified_context(), "insecure"
    if settings.tinvest_ssl_ca_bundle:
        return ssl.create_default_context(cafile=settings.tinvest_ssl_ca_bundle), "custom_bundle"
    return ssl.create_default_context(cafile=certifi.where()), "certifi"


class TInvestStream:
    DEFAULT_WS_URL = "wss://invest-public-api.tbank.ru/ws"
    DEFAULT_SUBPROTOCOL = "json-proto"

    def __init__(
        self,
        token: str | None,
        depth: int = 10,
        *,
        connector: Callable[..., Awaitable] | None = None,
        dry_run: bool = False,
        max_reconnect_attempts: int = 20,
        ws_url: str | None = None,
        ws_protocol: str | None = None,
        ssl_ca_bundle: str | None = None,
        ssl_insecure: bool = False,
    ):
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
        self.ws_url = ws_url or self.DEFAULT_WS_URL
        self.subprotocol = ws_protocol or self.DEFAULT_SUBPROTOCOL
        self._ssl_settings = SimpleNamespace(
            tinvest_ssl_ca_bundle=ssl_ca_bundle,
            tinvest_ssl_insecure=ssl_insecure,
        )

    @staticmethod
    def _normalize_ws_url(ws_url: str) -> str:
        if ws_url.endswith("/ws/"):
            return ws_url[:-1]
        return ws_url

    def _build_connect_kwargs(self, headers: dict[str, str]) -> dict[str, dict[str, str]]:
        params = inspect.signature(self.connector).parameters
        if "additional_headers" in params:
            return {"additional_headers": headers}
        if "extra_headers" in params:
            return {"extra_headers": headers}
        return {}

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
        subprotocols = [self.subprotocol]
        ssl_ctx, ssl_mode = build_ssl_context(self._ssl_settings)
        ws_url = self._normalize_ws_url(self.ws_url)
        if ws_url != self.ws_url:
            logger.info("Normalized TInvest stream URL: %s -> %s", self.ws_url, ws_url)
        if websockets is not None and self.connector is websockets.connect:
            version = getattr(websockets, "__version__", "unknown")
            logger.info(
                "Starting TInvest stream: url=%s subprotocol=%s token_set=%s websockets=%s ssl_mode=%s",
                ws_url,
                self.subprotocol,
                bool(self.token),
                version,
                ssl_mode,
            )
        else:
            logger.info(
                "Starting TInvest stream: url=%s subprotocol=%s token_set=%s ssl_mode=%s",
                ws_url,
                self.subprotocol,
                bool(self.token),
                ssl_mode,
            )
        while True:
            ws = None
            try:
                current_instruments = self._select_instruments(base_instruments, overload_level)
                if not current_instruments:
                    logger.info("No eligible shortlisted instruments to subscribe")
                    break
                connect_kwargs = self._build_connect_kwargs(headers)
                async with self.connector(
                    ws_url,
                    subprotocols=subprotocols,
                    ping_interval=20,
                    ping_timeout=20,
                    ssl=ssl_ctx,
                    **connect_kwargs,
                ) as ws:
                    server_subprotocol = getattr(ws, "subprotocol", None)
                    if server_subprotocol is None:
                        logger.warning("TInvest stream: server did not agree on subprotocol; reconnecting")
                        raise ConnectionError("Server did not agree on subprotocol")
                    logger.info("TInvest stream connected: server_subprotocol=%s", server_subprotocol)
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
            except (InvalidStatusCode, InvalidHandshake) as exc:  # pragma: no cover - network errors are expected in prod
                status = getattr(exc, "status_code", None) or getattr(exc, "status", None)
                logger.warning("TInvest stream handshake failed: status=%s error=%s", status, exc)
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
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - network errors are expected in prod
                close_code = getattr(ws, "close_code", None)
                close_reason = getattr(ws, "close_reason", None)
                logger.warning(
                    "TInvest stream disconnected: %s code=%s reason=%s",
                    exc,
                    close_code,
                    close_reason,
                )
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
