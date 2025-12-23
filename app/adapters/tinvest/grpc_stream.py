from __future__ import annotations

import asyncio
import logging
import random
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Protocol

import grpc
from tinkoff.invest.grpc import marketdata_pb2, marketdata_pb2_grpc

from ...domain.models import Instrument, OrderBookLevel, OrderBookSnapshot

logger = logging.getLogger(__name__)


class _GrpcSettings(Protocol):
    app_env: str
    tinvest_grpc_target_prod: str
    tinvest_grpc_target_sandbox: str
    tinvest_ssl_ca_bundle: str | None


def select_grpc_target(settings: _GrpcSettings) -> str:
    if settings.app_env == "sandbox":
        return settings.tinvest_grpc_target_sandbox
    return settings.tinvest_grpc_target_prod


def build_grpc_credentials(settings: _GrpcSettings) -> tuple[grpc.ChannelCredentials, str]:
    if settings.tinvest_ssl_ca_bundle:
        try:
            pem_bytes = Path(settings.tinvest_ssl_ca_bundle).read_bytes()
        except OSError as exc:
            logger.warning("Failed to read SSL CA bundle %s: %s", settings.tinvest_ssl_ca_bundle, exc)
        else:
            return grpc.ssl_channel_credentials(root_certificates=pem_bytes), "custom_bundle"
    return grpc.ssl_channel_credentials(), "default"


async def grpc_channel_ready(
    target: str,
    credentials: grpc.ChannelCredentials,
    *,
    timeout: float = 5.0,
) -> bool:
    channel = grpc.aio.secure_channel(target, credentials)
    try:
        await asyncio.wait_for(channel.channel_ready(), timeout=timeout)
        return True
    except Exception as exc:  # pragma: no cover - network dependent
        logger.warning("TInvest gRPC channel_ready failed: %s", exc)
        return False
    finally:
        await channel.close()


class TInvestGrpcStream:
    def __init__(
        self,
        token: str | None,
        depth: int,
        *,
        app_env: str,
        target_prod: str,
        target_sandbox: str,
        ssl_ca_bundle: str | None = None,
        dry_run: bool = False,
        batch_size: int = 50,
        batch_sleep_range: tuple[float, float] = (0.1, 0.3),
    ) -> None:
        self.token = token
        self.depth = depth
        self.enabled = bool(token)
        self.reconnect_count = 0
        self.dry_run = dry_run
        self.batch_size = max(1, batch_size)
        self.batch_sleep_range = batch_sleep_range
        self._last_active_instruments: list[Instrument] = []
        self._settings = _GrpcRuntimeSettings(
            app_env=app_env,
            tinvest_grpc_target_prod=target_prod,
            tinvest_grpc_target_sandbox=target_sandbox,
            tinvest_ssl_ca_bundle=ssl_ca_bundle,
        )

    async def subscribe(self, instruments: Iterable[Instrument]):
        if not self.enabled:
            return

        base_instruments = [i for i in instruments if i.figi or i.isin]
        if not base_instruments:
            return

        backoff_steps = [1, 2, 5, 10, 20, 30]
        backoff_index = 0
        while True:
            target = select_grpc_target(self._settings)
            credentials, ssl_mode = build_grpc_credentials(self._settings)
            logger.info(
                "Starting TInvest gRPC stream: target=%s token_set=%s ssl_mode=%s",
                target,
                bool(self.token),
                ssl_mode,
            )
            channel = grpc.aio.secure_channel(target, credentials)
            try:
                stub = marketdata_pb2_grpc.MarketDataStreamServiceStub(channel)
                current_instruments = self._select_instruments(base_instruments)
                if not current_instruments:
                    logger.info("No eligible shortlisted instruments to subscribe")
                    break
                instrument_map = self._build_instrument_map(current_instruments)
                request_iterator = self._request_iterator(current_instruments)
                metadata = (("authorization", f"Bearer {self.token}"),)
                responses = stub.MarketDataStream(request_iterator, metadata=metadata)
                self._last_active_instruments = current_instruments
                backoff_index = 0
                async for response in responses:
                    snapshot = self._parse_response(response, instrument_map)
                    if snapshot:
                        yield snapshot
                if self.dry_run:
                    break
                raise ConnectionError("TInvest gRPC stream closed")
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - network errors expected in prod
                logger.warning("TInvest gRPC stream disconnected: %s", exc)
                self.reconnect_count += 1
                if self.dry_run:
                    break
                sleep_for = backoff_steps[min(backoff_index, len(backoff_steps) - 1)]
                await asyncio.sleep(sleep_for)
                backoff_index = min(backoff_index + 1, len(backoff_steps) - 1)
            finally:
                await channel.close()

    async def _request_iterator(self, instruments: list[Instrument]):
        for batch in self._chunked(instruments, self.batch_size):
            request = marketdata_pb2.MarketDataRequest(
                subscribe_order_book_request=marketdata_pb2.SubscribeOrderBookRequest(
                    subscription_action=marketdata_pb2.SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE,
                    instruments=[
                        marketdata_pb2.OrderBookInstrument(
                            figi=instrument.figi or instrument.isin,
                            depth=self.depth,
                        )
                        for instrument in batch
                    ],
                )
            )
            yield request
            await asyncio.sleep(random.uniform(*self.batch_sleep_range))
        while True:
            await asyncio.sleep(3600)

    def _select_instruments(self, instruments: Iterable[Instrument]) -> list[Instrument]:
        filtered = [i for i in instruments if i.is_shortlisted and i.eligible]
        if not filtered:
            return []
        return sorted(filtered, key=lambda inst: (inst.nominal, inst.isin), reverse=True)

    def _chunked(self, items: list[Instrument], size: int):
        for idx in range(0, len(items), size):
            yield items[idx : idx + size]

    @property
    def active_subscription_count(self) -> int:
        return len(self._last_active_instruments)

    def _build_instrument_map(self, instruments: Iterable[Instrument]) -> dict[str, Instrument]:
        mapping: dict[str, Instrument] = {}
        for instrument in instruments:
            if instrument.figi:
                mapping[instrument.figi] = instrument
            if instrument.isin:
                mapping[instrument.isin] = instrument
        return mapping

    def _parse_response(
        self,
        response: marketdata_pb2.MarketDataResponse,
        instrument_map: dict[str, Instrument],
    ) -> OrderBookSnapshot | None:
        if response is None:
            return None
        if hasattr(response, "HasField") and not response.HasField("orderbook"):
            return None
        orderbook = getattr(response, "orderbook", None)
        if orderbook is None:
            return None

        instrument_id = orderbook.figi or getattr(orderbook, "instrument_uid", "") or getattr(
            orderbook, "instrument_id", ""
        )
        instrument = instrument_map.get(instrument_id)
        if not instrument:
            return None

        ts = self._parse_timestamp(orderbook.time)
        bids = []
        for item in orderbook.bids:
            price = self._parse_quotation(item.price)
            lots = max(int(getattr(item, "quantity", 0)), 0)
            if price and lots > 0:
                bids.append(OrderBookLevel(price=price, lots=lots))

        asks = []
        for item in orderbook.asks:
            price = self._parse_quotation(item.price)
            lots = max(int(getattr(item, "quantity", 0)), 0)
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

    def _parse_timestamp(self, value) -> datetime | None:
        if not value:
            return None
        if isinstance(value, datetime):
            ts = value
        elif hasattr(value, "ToDatetime"):
            ts = value.ToDatetime()
        else:
            seconds = int(getattr(value, "seconds", 0))
            nanos = int(getattr(value, "nanos", 0))
            ts = datetime.fromtimestamp(seconds + nanos / 1e9, tz=timezone.utc)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts.astimezone(timezone.utc)

    def _parse_quotation(self, value) -> float:
        if not value:
            return 0.0
        units = int(getattr(value, "units", 0))
        nano = int(getattr(value, "nano", 0))
        return units + nano / 1e9


class _GrpcRuntimeSettings:
    def __init__(
        self,
        *,
        app_env: str,
        tinvest_grpc_target_prod: str,
        tinvest_grpc_target_sandbox: str,
        tinvest_ssl_ca_bundle: str | None,
    ) -> None:
        self.app_env = app_env
        self.tinvest_grpc_target_prod = tinvest_grpc_target_prod
        self.tinvest_grpc_target_sandbox = tinvest_grpc_target_sandbox
        self.tinvest_ssl_ca_bundle = tinvest_ssl_ca_bundle
