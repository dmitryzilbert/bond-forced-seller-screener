from __future__ import annotations

import asyncio
import logging
import contextlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Protocol

import grpc

from ...tinvest.grpc_sdk import import_marketdata
from ...tinvest.ids import api_instrument_id
from ...tinvest.orderbook_stream import OrderbookStreamAdapter

from ...domain.models import Instrument, OrderBookLevel, OrderBookSnapshot
from ...services.metrics import get_metrics

logger = logging.getLogger(__name__)
_STREAM_DONE = object()


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
        self._metrics = get_metrics()
        self._settings = _GrpcRuntimeSettings(
            app_env=app_env,
            tinvest_grpc_target_prod=target_prod,
            tinvest_grpc_target_sandbox=target_sandbox,
            tinvest_ssl_ca_bundle=ssl_ca_bundle,
        )
        self._marketdata_pb2, self._marketdata_pb2_grpc, self._sdk_source_name = import_marketdata()

    async def subscribe(self, instruments: Iterable[Instrument]):
        if not self.enabled:
            return

        base_instruments = [i for i in instruments if self._has_instrument_id(i)]
        if not base_instruments:
            return

        backoff_steps = [1, 2, 5, 10, 20, 30]
        backoff_index = 0
        while True:
            target = select_grpc_target(self._settings)
            credentials, ssl_mode = build_grpc_credentials(self._settings)
            logger.info(
                "Starting TInvest gRPC stream: target=%s token_set=%s ssl_mode=%s sdk=%s",
                target,
                bool(self.token),
                ssl_mode,
                self._sdk_source_name,
            )
            try:
                current_instruments = self._select_instruments(base_instruments)
                if not current_instruments:
                    logger.info("No eligible shortlisted instruments to subscribe")
                    break
                instrument_map = self._build_instrument_map(current_instruments)
                instrument_ids = [api_instrument_id(inst) for inst in current_instruments]
                self._log_subscription_payload(current_instruments, instrument_ids)
                adapter = OrderbookStreamAdapter(
                    self.token,
                    target,
                    credentials,
                    marketdata_pb2=self._marketdata_pb2,
                    marketdata_pb2_grpc=self._marketdata_pb2_grpc,
                    sdk_source_name=self._sdk_source_name,
                )
                responses = adapter.subscribe_orderbook(
                    instrument_ids,
                    depth=self.depth,
                    order_book_type=self._orderbook_type_default(),
                )
                self._last_active_instruments = current_instruments
                backoff_index = 0
                first_message_event = asyncio.Event()
                ack_event = asyncio.Event()
                response_queue: asyncio.Queue = asyncio.Queue()
                reader_task = asyncio.create_task(
                    self._read_stream(responses, response_queue, first_message_event, ack_event)
                )
                try:
                    try:
                        await asyncio.wait_for(first_message_event.wait(), timeout=5.0)
                    except asyncio.TimeoutError:
                        logger.warning("Timed out waiting for first orderbook stream message")

                    try:
                        await asyncio.wait_for(ack_event.wait(), timeout=5.0)
                    except asyncio.TimeoutError:
                        logger.warning("Timed out waiting for orderbook subscribe ACK (continuing)")

                    ack_validated = False
                    while True:
                        response = await response_queue.get()
                        if response is _STREAM_DONE:
                            break
                        if isinstance(response, Exception):
                            raise response
                        self._handle_stream_response(response)
                        if self._response_has_subscribe_response(response) and not ack_validated:
                            ack = response.subscribe_order_book_response
                            ok_subs, err_subs = self._split_subscription_response(ack)
                            if len(ok_subs) == 0:
                                raise RuntimeError("Orderbook subscription rejected")
                            ok_instruments = self._select_instruments_from_subscriptions(
                                current_instruments, ok_subs
                            )
                            if ok_instruments:
                                instrument_map = self._build_instrument_map(ok_instruments)
                                self._last_active_instruments = ok_instruments
                            if err_subs:
                                err_ids = self._subscription_instrument_ids(err_subs)
                                logger.info(
                                    "Orderbook subscription errors for instruments: %s",
                                    ", ".join(sorted(err_ids)),
                                )
                            ack_validated = True
                            continue
                        if not ack_validated:
                            logger.debug("Skipping orderbook update before ACK")
                            continue
                        snapshot = self._parse_response(response, instrument_map)
                        if snapshot:
                            yield snapshot
                finally:
                    reader_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await reader_task
                if self.dry_run:
                    break
                raise ConnectionError("TInvest gRPC stream closed")
            except asyncio.CancelledError:
                logger.info("TInvest gRPC stream cancelled")
                break
            except Exception as exc:  # pragma: no cover - network errors expected in prod
                if isinstance(exc, grpc.aio.AioRpcError):
                    logger.warning(
                        "TInvest gRPC stream disconnected: code=%s details=%s",
                        exc.code(),
                        exc.details(),
                    )
                else:
                    logger.warning("TInvest gRPC stream disconnected: %s", exc)
                self.reconnect_count += 1
                if self.dry_run:
                    break
                sleep_for = backoff_steps[min(backoff_index, len(backoff_steps) - 1)]
                logger.info("Reconnect backoff: %.1fs", sleep_for)
                await asyncio.sleep(sleep_for)
                backoff_index = min(backoff_index + 1, len(backoff_steps) - 1)
            finally:
                pass

    def _orderbook_type_default(self):
        if hasattr(self._marketdata_pb2, "OrderBookType"):
            return getattr(self._marketdata_pb2.OrderBookType, "ORDER_BOOK_TYPE_ALL", None) or getattr(
                self._marketdata_pb2.OrderBookType, "ORDER_BOOK_TYPE_UNSPECIFIED", None
            )
        return None

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
            for key in self._instrument_keys(instrument):
                if key:
                    mapping[key] = instrument
        return mapping

    def _instrument_keys(self, instrument: Instrument) -> list[str]:
        keys = []
        if instrument.instrument_uid:
            keys.append(instrument.instrument_uid)
        if instrument.figi:
            keys.append(instrument.figi)
        if instrument.isin:
            keys.append(instrument.isin)
        return keys

    def _has_instrument_id(self, instrument: Instrument) -> bool:
        try:
            return bool(api_instrument_id(instrument))
        except ValueError:
            return False

    def _log_subscription_payload(self, instruments: list[Instrument], instrument_ids: list[str]) -> None:
        payloads = []
        for instrument, instrument_id in zip(instruments, instrument_ids):
            payloads.append(
                {
                    "instrument_id": instrument_id,
                    "instrument_uid": instrument.instrument_uid,
                    "figi": instrument.figi,
                    "ticker": instrument.ticker,
                    "class_code": instrument.class_code,
                }
            )
        logger.info(
            "Orderbook subscribe: depth=%s order_book_type=%s instruments=%s",
            self.depth,
            self._orderbook_type_default(),
            payloads,
        )

    def _parse_response(
        self,
        response,
        instrument_map: dict[str, Instrument],
    ) -> OrderBookSnapshot | None:
        if response is None:
            return None
        if hasattr(response, "HasField") and not response.HasField("orderbook"):
            return None
        orderbook = getattr(response, "orderbook", None)
        if orderbook is None:
            return None

        instrument_id = (
            getattr(orderbook, "instrument_uid", "")
            or getattr(orderbook, "instrument_id", "")
            or getattr(orderbook, "figi", "")
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

    async def _read_stream(
        self,
        responses,
        queue: asyncio.Queue,
        first_message_event: asyncio.Event,
        ack_event: asyncio.Event,
    ) -> None:
        try:
            async for response in responses:
                first_message_event.set()
                if self._response_has_subscribe_response(response) and not ack_event.is_set():
                    ack_event.set()
                await queue.put(response)
        except asyncio.CancelledError:
            logger.info("TInvest gRPC stream reader cancelled")
        except Exception as exc:  # pragma: no cover - network errors expected in prod
            logger.warning("TInvest gRPC stream reader error: %s", exc)
            await queue.put(exc)
        finally:
            await queue.put(_STREAM_DONE)

    def _response_has_subscribe_response(self, response) -> bool:
        if response is None:
            return False
        if hasattr(response, "HasField"):
            return response.HasField("subscribe_order_book_response")
        return hasattr(response, "subscribe_order_book_response")

    def _handle_stream_response(self, response) -> None:
        if response is None:
            return
        self._metrics.record_stream_message()
        if hasattr(response, "HasField") and response.HasField("ping"):
            ping = getattr(response, "ping", None)
            ping_ts = self._parse_timestamp(getattr(ping, "time", None)) if ping else None
            self._metrics.record_stream_ping(ts=ping_ts)
            return

        if hasattr(response, "HasField") and response.HasField("subscribe_order_book_response"):
            self._log_subscription_response(response.subscribe_order_book_response)
            return
        if hasattr(response, "HasField") and response.HasField("orderbook"):
            return
        logger.debug("Received empty/unknown stream message: %s", response)

    def _log_subscription_response(self, response) -> None:
        subscriptions = (
            getattr(response, "order_book_subscriptions", None)
            or getattr(response, "order_books_subscriptions", None)
            or []
        )
        if not subscriptions:
            return

        ok_subs, err_subs = self._split_subscription_response(response)
        tracking_id = getattr(response, "tracking_id", None)
        for item in subscriptions:
            status_raw = getattr(item, "subscription_status", None) or getattr(item, "status", None)
            status_name = self._subscription_status_name(status_raw)
            instrument_id = getattr(item, "instrument_id", None)
            figi = getattr(item, "figi", None)
            uid = getattr(item, "instrument_uid", None) or getattr(item, "uid", None)
            logger.info(
                "Orderbook subscription ACK: tracking_id=%s stream_id=%s subscription_id=%s status=%s "
                "instrument_id=%s figi=%s uid=%s",
                tracking_id,
                getattr(item, "stream_id", None),
                getattr(item, "subscription_id", None),
                status_name,
                instrument_id,
                figi,
                uid,
            )

        logger.info(
            "Orderbook subscriptions response: %d OK, %d ERROR",
            len(ok_subs),
            len(err_subs),
        )

        for item in err_subs:
            status_raw = getattr(item, "subscription_status", None) or getattr(item, "status", None)
            status_name = self._subscription_status_name(status_raw)
            instrument_id = getattr(item, "instrument_id", None)
            figi = getattr(item, "figi", None)
            uid = getattr(item, "instrument_uid", None) or getattr(item, "uid", None)
            logger.warning(
                "Orderbook subscription error: status=%s instrument_id=%s figi=%s uid=%s",
                status_name,
                instrument_id,
                figi,
                uid,
            )

    def _split_subscription_response(self, response) -> tuple[list, list]:
        subscriptions = (
            getattr(response, "order_book_subscriptions", None)
            or getattr(response, "order_books_subscriptions", None)
            or []
        )
        if not subscriptions:
            logger.warning("Orderbook subscribe ACK contains no subscriptions")
            return [], []

        success = getattr(self._marketdata_pb2, "SubscriptionStatus", None)
        success_value = (
            getattr(success, "SUBSCRIPTION_STATUS_SUCCESS", None) if success else None
        )
        ok_subs = []
        err_subs = []
        for item in subscriptions:
            status_raw = getattr(item, "subscription_status", None) or getattr(item, "status", None)
            if success_value is not None and status_raw == success_value:
                ok_subs.append(item)
            else:
                err_subs.append(item)
        return ok_subs, err_subs

    def _subscription_status_name(self, status_raw) -> str:
        enum_type = getattr(self._marketdata_pb2, "SubscriptionStatus", None)
        if enum_type and isinstance(status_raw, int):
            try:
                return enum_type.Name(status_raw)
            except ValueError:
                pass
        return getattr(status_raw, "name", None) or str(status_raw)

    def _subscription_instrument_ids(self, subscriptions: Iterable) -> set[str]:
        ids: set[str] = set()
        for item in subscriptions:
            for value in (
                getattr(item, "instrument_id", None),
                getattr(item, "figi", None),
                getattr(item, "instrument_uid", None) or getattr(item, "uid", None),
            ):
                if value:
                    ids.add(str(value))
        return ids

    def _select_instruments_from_subscriptions(
        self,
        instruments: list[Instrument],
        subscriptions: Iterable,
    ) -> list[Instrument]:
        ok_ids = self._subscription_instrument_ids(subscriptions)
        if not ok_ids:
            return instruments
        filtered = [inst for inst in instruments if ok_ids.intersection(self._instrument_keys(inst))]
        if not filtered:
            logger.warning("No instruments matched subscription ACK ids: %s", ", ".join(sorted(ok_ids)))
            return instruments
        return filtered


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
