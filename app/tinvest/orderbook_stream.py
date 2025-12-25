from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Iterable, Tuple

import grpc

from .grpc_sdk import import_marketdata

logger = logging.getLogger(__name__)


def build_orderbook_sub_request(
    marketdata_pb2,
    instrument_ids: Iterable[str],
    *,
    depth: int,
    order_book_type=None,
):
    instruments = []
    for instrument_id in instrument_ids:
        if order_book_type is None:
            instruments.append(
                marketdata_pb2.OrderBookInstrument(
                    instrument_id=instrument_id,
                    depth=depth,
                )
            )
        else:
            instruments.append(
                marketdata_pb2.OrderBookInstrument(
                    instrument_id=instrument_id,
                    depth=depth,
                    order_book_type=order_book_type,
                )
            )
    return marketdata_pb2.SubscribeOrderBookRequest(
        subscription_action=marketdata_pb2.SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE,
        instruments=instruments,
    )


def open_orderbook_stream(
    stub,
    marketdata_pb2,
    subscribe_request,
    *,
    metadata,
    heartbeat_interval_s: float,
    on_heartbeat=None,
) -> Tuple[Iterable, str]:
    async def request_iterator():
        yield marketdata_pb2.MarketDataRequest(subscribe_order_book_request=subscribe_request)
        heartbeat_request = build_get_my_subscriptions_request(marketdata_pb2)
        while True:
            if on_heartbeat:
                on_heartbeat()
            if heartbeat_request is not None:
                yield heartbeat_request
            await asyncio.sleep(heartbeat_interval_s)

    if hasattr(stub, "MarketDataStream") and hasattr(marketdata_pb2, "MarketDataRequest"):
        call = stub.MarketDataStream(request_iterator(), metadata=metadata)
        return call, "bidi"

    if hasattr(stub, "MarketDataServerSideStream") and hasattr(
        marketdata_pb2, "MarketDataServerSideStreamRequest"
    ):
        request = marketdata_pb2.MarketDataServerSideStreamRequest(
            subscribe_order_book_request=subscribe_request
        )
        call = stub.MarketDataServerSideStream(
            request,
            metadata=metadata,
            wait_for_ready=True,
        )
        return call, "server_side"

    raise AttributeError("MarketData stream API is not available")


def build_get_my_subscriptions_request(marketdata_pb2):
    if not hasattr(marketdata_pb2, "MarketDataRequest") or not hasattr(
        marketdata_pb2, "GetMySubscriptionsRequest"
    ):
        return None
    request = marketdata_pb2.MarketDataRequest()
    payload = marketdata_pb2.GetMySubscriptionsRequest()
    if hasattr(request, "get_my_subscriptions"):
        getattr(request, "get_my_subscriptions").CopyFrom(payload)
        return request
    if hasattr(request, "get_my_subscriptions_request"):
        getattr(request, "get_my_subscriptions_request").CopyFrom(payload)
        return request
    return None


class OrderbookStreamAdapter:
    def __init__(
        self,
        token: str,
        target: str,
        credentials: grpc.ChannelCredentials,
        *,
        marketdata_pb2=None,
        marketdata_pb2_grpc=None,
        sdk_source_name: str | None = None,
    ) -> None:
        self._token = token
        self._target = target
        self._credentials = credentials
        if marketdata_pb2 is None or marketdata_pb2_grpc is None:
            marketdata_pb2, marketdata_pb2_grpc, sdk_source_name = import_marketdata()
        self._marketdata_pb2 = marketdata_pb2
        self._marketdata_pb2_grpc = marketdata_pb2_grpc
        self._sdk_source_name = sdk_source_name or "unknown"

    @property
    def sdk_source_name(self) -> str:
        return self._sdk_source_name

    def _orderbook_type_default(self):
        if hasattr(self._marketdata_pb2, "OrderBookType"):
            return getattr(self._marketdata_pb2.OrderBookType, "ORDER_BOOK_TYPE_ALL", None) or getattr(
                self._marketdata_pb2.OrderBookType, "ORDER_BOOK_TYPE_UNSPECIFIED", None
            )
        return None

    async def subscribe_orderbook(
        self,
        instrument_ids: Iterable[str],
        *,
        depth: int,
        order_book_type=None,
        heartbeat_interval_s: float = 20.0,
        on_heartbeat=None,
    ):
        channel = grpc.aio.secure_channel(self._target, self._credentials)
        call = None
        try:
            stub = self._marketdata_pb2_grpc.MarketDataStreamServiceStub(channel)
            metadata = (("authorization", f"Bearer {self._token}"),)
            orderbook_type = order_book_type or self._orderbook_type_default()
            subscribe_request = build_orderbook_sub_request(
                self._marketdata_pb2,
                instrument_ids,
                depth=depth,
                order_book_type=orderbook_type,
            )
            call, stream_mode = open_orderbook_stream(
                stub,
                self._marketdata_pb2,
                subscribe_request,
                metadata=metadata,
                heartbeat_interval_s=heartbeat_interval_s,
                on_heartbeat=on_heartbeat,
            )
            logger.info("Orderbook stream mode: %s", stream_mode)

            async for response in call:
                yield response
        except asyncio.CancelledError:
            if call is not None:
                call.cancel()
            raise
        finally:
            if call is not None:
                with contextlib.suppress(Exception):
                    call.cancel()
            await channel.close()

    async def get_orderbook_snapshot(
        self,
        instrument_id: str,
        *,
        depth: int,
        order_book_type=None,
        timeout: float | None = None,
    ):
        channel = grpc.aio.secure_channel(self._target, self._credentials)
        try:
            stub_class = getattr(self._marketdata_pb2_grpc, "MarketDataServiceStub", None)
            if stub_class is None:
                logger.warning("MarketDataServiceStub is not available for orderbook bootstrap")
                return None
            if not hasattr(self._marketdata_pb2, "GetOrderBookRequest"):
                logger.warning("GetOrderBookRequest is not available for orderbook bootstrap")
                return None
            stub = stub_class(channel)
            metadata = (("authorization", f"Bearer {self._token}"),)
            orderbook_type = order_book_type or self._orderbook_type_default()
            request = self._marketdata_pb2.GetOrderBookRequest(
                instrument_id=instrument_id,
                depth=depth,
                order_book_type=orderbook_type,
            )
            if timeout is None:
                return await stub.GetOrderBook(request, metadata=metadata)
            return await asyncio.wait_for(
                stub.GetOrderBook(request, metadata=metadata),
                timeout=timeout,
            )
        finally:
            await channel.close()
