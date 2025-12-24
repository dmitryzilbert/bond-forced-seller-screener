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
) -> Tuple[Iterable, str]:
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

    async def request_iterator():
        yield marketdata_pb2.MarketDataRequest(
            subscribe_order_book_request=subscribe_request
        )
        while True:
            await asyncio.sleep(3600)

    call = stub.MarketDataStream(request_iterator(), metadata=metadata)
    return call, "bidi"


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
