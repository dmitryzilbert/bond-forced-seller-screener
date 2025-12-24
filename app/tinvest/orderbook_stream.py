from __future__ import annotations

import asyncio
import contextlib
from typing import Iterable

import grpc

from .grpc_sdk import import_marketdata


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
            instruments = []
            for instrument_id in instrument_ids:
                if orderbook_type is None:
                    instruments.append(
                        self._marketdata_pb2.OrderBookInstrument(
                            instrument_id=instrument_id,
                            depth=depth,
                        )
                    )
                else:
                    instruments.append(
                        self._marketdata_pb2.OrderBookInstrument(
                            instrument_id=instrument_id,
                            depth=depth,
                            order_book_type=orderbook_type,
                        )
                    )
            subscribe_request = self._marketdata_pb2.SubscribeOrderBookRequest(
                subscription_action=self._marketdata_pb2.SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE,
                instruments=instruments,
            )

            if hasattr(stub, "MarketDataServerSideStream") and hasattr(
                self._marketdata_pb2, "MarketDataServerSideStreamRequest"
            ):
                request = self._marketdata_pb2.MarketDataServerSideStreamRequest(
                    subscribe_order_book_request=subscribe_request
                )
                call = stub.MarketDataServerSideStream(
                    request,
                    metadata=metadata,
                    wait_for_ready=True,
                )
            else:
                async def request_iterator():
                    yield self._marketdata_pb2.MarketDataRequest(
                        subscribe_order_book_request=subscribe_request
                    )
                    while True:
                        await asyncio.sleep(3600)

                call = stub.MarketDataStream(request_iterator(), metadata=metadata)

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
