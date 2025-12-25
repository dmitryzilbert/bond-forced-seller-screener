import asyncio
from datetime import datetime
from types import SimpleNamespace

import httpx
import pytest

pytest.importorskip("grpc")
marketdata_pb2 = pytest.importorskip("t_tech.invest.grpc.marketdata_pb2")

from app.adapters.tinvest.client import TInvestClient
from app.domain.models import Instrument


@pytest.mark.integration
def test_tinvest_rest_and_stream_dry_run(monkeypatch):
    async def _run():
        async def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path.endswith("Bonds"):
                payload = {
                    "instruments": [
                        {
                            "isin": "RU000A0ZZZZZ",
                            "figi": "figi-test",
                            "name": "Test Bond",
                            "issuerName": "Test Issuer",
                            "nominal": {"units": 1000, "nano": 0},
                            "maturityDate": "2025-01-01T00:00:00Z",
                        }
                    ]
                }
                return httpx.Response(200, json=payload)
            if request.url.path.endswith("GetLastPrices"):
                payload = {
                    "lastPrices": [
                        {
                            "instrumentUid": "figi-test",
                            "price": {"units": 100, "nano": 500_000_000},
                        }
                    ]
                }
                return httpx.Response(200, json=payload)
            return httpx.Response(404)

        transport = httpx.MockTransport(handler)

        class DummyChannel:
            async def close(self):
                return None

        def fake_secure_channel(target, credentials):
            return DummyChannel()

        class DummySubscribeResponse:
            def __init__(self):
                self.order_book_subscriptions = [
                    SimpleNamespace(
                        subscription_status=marketdata_pb2.SubscriptionStatus.SUBSCRIPTION_STATUS_SUCCESS
                    )
                ]

        class DummyResponse:
            def __init__(self, orderbook=None, subscribe_order_book_response=None):
                self.orderbook = orderbook
                self.subscribe_order_book_response = subscribe_order_book_response
                self.tracking_id = "track-1"
                self.stream_id = "stream-1"

            def HasField(self, name: str) -> bool:
                return getattr(self, name, None) is not None

        class DummyStub:
            def __init__(self, channel):
                self.channel = channel

            def MarketDataStream(self, request_iterator, metadata=None):
                async def generator():
                    async for _ in request_iterator:
                        break
                    yield DummyResponse(subscribe_order_book_response=DummySubscribeResponse())
                    yield DummyResponse(
                        orderbook=SimpleNamespace(
                            figi="figi-test",
                            instrument_uid="uid-test",
                            instrument_id="uid-test",
                            time=datetime.utcnow(),
                            bids=[
                                SimpleNamespace(
                                    price=SimpleNamespace(units=1000, nano=0),
                                    quantity=2,
                                )
                            ],
                            asks=[],
                        )
                    )

                return generator()

            def MarketDataServerSideStream(self, request, metadata=None, wait_for_ready=None):
                async def generator():
                    yield DummyResponse(subscribe_order_book_response=DummySubscribeResponse())
                    yield DummyResponse(
                        orderbook=SimpleNamespace(
                            figi="figi-test",
                            instrument_uid="uid-test",
                            instrument_id="uid-test",
                            time=datetime.utcnow(),
                            bids=[
                                SimpleNamespace(
                                    price=SimpleNamespace(units=1000, nano=0),
                                    quantity=2,
                                )
                            ],
                            asks=[],
                        )
                    )

                return generator()

        monkeypatch.setattr("app.tinvest.orderbook_stream.grpc.aio.secure_channel", fake_secure_channel)
        monkeypatch.setattr(
            "app.adapters.tinvest.grpc_stream.import_marketdata",
            lambda: (
                marketdata_pb2,
                SimpleNamespace(MarketDataStreamServiceStub=DummyStub),
                "t_tech.invest.grpc",
            ),
        )

        client = TInvestClient(
            "token",
            account_id=None,
            depth=5,
            rest_transport=transport,
            dry_run=True,
            app_env="prod",
            grpc_target_prod="prod.example:443",
            grpc_target_sandbox="sandbox.example:443",
        )

        instruments = await client.list_bonds()
        assert instruments and instruments[0].isin == "RU000A0ZZZZZ"

        prices = await client.rest.last_prices([instruments[0].figi])
        assert prices["figi-test"] == 100.5

        instrument_models = [
            Instrument(
                isin=inst.isin,
                instrument_uid="uid-test",
                figi=inst.figi,
                name=inst.name,
                issuer=inst.issuer,
                nominal=inst.nominal,
                maturity_date=inst.maturity_date,
                eligible=True,
                is_shortlisted=True,
            )
            for inst in instruments
        ]

        stream = client.stream_orderbooks(instrument_models)
        snapshot = await stream.__anext__()
        assert snapshot.isin == "RU000A0ZZZZZ"
        assert snapshot.bids[0].price == 1000

        with pytest.raises(StopAsyncIteration):
            await stream.__anext__()

        await client.rest.close()

    asyncio.run(_run())
