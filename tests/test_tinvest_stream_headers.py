import asyncio
from datetime import date, datetime
from types import SimpleNamespace

import pytest

pytest.importorskip("grpc")
marketdata_pb2 = pytest.importorskip("t_tech.invest.grpc.marketdata_pb2")

from app.adapters.tinvest.grpc_stream import TInvestGrpcStream
from app.domain.models import Instrument


def test_grpc_subscribe_sends_metadata_and_request(monkeypatch):
    captured: dict = {}

    class DummyChannel:
        async def close(self):
            captured["closed"] = True

    def fake_secure_channel(target, credentials):
        captured["target"] = target
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
            captured["metadata"] = metadata

            async def generator():
                async for request in request_iterator:
                    captured["request"] = request
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
                                quantity=1,
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

    stream = TInvestGrpcStream(
        "secret",
        depth=1,
        app_env="prod",
        target_prod="prod.example:443",
        target_sandbox="sandbox.example:443",
        dry_run=True,
    )
    instrument = Instrument(
        isin="RU000A0TEST0",
        instrument_uid="uid-test",
        figi="figi-test",
        name="Test",
        issuer="Issuer",
        nominal=1000,
        maturity_date=date(2030, 1, 1),
        eligible=True,
        is_shortlisted=True,
    )

    async def _run():
        agen = stream.subscribe([instrument])
        snapshot = await agen.__anext__()
        assert snapshot.isin == instrument.isin
        await agen.aclose()

    asyncio.run(_run())

    assert captured["metadata"] == (("authorization", "Bearer secret"),)
    request = captured["request"]
    instruments = request.subscribe_order_book_request.instruments
    assert instruments[0].instrument_id == instrument.instrument_uid
    assert instruments[0].depth == 1
