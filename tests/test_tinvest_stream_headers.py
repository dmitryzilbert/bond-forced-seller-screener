import asyncio
from datetime import date, datetime
from types import SimpleNamespace

import pytest

pytest.importorskip("grpc")

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

    class DummyStub:
        def __init__(self, channel):
            self.channel = channel

        def MarketDataStream(self, request_iterator, metadata=None):
            captured["metadata"] = metadata

            async def generator():
                request = await request_iterator.__anext__()
                captured["request"] = request
                yield SimpleNamespace(
                    orderbook=SimpleNamespace(
                        figi="figi-test",
                        instrument_uid="",
                        instrument_id="",
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

    monkeypatch.setattr(
        "app.adapters.tinvest.grpc_stream.grpc.aio.secure_channel",
        fake_secure_channel,
    )
    monkeypatch.setattr(
        "app.adapters.tinvest.grpc_stream.marketdata_pb2_grpc.MarketDataStreamServiceStub",
        DummyStub,
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
    assert instruments[0].figi == instrument.figi
    assert instruments[0].depth == 1
