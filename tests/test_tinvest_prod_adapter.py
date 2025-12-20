import asyncio
import json

import httpx
import pytest

from app.adapters.tinvest.client import TInvestClient
from app.domain.models import Instrument


@pytest.mark.integration
@pytest.mark.asyncio
async def test_tinvest_rest_and_stream_dry_run():
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

    messages = [
        json.dumps(
            {
                "orderbook": {
                    "figi": "figi-test",
                    "time": "2024-01-01T12:00:00Z",
                    "bids": [{"price": {"units": 1000, "nano": 0}, "quantity": 2}],
                    "asks": [],
                }
            }
        )
    ]

    class DummyWS:
        def __init__(self, queue):
            self._queue = queue

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def __aiter__(self):
            return self

        async def __anext__(self):
            if not self._queue:
                raise StopAsyncIteration
            return self._queue.pop(0)

        async def send(self, message):
            # emulate ws send
            return None

    async def connector(*args, **kwargs):
        return DummyWS(messages.copy())

    client = TInvestClient(
        "token",
        account_id=None,
        depth=5,
        rest_transport=transport,
        stream_connector=connector,
        dry_run=True,
    )

    instruments = await client.list_bonds()
    assert instruments and instruments[0].isin == "RU000A0ZZZZZ"

    prices = await client.rest.last_prices([instruments[0].figi])
    assert prices["figi-test"] == 100.5

    instrument_models = [
        Instrument(
            isin=inst.isin,
            figi=inst.figi,
            name=inst.name,
            issuer=inst.issuer,
            nominal=inst.nominal,
            maturity_date=inst.maturity_date,
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
