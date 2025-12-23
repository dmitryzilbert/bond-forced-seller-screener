import asyncio
from datetime import date

import pytest

from app.adapters.tinvest.stream import TInvestStream
from app.domain.models import Instrument


async def _new_api_connector(url, *, additional_headers=None, **kwargs):  # pragma: no cover - used for signature
    return None


async def _legacy_api_connector(url, *, extra_headers=None, **kwargs):  # pragma: no cover - used for signature
    return None


def test_build_connect_kwargs_new_api():
    headers = {"Authorization": "Bearer token"}
    stream = TInvestStream("token", connector=_new_api_connector)

    connect_kwargs = stream._build_connect_kwargs(headers)

    assert connect_kwargs == {"additional_headers": headers}


def test_build_connect_kwargs_legacy_api():
    headers = {"Authorization": "Bearer token"}
    stream = TInvestStream("token", connector=_legacy_api_connector)

    connect_kwargs = stream._build_connect_kwargs(headers)

    assert connect_kwargs == {"extra_headers": headers}


def test_subscribe_sets_auth_header_and_subprotocols():
    captured: dict = {}

    class DummyWS:
        def __init__(self):
            self.subprotocol = "json-proto"
            self.close_code = 1000
            self.close_reason = "normal"

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def send(self, message):
            captured["sent"] = message

        def __aiter__(self):
            return self

        async def __anext__(self):
            raise StopAsyncIteration

    def connector(url, *, subprotocols=None, additional_headers=None, **kwargs):
        captured["url"] = url
        captured["subprotocols"] = subprotocols
        captured["headers"] = additional_headers
        return DummyWS()

    stream = TInvestStream("secret", depth=1, connector=connector, dry_run=True)
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
        with pytest.raises(StopAsyncIteration):
            await agen.__anext__()

    asyncio.run(_run())

    assert captured["headers"]["Authorization"].startswith("Bearer ")
    assert "json-proto" in captured["subprotocols"]
