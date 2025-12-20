import asyncio
import json
from datetime import date, datetime

from app.adapters.tinvest.stream import TInvestStream
from app.domain.models import Instrument


class DummyWS:
    def __init__(self, messages, send_log):
        self._messages = messages
        self._send_log = send_log

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def send(self, message):
        self._send_log.append(json.loads(message))

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._messages:
            raise ConnectionResetError("stream dropped")
        return self._messages.pop(0)


def _orderbook_payload(figi: str, ts: datetime, price: float):
    units = int(price)
    nano = int(round((price - units) * 1e9))
    return json.dumps(
        {
            "orderbook": {
                "figi": figi,
                "time": ts.isoformat(),
                "bids": [{"price": {"units": units, "nano": nano}, "quantity": 1}],
                "asks": [{"price": {"units": units + 1, "nano": nano}, "quantity": 1}],
            }
        }
    )


def test_stream_reconnects_and_resubscribes():
    async def _run():
        instrument = Instrument(
            isin="RU000A0ZZZZZ",
            figi="figi-test",
            name="Bond Test",
            issuer="Issuer",
            nominal=1000.0,
            maturity_date=date(2030, 1, 1),
            eligible=True,
            is_shortlisted=True,
        )

        send_log: list[dict] = []
        batches = [
            [_orderbook_payload(instrument.figi, datetime.utcnow(), 1000.0)],
            [_orderbook_payload(instrument.figi, datetime.utcnow(), 1001.0)],
        ]
        call_count = 0

        def connector(*args, **kwargs):
            nonlocal call_count
            if call_count >= len(batches):
                raise ConnectionResetError("connector exhausted")
            ws = DummyWS(batches[call_count], send_log)
            call_count += 1
            return ws

        stream = TInvestStream("token", depth=5, connector=connector, dry_run=False)
        gen = stream.subscribe([instrument])

        first = await gen.__anext__()
        second = await gen.__anext__()
        await gen.aclose()

        assert first.best_bid == 1000
        assert second.best_bid == 1001
        assert stream.reconnect_count == 1
        assert len(send_log) == 2
        for payload in send_log:
            instruments = payload["subscribeOrderBookRequest"]["instruments"]
            assert instruments[0]["instrumentId"] == instrument.figi
            assert instruments[0]["depth"] == 5

    asyncio.run(_run())
