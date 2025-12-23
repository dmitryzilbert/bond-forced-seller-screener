import pytest

from app.adapters.tinvest.stream import TInvestStream


@pytest.mark.parametrize(
    ("raw_url", "expected"),
    [
        ("wss://invest-public-api.tinkoff.ru/ws", "wss://invest-public-api.tinkoff.ru/ws/"),
        ("wss://invest-public-api.tinkoff.ru/ws/", "wss://invest-public-api.tinkoff.ru/ws/"),
        ("wss://invest-public-api.tinkoff.ru/api/ws", "wss://invest-public-api.tinkoff.ru/api/ws/"),
        ("wss://invest-public-api.tinkoff.ru/ws/stream", "wss://invest-public-api.tinkoff.ru/ws/stream"),
    ],
)
def test_normalize_ws_url(raw_url: str, expected: str) -> None:
    assert TInvestStream._normalize_ws_url(raw_url) == expected
