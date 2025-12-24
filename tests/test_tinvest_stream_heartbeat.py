from types import SimpleNamespace

import pytest

pytest.importorskip("grpc")

from app.adapters.tinvest.grpc_stream import TInvestGrpcStream
from app.services.metrics import get_metrics


def test_stream_message_updates_heartbeat():
    metrics = get_metrics()
    before_messages = metrics.stream_messages_total
    before_heartbeat = metrics.last_heartbeat_ts

    stream = TInvestGrpcStream(
        "token",
        depth=1,
        app_env="prod",
        target_prod="prod.example:443",
        target_sandbox="sandbox.example:443",
        dry_run=True,
    )

    stream._handle_stream_response(SimpleNamespace())

    assert metrics.stream_messages_total == before_messages + 1
    assert metrics.last_heartbeat_ts is not None
    if before_heartbeat is not None:
        assert metrics.last_heartbeat_ts >= before_heartbeat


def test_subscribe_response_updates_heartbeat():
    metrics = get_metrics()
    before_messages = metrics.stream_messages_total
    before_heartbeat = metrics.last_heartbeat_ts

    stream = TInvestGrpcStream(
        "token",
        depth=1,
        app_env="prod",
        target_prod="prod.example:443",
        target_sandbox="sandbox.example:443",
        dry_run=True,
    )

    class DummyResponse:
        def __init__(self):
            self.subscribe_order_book_response = SimpleNamespace(order_book_subscriptions=[])

        def HasField(self, name: str) -> bool:
            return name == "subscribe_order_book_response"

    stream._handle_stream_response(DummyResponse())

    assert metrics.stream_messages_total == before_messages + 1
    assert metrics.last_heartbeat_ts is not None
    if before_heartbeat is not None:
        assert metrics.last_heartbeat_ts >= before_heartbeat
