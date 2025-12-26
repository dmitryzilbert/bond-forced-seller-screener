from datetime import datetime, timezone

from app.domain.models import OrderBookLevel, OrderBookSnapshot
from app.services.pricing import price_from_snapshot


def test_price_from_snapshot_prefers_best_levels():
    snapshot = OrderBookSnapshot(
        isin="TEST",
        ts=datetime.now(timezone.utc),
        bids=[OrderBookLevel(price=100.0, lots=1)],
        asks=[OrderBookLevel(price=102.0, lots=1)],
        nominal=1000.0,
    )

    prices = price_from_snapshot(snapshot)

    assert prices["best_bid"] == 100.0
    assert prices["best_ask"] == 102.0
    assert prices["mid"] == 101.0
