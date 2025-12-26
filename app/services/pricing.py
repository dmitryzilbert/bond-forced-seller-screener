from __future__ import annotations

from ..domain.models import OrderBookSnapshot


def price_from_snapshot(snapshot: OrderBookSnapshot) -> dict[str, float | None]:
    best_bid = snapshot.best_bid or (snapshot.bids[0].price if snapshot.bids else None)
    best_ask = snapshot.best_ask or (snapshot.asks[0].price if snapshot.asks else None)
    if best_bid is not None and best_ask is not None:
        mid = (best_bid + best_ask) / 2
    else:
        mid = best_bid if best_bid is not None else best_ask
    return {
        "best_bid": best_bid,
        "best_ask": best_ask,
        "mid": mid,
    }
