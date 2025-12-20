from __future__ import annotations

from datetime import datetime
from ...domain.models import Instrument, OrderBookSnapshot, OrderBookLevel


def map_instrument_payload(payload: dict) -> Instrument:
    return Instrument(
        isin=payload.get("isin"),
        figi=payload.get("figi"),
        name=payload.get("name", ""),
        issuer=payload.get("issuer"),
        nominal=float(payload.get("nominal", 1000)),
        maturity_date=datetime.fromisoformat(payload.get("maturity_date")).date(),
        segment=payload.get("segment"),
    )


def map_orderbook_payload(payload: dict) -> OrderBookSnapshot:
    bids = [OrderBookLevel(price=level[0], lots=level[1]) for level in payload.get("bids", [])]
    asks = [OrderBookLevel(price=level[0], lots=level[1]) for level in payload.get("asks", [])]
    return OrderBookSnapshot(
        isin=payload["isin"],
        ts=datetime.fromisoformat(payload["ts"]),
        bids=bids,
        asks=asks,
        nominal=float(payload.get("nominal", 1000)),
    )
