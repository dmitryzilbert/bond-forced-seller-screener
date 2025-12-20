from __future__ import annotations

from datetime import datetime, date
from ...domain.models import Instrument, OrderBookSnapshot, OrderBookLevel


def map_instrument_payload(payload: dict) -> Instrument:
    maturity = payload.get("maturity_date")
    if isinstance(maturity, str):
        maturity_date = datetime.fromisoformat(maturity).date()
    elif isinstance(maturity, datetime):
        maturity_date = maturity.date()
    elif isinstance(maturity, date):
        maturity_date = maturity
    else:
        raise ValueError("maturity_date is required in instrument payload")

    return Instrument(
        isin=payload.get("isin"),
        figi=payload.get("figi"),
        name=payload.get("name", ""),
        issuer=payload.get("issuer"),
        nominal=float(payload.get("nominal", 1000)),
        maturity_date=maturity_date,
        segment=payload.get("segment"),
        amortization_flag=payload.get("amortization_flag"),
        has_call_offer=payload.get("has_call_offer"),
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
