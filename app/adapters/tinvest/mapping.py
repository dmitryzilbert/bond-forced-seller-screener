from __future__ import annotations

from datetime import datetime, date, timezone
from ...domain.models import Instrument, OrderBookSnapshot, OrderBookLevel


def _parse_ts(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        ts = value
    else:
        ts = datetime.fromisoformat(str(value).replace("Z", "+00:00"))

    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def _parse_price(value) -> float:
    if value is None:
        return 0.0
    if isinstance(value, str):
        return float(value.replace(",", "."))
    return float(value)


def _parse_lots(value) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _parse_floating_coupon(payload: dict) -> bool | None:
    value = (
        payload.get("floating_coupon_flag")
        or payload.get("floatingCouponFlag")
        or payload.get("coupon_is_floating")
        or payload.get("couponIsFloating")
    )
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes"}:
            return True
        if lowered in {"false", "0", "no"}:
            return False
    coupon_type = payload.get("coupon_type") or payload.get("couponType")
    if isinstance(coupon_type, str):
        coupon_type = coupon_type.strip().upper()
        if coupon_type in {"FLOAT", "FLOATING", "VARIABLE"}:
            return True
        if coupon_type in {"FIXED", "CONSTANT"}:
            return False
    return None


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
        instrument_uid=payload.get("instrument_uid") or payload.get("instrumentUid"),
        figi=payload.get("figi"),
        ticker=payload.get("ticker"),
        class_code=payload.get("class_code") or payload.get("classCode"),
        name=payload.get("name", ""),
        issuer=payload.get("issuer"),
        nominal=float(payload.get("nominal", 1000)),
        maturity_date=maturity_date,
        segment=payload.get("segment"),
        amortization_flag=payload.get("amortization_flag"),
        floating_coupon_flag=_parse_floating_coupon(payload),
        has_call_offer=payload.get("has_call_offer"),
    )


def map_orderbook_payload(payload: dict) -> OrderBookSnapshot:
    bids = [
        OrderBookLevel(price=_parse_price(level[0]), lots=_parse_lots(level[1]))
        for level in payload.get("bids", [])
    ]
    asks = [
        OrderBookLevel(price=_parse_price(level[0]), lots=_parse_lots(level[1]))
        for level in payload.get("asks", [])
    ]
    return OrderBookSnapshot(
        isin=payload["isin"],
        ts=_parse_ts(payload["ts"]),
        bids=bids,
        asks=asks,
        nominal=_parse_price(payload.get("nominal", 1000)),
    )
