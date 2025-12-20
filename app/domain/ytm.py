from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Iterable, Tuple


def year_fraction(start: date, end: date) -> float:
    return (end - start).days / 365.0


def build_cashflows(maturity_date: date, nominal: float, coupon_rate: float = 0.0, payments_per_year: int = 2) -> list[Tuple[date, float]]:
    flows: list[Tuple[date, float]] = []
    coupon = nominal * coupon_rate / payments_per_year
    current = maturity_date
    for _ in range(payments_per_year):
        flows.append((current, coupon))
    flows[-1] = (flows[-1][0], flows[-1][1] + nominal)
    return sorted(flows)


def ytm_from_cashflows(price: float, settlement: date, cashflows: Iterable[Tuple[date, float]], guess: float = 0.1) -> float:
    ytm = guess
    for _ in range(20):
        pv = 0.0
        dv = 0.0
        for dt, cf in cashflows:
            t = year_fraction(settlement, dt)
            discount = (1 + ytm) ** t
            pv += cf / discount
            dv -= t * cf / ((1 + ytm) ** (t + 1))
        diff = pv - price
        if abs(diff) < 1e-6:
            break
        ytm -= diff / dv if dv else 0
    return ytm


def ytm_from_price(price: float, nominal: float, maturity_date: date, settlement: date | None = None, coupon_rate: float = 0.0) -> float:
    settlement = settlement or date.today()
    cashflows = build_cashflows(maturity_date, nominal, coupon_rate=coupon_rate)
    return ytm_from_cashflows(price, settlement, cashflows)


def delta_bps(base: float, other: float) -> float:
    return round((other - base) * 10_000, 6)


def dirty_price_from_ytm(ytm: float, settlement: date, cashflows: Iterable[Tuple[date, float]]) -> float:
    price = 0.0
    for dt, cf in cashflows:
        t = year_fraction(settlement, dt)
        price += cf / ((1 + ytm) ** t)
    return price


def days_to_maturity(maturity: date, now: date | None = None) -> int:
    now = now or date.today()
    return max((maturity - now).days, 0)
