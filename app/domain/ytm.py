from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from math import isfinite
from typing import Iterable, List, Tuple

ACT_365 = "ACT/365"
NEAR_MATURITY_DAYS = 30


@dataclass
class YTMResult:
    ytm: float | None
    low_quality: bool = False


def year_fraction(start: date, end: date, *, daycount: str = ACT_365) -> float:
    if daycount != ACT_365:
        raise ValueError("Unsupported daycount: only ACT/365 is implemented")
    return (end - start).days / 365.0


def build_cashflows(
    maturity_date: date,
    nominal: float,
    *,
    coupon_rate: float = 0.0,
    payments_per_year: int = 2,
    settlement: date | None = None,
) -> list[Tuple[date, float]]:
    """Build a simple bullet cashflow schedule.

    The schedule approximates equal periods before maturity. It is intentionally
    conservative and should only be used when a precise coupon schedule is not
    available.
    """

    settlement = settlement or date.today()
    flows: list[Tuple[date, float]] = []

    # If coupon rate is effectively zero, only redemption matters.
    if coupon_rate == 0:
        return [(maturity_date, nominal)]

    coupon = nominal * coupon_rate / payments_per_year
    current = maturity_date
    step = timedelta(days=365 // payments_per_year)
    while current > settlement:
        flows.append((current, coupon))
        current -= step

    if not flows:
        flows = [(maturity_date, nominal + coupon)]
    else:
        flows[0] = (flows[0][0], flows[0][1] + nominal)

    return sorted(flows)


def _npv(rate: float, price: float, timed_cashflows: List[Tuple[float, float]]) -> float:
    total = 0.0
    for t, cf in timed_cashflows:
        discount = (1 + rate) ** t
        total += cf / discount
    return total - price


def yield_to_maturity(
    price_dirty: float,
    cashflows: Iterable[Tuple[date, float]],
    *,
    settlement_date: date | None = None,
    daycount: str = ACT_365,
    max_iter: int = 256,
    tolerance: float = 1e-9,
    lower_bound: float = -0.99,
    upper_bound: float = 5.0,
) -> float:
    """Compute YTM (as decimal) via a robust bisection method."""

    if price_dirty <= 0:
        raise ValueError("Price must be positive for YTM computation")

    settlement = settlement_date or date.today()
    timed_flows = [
        (max(year_fraction(settlement, dt, daycount=daycount), 0.0), cf)
        for dt, cf in cashflows
        if cf > 0 and dt >= settlement
    ]

    if not timed_flows:
        raise ValueError("Cashflows are empty or in the past")

    npv_lower = _npv(lower_bound, price_dirty, timed_flows)
    npv_upper = _npv(upper_bound, price_dirty, timed_flows)

    # Expand bounds if necessary to ensure a root exists within [lower, upper].
    expansion = upper_bound
    while npv_lower * npv_upper > 0 and expansion < 1000:
        expansion *= 2
        npv_upper = _npv(expansion, price_dirty, timed_flows)
        upper_bound = expansion

    if npv_lower * npv_upper > 0:
        raise ValueError("Failed to bracket YTM root")

    for _ in range(max_iter):
        mid = (lower_bound + upper_bound) / 2
        npv_mid = _npv(mid, price_dirty, timed_flows)
        if abs(npv_mid) < tolerance:
            return mid
        if npv_lower * npv_mid < 0:
            upper_bound = mid
            npv_upper = npv_mid
        else:
            lower_bound = mid
            npv_lower = npv_mid

    return mid


def simple_yield_proxy(price_dirty: float, redemption: float, days_to_maturity: int) -> float:
    days = max(days_to_maturity, NEAR_MATURITY_DAYS)
    simple_return = redemption / price_dirty - 1
    return simple_return * 365 / days


def compute_ytm(
    price_clean: float,
    nominal: float,
    maturity_date: date,
    *,
    accrued_interest: float | None = None,
    cashflows: Iterable[Tuple[date, float]] | None = None,
    settlement: date | None = None,
    coupon_rate: float = 0.0,
    payments_per_year: int = 2,
    eligible: bool = True,
) -> YTMResult:
    if not eligible:
        return YTMResult(None, True)

    settlement = settlement or date.today()
    dirty_price = price_clean + accrued_interest if accrued_interest is not None else price_clean
    low_quality = accrued_interest is None

    flows = list(cashflows) if cashflows else build_cashflows(
        maturity_date,
        nominal,
        coupon_rate=coupon_rate,
        payments_per_year=payments_per_year,
        settlement=settlement,
    )
    if cashflows is None or accrued_interest is None:
        low_quality = True

    flows = [(dt, cf) for dt, cf in flows if dt >= settlement and cf > 0]
    if not flows or dirty_price <= 0:
        return YTMResult(None, True)

    try:
        ytm_value = yield_to_maturity(dirty_price, flows, settlement_date=settlement)
    except ValueError:
        ytm_value = None

    if ytm_value is None or not isfinite(ytm_value):
        days_left = max((flows[-1][0] - settlement).days, 0)
        ytm_value = simple_yield_proxy(dirty_price, flows[-1][1], days_left)
        low_quality = True

    return YTMResult(ytm_value, low_quality)


def ytm_from_price(
    price: float,
    nominal: float,
    maturity_date: date,
    settlement: date | None = None,
    coupon_rate: float = 0.0,
    accrued_interest: float | None = None,
    cashflows: Iterable[Tuple[date, float]] | None = None,
    payments_per_year: int = 2,
    eligible: bool = True,
) -> float | None:
    result = compute_ytm(
        price,
        nominal,
        maturity_date,
        accrued_interest=accrued_interest,
        cashflows=cashflows,
        settlement=settlement,
        coupon_rate=coupon_rate,
        payments_per_year=payments_per_year,
        eligible=eligible,
    )
    return result.ytm


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
