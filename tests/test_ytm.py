from datetime import date, timedelta

from app.domain import ytm


def test_ytm_zero_coupon_monotonic():
    settlement = date(2024, 1, 1)
    maturity = date(2025, 1, 1)
    cashflows = [(maturity, 1000.0)]

    higher_yield = ytm.yield_to_maturity(900, cashflows, settlement_date=settlement)
    lower_yield = ytm.yield_to_maturity(950, cashflows, settlement_date=settlement)

    assert higher_yield > lower_yield
    assert 0.1 < higher_yield < 0.15


def test_coupon_bond_sanity_and_monotonic():
    settlement = date(2024, 1, 1)
    coupon1 = settlement + timedelta(days=182)
    maturity = date(2025, 1, 1)
    cashflows = [(coupon1, 25.0), (maturity, 25.0 + 1000.0)]

    ytm_low_price = ytm.yield_to_maturity(960, cashflows, settlement_date=settlement)
    ytm_high_price = ytm.yield_to_maturity(1000, cashflows, settlement_date=settlement)

    assert 0.08 < ytm_low_price < 0.11
    assert ytm_low_price > ytm_high_price


def test_near_maturity_proxy_is_finite():
    settlement = date(2024, 1, 1)
    maturity = settlement + timedelta(days=10)
    result = ytm.compute_ytm(995, 1000, maturity, settlement=settlement)

    assert result.ytm is not None
    assert result.ytm == result.ytm  # not NaN
    assert result.ytm < 1  # should not explode even near maturity


def test_ineligible_returns_none():
    settlement = date(2024, 1, 1)
    maturity = date(2024, 6, 1)
    res = ytm.ytm_from_price(980, 1000, maturity, settlement=settlement, eligible=False)
    assert res is None


def test_delta_bps():
    assert ytm.delta_bps(0.1, 0.105) == 50.0
