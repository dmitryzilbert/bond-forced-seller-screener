from datetime import date
from app.domain import ytm


def test_ytm_zero_coupon():
    maturity = date(2025, 1, 1)
    price = 900
    res = ytm.ytm_from_price(price, nominal=1000, maturity_date=maturity, settlement=date(2024, 1, 1))
    assert 0.11 < res < 0.13


def test_delta_bps():
    assert ytm.delta_bps(0.1, 0.105) == 50.0
