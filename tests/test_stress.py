from datetime import date

from app.domain.stress import is_stressed


def test_stress_by_high_ytm():
    flag = is_stressed(
        ytm_mid=0.41,
        clean_price_pct=95,
        spread_ytm_bps=100,
        instrument_maturity=date(2030, 1, 1),
        peer_dev_bps=100,
        stress_ytm_high_pct=40,
        stress_price_low_pct=90,
        stress_spread_ytm_bps=600,
        stress_dev_peer_bps=1500,
    )
    assert flag is True


def test_stress_by_low_price():
    flag = is_stressed(
        ytm_mid=0.05,
        clean_price_pct=80,
        spread_ytm_bps=100,
        instrument_maturity=date(2030, 1, 1),
        peer_dev_bps=100,
        stress_ytm_high_pct=40,
        stress_price_low_pct=90,
        stress_spread_ytm_bps=600,
        stress_dev_peer_bps=1500,
    )
    assert flag is True


def test_stress_by_wide_spread():
    flag = is_stressed(
        ytm_mid=0.05,
        clean_price_pct=95,
        spread_ytm_bps=700,
        instrument_maturity=date(2030, 1, 1),
        peer_dev_bps=100,
        stress_ytm_high_pct=40,
        stress_price_low_pct=90,
        stress_spread_ytm_bps=600,
        stress_dev_peer_bps=1500,
    )
    assert flag is True


def test_stress_by_peer_deviation():
    flag = is_stressed(
        ytm_mid=0.05,
        clean_price_pct=95,
        spread_ytm_bps=100,
        instrument_maturity=date(2030, 1, 1),
        peer_dev_bps=2000,
        stress_ytm_high_pct=40,
        stress_price_low_pct=90,
        stress_spread_ytm_bps=600,
        stress_dev_peer_bps=1500,
    )
    assert flag is True


def test_no_stress_when_below_thresholds():
    flag = is_stressed(
        ytm_mid=0.05,
        clean_price_pct=95,
        spread_ytm_bps=100,
        instrument_maturity=date(2030, 1, 1),
        peer_dev_bps=100,
        stress_ytm_high_pct=40,
        stress_price_low_pct=90,
        stress_spread_ytm_bps=600,
        stress_dev_peer_bps=1500,
    )
    assert flag is False
