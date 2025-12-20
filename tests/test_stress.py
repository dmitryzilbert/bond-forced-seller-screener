from datetime import date
from app.domain.stress import is_stressed


def test_stress_conditions():
    flag = is_stressed(
        ytm_mid=0.5,
        clean_price_pct=80,
        spread_ytm_bps=700,
        instrument_maturity=date(2030, 1, 1),
        peer_dev_bps=1600,
        stress_ytm_high_pct=40,
        stress_price_low_pct=90,
        stress_spread_ytm_bps=600,
        stress_dev_peer_bps=1500,
    )
    assert flag
