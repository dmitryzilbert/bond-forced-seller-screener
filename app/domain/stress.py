from __future__ import annotations

from datetime import date


def is_stressed(
    ytm_mid: float,
    clean_price_pct: float,
    spread_ytm_bps: float,
    instrument_maturity: date,
    peer_dev_bps: float,
    *,
    stress_ytm_high_pct: int,
    stress_price_low_pct: int,
    stress_spread_ytm_bps: int,
    stress_dev_peer_bps: int,
) -> bool:
    conditions = [
        ytm_mid * 100 >= stress_ytm_high_pct,
        clean_price_pct <= stress_price_low_pct,
        spread_ytm_bps >= stress_spread_ytm_bps,
        abs(peer_dev_bps) >= stress_dev_peer_bps,
    ]
    return any(conditions)
