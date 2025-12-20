from datetime import datetime, date, timedelta

import pytest

from app.domain import detector
from app.domain.detector import History, detect_event
from app.domain.models import Instrument, OrderBookSnapshot, OrderBookLevel
from app.settings import get_settings


@pytest.fixture(autouse=True)
def patch_ytm(monkeypatch):
    """Use simple linear YTM to keep test math deterministic."""

    def fake_ytm_from_price(price, nominal, maturity_date, eligible=True, **kwargs):
        return price / 1000  # 100 -> 0.1 (10%)

    monkeypatch.setattr(detector, "ytm_from_price", fake_ytm_from_price)
    monkeypatch.setattr(detector, "delta_bps", lambda base, other: round((other - base) * 10_000, 6))


def make_instrument(**kwargs):
    return Instrument(
        isin="RU000A0JX0J2",
        name="Bond",
        issuer="A",
        nominal=1000,
        maturity_date=date(2030, 1, 1),
        eligible=True,
        **kwargs,
    )


def make_snapshot(ts: datetime, asks: list[OrderBookLevel]):
    return OrderBookSnapshot(
        isin="RU000A0JX0J2",
        ts=ts,
        bids=[OrderBookLevel(price=100.0, lots=10)],
        asks=asks,
        nominal=1000,
    )


def test_ask_window_filters_and_notional():
    instrument = make_instrument()
    snap = make_snapshot(
        datetime.utcnow(),
        [
            OrderBookLevel(price=100.0, lots=10),  # ytm=0.1, delta=0
            OrderBookLevel(price=100.4, lots=5),   # ytm=0.1004, delta=40 bps -> include
            OrderBookLevel(price=110.0, lots=3),   # ytm=0.11, delta=100 bps -> exclude
        ],
    )

    lots, notional, ytm_mid, ytm_event = detector.compute_ask_window(
        snap,
        instrument,
        delta_ytm_max_bps=50,
    )

    assert lots == 15
    # notional = lots * nominal * price/100
    assert notional == pytest.approx(10 * 1000 * 1.0 + 5 * 1000 * 1.004)
    assert ytm_event == pytest.approx(0.1004)


def test_candidate_then_alert_with_novelty_and_hold():
    settings = get_settings()
    instrument = make_instrument()
    history = History(max_points=10, flush_interval_seconds=60)
    base_ts = datetime.utcnow()

    snap1 = make_snapshot(base_ts, [OrderBookLevel(price=100.0, lots=30)])
    event1 = detect_event(
        snap1,
        instrument,
        history,
        delta_ytm_max_bps=250,
        ask_window_min_lots=10,
        ask_window_min_notional=20_000,
        ask_window_kvol=1,
        novelty_window_updates=3,
        novelty_window_seconds=60,
        alert_hold_updates=2,
        spread_ytm_max_bps=300,
        near_maturity_days=settings.near_maturity_days,
        stress_params={
            "stress_ytm_high_pct": settings.stress_ytm_high_pct,
            "stress_price_low_pct": settings.stress_price_low_pct,
            "stress_spread_ytm_bps": settings.stress_spread_ytm_bps,
            "stress_dev_peer_bps": settings.stress_dev_peer_bps,
        },
    )
    assert event1 is not None
    assert event1.candidate is True
    assert event1.alert is False  # no novelty yet

    snap2 = make_snapshot(base_ts + timedelta(seconds=10), [OrderBookLevel(price=100.5, lots=40)])
    event2 = detect_event(
        snap2,
        instrument,
        history,
        delta_ytm_max_bps=250,
        ask_window_min_lots=10,
        ask_window_min_notional=20_000,
        ask_window_kvol=1,
        novelty_window_updates=3,
        novelty_window_seconds=60,
        alert_hold_updates=2,
        spread_ytm_max_bps=300,
        near_maturity_days=settings.near_maturity_days,
        stress_params={
            "stress_ytm_high_pct": settings.stress_ytm_high_pct,
            "stress_price_low_pct": settings.stress_price_low_pct,
            "stress_spread_ytm_bps": settings.stress_spread_ytm_bps,
            "stress_dev_peer_bps": settings.stress_dev_peer_bps,
        },
    )

    assert event2 is not None
    assert event2.candidate is True
    assert event2.alert is True
    assert event2.payload["novelty"] is True


def test_delta_ytm_bps_units():
    settings = get_settings()
    instrument = make_instrument()
    history = History()

    snapshot = make_snapshot(
        datetime.utcnow(),
        [
            OrderBookLevel(price=100.0, lots=1),  # best ask to anchor mid at 0.1
            OrderBookLevel(price=105.0, lots=30),  # ytm_level = 0.105
        ],
    )

    event = detect_event(
        snapshot,
        instrument,
        history,
        delta_ytm_max_bps=1000,
        ask_window_min_lots=1,
        ask_window_min_notional=1,
        ask_window_kvol=1,
        novelty_window_updates=1,
        novelty_window_seconds=60,
        alert_hold_updates=1,
        spread_ytm_max_bps=300,
        near_maturity_days=settings.near_maturity_days,
        stress_params={
            "stress_ytm_high_pct": settings.stress_ytm_high_pct,
            "stress_price_low_pct": settings.stress_price_low_pct,
            "stress_spread_ytm_bps": settings.stress_spread_ytm_bps,
            "stress_dev_peer_bps": settings.stress_dev_peer_bps,
        },
    )

    assert event is not None
    assert event.delta_ytm_bps == 50  # (0.105 - 0.1) * 10_000
    assert isinstance(event.delta_ytm_bps, int)
