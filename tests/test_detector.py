from datetime import datetime, date
from app.domain.detector import History, detect_event
from app.domain.models import Instrument, OrderBookSnapshot, OrderBookLevel
from app.settings import get_settings


def make_snapshot():
    return OrderBookSnapshot(
        isin="RU000A0JX0J2",
        ts=datetime.utcnow(),
        bids=[OrderBookLevel(price=98.0, lots=50)],
        asks=[OrderBookLevel(price=90.0, lots=40), OrderBookLevel(price=90.5, lots=20)],
        nominal=1000,
    )


def test_candidate_detected():
    inst = Instrument(
        isin="RU000A0JX0J2",
        name="Bond",
        issuer="A",
        nominal=1000,
        maturity_date=date(2030, 1, 1),
        eligible=True,
    )
    settings = get_settings()
    history = History()
    event = detect_event(
        make_snapshot(),
        inst,
        history,
        delta_ytm_max_bps=250,
        ask_window_min_lots=10,
        ask_window_min_notional=50_000,
        ask_window_kvol=1,
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
    assert event.candidate
    assert event.alert
    assert event.ask_lots_window >= 40
