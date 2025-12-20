from datetime import datetime, date
from app.adapters.telegram.formatters import format_message
from app.domain.models import Event, Instrument


def test_format_message_contains_flags():
    event = Event(
        isin="RU000A0JX0J2",
        ts=datetime.utcnow(),
        ytm_mid=0.1,
        ytm_event=0.12,
        delta_ytm_bps=200,
        ask_lots_window=100,
        ask_notional_window=1_000_000,
        spread_ytm_bps=50,
        score=5.0,
        stress_flag=True,
        near_maturity_flag=True,
        payload={"best_ask": 90.0},
    )
    instrument = Instrument(isin="RU000A0JX0J2", name="Bond", issuer="A", nominal=1000, maturity_date=date.today())
    msg = format_message(event, instrument, "http://localhost:8000")
    assert "stress" in msg
    assert "near_maturity" in msg
    assert "RU000A0JX0J2" in msg
