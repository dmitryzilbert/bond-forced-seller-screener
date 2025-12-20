from datetime import datetime, date

from app.adapters.telegram.formatters import format_message
from app.domain.models import Event, Instrument


def test_format_message_snapshot(monkeypatch):
    monkeypatch.setattr(
        "app.adapters.telegram.formatters.days_to_maturity",
        lambda _: 120,
    )

    event = Event(
        isin="RU000A0JX0J2",
        ts=datetime(2024, 1, 1),
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
    instrument = Instrument(
        isin="RU000A0JX0J2",
        name="Bond",
        issuer="Issuer",
        nominal=1000,
        maturity_date=date(2025, 1, 1),
        has_call_offer=False,
        amortization_flag=False,
    )

    msg = format_message(event, instrument, "http://localhost:8000")

    assert msg == (
        "*Issuer / Bond*\n"
        "ISIN: `RU000A0JX0J2`\n"
        "Погашение: 2025-01-01 (120d)\n"
        "Best ask: 90.00\n"
        "YTM (mid/event): 10.00% → 12.00% (Δ +200.0 bps)\n"
        "AskVolWindowLots: 100\n"
        "AskVolWindowNotional: 1,000,000 ₽\n"
        "Spread (YTM): 50.0 bps | Score: 5.00\n"
        "Оферта (CALL): нет\n"
        "Амортизация: нет\n"
        "Flags: near_maturity, stress\n"
        "Dashboard: http://localhost:8000/instrument/RU000A0JX0J2"
    )
