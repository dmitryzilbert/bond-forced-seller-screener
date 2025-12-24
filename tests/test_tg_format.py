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


def test_format_message_contains_test_alert_fields(monkeypatch):
    monkeypatch.setattr(
        "app.adapters.telegram.formatters.days_to_maturity",
        lambda _: 10,
    )

    event = Event(
        isin="TEST00000000",
        ts=datetime(2024, 1, 1),
        ytm_mid=0.12,
        ytm_event=0.13,
        delta_ytm_bps=100,
        ask_lots_window=50,
        ask_notional_window=500_000,
        spread_ytm_bps=120.0,
        score=9.5,
        payload={"best_ask": 101.23, "eligible": True},
    )
    instrument = Instrument(
        isin="TEST00000000",
        figi="TESTFIGI",
        name="Test Bond",
        issuer="Test Issuer",
        nominal=1000.0,
        maturity_date=date(2024, 12, 31),
        eligible=True,
        is_shortlisted=True,
        eligibility_checked_at=datetime(2024, 1, 1),
    )

    msg = format_message(event, instrument, "http://localhost:8000")

    assert "ISIN: `TEST00000000`" in msg
    assert "YTM (mid/event): 12.00% → 13.00% (Δ +100.0 bps)" in msg
    assert "AskVolWindowNotional: 500,000 ₽" in msg
    assert "Dashboard: http://localhost:8000/instrument/TEST00000000" in msg
