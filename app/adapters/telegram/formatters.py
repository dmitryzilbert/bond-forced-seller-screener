from __future__ import annotations

from datetime import date
from ...domain.models import Event, Instrument
from ...domain.ytm import days_to_maturity


def format_message(event: Event, instrument: Instrument, dashboard_url: str) -> str:
    d2m = days_to_maturity(instrument.maturity_date)
    lines = [
        f"*{instrument.name}* ({instrument.isin})",
        f"Maturity: {instrument.maturity_date} ({d2m}d)",
        f"Best ask: {event.payload.get('best_ask', 'n/a') if event.payload else 'n/a'}",
        f"YTM mid: {event.ytm_mid:.2%}",
        f"YTM event: {event.ytm_event:.2%} (Δ {event.delta_ytm_bps:.1f} bps)",
        f"AskWindow: {event.ask_lots_window} lots / {event.ask_notional_window:,.0f} ₽",
        f"Spread: {event.spread_ytm_bps:.1f} bps",
    ]
    flags = []
    if event.near_maturity_flag:
        flags.append("near_maturity")
    if event.stress_flag:
        flags.append("stress")
    if flags:
        lines.append("Flags: " + ", ".join(flags))
    lines.append(f"Dashboard: {dashboard_url}/instrument/{instrument.isin}")
    return "\n".join(lines)
