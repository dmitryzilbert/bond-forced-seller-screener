from __future__ import annotations

from datetime import date
from ...domain.models import Event, Instrument
from ...domain.ytm import days_to_maturity


def _fmt_bool(value: bool | None) -> str:
    return "да" if value else "нет"


def _fmt_best_ask(event: Event) -> str:
    best_ask = (event.payload or {}).get("best_ask")
    if best_ask is None:
        return "n/a"
    return f"{best_ask:.2f}"


def format_message(event: Event, instrument: Instrument, dashboard_url: str) -> str:
    d2m = days_to_maturity(instrument.maturity_date)
    lines = [
        f"*{instrument.issuer or 'Эмитент н/д'} / {instrument.name}*",
        f"ISIN: `{instrument.isin}`",
        f"Погашение: {instrument.maturity_date} ({d2m}d)",
        f"Best ask: {_fmt_best_ask(event)}",
        (
            f"YTM (mid/event): {event.ytm_mid:.2%} → {event.ytm_event:.2%} "
            f"(Δ {event.delta_ytm_bps:+.1f} bps)"
        ),
        f"AskVolWindowLots: {event.ask_lots_window:.0f}",
        f"AskVolWindowNotional: {event.ask_notional_window:,.0f} ₽",
        f"Spread (YTM): {event.spread_ytm_bps:.1f} bps | Score: {event.score:.2f}",
        f"Оферта (CALL): {_fmt_bool(instrument.has_call_offer)}",
        f"Амортизация: {_fmt_bool(instrument.amortization_flag)}",
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
