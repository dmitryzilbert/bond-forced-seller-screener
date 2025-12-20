from __future__ import annotations

from datetime import datetime
from statistics import median
from typing import Iterable

from .models import Event, Instrument, OrderBookSnapshot
from .scoring import score_event
from .ytm import delta_bps, ytm_from_price
from .maturity import is_near_maturity
from .stress import is_stressed


class DetectionResult(Event):
    candidate: bool = False
    alert: bool = False


class History:
    def __init__(self) -> None:
        self.ask_windows: dict[str, list[float]] = {}

    def push_window(self, isin: str, value: float) -> None:
        self.ask_windows.setdefault(isin, []).append(value)
        if len(self.ask_windows[isin]) > 200:
            self.ask_windows[isin] = self.ask_windows[isin][-200:]

    def rolling_median(self, isin: str) -> float:
        values = self.ask_windows.get(isin, [])
        return median(values) if values else 0.0


def compute_ask_window(snapshot: OrderBookSnapshot, instrument: Instrument, *, delta_ytm_max_bps: int):
    if not snapshot.asks:
        return 0.0, 0.0, 0.0, 0.0

    ytm_bid1 = ytm_from_price(snapshot.best_bid or snapshot.best_ask or instrument.nominal, instrument.nominal, instrument.maturity_date)
    ytm_ask1 = ytm_from_price(snapshot.best_ask or snapshot.best_bid or instrument.nominal, instrument.nominal, instrument.maturity_date)
    ytm_mid = (ytm_bid1 + ytm_ask1) / 2
    ask_window_lots = 0
    ask_window_notional = 0.0
    ytm_event = ytm_mid

    for lvl in snapshot.asks:
        ytm_level = ytm_from_price(lvl.price, instrument.nominal, instrument.maturity_date)
        if abs(delta_bps(ytm_mid, ytm_level)) <= delta_ytm_max_bps:
            ask_window_lots += lvl.lots
            ask_window_notional += lvl.lots * instrument.nominal
            ytm_event = max(ytm_event, ytm_level)

    return ask_window_lots, ask_window_notional, ytm_mid, ytm_event


def detect_event(
    snapshot: OrderBookSnapshot,
    instrument: Instrument,
    history: History,
    *,
    delta_ytm_max_bps: int,
    ask_window_min_lots: int,
    ask_window_min_notional: int,
    ask_window_kvol: int,
    spread_ytm_max_bps: int,
    near_maturity_days: int,
    stress_params: dict,
) -> DetectionResult | None:
    ask_lots, ask_notional, ytm_mid, ytm_event = compute_ask_window(snapshot, instrument, delta_ytm_max_bps=delta_ytm_max_bps)
    history.push_window(snapshot.isin, ask_lots)
    rolling = history.rolling_median(snapshot.isin)

    candidate = (
        ask_lots >= ask_window_min_lots
        and ask_notional >= ask_window_min_notional
        and (rolling == 0 or ask_lots >= ask_window_kvol * rolling)
    )

    spread_ytm = abs(delta_bps(ytm_mid, ytm_event))
    delta_event_bps = delta_bps(ytm_mid, ytm_event)
    stress_flag = is_stressed(
        ytm_mid,
        clean_price_pct=snapshot.best_ask or 0,
        spread_ytm_bps=spread_ytm,
        instrument_maturity=instrument.maturity_date,
        peer_dev_bps=spread_ytm,
        **stress_params,
    )
    near_flag = is_near_maturity(instrument.maturity_date, near_maturity_days=near_maturity_days)

    if not candidate:
        return None

    score = score_event(delta_event_bps, ask_notional, spread_ytm, stress_flag)
    event = DetectionResult(
        isin=snapshot.isin,
        ts=snapshot.ts,
        ytm_mid=ytm_mid,
        ytm_event=ytm_event,
        delta_ytm_bps=delta_event_bps,
        ask_lots_window=ask_lots,
        ask_notional_window=ask_notional,
        spread_ytm_bps=spread_ytm,
        score=score,
        stress_flag=stress_flag,
        near_maturity_flag=near_flag,
        payload={"rolling": rolling},
        candidate=True,
        alert=candidate,
    )
    return event
