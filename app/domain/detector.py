from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from statistics import median

from .models import Event, Instrument, OrderBookSnapshot
from .scoring import score_event
from .ytm import delta_bps, ytm_from_price
from .maturity import is_near_maturity, maturity_bucket
from .stress import is_stressed


class DetectionResult(Event):
    candidate: bool = False
    alert: bool = False


@dataclass
class WindowPoint:
    ts: datetime
    lots: float
    notional: float


@dataclass
class InstrumentHistory:
    windows: list[WindowPoint]
    candidate_streak: int
    last_flush: datetime


class History:
    def __init__(self, *, max_points: int = 200, flush_interval_seconds: int = 600) -> None:
        self.max_points = max_points
        self.flush_interval = timedelta(seconds=flush_interval_seconds)
        self._data: dict[str, InstrumentHistory] = {}
        self._peer_ytm: dict[str, float] = {}
        self._peer_bucket: dict[str, str] = {}

    def _history(self, isin: str) -> InstrumentHistory:
        now = datetime.utcnow()
        if isin not in self._data:
            self._data[isin] = InstrumentHistory(windows=[], candidate_streak=0, last_flush=now)
        return self._data[isin]

    def _flush(self, isin: str, *, now: datetime) -> None:
        history = self._history(isin)
        if (now - history.last_flush) < self.flush_interval:
            return
        cutoff = now - self.flush_interval
        history.windows = [p for p in history.windows if p.ts >= cutoff]
        if len(history.windows) > self.max_points:
            history.windows = history.windows[-self.max_points :]
        history.last_flush = now

    def push_window(self, isin: str, *, ts: datetime, lots: float, notional: float) -> None:
        history = self._history(isin)
        history.windows.append(WindowPoint(ts=ts, lots=lots, notional=notional))
        if len(history.windows) > self.max_points:
            history.windows = history.windows[-self.max_points :]
        self._flush(isin, now=ts)

    def update_peer(self, isin: str, *, bucket: str, ytm_mid: float, eligible: bool) -> None:
        if not eligible:
            self._peer_ytm.pop(isin, None)
            self._peer_bucket.pop(isin, None)
            return
        self._peer_ytm[isin] = ytm_mid
        self._peer_bucket[isin] = bucket

    def peer_median(self, bucket: str) -> float | None:
        values = [ytm for isin, ytm in self._peer_ytm.items() if self._peer_bucket.get(isin) == bucket]
        return median(values) if values else None

    def rolling_median(self, isin: str, *, metric: str, now: datetime) -> float:
        self._flush(isin, now=now)
        values = [getattr(p, metric) for p in self._history(isin).windows]
        return median(values) if values else 0.0

    def novelty(
        self,
        isin: str,
        *,
        now: datetime,
        current_notional: float,
        window_updates: int,
        window_seconds: int,
    ) -> bool:
        self._flush(isin, now=now)
        history = self._history(isin)
        if not history.windows:
            return False

        cutoff = now - timedelta(seconds=window_seconds)
        recent = [p.notional for p in history.windows if p.ts >= cutoff]
        if len(recent) > window_updates:
            recent = recent[-window_updates:]

        if not recent:
            return False

        previous_peak = max(recent)
        return current_notional > previous_peak

    def update_candidate_streak(self, isin: str, *, is_candidate: bool) -> int:
        history = self._history(isin)
        history.candidate_streak = history.candidate_streak + 1 if is_candidate else 0
        return history.candidate_streak


def compute_ask_window(snapshot: OrderBookSnapshot, instrument: Instrument, *, delta_ytm_max_bps: int):
    if not snapshot.asks or not instrument.eligible:
        return 0.0, 0.0, 0.0, 0.0

    ytm_bid1 = ytm_from_price(
        snapshot.best_bid or snapshot.best_ask or instrument.nominal,
        instrument.nominal,
        instrument.maturity_date,
        eligible=instrument.eligible,
    )
    ytm_ask1 = ytm_from_price(
        snapshot.best_ask or snapshot.best_bid or instrument.nominal,
        instrument.nominal,
        instrument.maturity_date,
        eligible=instrument.eligible,
    )
    if ytm_bid1 is None or ytm_ask1 is None:
        return 0.0, 0.0, 0.0, 0.0

    ytm_mid = (ytm_bid1 + ytm_ask1) / 2
    ask_window_lots = 0
    ask_window_notional = 0.0
    ytm_event = ytm_mid

    for lvl in snapshot.asks:
        ytm_level = ytm_from_price(
            lvl.price,
            instrument.nominal,
            instrument.maturity_date,
            eligible=instrument.eligible,
        )
        if ytm_level is None:
            continue
        delta = delta_bps(ytm_mid, ytm_level)
        if delta <= delta_ytm_max_bps:
            ask_window_lots += lvl.lots
            ask_window_notional += lvl.lots * instrument.nominal * lvl.price / 100
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
    novelty_window_updates: int,
    novelty_window_seconds: int,
    alert_hold_updates: int,
    spread_ytm_max_bps: int,
    near_maturity_days: int,
    stress_params: dict,
) -> DetectionResult | None:
    if not instrument.eligible:
        return None

    rolling_lots = history.rolling_median(snapshot.isin, metric="lots", now=snapshot.ts)
    rolling_notional = history.rolling_median(snapshot.isin, metric="notional", now=snapshot.ts)
    ask_lots, ask_notional, ytm_mid, ytm_event = compute_ask_window(
        snapshot, instrument, delta_ytm_max_bps=delta_ytm_max_bps
    )
    bucket = maturity_bucket(instrument.maturity_date)
    history.update_peer(snapshot.isin, bucket=bucket, ytm_mid=ytm_mid, eligible=instrument.eligible)
    peer_median_ytm = history.peer_median(bucket) or ytm_mid
    dev_to_peer_bps = delta_bps(peer_median_ytm, ytm_mid)
    novelty = history.novelty(
        snapshot.isin,
        now=snapshot.ts,
        current_notional=ask_notional,
        window_updates=novelty_window_updates,
        window_seconds=novelty_window_seconds,
    )
    history.push_window(snapshot.isin, ts=snapshot.ts, lots=ask_lots, notional=ask_notional)

    candidate = (
        ask_lots >= ask_window_min_lots
        and ask_notional >= ask_window_min_notional
        and (rolling_lots == 0 or ask_lots >= ask_window_kvol * rolling_lots)
        and (rolling_notional == 0 or ask_notional >= ask_window_kvol * rolling_notional)
    )

    streak = history.update_candidate_streak(snapshot.isin, is_candidate=candidate)

    spread_ytm = abs(delta_bps(ytm_mid, ytm_event))
    delta_event_bps = int(round(delta_bps(ytm_mid, ytm_event)))
    stress_flag = is_stressed(
        ytm_mid,
        clean_price_pct=snapshot.best_ask or 0,
        spread_ytm_bps=spread_ytm,
        instrument_maturity=instrument.maturity_date,
        peer_dev_bps=dev_to_peer_bps,
        **stress_params,
    )
    near_flag = is_near_maturity(instrument.maturity_date, near_maturity_days=near_maturity_days)

    if not candidate:
        return None

    score = score_event(delta_event_bps, ask_notional, spread_ytm, stress_flag)
    alert = candidate and novelty and streak >= alert_hold_updates
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
        payload={
            "rolling_lots": rolling_lots,
            "rolling_notional": rolling_notional,
            "eligible": instrument.eligible,
            "eligible_reason": instrument.eligible_reason,
            "has_call_offer": instrument.has_call_offer,
            "amortization_flag": instrument.amortization_flag,
            "best_bid": snapshot.best_bid,
            "best_ask": snapshot.best_ask,
            "ask_window_notional_definition": "cash_value_lots_nominal_price_pct",
            "novelty": novelty,
            "maturity_bucket": bucket,
            "peer_median_ytm": peer_median_ytm,
            "dev_to_peer_bps": dev_to_peer_bps,
        },
        candidate=True,
        alert=alert,
    )
    return event
