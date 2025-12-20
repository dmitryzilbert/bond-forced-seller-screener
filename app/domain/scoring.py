from __future__ import annotations

import math
from .models import Event


def score_event(delta_ytm_bps: float, ask_notional_window: float, spread_ytm_bps: float, stress_flag: bool = False) -> float:
    base = max(delta_ytm_bps, 0) / 10
    volume_score = math.log1p(ask_notional_window / 100_000)
    spread_penalty = max(1.0, spread_ytm_bps / 100)
    stress_bonus = 0.5 if stress_flag else 0.0
    return (base + volume_score + stress_bonus) / spread_penalty


def rerank(events: list[Event]) -> list[Event]:
    return sorted(events, key=lambda e: e.score, reverse=True)
