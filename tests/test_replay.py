from __future__ import annotations

from datetime import datetime, timedelta
import asyncio
import pytest

from app.domain.models import Event, OrderBookLevel, OrderBookSnapshot
from app.services.replay import ReplayService


class FakeEventRepo:
    def __init__(self, events):
        self.events = events

    async def list_recent(self, limit: int = 200):
        return self.events[:limit]


class FakeSnapshotRepo:
    def __init__(self, snapshots):
        self.snapshots = snapshots

    async def list_all(self):
        return self.snapshots


def test_replay_touch_fill_positive_pnl():
    ts = datetime(2024, 1, 1, 12, 0, 0)
    event = Event(
        isin="TEST",
        ts=ts,
        ytm_mid=0.0,
        ytm_event=0.0,
        delta_ytm_bps=0,
        ask_lots_window=10,
        ask_notional_window=20000,
        spread_ytm_bps=0.0,
        score=1.0,
        stress_flag=False,
        near_maturity_flag=False,
        payload={"eligible": True},
    )

    entry_snapshot = OrderBookSnapshot(
        isin="TEST",
        ts=ts,
        bids=[OrderBookLevel(price=100.0, lots=10)],
        asks=[OrderBookLevel(price=101.0, lots=10)],
    )
    exit_snapshot = OrderBookSnapshot(
        isin="TEST",
        ts=ts + timedelta(minutes=5),
        bids=[OrderBookLevel(price=102.0, lots=10)],
        asks=[OrderBookLevel(price=102.0, lots=10)],
    )

    replay = ReplayService(
        snapshot_repo=FakeSnapshotRepo([entry_snapshot, exit_snapshot]),
        event_repo=FakeEventRepo([event]),
    )

    report = asyncio.run(replay.run(minutes=5, mode="touch"))

    assert report["summary"]["trades"] == 1
    assert report["summary"]["hit_rate"] == 1.0

    trade = report["trades"][0]
    assert trade["lots"] == 10
    assert pytest.approx(trade["pnl"], rel=1e-6) == 100.0
    assert report["summary"]["avg_pnl"] == trade["pnl"]
