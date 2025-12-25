from __future__ import annotations

import asyncio
from collections import defaultdict
from datetime import timedelta
from typing import Literal

from ..domain.models import Event, OrderBookSnapshot
from ..storage.repo import EventRepository, SnapshotRepository


class ReplayService:
    def __init__(self, snapshot_repo: SnapshotRepository, event_repo: EventRepository):
        self.snapshot_repo = snapshot_repo
        self.event_repo = event_repo

    async def run(
        self,
        *,
        minutes: int = 5,
        mode: Literal["touch", "buffer", "volume_cap"] = "touch",
        buffer_bps: float = 5.0,
        volume_cap: float = 1.0,
        exit_on: Literal["mid", "bid"] = "mid",
        limit_events: int = 200,
    ) -> dict:
        events = await self.event_repo.list_recent(limit=limit_events)
        snapshots = await self.snapshot_repo.list_all()
        snapshots_by_isin = self._group_snapshots(snapshots)

        trades = []
        for event in events:
            if not self._is_eligible(event):
                continue
            candidates = snapshots_by_isin.get(event.isin, [])
            entry = self._find_entry(candidates, event.ts)
            if entry is None:
                continue

            exit_ts = event.ts + timedelta(minutes=minutes)
            exit_snapshot = self._find_entry(candidates, exit_ts)
            if exit_snapshot is None:
                continue

            trade = self._simulate_trade(
                event,
                entry,
                exit_snapshot,
                mode=mode,
                buffer_bps=buffer_bps,
                volume_cap=volume_cap,
                exit_on=exit_on,
            )
            if trade is not None:
                trades.append(trade)

        return self._report(trades)

    def _report(self, trades: list[dict]) -> dict:
        if not trades:
            return {"trades": [], "summary": {"trades": 0, "hit_rate": 0.0, "avg_pnl": 0.0}}

        total_pnl = sum(t["pnl"] for t in trades)
        winners = [t for t in trades if t["pnl"] > 0]
        pnl_by_isin: dict[str, float] = defaultdict(float)
        for trade in trades:
            pnl_by_isin[trade["isin"]] += trade["pnl"]

        top = sorted(pnl_by_isin.items(), key=lambda item: item[1], reverse=True)
        summary = {
            "trades": len(trades),
            "hit_rate": len(winners) / len(trades),
            "avg_pnl": total_pnl / len(trades),
            "worst": min(trades, key=lambda t: t["pnl"]),
            "best": max(trades, key=lambda t: t["pnl"]),
            "top_instruments": top[:3],
            "anti_top_instruments": list(reversed(top[-3:])),
        }
        return {"trades": trades, "summary": summary}

    def _simulate_trade(
        self,
        event: Event,
        entry: OrderBookSnapshot,
        exit_snapshot: OrderBookSnapshot,
        *,
        mode: Literal["touch", "buffer", "volume_cap"],
        buffer_bps: float,
        volume_cap: float,
        exit_on: Literal["mid", "bid"],
    ) -> dict | None:
        if not entry.asks:
            return None

        best_ask = entry.asks[0]
        nominal = entry.nominal or exit_snapshot.nominal
        if nominal is None:
            payload = event.payload or {}
            nominal = float(payload.get("nominal", 1000.0))
        per_lot_notional = best_ask.price * nominal / 100

        price = best_ask.price
        if mode == "buffer":
            price = best_ask.price * (1 + buffer_bps / 10000)

        cap_notional = event.ask_notional_window * volume_cap if mode == "volume_cap" else None
        available_notional = best_ask.lots * per_lot_notional
        target_notional = min(cap_notional, available_notional) if cap_notional else available_notional
        max_lots = int(target_notional // per_lot_notional) if per_lot_notional else 0
        lots = min(best_ask.lots, max_lots)
        if lots <= 0:
            return None

        exit_price = exit_snapshot.mid_price if exit_on == "mid" else exit_snapshot.best_bid
        if exit_price is None:
            return None

        pnl = (exit_price - price) * nominal / 100 * lots
        return {
            "isin": event.isin,
            "entry_ts": entry.ts,
            "exit_ts": exit_snapshot.ts,
            "entry_price": price,
            "exit_price": exit_price,
            "lots": lots,
            "pnl": pnl,
        }

    def _group_snapshots(self, snapshots: list[OrderBookSnapshot]) -> dict[str, list[OrderBookSnapshot]]:
        grouped: dict[str, list[OrderBookSnapshot]] = defaultdict(list)
        for snapshot in snapshots:
            grouped[snapshot.isin].append(snapshot)
        for items in grouped.values():
            items.sort(key=lambda s: s.ts)
        return grouped

    def _find_entry(self, snapshots: list[OrderBookSnapshot], ts) -> OrderBookSnapshot | None:
        for snapshot in snapshots:
            if snapshot.ts >= ts:
                return snapshot
        return None

    def _is_eligible(self, event: Event) -> bool:
        payload = event.payload or {}
        if isinstance(payload, dict) and "eligible" in payload:
            return bool(payload.get("eligible"))
        return True
