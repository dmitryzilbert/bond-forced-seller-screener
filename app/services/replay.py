from __future__ import annotations

import asyncio
from typing import Literal
from ..storage.repo import SnapshotRepository, EventRepository


class ReplayService:
    def __init__(self, snapshot_repo: SnapshotRepository, event_repo: EventRepository):
        self.snapshot_repo = snapshot_repo
        self.event_repo = event_repo

    async def run(self, minutes: int = 5, mode: Literal["touch", "buffer"] = "touch") -> dict:
        events = await self.event_repo.list_recent(limit=100)
        # simplified mock report
        return {
            "mode": mode,
            "events": len(events),
            "avg_pnl": 0.0,
        }
