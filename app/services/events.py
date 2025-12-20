from __future__ import annotations

import datetime
from typing import List

from ..domain.models import Event
from ..storage.repo import EventRepository
from ..settings import Settings


class EventService:
    def __init__(self, settings: Settings):
        self.repo = EventRepository(settings)

    async def save_event(self, event: Event):
        await self.repo.add_event(event)

    async def recent_events(self, limit: int = 30) -> List[Event]:
        return await self.repo.list_recent(limit=limit)

    async def events_by_isin(self, isin: str, limit: int = 50) -> List[Event]:
        return await self.repo.list_by_isin(isin, limit=limit)
