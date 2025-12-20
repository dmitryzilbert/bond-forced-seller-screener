from __future__ import annotations

from fastapi import APIRouter
from ..services.events import EventService
from ..settings import get_settings

api_router = APIRouter()
settings = get_settings()
event_service = EventService(settings)


@api_router.get("/events")
async def events():
    return [e.model_dump() for e in await event_service.recent_events()]
