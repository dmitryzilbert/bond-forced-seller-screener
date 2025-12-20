from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from ..services.events import EventService
from ..settings import get_settings

templates = Jinja2Templates(directory="app/web/templates")
view_router = APIRouter()
settings = get_settings()
event_service = EventService(settings)


@view_router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    events = await event_service.recent_events()
    return templates.TemplateResponse("index.html", {"request": request, "events": events})


@view_router.get("/instrument/{isin}", response_class=HTMLResponse)
async def instrument_page(request: Request, isin: str):
    events = await event_service.events_by_isin(isin)
    return templates.TemplateResponse("instrument.html", {"request": request, "events": events, "isin": isin})
