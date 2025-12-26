from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from ..services.events import EventService, EventFilter
from ..services.orderbooks import OrderbookService
from ..settings import get_settings

templates = Jinja2Templates(directory="app/web/templates")
view_router = APIRouter()
settings = get_settings()
event_service = EventService(settings)
orderbook_service = OrderbookService(settings)


@view_router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    events = await event_service.filtered_events(EventFilter(eligible=True))
    events_json = jsonable_encoder(events)
    return templates.TemplateResponse(
        "index.html", {"request": request, "events": events_json}
    )


@view_router.get("/instrument/{isin}", response_class=HTMLResponse)
async def instrument_page(request: Request, isin: str):
    events = await event_service.filtered_events(
        EventFilter(hours=24 * 7, limit=100, sort_by="ts", order="desc", isin=isin, eligible=None)
    )
    events_json = jsonable_encoder(events)
    snapshot = await orderbook_service.latest_snapshot(isin)
    best_bid = snapshot.best_bid if snapshot else None
    best_ask = snapshot.best_ask if snapshot else None
    return templates.TemplateResponse(
        "instrument.html",
        {
            "request": request,
            "events": events_json,
            "isin": isin,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "book_ts": snapshot.ts.isoformat() if snapshot else None,
        },
    )
