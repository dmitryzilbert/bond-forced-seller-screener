from __future__ import annotations

import logging
from fastapi import APIRouter, HTTPException
from fastapi.encoders import jsonable_encoder
from typing import Optional
from ..services.events import EventService, EventFilter
from ..services.instrument_summary import InstrumentSummaryService
from ..services.orderbooks import OrderbookService
from ..settings import get_settings

api_router = APIRouter()
settings = get_settings()
event_service = EventService(settings)
orderbook_service = OrderbookService(settings)
instrument_summary_service = InstrumentSummaryService(settings)
logger = logging.getLogger(__name__)


@api_router.get("/events")
async def events(
    hours: int = 24,
    limit: int = 30,
    sort_by: str = "score",
    order: str = "desc",
    stress_flag: Optional[bool] = None,
    near_maturity_flag: Optional[bool] = None,
    min_notional: Optional[float] = None,
    min_delta_bps: Optional[float] = None,
    candidate: Optional[bool] = None,
    alert: Optional[bool] = None,
    eligible: Optional[bool] = None,
    has_call_offer: Optional[bool] = None,
    amortization_flag: Optional[bool] = None,
    eligible_reason: Optional[str] = None,
    isin: Optional[str] = None,
):
    params = EventFilter(
        hours=hours,
        limit=limit,
        sort_by=sort_by if sort_by in ("score", "ytm_event", "ts") else "score",
        order=order if order in ("asc", "desc") else "desc",
        stress_flag=stress_flag,
        near_maturity_flag=near_maturity_flag,
        min_notional=min_notional,
        min_delta_bps=min_delta_bps,
        candidate=candidate,
        alert=alert,
        eligible=eligible,
        has_call_offer=has_call_offer,
        amortization_flag=amortization_flag,
        eligible_reason=eligible_reason,
        isin=isin,
    )
    events, pre_filter_count = await event_service.filtered_events_with_prefilter_count(params)
    logger.info(
        "events_pre_filter_count=%s db_url=%s",
        pre_filter_count,
        settings.database_url,
    )
    return [e.model_dump() for e in events]


@api_router.get("/instrument/{isin}")
async def instrument_summary(isin: str):
    summary = await instrument_summary_service.get_summary(isin)
    return jsonable_encoder(summary)


@api_router.post("/instrument/{isin}/refresh")
async def instrument_refresh(isin: str):
    try:
        summary = await instrument_summary_service.refresh_snapshot(isin, depth=10)
    except ValueError:
        raise HTTPException(status_code=404, detail="instrument_not_found")
    return jsonable_encoder(summary)


@api_router.get("/snapshots")
async def snapshots(limit: int = 50):
    limit = max(1, min(limit, 200))
    snapshots_data = await orderbook_service.latest_snapshots(limit)
    return [
        {
            "isin": snap["isin"],
            "ts": snap["ts"].isoformat(),
            "best_bid": snap["best_bid"],
            "best_ask": snap["best_ask"],
            "mid": snap["mid"],
            "bids": snap["bids"],
            "asks": snap["asks"],
        }
        for snap in snapshots_data
    ]
