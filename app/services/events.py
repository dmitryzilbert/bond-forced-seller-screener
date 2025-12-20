from __future__ import annotations

import datetime
from dataclasses import dataclass
from typing import List, Literal, Optional

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

    async def filtered_events(self, params: "EventFilter") -> List[Event]:
        hours = params.hours or 24
        limit = params.limit or 30
        since = datetime.datetime.utcnow() - datetime.timedelta(hours=hours)
        events = await self.repo.list_filtered(
            since=since,
            limit=limit,
            sort_by=params.sort_by,
            order=params.order,
            stress_flag=params.stress_flag,
            near_maturity_flag=params.near_maturity_flag,
            min_notional=params.min_notional,
            min_delta_bps=params.min_delta_bps,
            isin=params.isin,
        )

        def matches_payload(event: Event) -> bool:
            payload = event.payload or {}

            if params.eligible is not None:
                if payload.get("eligible") is None:
                    return False
                if bool(payload.get("eligible")) is not params.eligible:
                    return False

            if params.has_call_offer is not None:
                if payload.get("has_call_offer") is None:
                    return False
                if bool(payload.get("has_call_offer")) is not params.has_call_offer:
                    return False

            if params.amortization_flag is not None:
                if payload.get("amortization_flag") is None:
                    return False
                if bool(payload.get("amortization_flag")) is not params.amortization_flag:
                    return False

            if params.eligible_reason:
                reason = payload.get("eligible_reason") or payload.get("eligibleReason") or payload.get("eligible_reason")
                if not reason or params.eligible_reason.lower() not in str(reason).lower():
                    return False

            return True

        filtered = [event for event in events if matches_payload(event)]
        return filtered


@dataclass
class EventFilter:
    hours: int = 24
    limit: int = 30
    sort_by: Literal["score", "ytm_event", "ts"] = "score"
    order: Literal["asc", "desc"] = "desc"
    stress_flag: Optional[bool] = None
    near_maturity_flag: Optional[bool] = None
    min_notional: Optional[float] = None
    min_delta_bps: Optional[float] = None
    eligible: Optional[bool] = None
    has_call_offer: Optional[bool] = None
    amortization_flag: Optional[bool] = None
    eligible_reason: Optional[str] = None
    isin: Optional[str] = None
