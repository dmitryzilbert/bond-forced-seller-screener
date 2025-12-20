from __future__ import annotations

import logging
from datetime import datetime
from typing import Iterable

from .rest import TInvestRestClient
from .stream import TInvestStream
from .mapping import map_instrument_payload
from ...domain.models import Instrument, OrderBookSnapshot

logger = logging.getLogger(__name__)


class TInvestClient:
    def __init__(
        self,
        token: str | None,
        account_id: str | None,
        depth: int = 10,
        *,
        rest_transport=None,
        stream_connector=None,
        dry_run: bool = False,
    ):
        self.rest = TInvestRestClient(token, transport=rest_transport)
        self.stream = TInvestStream(token, depth=depth, connector=stream_connector, dry_run=dry_run)
        self.account_id = account_id

    async def list_bonds(self) -> list[Instrument]:
        if not self.rest.enabled:
            logger.info("TInvest REST disabled (no token)")
            return []
        payloads = await self.rest.list_bonds()
        return [map_instrument_payload(p) for p in payloads]

    async def has_future_call_offer(self, instrument: Instrument) -> bool | None:
        if not self.rest.enabled:
            return None
        instrument_id = instrument.figi or instrument.isin
        if not instrument_id:
            return None
        events = await self.rest.get_bond_events(instrument_id)
        now = datetime.utcnow().date()
        for event in events:
            event_type = event.get("eventType") or event.get("type") or event.get("event_type")
            if not event_type:
                continue
            if str(event_type).upper().endswith("CALL") or str(event_type).upper() == "CALL":
                date_str = (
                    event.get("eventDate")
                    or event.get("callDate")
                    or event.get("date")
                    or event.get("recordDate")
                )
                if date_str:
                    try:
                        event_date = datetime.fromisoformat(str(date_str).replace("Z", "+00:00")).date()
                        if event_date >= now:
                            return True
                    except ValueError:
                        continue
                else:
                    return True
        return False

    async def stream_orderbooks(self, instruments: Iterable[Instrument]):
        if not self.stream.enabled:
            logger.info("TInvest stream disabled (no token)")
            return
        async for snapshot in self.stream.subscribe(instruments):
            yield snapshot

