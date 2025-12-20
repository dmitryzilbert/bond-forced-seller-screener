from __future__ import annotations

import logging
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

    async def stream_orderbooks(self, instruments: Iterable[Instrument]):
        if not self.stream.enabled:
            logger.info("TInvest stream disabled (no token)")
            return
        async for snapshot in self.stream.subscribe(instruments):
            yield snapshot

