from __future__ import annotations

import asyncio
import logging
from typing import Iterable

from ...domain.models import OrderBookLevel, OrderBookSnapshot

logger = logging.getLogger(__name__)


class TInvestStream:
    def __init__(self, token: str | None, depth: int = 10):
        self.token = token
        self.depth = depth
        self.enabled = bool(token)

    async def subscribe(self, isins: Iterable[str]):
        if not self.enabled:
            return
        logger.info("TInvest stream stub running for isins=%s", list(isins))
        while True:
            await asyncio.sleep(5)
            # Placeholder: real implementation would yield snapshots
            for isin in isins:
                yield OrderBookSnapshot(isin=isin, ts=None, bids=[], asks=[])
