from __future__ import annotations

import json
from pathlib import Path
from typing import List
from ..domain.models import Instrument
from ..settings import Settings
from ..adapters.tinvest.client import TInvestClient


class UniverseService:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.client = TInvestClient(settings.tinvest_token, settings.tinvest_account_id, depth=settings.orderbook_depth)
        self.instruments: list[Instrument] = []

    async def load(self) -> List[Instrument]:
        if self.settings.app_env == "mock":
            path = Path("fixtures/instruments.json")
            self.instruments = [Instrument(**item) for item in json.loads(path.read_text())]
            return self.instruments
        instruments = await self.client.list_bonds()
        self.instruments = instruments
        return instruments

    async def shortlist(self) -> List[Instrument]:
        if not self.instruments:
            await self.load()
        return self.instruments[: self.settings.shortlist_max]
