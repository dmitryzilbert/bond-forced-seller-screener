from __future__ import annotations

import asyncio
import json
from pathlib import Path
import logging
from datetime import datetime

from ..domain.models import OrderBookSnapshot
from ..domain.detector import History, detect_event
from ..adapters.tinvest.client import TInvestClient
from ..adapters.tinvest.mapping import map_orderbook_payload
from ..settings import Settings
from ..services.events import EventService
from ..services.universe import UniverseService
from ..adapters.telegram.bot import TelegramBot

logger = logging.getLogger(__name__)


class OrderbookOrchestrator:
    def __init__(self, settings: Settings, universe: UniverseService, events: EventService, telegram: TelegramBot):
        self.settings = settings
        self.universe = universe
        self.events = events
        self.telegram = telegram
        self.history = History()
        self.client = TInvestClient(settings.tinvest_token, settings.tinvest_account_id, depth=settings.orderbook_depth)

    async def run_mock_stream(self):
        instruments = await self.universe.shortlist()
        payloads = Path("fixtures/orderbooks.ndjson").read_text().splitlines()
        for line in payloads:
            data = json.loads(line)
            snapshot = map_orderbook_payload(data)
            instrument = next((i for i in instruments if i.isin == snapshot.isin), None)
            if not instrument:
                continue
            event = detect_event(
                snapshot,
                instrument,
                self.history,
                delta_ytm_max_bps=self.settings.delta_ytm_max_bps,
                ask_window_min_lots=self.settings.ask_window_min_lots,
                ask_window_min_notional=self.settings.ask_window_min_notional,
                ask_window_kvol=self.settings.ask_window_kvol,
                spread_ytm_max_bps=self.settings.spread_ytm_max_bps,
                near_maturity_days=self.settings.near_maturity_days,
                stress_params={
                    "stress_ytm_high_pct": self.settings.stress_ytm_high_pct,
                    "stress_price_low_pct": self.settings.stress_price_low_pct,
                    "stress_spread_ytm_bps": self.settings.stress_spread_ytm_bps,
                    "stress_dev_peer_bps": self.settings.stress_dev_peer_bps,
                },
            )
            if event:
                await self.events.save_event(event)
                await self.telegram.send_event(event, instrument)
            await asyncio.sleep(0.05)

    async def run_prod_stream(self):
        instruments = await self.universe.shortlist()
        async for snapshot in self.client.stream_orderbooks([i.isin for i in instruments]):
            instrument = next((i for i in instruments if i.isin == snapshot.isin), None)
            if not instrument:
                continue
            event = detect_event(
                snapshot,
                instrument,
                self.history,
                delta_ytm_max_bps=self.settings.delta_ytm_max_bps,
                ask_window_min_lots=self.settings.ask_window_min_lots,
                ask_window_min_notional=self.settings.ask_window_min_notional,
                ask_window_kvol=self.settings.ask_window_kvol,
                spread_ytm_max_bps=self.settings.spread_ytm_max_bps,
                near_maturity_days=self.settings.near_maturity_days,
                stress_params={
                    "stress_ytm_high_pct": self.settings.stress_ytm_high_pct,
                    "stress_price_low_pct": self.settings.stress_price_low_pct,
                    "stress_spread_ytm_bps": self.settings.stress_spread_ytm_bps,
                    "stress_dev_peer_bps": self.settings.stress_dev_peer_bps,
                },
            )
            if event:
                await self.events.save_event(event)
                await self.telegram.send_event(event, instrument)
