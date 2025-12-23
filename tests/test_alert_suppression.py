import asyncio
from datetime import datetime
from unittest.mock import AsyncMock

import pytest

from app.domain.detector import DetectionResult
from app.domain.models import Instrument, OrderBookLevel, OrderBookSnapshot
from app.services.orderbooks import OrderbookOrchestrator
from app.settings import get_settings


def test_alert_suppressed_for_missing_data(monkeypatch):
    settings = get_settings()
    settings.app_env = "mock"
    events = AsyncMock()
    events.save_event = AsyncMock()
    telegram = AsyncMock()
    telegram.send_event = AsyncMock()
    orchestrator = OrderbookOrchestrator(
        settings,
        universe=AsyncMock(),
        events=events,
        telegram=telegram,
    )

    instrument = Instrument(
        isin="SUPPRESS1",
        name="Suppress", 
        maturity_date=datetime(2030, 1, 1).date(),
        amortization_flag=False,
        has_call_offer=False,
        eligible=True,
        eligible_reason="ok",
        is_shortlisted=True,
        needs_enrichment=True,
        missing_reasons=["missing_price"],
    )

    event = DetectionResult(
        isin=instrument.isin,
        ts=datetime.utcnow(),
        ytm_mid=10.0,
        ytm_event=11.0,
        delta_ytm_bps=100,
        ask_lots_window=10,
        ask_notional_window=1_000_000,
        spread_ytm_bps=50,
        score=1.0,
        candidate=True,
        alert=True,
    )

    monkeypatch.setattr("app.services.orderbooks.detect_event", lambda *args, **kwargs: event)

    snapshot = OrderBookSnapshot(
        isin=instrument.isin,
        ts=datetime.utcnow(),
        bids=[OrderBookLevel(price=100.0, lots=1)],
        asks=[OrderBookLevel(price=101.0, lots=1)],
        nominal=1000,
    )

    asyncio.run(orchestrator._handle_snapshot(snapshot, {instrument.isin: instrument}, persist=False))

    orchestrator.events.save_event.assert_awaited()
    orchestrator.telegram.send_event.assert_not_called()
