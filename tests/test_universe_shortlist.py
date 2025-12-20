from __future__ import annotations

import asyncio
from datetime import date, datetime
from unittest.mock import AsyncMock

import pytest

from app.domain.models import Instrument, OrderBookLevel, OrderBookSnapshot
from app.services.universe import UniverseService
from app.settings import get_settings
from app.storage import db as db_module


@pytest.fixture
def universe(monkeypatch, tmp_path):
    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{tmp_path}/test.db")
    monkeypatch.setenv("APP_ENV", "mock")
    db_module._engine = None
    db_module._session_factory = None
    asyncio.run(db_module.init_db())
    settings = get_settings()
    service = UniverseService(settings)
    yield service
    db_module._engine = None
    db_module._session_factory = None


def test_amortization_not_eligible(universe: UniverseService, monkeypatch):
    instrument = Instrument(
        isin="AMORT1",
        name="Amortizing",
        maturity_date=date(2030, 1, 1),
        amortization_flag=True,
        has_call_offer=False,
    )
    monkeypatch.setattr(universe, "load_source_instruments", AsyncMock(return_value=[instrument]))
    asyncio.run(universe.rebuild_shortlist())
    instruments = asyncio.run(universe.load())
    assert instruments[0].eligible is False
    assert instruments[0].eligible_reason == "amortization"


def test_call_offer_not_eligible(universe: UniverseService, monkeypatch):
    instrument = Instrument(
        isin="CALL1",
        name="Callable",
        maturity_date=date(2030, 1, 1),
        amortization_flag=False,
        has_call_offer=True,
    )
    monkeypatch.setattr(universe, "load_source_instruments", AsyncMock(return_value=[instrument]))
    asyncio.run(universe.rebuild_shortlist())
    instruments = asyncio.run(universe.load())
    assert instruments[0].eligible is False
    assert instruments[0].eligible_reason == "call_offer"


def test_plain_bullet_is_eligible(universe: UniverseService, monkeypatch):
    instrument = Instrument(
        isin="BULLET1",
        name="Plain",
        maturity_date=date(2030, 1, 1),
        amortization_flag=False,
        has_call_offer=False,
    )
    monkeypatch.setattr(universe, "load_source_instruments", AsyncMock(return_value=[instrument]))
    asyncio.run(universe.rebuild_shortlist())
    instruments = asyncio.run(universe.load())
    assert instruments[0].eligible is True
    assert instruments[0].eligible_reason == "ok"


def test_shortlist_filters_by_liveness(universe: UniverseService, monkeypatch):
    live_instrument = Instrument(
        isin="LIVE1",
        name="Live",
        maturity_date=date(2030, 1, 1),
        amortization_flag=False,
        has_call_offer=False,
    )
    slow_instrument = Instrument(
        isin="SLOW1",
        name="Slow",
        maturity_date=date(2030, 1, 1),
        amortization_flag=False,
        has_call_offer=False,
    )
    monkeypatch.setattr(
        universe,
        "load_source_instruments",
        AsyncMock(return_value=[live_instrument, slow_instrument]),
    )

    live_snapshots = [
        OrderBookSnapshot(
            isin="LIVE1",
            ts=datetime(2024, 1, 1, 10, 0, 0),
            bids=[OrderBookLevel(price=105.0, lots=3)],
            asks=[],
            nominal=1000,
        ),
        OrderBookSnapshot(
            isin="LIVE1",
            ts=datetime(2024, 1, 1, 10, 30, 0),
            bids=[OrderBookLevel(price=106.0, lots=3)],
            asks=[],
            nominal=1000,
        ),
    ]
    slow_snapshots = [
        OrderBookSnapshot(
            isin="SLOW1",
            ts=datetime(2024, 1, 1, 10, 0, 0),
            bids=[OrderBookLevel(price=90.0, lots=1)],
            asks=[],
            nominal=1000,
        )
    ]

    async def fake_snapshots():
        return live_snapshots + slow_snapshots

    monkeypatch.setattr(universe, "_load_orderbook_snapshots", fake_snapshots)
    asyncio.run(universe.rebuild_shortlist())
    shortlisted = asyncio.run(universe.shortlist())
    assert {i.isin for i in shortlisted} == {"LIVE1"}
