from __future__ import annotations

import asyncio
from datetime import date, datetime
from unittest.mock import AsyncMock

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
    monkeypatch.setenv("TINVEST_STREAM_TRANSPORT", "ws")
    db_module._engine = None
    db_module._session_factory = None
    asyncio.run(db_module.init_db())
    settings = get_settings()
    service = UniverseService(settings)
    yield service
    db_module._engine = None
    db_module._session_factory = None


@pytest.fixture
def universe_factory(monkeypatch, tmp_path):
    def builder(**env):
        db_path = tmp_path / f"test_{len(list(tmp_path.iterdir()))}.db"
        monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
        monkeypatch.setenv("APP_ENV", "mock")
        monkeypatch.setenv("TINVEST_STREAM_TRANSPORT", "ws")
        for key, value in env.items():
            monkeypatch.setenv(key, str(value))
        db_module._engine = None
        db_module._session_factory = None
        asyncio.run(db_module.init_db())
        settings = get_settings()
        return UniverseService(settings)

    yield builder
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
        figi="FIGI1",
        maturity_date=date(2030, 1, 1),
        amortization_flag=False,
        has_call_offer=False,
    )
    slow_instrument = Instrument(
        isin="SLOW1",
        name="Slow",
        figi="FIGI2",
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


def test_shortlist_missing_modes(universe_factory, monkeypatch):
    instrument_a = Instrument(
        isin="MISS1",
        name="Missing",
        maturity_date=date(2030, 1, 1),
        amortization_flag=False,
        has_call_offer=False,
        figi=None,
    )
    instrument_b = Instrument(
        isin="AMORT2",
        name="Amort", 
        maturity_date=date(2030, 1, 1),
        amortization_flag=True,
        has_call_offer=False,
    )
    instrument_c = Instrument(
        isin="CALLUNK",
        name="Call Unknown",
        maturity_date=date(2030, 1, 1),
        amortization_flag=False,
        has_call_offer=None,
    )

    universe_default = universe_factory()
    monkeypatch.setattr(
        universe_default,
        "load_source_instruments",
        AsyncMock(return_value=[instrument_a, instrument_b, instrument_c]),
    )
    monkeypatch.setattr(universe_default, "_load_orderbook_snapshots", AsyncMock(return_value=[]))

    summary_default = asyncio.run(universe_default.rebuild_shortlist())
    assert summary_default.shortlisted_size == 0
    assert summary_default.exclusion_reasons.get("missing_data") == 1
    assert summary_default.missing_reasons.get("missing_price") == 1

    universe_allow_missing = universe_factory(ALLOW_MISSING_DATA_TO_SHORTLIST="true")
    monkeypatch.setattr(
        universe_allow_missing,
        "load_source_instruments",
        AsyncMock(return_value=[instrument_a, instrument_b]),
    )
    monkeypatch.setattr(universe_allow_missing, "_load_orderbook_snapshots", AsyncMock(return_value=[]))

    summary_allow = asyncio.run(universe_allow_missing.rebuild_shortlist())
    shortlisted_allow = asyncio.run(universe_allow_missing.shortlist())

    assert summary_allow.shortlisted_size == 1
    assert shortlisted_allow[0].needs_enrichment is True
    assert "missing_price" in shortlisted_allow[0].missing_reasons

    instrument_call_unknown = Instrument(
        isin="CALL2",
        name="Callable Unknown",
        maturity_date=date(2030, 1, 1),
        amortization_flag=False,
        has_call_offer=None,
    )
    snapshots = [
        OrderBookSnapshot(
            isin="CALL2",
            ts=datetime(2024, 1, 1, 10, 0, 0),
            bids=[OrderBookLevel(price=101.0, lots=100)],
            asks=[OrderBookLevel(price=102.0, lots=100)],
            nominal=1000,
        ),
        OrderBookSnapshot(
            isin="CALL2",
            ts=datetime(2024, 1, 1, 11, 0, 0),
            bids=[OrderBookLevel(price=101.0, lots=100)],
            asks=[OrderBookLevel(price=102.0, lots=100)],
            nominal=1000,
        ),
    ]

    universe_offer = universe_factory(EXCLUDE_CALL_OFFER_UNKNOWN="false")
    monkeypatch.setattr(universe_offer, "load_source_instruments", AsyncMock(return_value=[instrument_call_unknown]))
    monkeypatch.setattr(universe_offer, "_load_orderbook_snapshots", AsyncMock(return_value=snapshots))

    summary_offer = asyncio.run(universe_offer.rebuild_shortlist())
    shortlisted_offer = asyncio.run(universe_offer.shortlist())

    assert summary_offer.shortlisted_size == 1
    assert shortlisted_offer[0].offer_unknown is True
