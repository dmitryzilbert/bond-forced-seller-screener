import asyncio
from datetime import date, datetime, timezone
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from app.domain.models import OrderBookLevel, OrderBookSnapshot
from app.main import app
from app.settings import get_settings
from app.storage import db as db_module
from app.storage.schema import InstrumentORM
from app.web import api as api_module


def test_refresh_endpoint_updates_snapshot(monkeypatch, tmp_path):
    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{tmp_path}/test.db")
    monkeypatch.setenv("APP_ENV", "mock")
    if hasattr(get_settings, "cache_clear"):
        get_settings.cache_clear()
    db_module._engine = None
    db_module._session_factory = None
    asyncio.run(db_module.init_db())

    async def _seed():
        session_factory = db_module.async_session_factory()
        async with session_factory() as session:
            session.add(
                InstrumentORM(
                    isin="RU000A0REF",
                    figi="FIGI2",
                    name="Bond",
                    issuer="Issuer",
                    nominal=1000.0,
                    maturity_date=date(2030, 1, 1),
                    segment="",
                    updated_at=None,
                    amortization_flag=False,
                    has_call_offer=False,
                    eligible=True,
                    eligible_reason="ok",
                    eligibility_checked_at=None,
                    is_shortlisted=True,
                )
            )
            await session.commit()

    asyncio.run(_seed())

    session_factory = db_module.async_session_factory()
    api_module.instrument_summary_service.session_factory = session_factory
    api_module.instrument_summary_service.orderbook_service.session_factory = session_factory

    snapshot = OrderBookSnapshot(
        isin="RU000A0REF",
        ts=datetime.now(timezone.utc),
        bids=[OrderBookLevel(price=99.0, lots=1)],
        asks=[OrderBookLevel(price=101.0, lots=1)],
        nominal=1000.0,
    )
    api_module.instrument_summary_service.client.fetch_orderbook_snapshot = AsyncMock(return_value=snapshot)

    with TestClient(app) as client:
        response = client.post("/api/instrument/RU000A0REF/refresh")

    assert response.status_code == 200
    payload = response.json()
    assert payload["latest_snapshot"]["mid"] == 100.0
