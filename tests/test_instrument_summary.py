import asyncio
from datetime import date, datetime, timezone

import pytest

from app.domain.ytm import ytm_from_price
from app.services.instrument_summary import InstrumentSummaryService
from app.settings import get_settings
from app.storage import db as db_module
from app.storage.schema import InstrumentORM, OrderbookSnapshotORM


@pytest.fixture
def summary_service(monkeypatch, tmp_path):
    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{tmp_path}/test.db")
    monkeypatch.setenv("APP_ENV", "mock")
    if hasattr(get_settings, "cache_clear"):
        get_settings.cache_clear()
    db_module._engine = None
    db_module._session_factory = None
    asyncio.run(db_module.init_db())
    settings = get_settings()
    yield InstrumentSummaryService(settings)
    db_module._engine = None
    db_module._session_factory = None


def test_instrument_summary_returns_ytm_mid(summary_service):
    async def _seed():
        session_factory = db_module.async_session_factory()
        async with session_factory() as session:
            instrument = InstrumentORM(
                isin="RU000A0TEST",
                figi="FIGI1",
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
            session.add(instrument)
            snapshot = OrderbookSnapshotORM(
                isin="RU000A0TEST",
                ts=datetime.now(timezone.utc),
                bids_json=[{"price": 100.0, "lots": 1}],
                asks_json=[{"price": 102.0, "lots": 1}],
                best_bid=100.0,
                best_ask=102.0,
                nominal=1000.0,
            )
            session.add(snapshot)
            await session.commit()

    async def _fetch():
        await _seed()
        summary = await summary_service.get_summary("RU000A0TEST")
        return summary

    summary = asyncio.run(_fetch())
    expected = ytm_from_price(101.0, 1000.0, date(2030, 1, 1), eligible=True)
    assert summary["ytm_mid"] == expected
