from datetime import date, datetime, timezone
from types import SimpleNamespace

from app.domain.models import Instrument, OrderBookLevel, OrderBookSnapshot
from app.services.metrics import get_metrics
from app.services.orderbooks import OrderbookOrchestrator
from app.settings import Settings


def test_bootstrap_fetches_snapshots_and_updates_metrics():
    metrics = get_metrics()
    metrics.snapshots_ingested_total = 0

    settings = Settings()
    settings.orderbook_bootstrap_concurrency = 2
    settings.orderbook_bootstrap_rps = 1000.0
    settings.orderbook_bootstrap_timeout_s = 0.1
    orchestrator = OrderbookOrchestrator(
        settings,
        universe=SimpleNamespace(),
        events=SimpleNamespace(),
        telegram=SimpleNamespace(),
    )

    instruments = [
        Instrument(
            isin=f"RU000A{i:04d}",
            name="Bond",
            issuer="Issuer",
            nominal=1000.0,
            maturity_date=date(2030, 1, 1),
            eligible=True,
            is_shortlisted=True,
        )
        for i in range(3)
    ]
    instrument_map = {inst.isin: inst for inst in instruments}

    calls: list[str] = []

    async def fake_fetch_orderbook_snapshot(inst, *, depth, timeout):
        calls.append(inst.isin)
        return OrderBookSnapshot(
            isin=inst.isin,
            ts=datetime.now(timezone.utc),
            bids=[OrderBookLevel(price=100.0, lots=1)],
            asks=[],
            nominal=inst.nominal,
        )

    async def fake_handle_snapshot(snapshot, instrument_map, *, persist):
        metrics.record_snapshot(ts=datetime.now(timezone.utc))

    orchestrator.client = SimpleNamespace(fetch_orderbook_snapshot=fake_fetch_orderbook_snapshot)
    orchestrator._handle_snapshot = fake_handle_snapshot

    async def _run():
        await orchestrator._bootstrap_snapshots(instruments, instrument_map)

    import asyncio

    asyncio.run(_run())

    assert calls == [inst.isin for inst in instruments]
    assert metrics.snapshots_ingested_total == len(instruments)
