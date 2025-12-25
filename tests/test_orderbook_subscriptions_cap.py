from types import SimpleNamespace
from datetime import date

from app.domain.models import Instrument
from app.services.orderbooks import OrderbookOrchestrator
from app.settings import Settings
from app.services.metrics import get_metrics


def test_orderbook_subscriptions_are_capped(caplog):
    metrics = get_metrics()
    metrics.orderbook_subscriptions_capped_total = 0
    metrics.orderbook_subscriptions_requested = 0

    settings = Settings()
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
        for i in range(500)
    ]

    with caplog.at_level("INFO"):
        capped = orchestrator._filter_shortlist(instruments)

    assert len(capped) == settings.orderbook_max_subscriptions_per_stream
    assert metrics.orderbook_subscriptions_requested == 500
    assert metrics.orderbook_subscriptions_capped_total == 1
    assert any("requested=500 capped_to=300 due_to_stream_limit" in rec.message for rec in caplog.records)
