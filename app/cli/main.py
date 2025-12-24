from __future__ import annotations

import asyncio
from datetime import datetime
import json
from pathlib import Path
import typer
from sqlalchemy import select, func

from ..domain.models import Event, Instrument
from ..domain.detector import History, detect_event
from ..adapters.tinvest.mapping import map_orderbook_payload
from ..services.universe import UniverseService
from ..services.replay import ReplayService
from ..services.events import EventService
from ..adapters.telegram.bot import TelegramBot
from ..settings import get_settings
from ..storage.repo import SnapshotRepository, EventRepository
from ..storage.db import async_session_factory
from ..storage.schema import InstrumentORM
from ..services.metrics import get_metrics

app = typer.Typer(help="Bond forced seller screener")
tinvest_app = typer.Typer(help="T-Invest tools")
telegram_app = typer.Typer(help="Telegram tools")
app.add_typer(tinvest_app, name="tinvest")
app.add_typer(telegram_app, name="telegram")


@app.command()
def run():
    """Запустить web + worker"""
    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=False)


async def _shortlist_rebuild():
    settings = get_settings()
    universe = UniverseService(settings)
    summary = await universe.rebuild_shortlist()
    typer.echo(
        f"Universe: {summary.universe_size}, eligible: {summary.eligible_size}, shortlisted: {summary.shortlisted_size}"
    )
    top_reasons = sorted(summary.exclusion_reasons.items(), key=lambda item: item[1], reverse=True)[:3]
    if top_reasons:
        typer.echo("Top exclusion reasons:")
        for reason, count in top_reasons:
            typer.echo(f"- {reason}: {count}")
    missing_reasons = getattr(summary, "missing_reasons", {}) or {}
    missing_examples = getattr(summary, "missing_examples", []) or []
    if missing_reasons:
        top_missing = sorted(missing_reasons.items(), key=lambda item: item[1], reverse=True)[:10]
        typer.echo("Top missing_* reasons:")
        for reason, count in top_missing:
            typer.echo(f"- {reason}: {count}")
        if missing_examples:
            typer.echo("Examples with missing data:")
            for example in missing_examples:
                missing = ", ".join(example.get("missing", []))
                typer.echo(f"- {example.get('isin')} / {example.get('figi')}: {missing}")
    if summary.shortlisted_size == 0 and missing_reasons:
        typer.echo("shortlisted=0 because missing_data; consider allow_missing_data_to_shortlist=true")


@app.command("shortlist:rebuild")
def shortlist_rebuild():
    asyncio.run(_shortlist_rebuild())


async def _backtest_replay(
    minutes: int = 5,
    mode: str = "touch",
    buffer_bps: float = 5.0,
    volume_cap: float = 1.0,
    exit_on: str = "mid",
):
    settings = get_settings()
    session = async_session_factory()()
    event_repo = EventRepository(settings)
    snapshot_repo = SnapshotRepository(session)
    replay = ReplayService(snapshot_repo, event_repo)
    result = await replay.run(
        minutes=minutes,
        mode=mode,  # type: ignore[arg-type]
        buffer_bps=buffer_bps,
        volume_cap=volume_cap,
        exit_on=exit_on,  # type: ignore[arg-type]
    )
    typer.echo(result)


@app.command("backtest:replay")
def backtest_replay(
    minutes: int = 5,
    mode: str = "touch",
    buffer_bps: float = 5.0,
    volume_cap: float = 1.0,
    exit_on: str = "mid",
):
    asyncio.run(
        _backtest_replay(
            minutes=minutes,
            mode=mode,
            buffer_bps=buffer_bps,
            volume_cap=volume_cap,
            exit_on=exit_on,
        )
    )


async def _detector_replay(input_path: Path, *, send_telegram: bool = False):
    settings = get_settings()
    universe = UniverseService(settings)
    events = EventService(settings)
    telegram = TelegramBot(settings) if send_telegram else None
    metrics = get_metrics()
    history = History(
        max_points=settings.ask_window_history_size,
        flush_interval_seconds=settings.ask_window_flush_seconds,
    )

    instruments = await universe.shortlist()
    instrument_map = {
        instrument.isin: instrument
        for instrument in instruments
        if getattr(instrument, "is_shortlisted", False) and getattr(instrument, "eligible", False)
    }
    snapshots_processed = 0
    events_created = 0
    tg_sent_before = metrics.tg_sent_total

    def alert_suppression_reason(instrument) -> str | None:
        if getattr(instrument, "needs_enrichment", False) and settings.suppress_alerts_when_missing_data:
            return "missing_data"
        if getattr(instrument, "offer_unknown", False) and settings.suppress_alerts_when_offer_unknown:
            return "offer_unknown"
        return None

    for line in input_path.read_text().splitlines():
        if not line.strip():
            continue
        data = json.loads(line)
        snapshot = map_orderbook_payload(data)
        if not snapshot or (not snapshot.bids and not snapshot.asks):
            continue

        instrument = instrument_map.get(snapshot.isin)
        if not instrument:
            continue

        event = detect_event(
            snapshot,
            instrument,
            history,
            delta_ytm_max_bps=settings.delta_ytm_max_bps,
            ask_window_min_lots=settings.ask_window_min_lots,
            ask_window_min_notional=settings.ask_window_min_notional,
            ask_window_kvol=settings.ask_window_kvol,
            novelty_window_updates=settings.novelty_window_updates,
            novelty_window_seconds=settings.novelty_window_seconds,
            alert_hold_updates=settings.alert_hold_updates,
            spread_ytm_max_bps=settings.spread_ytm_max_bps,
            near_maturity_days=settings.near_maturity_days,
            stress_params={
                "stress_ytm_high_pct": settings.stress_ytm_high_pct,
                "stress_price_low_pct": settings.stress_price_low_pct,
                "stress_spread_ytm_bps": settings.stress_spread_ytm_bps,
                "stress_dev_peer_bps": settings.stress_dev_peer_bps,
            },
        )

        if event:
            event.payload = {
                **(event.payload or {}),
                "needs_enrichment": getattr(instrument, "needs_enrichment", False),
                "missing_reasons": getattr(instrument, "missing_reasons", []),
                "offer_unknown": getattr(instrument, "offer_unknown", False),
            }
            metrics.record_candidate()
            if event.alert:
                suppression_reason = alert_suppression_reason(instrument)
                if suppression_reason:
                    event.payload = {
                        **(event.payload or {}),
                        "alert_suppressed_reason": suppression_reason,
                    }
                else:
                    if send_telegram and telegram:
                        if event.stress_flag:
                            pass
                        else:
                            await telegram.send_event(event, instrument)
                    metrics.record_alert()
            await events.save_event(event)
            events_created += 1

        snapshots_processed += 1
        metrics.record_snapshot(ts=datetime.utcnow())

    if telegram:
        await telegram.close()

    tg_sent_after = metrics.tg_sent_total
    typer.echo(
        "Summary: snapshots_processed="
        f"{snapshots_processed} events_created={events_created} tg_sent={tg_sent_after - tg_sent_before}"
    )


@app.command("detector:replay")
def detector_replay(
    input_path: Path = typer.Option(
        ...,
        "--input",
        exists=True,
        dir_okay=False,
        help="Path to NDJSON orderbook snapshots.",
    ),
    send_telegram: bool = typer.Option(
        False,
        "--send-telegram",
        help="Send Telegram alerts for detected events.",
    ),
):
    asyncio.run(_detector_replay(input_path, send_telegram=send_telegram))


@tinvest_app.command("grpc-check")
def tinvest_grpc_check():
    settings = get_settings()
    from ..adapters.tinvest.grpc_stream import build_grpc_credentials, grpc_channel_ready, select_grpc_target

    target = select_grpc_target(settings)
    credentials, ssl_mode = build_grpc_credentials(settings)
    token_set = bool(settings.tinvest_token)
    typer.echo(f"target={target} ssl_mode={ssl_mode} token_set={token_set}")
    ok = asyncio.run(grpc_channel_ready(target, credentials))
    raise typer.Exit(code=0 if ok else 1)


async def _telegram_test_alert(settings):
    telegram = TelegramBot(settings)
    now = asyncio.get_event_loop().time()
    event = Event(
        isin="TEST00000000",
        ts=datetime.utcnow(),
        ytm_mid=0.12,
        ytm_event=0.13,
        delta_ytm_bps=100,
        ask_lots_window=50,
        ask_notional_window=500_000,
        spread_ytm_bps=120.0,
        score=9.5,
        payload={"best_ask": 101.23, "eligible": True},
    )
    instrument = Instrument(
        isin="TEST00000000",
        figi="TESTFIGI",
        name="Test Bond",
        issuer="Test Issuer",
        nominal=1000.0,
        maturity_date=datetime.utcnow().date(),
        eligible=True,
        is_shortlisted=True,
        eligibility_checked_at=datetime.utcnow(),
    )
    await telegram.send_event(event, instrument)
    await telegram.close()
    typer.echo(f"Telegram test alert sent at {now:.3f}")


@telegram_app.command("test-alert")
def telegram_test_alert():
    """Отправить тестовый алерт в Telegram."""
    settings = get_settings()
    if not settings.telegram_bot_token or not settings.telegram_chat_id:
        typer.echo("Telegram is not configured. Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID.")
        raise typer.Exit(code=1)
    asyncio.run(_telegram_test_alert(settings))


async def _diagnose():
    settings = get_settings()
    session_factory = async_session_factory()
    async with session_factory() as session:
        instruments_count = (
            await session.execute(select(func.count(InstrumentORM.isin)))
        ).scalar_one()
        shortlisted_count = (
            await session.execute(
                select(func.count(InstrumentORM.isin)).where(InstrumentORM.is_shortlisted.is_(True))
            )
        ).scalar_one()

    metrics = get_metrics()
    last_update_ts = metrics.last_update_ts.isoformat() if metrics.last_update_ts else None
    last_heartbeat_ts = metrics.last_heartbeat_ts.isoformat() if metrics.last_heartbeat_ts else None

    typer.echo(f"database_url={settings.database_url}")
    typer.echo(f"instruments_count={instruments_count}")
    typer.echo(f"shortlisted_count={shortlisted_count}")
    typer.echo(f"last_update_ts={last_update_ts}")
    typer.echo(f"last_heartbeat_ts={last_heartbeat_ts}")
    typer.echo(
        "thresholds="
        + ", ".join(
            [
                f"delta_ytm_max_bps={settings.delta_ytm_max_bps}",
                f"ask_window_min_lots={settings.ask_window_min_lots}",
                f"ask_window_min_notional={settings.ask_window_min_notional}",
                f"spread_ytm_max_bps={settings.spread_ytm_max_bps}",
                f"alert_cooldown_min={settings.alert_cooldown_min}",
                f"alert_hold_updates={settings.alert_hold_updates}",
                f"shortlist_min_notional={settings.shortlist_min_notional}",
                f"shortlist_min_updates_per_hour={settings.shortlist_min_updates_per_hour}",
                f"allow_missing_data_to_shortlist={settings.allow_missing_data_to_shortlist}",
                f"suppress_alerts_when_missing_data={settings.suppress_alerts_when_missing_data}",
                f"suppress_alerts_when_offer_unknown={settings.suppress_alerts_when_offer_unknown}",
            ]
        )
    )


@app.command("diagnose")
def diagnose():
    """Печатает ключевые метрики и настройки для диагностики."""
    asyncio.run(_diagnose())


def main():
    app()


if __name__ == "__main__":
    main()
