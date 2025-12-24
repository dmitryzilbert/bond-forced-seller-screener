from __future__ import annotations

import asyncio
from datetime import datetime
import typer
from sqlalchemy import select, func

from ..domain.models import Event, Instrument
from ..services.universe import UniverseService
from ..services.replay import ReplayService
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
