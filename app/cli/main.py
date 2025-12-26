from __future__ import annotations

import asyncio
from datetime import datetime
import importlib
import json
from pathlib import Path
from typing import Callable, TypeVar

import typer

app = typer.Typer(help="Bond forced seller screener")
tinvest_app = typer.Typer(help="T-Invest tools")
telegram_app = typer.Typer(help="Telegram tools")
app.add_typer(tinvest_app, name="tinvest")
app.add_typer(telegram_app, name="telegram")

UniverseService = None
get_settings = None

T = TypeVar("T")


def _load_symbol(name: str, module_path: str, attr: str):
    override = globals().get(name)
    if override is not None:
        return override
    module = importlib.import_module(module_path)
    return getattr(module, attr)


def _load_dependencies(command_name: str, loader: Callable[[], T]) -> T:
    try:
        return loader()
    except Exception as exc:
        typer.secho(
            f"Failed to import dependencies for '{command_name}'. Original error: {exc}",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=1) from exc


@app.command()
def run():
    """Запустить web + worker"""
    uvicorn = _load_dependencies(
        "run",
        lambda: importlib.import_module("uvicorn"),
    )
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=False)


async def _shortlist_rebuild():
    def load_shortlist_deps():
        return (
            _load_symbol("UniverseService", "app.services.universe", "UniverseService"),
            _load_symbol("get_settings", "app.settings", "get_settings"),
        )

    UniverseServiceLocal, get_settings_local = _load_dependencies(
        "shortlist:rebuild",
        load_shortlist_deps,
    )
    settings = get_settings_local()
    universe = UniverseServiceLocal(settings)
    summary = await universe.rebuild_shortlist()
    typer.echo(
        f"Universe: {summary.universe_size}, eligible: {summary.eligible_size}, shortlisted: {summary.shortlisted_size}"
    )
    if getattr(summary, "price_enriched", 0):
        typer.echo(f"Prices enriched: {summary.price_enriched}")
    typer.echo(f"Bond events failures: {getattr(summary, 'failed_bond_events', 0)}")
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
    def load_backtest_deps():
        return (
            _load_symbol("get_settings", "app.settings", "get_settings"),
            _load_symbol("EventRepository", "app.storage.repo", "EventRepository"),
            _load_symbol("SnapshotRepository", "app.storage.repo", "SnapshotRepository"),
            _load_symbol("async_session_factory", "app.storage.db", "async_session_factory"),
            _load_symbol("ReplayService", "app.services.replay", "ReplayService"),
        )

    (
        get_settings_local,
        EventRepositoryLocal,
        SnapshotRepositoryLocal,
        async_session_factory_local,
        ReplayServiceLocal,
    ) = _load_dependencies("backtest:replay", load_backtest_deps)
    settings = get_settings_local()
    session = async_session_factory_local()()
    event_repo = EventRepositoryLocal(settings)
    snapshot_repo = SnapshotRepositoryLocal(session)
    replay = ReplayServiceLocal(snapshot_repo, event_repo)
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
    def load_detector_deps():
        return (
            _load_symbol("get_settings", "app.settings", "get_settings"),
            _load_symbol("UniverseService", "app.services.universe", "UniverseService"),
            _load_symbol("EventService", "app.services.events", "EventService"),
            _load_symbol("TelegramBot", "app.adapters.telegram.bot", "TelegramBot"),
            _load_symbol("get_metrics", "app.services.metrics", "get_metrics"),
            _load_symbol("History", "app.domain.detector", "History"),
            _load_symbol("detect_event", "app.domain.detector", "detect_event"),
            _load_symbol("map_orderbook_payload", "app.adapters.tinvest.mapping", "map_orderbook_payload"),
        )

    (
        get_settings_local,
        UniverseServiceLocal,
        EventServiceLocal,
        TelegramBotLocal,
        get_metrics_local,
        HistoryLocal,
        detect_event_local,
        map_orderbook_payload_local,
    ) = _load_dependencies("detector:replay", load_detector_deps)
    settings = get_settings_local()
    universe = UniverseServiceLocal(settings)
    events = EventServiceLocal(settings)
    telegram = TelegramBotLocal(settings) if send_telegram else None
    metrics = get_metrics_local()
    history = HistoryLocal(
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
        snapshot = map_orderbook_payload_local(data)
        if not snapshot or (not snapshot.bids and not snapshot.asks):
            continue

        instrument = instrument_map.get(snapshot.isin)
        if not instrument:
            continue

        event = detect_event_local(
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
    def load_tinvest_deps():
        return (
            _load_symbol("get_settings", "app.settings", "get_settings"),
            _load_symbol("build_grpc_credentials", "app.adapters.tinvest.grpc_stream", "build_grpc_credentials"),
            _load_symbol("grpc_channel_ready", "app.adapters.tinvest.grpc_stream", "grpc_channel_ready"),
            _load_symbol("select_grpc_target", "app.adapters.tinvest.grpc_stream", "select_grpc_target"),
        )

    (
        get_settings_local,
        build_grpc_credentials,
        grpc_channel_ready,
        select_grpc_target,
    ) = _load_dependencies("tinvest grpc-check", load_tinvest_deps)
    settings = get_settings_local()
    target = select_grpc_target(settings)
    credentials, ssl_mode = build_grpc_credentials(settings)
    token_set = bool(settings.tinvest_token)
    typer.echo(f"target={target} ssl_mode={ssl_mode} token_set={token_set}")
    ok = asyncio.run(grpc_channel_ready(target, credentials))
    raise typer.Exit(code=0 if ok else 1)


async def _telegram_test_alert(settings):
    TelegramBotLocal, EventLocal, InstrumentLocal = _load_dependencies(
        "telegram test-alert",
        lambda: (
            _load_symbol("TelegramBot", "app.adapters.telegram.bot", "TelegramBot"),
            _load_symbol("Event", "app.domain.models", "Event"),
            _load_symbol("Instrument", "app.domain.models", "Instrument"),
        ),
    )
    telegram = TelegramBotLocal(settings)
    now = asyncio.get_event_loop().time()
    event = EventLocal(
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
    instrument = InstrumentLocal(
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
    get_settings_local = _load_dependencies(
        "telegram test-alert",
        lambda: _load_symbol("get_settings", "app.settings", "get_settings"),
    )
    settings = get_settings_local()
    if not settings.telegram_bot_token or not settings.telegram_chat_id:
        typer.echo("Telegram is not configured. Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID.")
        raise typer.Exit(code=1)
    asyncio.run(_telegram_test_alert(settings))


async def _diagnose():
    def load_diagnose_deps():
        return (
            _load_symbol("get_settings", "app.settings", "get_settings"),
            _load_symbol("async_session_factory", "app.storage.db", "async_session_factory"),
            _load_symbol("InstrumentORM", "app.storage.schema", "InstrumentORM"),
            _load_symbol("get_metrics", "app.services.metrics", "get_metrics"),
            _load_symbol("select", "sqlalchemy", "select"),
            _load_symbol("func", "sqlalchemy", "func"),
        )

    (
        get_settings_local,
        async_session_factory_local,
        InstrumentORMLocal,
        get_metrics_local,
        select,
        func,
    ) = _load_dependencies("diagnose", load_diagnose_deps)
    settings = get_settings_local()
    session_factory = async_session_factory_local()
    async with session_factory() as session:
        instruments_count = (
            await session.execute(select(func.count(InstrumentORMLocal.isin)))
        ).scalar_one()
        shortlisted_count = (
            await session.execute(
                select(func.count(InstrumentORMLocal.isin)).where(InstrumentORMLocal.is_shortlisted.is_(True))
            )
        ).scalar_one()

    metrics = get_metrics_local()
    last_update_ts = metrics.last_update_ts.isoformat() if metrics.last_update_ts else None
    last_stream_message_ts = (
        metrics.last_stream_message_ts.isoformat() if metrics.last_stream_message_ts else None
    )
    last_worker_heartbeat_ts = (
        metrics.last_worker_heartbeat_ts.isoformat() if metrics.last_worker_heartbeat_ts else None
    )

    typer.echo(f"database_url={settings.database_url}")
    typer.echo(f"instruments_count={instruments_count}")
    typer.echo(f"shortlisted_count={shortlisted_count}")
    typer.echo(f"last_update_ts={last_update_ts}")
    typer.echo(f"last_stream_message_ts={last_stream_message_ts}")
    typer.echo(f"last_worker_heartbeat_ts={last_worker_heartbeat_ts}")
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
