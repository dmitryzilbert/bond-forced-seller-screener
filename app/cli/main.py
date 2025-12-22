from __future__ import annotations

import asyncio
import typer

from ..main import create_app
from ..services.universe import UniverseService
from ..services.orderbooks import OrderbookOrchestrator
from ..services.events import EventService
from ..services.replay import ReplayService
from ..adapters.telegram.bot import TelegramBot
from ..settings import get_settings
from ..storage.repo import SnapshotRepository, EventRepository
from ..storage.db import async_session_factory

app = typer.Typer(help="Bond forced seller screener")


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


def main():
    app()


if __name__ == "__main__":
    main()
