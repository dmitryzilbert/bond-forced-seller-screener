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


@app.command("shortlist:rebuild")
async def shortlist_rebuild():
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


@app.command("backtest:replay")
async def backtest_replay(minutes: int = 5, mode: str = "touch"):
    settings = get_settings()
    session = async_session_factory()()
    event_repo = EventRepository(settings)
    snapshot_repo = SnapshotRepository(session)
    replay = ReplayService(snapshot_repo, event_repo)
    result = await replay.run(minutes=minutes, mode=mode)  # type: ignore[arg-type]
    typer.echo(result)


def main():
    app()


if __name__ == "__main__":
    main()
