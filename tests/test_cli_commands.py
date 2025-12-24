import subprocess
import sys
import warnings

from typer.testing import CliRunner

from app.cli import main as cli_main


def test_shortlist_rebuild_no_runtime_warning(monkeypatch):
    class DummySummary:
        universe_size = 3
        eligible_size = 2
        shortlisted_size = 1
        exclusion_reasons = {"ok": 1}

    class DummyUniverse:
        def __init__(self, settings):
            self.settings = settings

        async def rebuild_shortlist(self):
            return DummySummary()

    def fake_settings():
        return object()

    monkeypatch.setattr(cli_main, "UniverseService", DummyUniverse)
    monkeypatch.setattr(cli_main, "get_settings", fake_settings)

    with warnings.catch_warnings(record=True) as caught:
        result = cli_main.shortlist_rebuild()

    assert result is None
    assert not any(issubclass(w.category, RuntimeWarning) for w in caught)


def test_cli_help_registers_subcommands():
    runner = CliRunner()

    result = runner.invoke(cli_main.app, ["telegram", "--help"])
    assert result.exit_code == 0
    assert "test-alert" in result.output

    result = runner.invoke(cli_main.app, ["tinvest", "--help"])
    assert result.exit_code == 0
    assert "grpc-check" in result.output


def test_cli_help_includes_commands():
    result = subprocess.run(
        [sys.executable, "-m", "app.cli.main", "--help"],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    output = result.stdout
    assert "run" in output
    assert "shortlist:rebuild" in output
