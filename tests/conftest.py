import pytest


@pytest.fixture(autouse=True)
def _stable_env(monkeypatch):
    # строгий дефолт для shortlist
    monkeypatch.setenv("allow_missing_data_to_shortlist", "false")
    monkeypatch.setenv("ALLOW_MISSING_DATA_TO_SHORTLIST", "false")

    # подавление алертов при missing данных (по тестам ожидается true)
    monkeypatch.setenv("suppress_alerts_when_missing_data", "true")
    monkeypatch.setenv("SUPPRESS_ALERTS_WHEN_MISSING_DATA", "true")

    from app.settings import get_settings

    if hasattr(get_settings, "cache_clear"):
        get_settings.cache_clear()
