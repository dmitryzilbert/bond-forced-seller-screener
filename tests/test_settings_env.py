from pathlib import Path

from app.settings import Settings


def test_settings_reads_env_file(monkeypatch, tmp_path: Path):
    env_content = """
app_env=prod
tinvest_token=xxx
telegram_bot_token=yyy
telegram_chat_id=123
""".strip()

    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text(env_content)

    settings = Settings()

    assert settings.app_env == "prod"
    assert settings.tinvest_token == "xxx"
    assert settings.telegram_bot_token == "yyy"
    assert settings.telegram_chat_id == "123"
