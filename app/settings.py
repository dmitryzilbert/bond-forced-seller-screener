from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    app_env: str = Field("mock", alias="APP_ENV")
    log_level: str = Field("INFO", alias="LOG_LEVEL")
    database_url: str = Field("sqlite+aiosqlite:///./mock.db", alias="DATABASE_URL")

    tinvest_token: str | None = Field(None, alias="TINVEST_TOKEN")
    tinvest_account_id: str | None = Field(None, alias="TINVEST_ACCOUNT_ID")

    orderbook_depth: int = Field(10, alias="ORDERBOOK_DEPTH")
    shortlist_max: int = Field(500, alias="SHORTLIST_MAX")
    shortlist_min_notional: int = Field(200_000, alias="SHORTLIST_MIN_NOTIONAL")
    shortlist_min_updates_per_hour: int = Field(2, alias="SHORTLIST_MIN_UPDATES_PER_HOUR")

    delta_ytm_max_bps: int = Field(120, alias="DELTA_YTM_MAX_BPS")
    ask_window_min_lots: int = Field(20, alias="ASK_WINDOW_MIN_LOTS")
    ask_window_min_notional: int = Field(300_000, alias="ASK_WINDOW_MIN_NOTIONAL")
    ask_window_kvol: int = Field(6, alias="ASK_WINDOW_KVOL")

    spread_ytm_max_bps: int = Field(300, alias="SPREAD_YTM_MAX_BPS")
    alert_cooldown_min: int = Field(60, alias="ALERT_COOLDOWN_MIN")
    alert_night_silent_start: int = Field(23, alias="ALERT_NIGHT_SILENT_START")
    alert_night_silent_end: int = Field(7, alias="ALERT_NIGHT_SILENT_END")

    stress_ytm_high_pct: int = Field(40, alias="STRESS_YTM_HIGH_PCT")
    stress_price_low_pct: int = Field(90, alias="STRESS_PRICE_LOW_PCT")
    stress_spread_ytm_bps: int = Field(600, alias="STRESS_SPREAD_YTM_BPS")
    stress_dev_peer_bps: int = Field(1500, alias="STRESS_DEV_PEER_BPS")

    near_maturity_days: int = Field(14, alias="NEAR_MATURITY_DAYS")

    telegram_bot_token: str | None = Field(None, alias="TELEGRAM_BOT_TOKEN")
    telegram_chat_id: str | None = Field(None, alias="TELEGRAM_CHAT_ID")

    public_dashboard_url: str = Field("http://localhost:8000", alias="PUBLIC_DASHBOARD_URL")

    class Config:
        env_file = ".env"
        case_sensitive = False


def get_settings() -> Settings:
    return Settings()
