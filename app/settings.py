from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    app_env: str = Field("mock", alias="APP_ENV")
    log_level: str = Field("INFO", alias="LOG_LEVEL")
    database_url: str = Field("sqlite+aiosqlite:///./mock.db", alias="DATABASE_URL")

    tinvest_token: str | None = Field(None, alias="TINVEST_TOKEN")
    tinvest_account_id: str | None = Field(None, alias="TINVEST_ACCOUNT_ID")
    tinvest_ssl_ca_bundle: str | None = Field(None, alias="TINVEST_SSL_CA_BUNDLE")
    tinvest_grpc_target_prod: str = Field(
        "invest-public-api.tbank.ru:443",
        alias="TINVEST_GRPC_TARGET_PROD",
    )
    tinvest_grpc_target_sandbox: str = Field(
        "sandbox-invest-public-api.tbank.ru:443",
        alias="TINVEST_GRPC_TARGET_SANDBOX",
    )

    orderbook_depth: int = Field(10, alias="ORDERBOOK_DEPTH")
    orderbook_max_subscriptions_per_stream: int = Field(
        300,
        alias="ORDERBOOK_MAX_SUBSCRIPTIONS_PER_STREAM",
    )
    stream_heartbeat_interval_s: int = Field(20, alias="STREAM_HEARTBEAT_INTERVAL_S")
    orderbook_bootstrap_timeout_s: float = Field(8.0, alias="ORDERBOOK_BOOTSTRAP_TIMEOUT_S")
    orderbook_bootstrap_enabled: bool = Field(True, alias="ORDERBOOK_BOOTSTRAP_ENABLED")
    orderbook_bootstrap_concurrency: int = Field(
        8,
        alias="ORDERBOOK_BOOTSTRAP_CONCURRENCY",
    )
    orderbook_bootstrap_rps: float = Field(10.0, alias="ORDERBOOK_BOOTSTRAP_RPS")
    orderbook_poll_enabled: bool = Field(False, alias="ORDERBOOK_POLL_ENABLED")
    orderbook_poll_interval_s: int = Field(60, alias="ORDERBOOK_POLL_INTERVAL_S")
    shortlist_max: int = Field(500, alias="SHORTLIST_MAX")
    shortlist_min_notional: int = Field(200_000, alias="SHORTLIST_MIN_NOTIONAL")
    shortlist_min_updates_per_hour: int = Field(2, alias="SHORTLIST_MIN_UPDATES_PER_HOUR")
    allow_missing_data_to_shortlist: bool = Field(False, alias="ALLOW_MISSING_DATA_TO_SHORTLIST")
    suppress_alerts_when_missing_data: bool = Field(True, alias="SUPPRESS_ALERTS_WHEN_MISSING_DATA")
    exclude_call_offer_unknown: bool = Field(True, alias="EXCLUDE_CALL_OFFER_UNKNOWN")
    suppress_alerts_when_offer_unknown: bool = Field(True, alias="SUPPRESS_ALERTS_WHEN_OFFER_UNKNOWN")

    delta_ytm_max_bps: int = Field(120, alias="DELTA_YTM_MAX_BPS")
    ask_window_min_lots: int = Field(20, alias="ASK_WINDOW_MIN_LOTS")
    ask_window_min_notional: int = Field(300_000, alias="ASK_WINDOW_MIN_NOTIONAL")
    ask_window_kvol: int = Field(6, alias="ASK_WINDOW_KVOL")
    ask_window_history_size: int = Field(200, alias="ASK_WINDOW_HISTORY_SIZE")
    ask_window_flush_seconds: int = Field(600, alias="ASK_WINDOW_FLUSH_SECONDS")
    novelty_window_updates: int = Field(3, alias="NOVELTY_WINDOW_UPDATES")
    novelty_window_seconds: int = Field(60, alias="NOVELTY_WINDOW_SECONDS")
    alert_hold_updates: int = Field(2, alias="ALERT_HOLD_UPDATES")

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

    liveness_max_stale_seconds: int = Field(180, alias="LIVENESS_MAX_STALE_SECONDS")
    liveness_alert_minutes: int = Field(5, alias="LIVENESS_ALERT_MINUTES")
    liveness_alert_cooldown_minutes: int = Field(30, alias="LIVENESS_ALERT_COOLDOWN_MINUTES")
    tinvest_ping_delay_ms: int = Field(30000, alias="TINVEST_PING_DELAY_MS")
    price_max_age_s: int = Field(24 * 60 * 60, alias="PRICE_MAX_AGE_S")


def get_settings() -> Settings:
    return Settings()
