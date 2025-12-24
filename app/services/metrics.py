from __future__ import annotations

from datetime import datetime, timezone


class Metrics:
    def __init__(self) -> None:
        now = datetime.now(timezone.utc)
        self.snapshots_ingested_total = 0
        self.events_candidate_total = 0
        self.events_alert_total = 0
        self.tg_sent_total = 0
        self.stream_messages_total = 0
        self.stream_pings_total = 0
        self.stream_reconnect_total = 0
        self.eligible_instruments_total = 0
        self.shortlisted_instruments_total = 0
        self.orderbook_subscriptions_ok_total = 0
        self.orderbook_subscriptions_limit_exceeded_total = 0
        self.orderbook_subscriptions_error_total: dict[str, int] = {}
        self.last_update_ts: datetime | None = now
        self.last_heartbeat_ts: datetime | None = now
        self._last_liveness_alert: datetime | None = None

    def record_snapshot(self, *, ts: datetime | None = None) -> None:
        self.snapshots_ingested_total += 1
        self.last_update_ts = ts or datetime.now(timezone.utc)

    def record_candidate(self) -> None:
        self.events_candidate_total += 1

    def record_alert(self) -> None:
        self.events_alert_total += 1

    def record_tg_sent(self) -> None:
        self.tg_sent_total += 1

    def record_stream_message(self) -> None:
        self.stream_messages_total += 1
        self.last_heartbeat_ts = datetime.now(timezone.utc)

    def record_stream_ping(self, *, ts: datetime | None = None) -> None:
        self.stream_pings_total += 1
        self.last_heartbeat_ts = ts or datetime.now(timezone.utc)

    def set_stream_reconnects(self, value: int) -> None:
        self.stream_reconnect_total = value

    def set_instrument_totals(self, *, eligible: int, shortlisted: int) -> None:
        self.eligible_instruments_total = eligible
        self.shortlisted_instruments_total = shortlisted

    def record_orderbook_subscriptions(
        self,
        *,
        ok_total: int,
        error_counts: dict[str, int],
        limit_exceeded_total: int,
    ) -> None:
        self.orderbook_subscriptions_ok_total += max(ok_total, 0)
        self.orderbook_subscriptions_limit_exceeded_total += max(limit_exceeded_total, 0)
        for status_name, count in error_counts.items():
            if count <= 0:
                continue
            self.orderbook_subscriptions_error_total[status_name] = (
                self.orderbook_subscriptions_error_total.get(status_name, 0) + count
            )

    def should_send_liveness_alert(self, *, now: datetime, cooldown_minutes: int, threshold_minutes: int) -> bool:
        if self.last_update_ts is None:
            return False
        stale_seconds = (now - self.last_update_ts).total_seconds()
        if stale_seconds < threshold_minutes * 60:
            return False
        if self._last_liveness_alert is None:
            return True
        return (now - self._last_liveness_alert).total_seconds() >= cooldown_minutes * 60

    def mark_liveness_alert_sent(self, *, now: datetime) -> None:
        self._last_liveness_alert = now

    def render(self) -> str:
        lines = [
            f"snapshots_ingested_total {self.snapshots_ingested_total}",
            f"events_candidate_total {self.events_candidate_total}",
            f"events_alert_total {self.events_alert_total}",
            f"tg_sent_total {self.tg_sent_total}",
            f"stream_messages_total {self.stream_messages_total}",
            f"stream_pings_total {self.stream_pings_total}",
            f"stream_reconnect_total {self.stream_reconnect_total}",
            f"eligible_instruments_total {self.eligible_instruments_total}",
            f"shortlisted_instruments_total {self.shortlisted_instruments_total}",
            f"orderbook_subscriptions_ok_total {self.orderbook_subscriptions_ok_total}",
            (
                "orderbook_subscriptions_limit_exceeded_total "
                f"{self.orderbook_subscriptions_limit_exceeded_total}"
            ),
        ]
        for status_name, count in sorted(self.orderbook_subscriptions_error_total.items()):
            lines.append(
                f'orderbook_subscriptions_error_total{{status_name="{status_name}"}} {count}'
            )
        return "\n".join(lines) + "\n"


_METRICS = Metrics()


def get_metrics() -> Metrics:
    return _METRICS
