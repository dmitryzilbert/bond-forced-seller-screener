from __future__ import annotations

from datetime import datetime, timezone


class Metrics:
    def __init__(self) -> None:
        now = datetime.now(timezone.utc)
        self.snapshots_ingested_total = 0
        self.events_candidate_total = 0
        self.events_alert_total = 0
        self.events_saved_total = 0
        self.events_save_error_total: dict[str, int] = {}
        self.tg_sent_total = 0
        self.stream_messages_total = 0
        self.stream_pings_total = 0
        self.stream_payload_total: dict[str, int] = {}
        self.stream_reconnect_total = 0
        self.stream_unmapped_orderbooks_total = 0
        self.parse_success_total = 0
        self.parse_error_total: dict[str, int] = {}
        self.eligible_instruments_total = 0
        self.shortlisted_instruments_total = 0
        self.orderbook_subscriptions_ok_total = 0
        self.orderbook_subscriptions_limit_exceeded_total = 0
        self.orderbook_subscriptions_capped_total = 0
        self.orderbook_subscriptions_requested = 0
        self.orderbook_subscriptions_error_total: dict[str, int] = {}
        self.orderbook_bootstrap_attempt_total = 0
        self.orderbook_bootstrap_success_total = 0
        self.orderbook_bootstrap_fetch_ok_total = 0
        self.orderbook_bootstrap_persist_ok_total = 0
        self.orderbook_bootstrap_persist_error_total: dict[str, int] = {}
        self.orderbook_bootstrap_error_total: dict[str, int] = {}
        self.snapshot_dropped_total: dict[str, int] = {}
        self.universe_price_enrich_attempt_total = 0
        self.universe_price_enrich_success_total = 0
        self.universe_price_enrich_error_total: dict[str, int] = {}
        self.last_update_ts: datetime | None = now
        self.last_stream_message_ts: datetime | None = now
        self.last_worker_heartbeat_ts: datetime | None = now
        self._last_liveness_alert: datetime | None = None

    def record_snapshot(self, *, ts: datetime | None = None) -> None:
        self.snapshots_ingested_total += 1
        self.last_update_ts = ts or datetime.now(timezone.utc)

    def record_candidate(self) -> None:
        self.events_candidate_total += 1

    def record_alert(self) -> None:
        self.events_alert_total += 1

    def record_event_saved(self) -> None:
        self.events_saved_total += 1

    def record_event_save_error(self, exc_name: str) -> None:
        if not exc_name:
            exc_name = "unknown"
        self.events_save_error_total[exc_name] = self.events_save_error_total.get(exc_name, 0) + 1

    def record_tg_sent(self) -> None:
        self.tg_sent_total += 1

    def record_stream_message(self) -> None:
        self.stream_messages_total += 1
        self.last_stream_message_ts = datetime.now(timezone.utc)

    def record_stream_ping(self, *, ts: datetime | None = None) -> None:
        self.stream_pings_total += 1
        if ts is None:
            return
        self.last_stream_message_ts = ts

    def record_stream_payload(self, payload: str | None) -> None:
        name = payload or "unknown"
        self.stream_payload_total[name] = self.stream_payload_total.get(name, 0) + 1

    def record_stream_unmapped_orderbook(self) -> None:
        self.stream_unmapped_orderbooks_total += 1

    def record_parse_success(self) -> None:
        self.parse_success_total += 1

    def record_parse_error(self, exc_name: str) -> None:
        if not exc_name:
            exc_name = "unknown"
        self.parse_error_total[exc_name] = self.parse_error_total.get(exc_name, 0) + 1

    def record_worker_heartbeat(self, *, ts: datetime | None = None) -> None:
        self.last_worker_heartbeat_ts = ts or datetime.now(timezone.utc)

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

    def record_orderbook_subscriptions_capped(self) -> None:
        self.orderbook_subscriptions_capped_total += 1

    def set_orderbook_subscriptions_requested(self, value: int) -> None:
        self.orderbook_subscriptions_requested = max(value, 0)

    def record_orderbook_bootstrap_attempt(self, count: int = 1) -> None:
        self.orderbook_bootstrap_attempt_total += max(count, 0)

    def record_orderbook_bootstrap_success(self, count: int = 1) -> None:
        self.orderbook_bootstrap_success_total += max(count, 0)

    def record_orderbook_bootstrap_fetch_ok(self, count: int = 1) -> None:
        self.orderbook_bootstrap_fetch_ok_total += max(count, 0)

    def record_orderbook_bootstrap_persist_ok(self, count: int = 1) -> None:
        self.orderbook_bootstrap_persist_ok_total += max(count, 0)

    def record_orderbook_bootstrap_persist_error(self, exc_name: str, count: int = 1) -> None:
        if count <= 0:
            return
        if not exc_name:
            exc_name = "unknown"
        self.orderbook_bootstrap_persist_error_total[exc_name] = (
            self.orderbook_bootstrap_persist_error_total.get(exc_name, 0) + count
        )

    def record_orderbook_bootstrap_error(self, reason: str, count: int = 1) -> None:
        if count <= 0:
            return
        self.orderbook_bootstrap_error_total[reason] = (
            self.orderbook_bootstrap_error_total.get(reason, 0) + count
        )

    def record_snapshot_dropped(self, reason: str, count: int = 1) -> None:
        if count <= 0:
            return
        if not reason:
            reason = "unknown"
        self.snapshot_dropped_total[reason] = self.snapshot_dropped_total.get(reason, 0) + count

    def record_universe_price_enrich_attempt(self, count: int = 1) -> None:
        self.universe_price_enrich_attempt_total += max(count, 0)

    def record_universe_price_enrich_success(self, count: int = 1) -> None:
        self.universe_price_enrich_success_total += max(count, 0)

    def record_universe_price_enrich_error(self, reason: str, count: int = 1) -> None:
        if count <= 0:
            return
        if not reason:
            reason = "unknown"
        self.universe_price_enrich_error_total[reason] = (
            self.universe_price_enrich_error_total.get(reason, 0) + count
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
            f"events_saved_total {self.events_saved_total}",
            f"tg_sent_total {self.tg_sent_total}",
            f"stream_messages_total {self.stream_messages_total}",
            f"stream_pings_total {self.stream_pings_total}",
            f"stream_reconnect_total {self.stream_reconnect_total}",
            f"stream_unmapped_orderbooks_total {self.stream_unmapped_orderbooks_total}",
            f"parse_success_total {self.parse_success_total}",
            f"eligible_instruments_total {self.eligible_instruments_total}",
            f"shortlisted_instruments_total {self.shortlisted_instruments_total}",
            f"orderbook_subscriptions_requested {self.orderbook_subscriptions_requested}",
            f"orderbook_subscriptions_ok_total {self.orderbook_subscriptions_ok_total}",
            (
                "orderbook_subscriptions_limit_exceeded_total "
                f"{self.orderbook_subscriptions_limit_exceeded_total}"
            ),
            f"orderbook_subscriptions_capped_total {self.orderbook_subscriptions_capped_total}",
            f"orderbook_bootstrap_attempt_total {self.orderbook_bootstrap_attempt_total}",
            f"orderbook_bootstrap_success_total {self.orderbook_bootstrap_success_total}",
            f"orderbook_bootstrap_fetch_ok_total {self.orderbook_bootstrap_fetch_ok_total}",
            f"orderbook_bootstrap_persist_ok_total {self.orderbook_bootstrap_persist_ok_total}",
            f"universe_price_enrich_attempt_total {self.universe_price_enrich_attempt_total}",
            f"universe_price_enrich_success_total {self.universe_price_enrich_success_total}",
        ]
        for payload_name, count in sorted(self.stream_payload_total.items()):
            lines.append(f'stream_payload_total{{payload="{payload_name}"}} {count}')
        for exc_name, count in sorted(self.parse_error_total.items()):
            lines.append(f'parse_error_total{{exc="{exc_name}"}} {count}')
        for exc_name, count in sorted(self.events_save_error_total.items()):
            lines.append(f'events_save_error_total{{exc="{exc_name}"}} {count}')
        for status_name, count in sorted(self.orderbook_subscriptions_error_total.items()):
            lines.append(
                f'orderbook_subscriptions_error_total{{status_name="{status_name}"}} {count}'
            )
        for reason, count in sorted(self.orderbook_bootstrap_error_total.items()):
            lines.append(f'orderbook_bootstrap_error_total{{reason="{reason}"}} {count}')
        for exc_name, count in sorted(self.orderbook_bootstrap_persist_error_total.items()):
            lines.append(f'orderbook_bootstrap_persist_error_total{{exc="{exc_name}"}} {count}')
        for reason, count in sorted(self.snapshot_dropped_total.items()):
            lines.append(f'snapshot_dropped_total{{reason="{reason}"}} {count}')
        for reason, count in sorted(self.universe_price_enrich_error_total.items()):
            lines.append(f'universe_price_enrich_error_total{{reason="{reason}"}} {count}')
        return "\n".join(lines) + "\n"


_METRICS = Metrics()


def get_metrics() -> Metrics:
    return _METRICS
