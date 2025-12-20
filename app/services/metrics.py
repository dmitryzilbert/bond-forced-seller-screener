from __future__ import annotations

from datetime import datetime, timezone


class Metrics:
    def __init__(self) -> None:
        now = datetime.now(timezone.utc)
        self.snapshots_ingested_total = 0
        self.events_candidate_total = 0
        self.events_alert_total = 0
        self.tg_sent_total = 0
        self.stream_reconnect_total = 0
        self.eligible_instruments_total = 0
        self.shortlisted_instruments_total = 0
        self.last_update_ts: datetime | None = now
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

    def set_stream_reconnects(self, value: int) -> None:
        self.stream_reconnect_total = value

    def set_instrument_totals(self, *, eligible: int, shortlisted: int) -> None:
        self.eligible_instruments_total = eligible
        self.shortlisted_instruments_total = shortlisted

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
            f"stream_reconnect_total {self.stream_reconnect_total}",
            f"eligible_instruments_total {self.eligible_instruments_total}",
            f"shortlisted_instruments_total {self.shortlisted_instruments_total}",
        ]
        return "\n".join(lines) + "\n"


_METRICS = Metrics()


def get_metrics() -> Metrics:
    return _METRICS
