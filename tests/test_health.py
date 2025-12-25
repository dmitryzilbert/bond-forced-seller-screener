from fastapi.testclient import TestClient
from datetime import datetime, timedelta, timezone

from app.main import app
from app.services.metrics import get_metrics


def test_health_mock_ok():
    with TestClient(app) as client:
        response = client.get("/health")

        assert response.status_code == 200
        payload = response.json()
        assert payload.get("status") == "ok"


def test_health_ok_with_worker_heartbeat_only():
    metrics = get_metrics()
    original_stream_ts = metrics.last_stream_message_ts
    original_worker_ts = metrics.last_worker_heartbeat_ts
    try:
        now = datetime.now(timezone.utc)
        metrics.last_stream_message_ts = now - timedelta(seconds=3600)
        metrics.last_worker_heartbeat_ts = now

        with TestClient(app) as client:
            response = client.get("/health")

        assert response.status_code == 200
        payload = response.json()
        assert payload.get("status") == "ok"
    finally:
        metrics.last_stream_message_ts = original_stream_ts
        metrics.last_worker_heartbeat_ts = original_worker_ts
