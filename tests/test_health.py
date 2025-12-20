from fastapi.testclient import TestClient

from app.main import app


def test_health_mock_ok():
    with TestClient(app) as client:
        response = client.get("/health")

        assert response.status_code == 200
        payload = response.json()
        assert payload.get("status") == "ok"
