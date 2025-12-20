from fastapi.testclient import TestClient

from app.main import app


def test_api_events_returns_json():
    with TestClient(app) as client:
        response = client.get("/api/events")

        assert response.status_code == 200
        assert isinstance(response.json(), list)
