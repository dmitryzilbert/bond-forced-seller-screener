from datetime import datetime, timezone

from fastapi.encoders import jsonable_encoder

from app.domain.models import Event


def test_jsonable_encoder_serializes_event_datetime():
    event = Event(
        isin="TEST123",
        ts=datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc),
        ytm_mid=1.1,
        ytm_event=1.2,
        delta_ytm_bps=10,
        ask_lots_window=1.0,
        ask_notional_window=1000.0,
        spread_ytm_bps=5.0,
        score=0.9,
    )

    encoded = jsonable_encoder([event])

    assert encoded[0]["ts"] == "2024-01-01T12:00:00Z"
