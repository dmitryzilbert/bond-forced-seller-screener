from app.domain.scoring import score_event


def test_score_monotonic():
    base = score_event(50, 300_000, 100)
    higher = score_event(150, 600_000, 50)
    assert higher > base
