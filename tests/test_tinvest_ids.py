from types import SimpleNamespace

import pytest

from app.tinvest.ids import api_instrument_id


def test_api_instrument_id_prefers_uid():
    instrument = SimpleNamespace(
        instrument_uid="uid-123",
        figi="figi-123",
        ticker="TST",
        class_code="TQOB",
    )
    assert api_instrument_id(instrument) == "uid-123"


def test_api_instrument_id_falls_back_to_figi():
    instrument = SimpleNamespace(instrument_uid=None, figi="figi-123", ticker="TST", class_code="TQOB")
    assert api_instrument_id(instrument) == "figi-123"


def test_api_instrument_id_uses_ticker_class_code():
    instrument = SimpleNamespace(instrument_uid=None, figi=None, ticker="TST", class_code="TQOB")
    assert api_instrument_id(instrument) == "TST_TQOB"


def test_api_instrument_id_raises_when_missing():
    instrument = SimpleNamespace(instrument_uid=None, figi=None, ticker=None, class_code=None)
    with pytest.raises(ValueError):
        api_instrument_id(instrument)
