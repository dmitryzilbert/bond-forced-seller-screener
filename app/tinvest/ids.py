from __future__ import annotations


def _read_value(obj, *names):
    for name in names:
        if isinstance(obj, dict):
            value = obj.get(name)
        else:
            value = getattr(obj, name, None)
        if value:
            return value
    return None


def api_instrument_id(instrument) -> str:
    instrument_uid = _read_value(instrument, "instrument_uid", "instrumentUid")
    if instrument_uid:
        return str(instrument_uid)

    figi = _read_value(instrument, "figi")
    if figi:
        return str(figi)

    ticker = _read_value(instrument, "ticker")
    class_code = _read_value(instrument, "class_code", "classCode")
    if ticker and class_code:
        return f"{ticker}_{class_code}"

    raise ValueError("Instrument identifier is missing (instrument_uid/figi/ticker+class_code)")
