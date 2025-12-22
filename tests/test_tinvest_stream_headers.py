from app.adapters.tinvest.stream import TInvestStream


async def _new_api_connector(url, *, additional_headers=None, **kwargs):  # pragma: no cover - used for signature
    return None


async def _legacy_api_connector(url, *, extra_headers=None, **kwargs):  # pragma: no cover - used for signature
    return None


def test_build_connect_kwargs_new_api():
    headers = {"Authorization": "Bearer token"}
    stream = TInvestStream("token", connector=_new_api_connector)

    connect_kwargs = stream._build_connect_kwargs(headers)

    assert connect_kwargs == {"additional_headers": headers}


def test_build_connect_kwargs_legacy_api():
    headers = {"Authorization": "Bearer token"}
    stream = TInvestStream("token", connector=_legacy_api_connector)

    connect_kwargs = stream._build_connect_kwargs(headers)

    assert connect_kwargs == {"extra_headers": headers}
