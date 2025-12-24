import pytest


def test_import_sdk():
    try:
        from t_tech.invest import Client  # noqa: F401
    except ModuleNotFoundError as exc:
        raise ImportError(
            "t_tech.invest SDK is not installed. Install t-tech-investments>=0.3.0",
        ) from exc
