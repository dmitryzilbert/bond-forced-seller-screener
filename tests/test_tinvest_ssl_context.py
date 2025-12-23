import ssl
from types import SimpleNamespace

import certifi

from app.adapters.tinvest.stream import build_ssl_context


def test_build_ssl_context_insecure():
    settings = SimpleNamespace(tinvest_ssl_insecure=True, tinvest_ssl_ca_bundle=certifi.where())

    ctx, mode = build_ssl_context(settings)

    assert mode == "insecure"
    assert ctx.verify_mode == ssl.CERT_NONE


def test_build_ssl_context_custom_bundle():
    settings = SimpleNamespace(tinvest_ssl_insecure=False, tinvest_ssl_ca_bundle=certifi.where())

    ctx, mode = build_ssl_context(settings)

    assert mode == "custom_bundle"
    assert ctx.verify_mode == ssl.CERT_REQUIRED


def test_build_ssl_context_default_certifi():
    settings = SimpleNamespace(tinvest_ssl_insecure=False, tinvest_ssl_ca_bundle=None)

    ctx, mode = build_ssl_context(settings)

    assert mode == "certifi"
    assert ctx.verify_mode == ssl.CERT_REQUIRED
