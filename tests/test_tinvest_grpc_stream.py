from types import SimpleNamespace

import pytest

pytest.importorskip("grpc")

from app.adapters.tinvest.grpc_stream import build_grpc_credentials, select_grpc_target


def test_select_grpc_target_sandbox():
    settings = SimpleNamespace(
        app_env="sandbox",
        tinvest_grpc_target_prod="prod.example:443",
        tinvest_grpc_target_sandbox="sandbox.example:443",
        tinvest_ssl_ca_bundle=None,
    )

    assert select_grpc_target(settings) == "sandbox.example:443"


def test_select_grpc_target_prod():
    settings = SimpleNamespace(
        app_env="prod",
        tinvest_grpc_target_prod="prod.example:443",
        tinvest_grpc_target_sandbox="sandbox.example:443",
        tinvest_ssl_ca_bundle=None,
    )

    assert select_grpc_target(settings) == "prod.example:443"


def test_build_grpc_credentials_reads_bundle(monkeypatch, tmp_path):
    pem_path = tmp_path / "ca.pem"
    pem_path.write_bytes(b"dummy-pem")
    captured = {}

    def fake_ssl_channel_credentials(root_certificates=None):
        captured["root_certificates"] = root_certificates
        return object()

    monkeypatch.setattr(
        "app.adapters.tinvest.grpc_stream.grpc.ssl_channel_credentials",
        fake_ssl_channel_credentials,
    )

    settings = SimpleNamespace(
        app_env="prod",
        tinvest_grpc_target_prod="prod.example:443",
        tinvest_grpc_target_sandbox="sandbox.example:443",
        tinvest_ssl_ca_bundle=str(pem_path),
    )

    credentials, mode = build_grpc_credentials(settings)

    assert credentials is not None
    assert mode == "custom_bundle"
    assert captured["root_certificates"] == b"dummy-pem"
