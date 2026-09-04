"""TLS support: unit tests (target parsing, scheme-preserving gRPC-port guess, `ca=` config
resolution, and the verify=False/no-ca ValueError -- none of which need a running engine) plus a
live suite against `tls_engine` (see conftest.py): a real HTTPS/TLS-gRPC StreamsForge instance,
its certificate minted by `tools/tls/dev-cert.sh`.
"""

from __future__ import annotations

import uuid

import pytest

import streamsforge
from streamsforge import _config
from streamsforge._grpc import parse_grpc_target
from streamsforge import _default_grpc_target


# ============================================================================
# Unit tests -- no engine.
# ============================================================================


@pytest.mark.parametrize(
    "target, expected",
    [
        ("localhost:9299", ("localhost:9299", False)),
        ("http://localhost:9299", ("localhost:9299", False)),
        ("https://localhost:9299", ("localhost:9299", True)),
    ],
)
def test_parse_grpc_target_three_shapes(target, expected):
    assert parse_grpc_target(target) == expected


def test_default_grpc_target_preserves_https_scheme():
    assert _default_grpc_target("https://example.com:5199") == "https://example.com:5299"


def test_default_grpc_target_plain_http_stays_schemeless():
    assert _default_grpc_target("http://example.com:5199") == "example.com:5299"


def test_ca_resolved_from_explicit_kwarg(tmp_path):
    cfg = _config.resolve(ca="/explicit/ca.pem", config_path=tmp_path / "missing.toml")
    assert cfg.ca == "/explicit/ca.pem"


def test_ca_resolved_from_env(monkeypatch, tmp_path):
    monkeypatch.setenv("STREAMSFORGE_CA", "/env/ca.pem")
    cfg = _config.resolve(config_path=tmp_path / "missing.toml")
    assert cfg.ca == "/env/ca.pem"


def test_ca_resolved_from_toml(tmp_path, monkeypatch):
    monkeypatch.delenv("STREAMSFORGE_CA", raising=False)
    toml_path = tmp_path / "config.toml"
    toml_path.write_text('ca = "/toml/ca.pem"\n')
    cfg = _config.resolve(config_path=toml_path)
    assert cfg.ca == "/toml/ca.pem"


def test_ca_explicit_kwarg_beats_env_and_toml(monkeypatch, tmp_path):
    monkeypatch.setenv("STREAMSFORGE_CA", "/env/ca.pem")
    toml_path = tmp_path / "config.toml"
    toml_path.write_text('ca = "/toml/ca.pem"\n')
    cfg = _config.resolve(ca="/explicit/ca.pem", config_path=toml_path)
    assert cfg.ca == "/explicit/ca.pem"


def test_verify_false_with_https_grpc_and_no_ca_raises_value_error(monkeypatch, tmp_path):
    # No live server needed: connect() raises this before attempting any network call -- see
    # __init__.py's connect(), which checks it right after resolving config, ahead of both the
    # AuthClient construction (no I/O) and the gRPC dial attempt.
    monkeypatch.delenv("STREAMSFORGE_CA", raising=False)
    with pytest.raises(ValueError, match="verify=False"):
        streamsforge.connect(
            url="https://127.0.0.1:1",
            user="admin",
            password="admin123!",
            transport="grpc",
            verify=False,
            token="unused",
        )


def test_verify_false_with_explicit_https_grpc_target_and_no_ca_raises(monkeypatch):
    monkeypatch.delenv("STREAMSFORGE_CA", raising=False)
    with pytest.raises(ValueError, match="verify=False"):
        streamsforge.connect(
            url="http://127.0.0.1:1",
            grpc="https://127.0.0.1:2",
            user="admin",
            password="admin123!",
            transport="auto",
            verify=False,
            token="unused",
        )


def test_verify_false_with_plaintext_grpc_target_does_not_raise_value_error(monkeypatch):
    # Sanity check on the guard's scope: verify=False is fine for a plaintext (non-TLS) gRPC
    # target -- this should get as far as a real (refused) connection attempt, not the ValueError.
    monkeypatch.delenv("STREAMSFORGE_CA", raising=False)
    with pytest.raises(streamsforge.StreamsForgeError) as excinfo:
        streamsforge.connect(
            url="http://127.0.0.1:1",
            user="admin",
            password="admin123!",
            transport="grpc",
            verify=False,
            token="unused",
        )
    assert not isinstance(excinfo.value, ValueError)


# ============================================================================
# Live tests -- real TLS engine (conftest.py's tls_engine fixture).
# ============================================================================


def _push(sf, engine, rows):
    return sf.push(engine["source"], rows)


def test_grpc_over_tls_lists_tables_and_receives_pushed_rows(tls_engine):
    sf = streamsforge.connect(
        url=tls_engine["base_url"],
        grpc=tls_engine["grpc"],
        user=tls_engine["user"],
        password=tls_engine["password"],
        ca=tls_engine["ca"],
        transport="grpc",
    )
    try:
        assert sf.transport_name == "grpc"

        names = {t["name"] for t in sf.tables()}
        assert tls_engine["latest_table"] in names
        assert tls_engine["agg_table"] in names

        trade_id = f"t-{uuid.uuid4().hex[:8]}"
        ack = _push(sf, tls_engine, [{"trade_id": trade_id, "desk": "Rates", "notional": 100.0}])
        assert ack.get("accepted", ack.get("Accepted")) == 1

        t = sf.table(tls_engine["latest_table"], key=["trade_id"], timeout=30)
        try:
            df = t.wait_for(lambda d: trade_id in set(d.get("trade_id", [])), timeout=20)
            row = df[df["trade_id"] == trade_id].iloc[0]
            assert row["desk"] == "Rates"
            assert row["notional"] == 100.0
        finally:
            t.close()
    finally:
        sf.close()


def test_signalr_over_tls_receives_pushed_rows(tls_engine):
    sf = streamsforge.connect(
        url=tls_engine["base_url"],
        grpc=tls_engine["grpc"],
        user=tls_engine["user"],
        password=tls_engine["password"],
        ca=tls_engine["ca"],
        transport="signalr:ws",
    )
    try:
        assert sf.transport_name == "signalr:ws"

        trade_id = f"t-{uuid.uuid4().hex[:8]}"
        ack = _push(sf, tls_engine, [{"trade_id": trade_id, "desk": "FX", "notional": 42.0}])
        assert ack.get("accepted", ack.get("Accepted")) == 1

        t = sf.table(tls_engine["latest_table"], key=["trade_id"], timeout=30)
        try:
            df = t.wait_for(lambda d: trade_id in set(d.get("trade_id", [])), timeout=20)
            row = df[df["trade_id"] == trade_id].iloc[0]
            assert row["desk"] == "FX"
            assert row["notional"] == 42.0
        finally:
            t.close()
    finally:
        sf.close()


def test_connect_over_https_without_ca_raises(tls_engine):
    # The dev certificate is self-signed and is its own trust anchor (see tools/tls/dev-cert.sh) --
    # with no ca= given, verification falls back to the system trust store, which does not know
    # it, so the handshake fails and connect() (transport="grpc", so the failure is not swallowed
    # by auto's SignalR fallback) surfaces it as a StreamsForgeError.
    with pytest.raises(streamsforge.StreamsForgeError):
        streamsforge.connect(
            url=tls_engine["base_url"],
            grpc=tls_engine["grpc"],
            user=tls_engine["user"],
            password=tls_engine["password"],
            transport="grpc",
        )
