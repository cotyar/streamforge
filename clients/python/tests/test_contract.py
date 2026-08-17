"""Contract tests against a real, isolated StreamForge instance (see conftest.py), parametrized
over every live transport -- one set of assertions proving gRPC and all three SignalR wire modes
are actually interchangeable, per the Transport interface's whole reason for existing.
"""

from __future__ import annotations

import time
import uuid

import pytest

import streamforge

TRANSPORTS = ["grpc", "signalr:ws", "signalr:sse", "signalr:lp"]


@pytest.fixture(params=TRANSPORTS)
def sf(request, engine):
    client = streamforge.connect(
        url=engine["base_url"],
        grpc=engine["grpc"],
        user=engine["user"],
        password=engine["password"],
        transport=request.param,
    )
    assert client.transport_name == request.param
    yield client
    client.close()


def _push(sf, engine, rows, idempotency_key=None):
    ack = sf.push(engine["source"], rows, idempotency_key=idempotency_key)
    return ack


def test_handshake_and_snapshot(sf, engine):
    # A freshly-imported table has no rows yet -- snapshot must succeed and be empty, not error.
    df = sf.snapshot(engine["latest_table"])
    assert list(df.columns) == [] or len(df) == 0


def test_push_then_live_table_sees_it(sf, engine):
    trade_id = f"t-{uuid.uuid4().hex[:8]}"
    ack = _push(sf, engine, [{"trade_id": trade_id, "desk": "Rates", "notional": 100.0}])
    assert ack.get("accepted", ack.get("Accepted")) == 1

    t = sf.table(engine["latest_table"], key=["trade_id"], timeout=30)
    try:
        df = t.wait_for(lambda d: trade_id in set(d.get("trade_id", [])), timeout=20)
        row = df[df["trade_id"] == trade_id].iloc[0]
        assert row["desk"] == "Rates"
        assert row["notional"] == 100.0
    finally:
        t.close()


def test_supersession_latest_by(sf, engine):
    trade_id = f"t-{uuid.uuid4().hex[:8]}"
    t = sf.table(engine["latest_table"], key=["trade_id"], timeout=30)
    try:
        _push(sf, engine, [{"trade_id": trade_id, "desk": "Rates", "notional": 100.0}])
        t.wait_for(lambda d: trade_id in set(d.get("trade_id", [])), timeout=20)

        _push(sf, engine, [{"trade_id": trade_id, "desk": "Rates", "notional": 250.0}])

        def superseded(df):
            match = df[df["trade_id"] == trade_id]
            return len(match) == 1 and match.iloc[0]["notional"] == 250.0

        df = t.wait_for(superseded, timeout=20)
        # LATEST BY: exactly one row for this trade_id, never two.
        assert len(df[df["trade_id"] == trade_id]) == 1
    finally:
        t.close()


def test_global_aggregate_reflects_pushes(sf, engine):
    desk = f"Desk-{uuid.uuid4().hex[:6]}"
    agg = sf.table(engine["agg_table"], key=["desk"], timeout=30)
    try:
        _push(sf, engine, [{"trade_id": f"a-{uuid.uuid4().hex[:8]}", "desk": desk, "notional": 40.0}])
        _push(sf, engine, [{"trade_id": f"b-{uuid.uuid4().hex[:8]}", "desk": desk, "notional": 60.0}])

        def totals_100(df):
            match = df[df["desk"] == desk]
            return len(match) == 1 and match.iloc[0]["total"] == 100.0

        agg.wait_for(totals_100, timeout=20)
    finally:
        agg.close()


def test_on_change_callback_fires(sf, engine):
    trade_id = f"t-{uuid.uuid4().hex[:8]}"
    t = sf.table(engine["latest_table"], key=["trade_id"], timeout=30)
    seen = []
    stop = t.on_change(lambda df: seen.append(len(df)))
    try:
        _push(sf, engine, [{"trade_id": trade_id, "desk": "FX", "notional": 5.0}])
        t.wait_for(lambda d: trade_id in set(d.get("trade_id", [])), timeout=20)
        # wait_for can return as soon as the delta is applied, up to FLUSH_S (~120ms) before the
        # coalesced on_change callback actually fires -- give it a short grace window rather than
        # asserting the instant wait_for returns.
        deadline = time.monotonic() + 5
        while not seen and time.monotonic() < deadline:
            time.sleep(0.05)
        assert len(seen) >= 1
    finally:
        stop()
        t.close()


def test_ingest_row_errors_on_bad_row(sf, engine):
    # A string where a Double is declared fails coercion under this source's default
    # OnCoercionFailure -- a real rejection, not a lenient null-fill.
    with pytest.raises(streamforge.IngestRejected) as excinfo:
        _push(sf, engine, [{"trade_id": f"t-{uuid.uuid4().hex[:8]}", "desk": "Ops", "notional": "not-a-number"}])
    assert excinfo.value.row_errors or str(excinfo.value)


def test_validate_rejects_bad_sql(sf):
    with pytest.raises(streamforge.SqlError) as excinfo:
        sf.sql("SELECT nonexistent_column FROM nowhere_table", name=f"bad_{uuid.uuid4().hex[:6]}")
    err = excinfo.value
    assert err.diagnostics
    assert "line" in str(err) or err.diagnostics[0].get("message")


def test_adhoc_sql_roundtrip(sf, engine):
    name = f"adhoc_roundtrip_{uuid.uuid4().hex[:6]}"
    q = sf.sql(
        f"SELECT desk, SUM(notional) AS total FROM {engine['latest_table']} GROUP BY desk",
        name=name,
        key=["desk"],
        timeout=30,
    )
    try:
        assert q.ready
        listing = sf.adhoc()
        assert name in set(listing.get("name", []))
    finally:
        q.close()
        assert sf.drop_adhoc(name) is True
        assert sf.drop_adhoc(name) is False  # already gone


def test_reader_thread_reconnects_after_close(sf, engine):
    # Not a network-kill test (no fault injection harness here) -- exercises that closing and
    # re-subscribing to the SAME table produces a fresh, correctly-seeded LiveTable rather than
    # reusing stale state, which is the property reconnect leans on.
    trade_id = f"t-{uuid.uuid4().hex[:8]}"
    _push(sf, engine, [{"trade_id": trade_id, "desk": "Credit", "notional": 12.0}])

    t1 = sf.table(engine["latest_table"], key=["trade_id"], timeout=30)
    t1.wait_for(lambda d: trade_id in set(d.get("trade_id", [])), timeout=20)
    t1.close()

    # The engine's row store (what a snapshot read sees) trails the live delta stream by a short,
    # measured interval -- wait for the SNAPSHOT itself to catch up before subscribing again: a
    # fresh subscription gets no backfill, so if the snapshot hasn't caught up yet, t2 would wait
    # forever for a delta that will never come (the row isn't new, nothing will re-assert it).
    deadline = time.monotonic() + 15
    while trade_id not in set(sf.snapshot(engine["latest_table"]).get("trade_id", [])):
        if time.monotonic() >= deadline:
            pytest.fail("engine snapshot never caught up with the earlier push")
        time.sleep(0.25)

    t2 = sf.table(engine["latest_table"], key=["trade_id"], timeout=30)
    try:
        df = t2.wait_for(lambda d: trade_id in set(d.get("trade_id", [])), timeout=20)
        assert (df[df["trade_id"] == trade_id]["desk"] == "Credit").all()
    finally:
        t2.close()
