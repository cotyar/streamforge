"""Unit tests for LiveTable's on_change scheduling (leading-edge + trailing-coalesce, `flush_ms`)
-- driven against a hand-rolled fake Transport, no engine and no network, since none of this needs
a server: it is pure LiveTable/on_change wiring. See test_contract.py for the (separate) real-engine
contract suite, and live.py's module docstring for the semantics under test here.
"""

from __future__ import annotations

import queue
import threading
import time

import pytest

from streamforge._transport import CancellableIterator
from streamforge.live import LiveTable

_STOP = object()


class FakeTransport:
    """A controllable Transport: `push()` feeds one (deltas, seq) batch straight to whatever
    LiveTable reader thread is currently blocked in subscribe()'s generator -- no polling, so a
    push is delivered to the reader as soon as the GIL schedules it, keeping the timing assertions
    below tight instead of padded out by an artificial poll interval."""

    name = "fake"

    def __init__(self, snapshot_rows=None, snapshot_seq: int = 0) -> None:
        self._snapshot_rows = list(snapshot_rows or [])
        self._snapshot_seq = snapshot_seq
        self._seq = snapshot_seq
        self._queue: "queue.Queue" = queue.Queue()

    def snapshot(self, table_name: str, limit: int = 500):
        return list(self._snapshot_rows), self._snapshot_seq

    def subscribe(self, table_name: str):
        q = self._queue

        def gen():
            while True:
                item = q.get()
                if item is _STOP:
                    return
                yield item

        return CancellableIterator(gen(), cancel=lambda: q.put(_STOP))

    def push(self, row: dict, weight: float = 1) -> None:
        self._seq += 1
        self._queue.put(([(row, weight)], self._seq))

    def close(self) -> None:
        pass


def _wait_until(pred, timeout: float = 2.0, interval: float = 0.005) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return True
        time.sleep(interval)
    return pred()


@pytest.fixture
def transport():
    return FakeTransport()


def test_lone_update_on_quiet_table_publishes_without_artificial_delay(transport):
    # The bug this change fixes: a pure trailing-edge window delays a lone update by the whole
    # window even though coalescing has nothing to save there. flush_ms=16 (the new default) is
    # used here mainly to prove it isn't hard-coded; the leading-edge path is timing-independent --
    # the very first change on a freshly-subscribed table always has "no last publish yet", so it
    # always takes the leading edge, whatever the window is.
    table = LiveTable(transport, "t", ["id"], timeout=5, flush_ms=16)
    try:
        fired_at: list[float] = []
        table.on_change(lambda df: fired_at.append(time.monotonic()))

        t0 = time.monotonic()
        transport.push({"id": 1, "v": "a"})
        assert _wait_until(lambda: len(fired_at) >= 1, timeout=2.0)

        elapsed = fired_at[0] - t0
        # Well under the OLD 120ms trailing-only window -- the whole point of the fix.
        assert elapsed < 0.05, f"lone update took {elapsed * 1000:.1f}ms -- expected near-zero delay"
    finally:
        table.close()


def test_burst_inside_one_window_produces_exactly_one_publish(transport):
    # A generous window (200ms) makes the timing assertions robust on a loaded CI box while still
    # being trivially distinguishable from "no coalescing at all".
    table = LiveTable(transport, "t", ["id"], timeout=5, flush_ms=200)
    try:
        calls: list[int] = []
        table.on_change(lambda df: calls.append(len(df)))

        # First change: nothing published yet, so this one takes the leading edge and establishes
        # last_publish. Isolate it from the burst below so the burst's own count is unambiguous.
        transport.push({"id": 1, "v": "a"})
        assert _wait_until(lambda: len(calls) == 1, timeout=2.0)

        # A burst of further updates, all well inside the 200ms window since that first publish --
        # every one of them must merge into a SINGLE trailing publish, not fire one callback each.
        for i in range(2, 8):
            transport.push({"id": i, "v": "a"})

        assert _wait_until(lambda: len(calls) == 2, timeout=2.0)
        # Give it a further grace window past the deadline to prove nothing extra trickles in.
        time.sleep(0.3)
        assert len(calls) == 2
        # The single coalesced publish carries the FULL current snapshot (7 rows), not a diff.
        assert calls[-1] == 7
    finally:
        table.close()


def test_flush_ms_zero_publishes_per_batch(transport):
    # flush_ms=0 means no coalescing at all: every applied batch gets its own callback, even when
    # several batches are queued back-to-back before the reader thread gets a chance to run.
    table = LiveTable(transport, "t", ["id"], timeout=5, flush_ms=0)
    try:
        calls: list[int] = []
        table.on_change(lambda df: calls.append(len(df)))

        for i in range(1, 4):
            transport.push({"id": i, "v": "a"})

        assert _wait_until(lambda: len(calls) == 3, timeout=2.0)
        # No merging: each callback sees the table growing one row at a time.
        assert calls == [1, 2, 3]
    finally:
        table.close()


def test_close_does_not_publish_after_teardown(transport):
    # A trailing publish scheduled just before close() must never fire once the table is closed --
    # and close() itself must not hang waiting on it (the reader thread's queue.get() is bounded by
    # the pending deadline, never by the window unconditionally).
    table = LiveTable(transport, "t", ["id"], timeout=5, flush_ms=200)
    calls: list[int] = []
    table.on_change(lambda df: calls.append(len(df)))

    transport.push({"id": 1, "v": "a"})  # leading edge
    assert _wait_until(lambda: len(calls) == 1, timeout=2.0)
    transport.push({"id": 2, "v": "a"})  # scheduled as a trailing publish, due in ~200ms

    close_thread = threading.Thread(target=table.close)
    t0 = time.monotonic()
    close_thread.start()
    close_thread.join(timeout=5)
    assert not close_thread.is_alive(), "close() hung waiting on the reader thread"
    assert time.monotonic() - t0 < 2.0, "close() should not block for anything like the flush window"

    # The pending trailing publish must have been dropped, not fired during/after teardown.
    assert len(calls) == 1
