"""LiveTable: one table's Z-set state, kept current by a single background reader thread.

The reader thread runs subscribe -> buffer -> snapshot -> replay (_zset.py's module docstring
explains why buffering is necessary), then keeps applying live deltas as they arrive -- every
batch is applied to the Z-set immediately, regardless of coalescing -- and notifies `on_change`
callbacks with a leading-edge + trailing-coalesce window (`flush_ms`, default 16ms -- one frame at
60Hz, the shortest interval a UI could even display): if at least `flush_ms` has elapsed since the
last callback, the new state is published immediately (no wait); otherwise it is merged into
whatever else lands before `last_publish + flush_ms` and published exactly once when that deadline
is reached. This means a lone update on an otherwise-quiet table is delivered with no artificial
delay -- exactly the case a pure trailing-edge window handles worst, since coalescing has nothing
to save there. The window only matters once deltas start arriving faster than `flush_ms` apart: a
Monte-Carlo-style firehose (tens of thousands of deltas/sec) would otherwise fire one callback per
delta and melt the consumer, so batches inside one window still collapse into a single callback
carrying the latest state. `flush_ms=0` disables coalescing entirely -- one callback per applied
batch. The window changes WHEN a consumer is told, never WHAT: every callback (leading or
trailing) is handed a full, current snapshot, not a diff.

`.df` is a projection, not a mirror: it is built fresh from the current dict on every read/every
on_change call. A "live DataFrame" that mutated in place would race the reader thread with no
change notification pandas could give a consumer -- see design doc §2.
"""

from __future__ import annotations

import logging
import queue
import threading
import time
from typing import Callable

import pandas as pd

from . import _zset
from .errors import NotReady, StreamsForgeError

logger = logging.getLogger("streamsforge")

DEFAULT_FLUSH_MS = 16  # one frame at 60Hz -- a UI cannot display more than one frame per 16ms
_MAX_BACKOFF_S = 15.0


class LiveTable:
    def __init__(
        self,
        transport,
        table_name: str,
        key_fields: list[str] | None,
        timeout: float = 30,
        flush_ms: float = DEFAULT_FLUSH_MS,
    ) -> None:
        self._transport = transport
        self._table_name = table_name
        self._key_fields = key_fields
        self._zset = _zset.ZSet(key_fields)
        self._lock = threading.RLock()
        self._ready = threading.Event()
        self._closed = threading.Event()
        self._reconnects = 0
        self._seq = 0
        self._callbacks: list[Callable[[pd.DataFrame], None]] = []
        # Leading-edge + trailing-coalesce on_change scheduling state -- touched only from the
        # reader thread (_live_loop), so it needs none of _lock's protection.
        self._flush_s = max(0.0, flush_ms) / 1000.0
        self._last_publish: float | None = None
        self._pending_deadline: float | None = None
        self._pending_touched: set[str] = set()
        self._thread = threading.Thread(
            target=self._run, name=f"sf-live[{table_name}]", daemon=True
        )
        self._thread.start()
        if not self._ready.wait(timeout):
            self._closed.set()
            raise NotReady(
                f"table '{table_name}' did not fill within {timeout}s -- a brand-new table gets "
                "no backfill, so this is expected until something pushes to it"
            )

    # ---- public surface ----

    @property
    def rows(self) -> list[dict]:
        with self._lock:
            return list(self._zset.rows())

    @property
    def df(self) -> pd.DataFrame:
        return pd.DataFrame(self.rows)

    @property
    def ready(self) -> bool:
        return self._ready.is_set()

    @property
    def reconnects(self) -> int:
        return self._reconnects

    @property
    def seq(self) -> int:
        with self._lock:
            return self._seq

    def value(self, col: str, **keys):
        for row in self.rows:
            if all(row.get(k) == v for k, v in keys.items()):
                return row.get(col)
        return None

    def wait_for(self, pred: Callable[[pd.DataFrame], bool], timeout: float = 30) -> pd.DataFrame:
        """Poll `pred(self.df)` until it's true. A predicate that indexes a column which doesn't
        exist yet (an empty table has no columns at all) raises KeyError rather than returning a
        false-y value -- treated the same as "not yet", not as a bug in the predicate, since
        "the column doesn't exist yet" is exactly the state wait_for exists to wait out."""
        deadline = time.monotonic() + timeout
        while True:
            df = self.df
            try:
                if pred(df):
                    return df
            except (KeyError, AttributeError, IndexError):
                pass
            if time.monotonic() >= deadline:
                raise NotReady(f"wait_for on '{self._table_name}' timed out after {timeout}s")
            time.sleep(0.05)

    def on_change(self, cb: Callable[[pd.DataFrame], None]) -> Callable[[], None]:
        with self._lock:
            self._callbacks.append(cb)

        def unsubscribe() -> None:
            with self._lock:
                if cb in self._callbacks:
                    self._callbacks.remove(cb)

        return unsubscribe

    def close(self) -> None:
        self._closed.set()
        self._thread.join(timeout=5)

    def __enter__(self) -> "LiveTable":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # ---- reader thread ----

    def _run(self) -> None:
        backoff = 1.0
        first_attempt = True
        while not self._closed.is_set():
            try:
                self._subscribe_snapshot_replay(first_attempt)
                backoff = 1.0
            except _Stopped:
                return
            except Exception as exc:
                if self._closed.is_set():
                    return
                self._ready.clear()
                self._reconnects += 1
                logger.warning(
                    "streamsforge: %s reader error (reconnect #%d in %.1fs): %s",
                    self._table_name, self._reconnects, backoff, exc,
                )
                if self._closed.wait(backoff):
                    return
                backoff = min(_MAX_BACKOFF_S, backoff * 2)
            first_attempt = False

    def _subscribe_snapshot_replay(self, first_attempt: bool) -> None:
        # A resumed connection without a fresh snapshot silently corrupts the Z-set (deltas
        # emitted while it was down are gone), so every (re)connect starts from a clean reducer.
        with self._lock:
            self._zset = _zset.ZSet(self._key_fields)
        # A fresh subscription is itself "a quiet table" from the on_change scheduler's point of
        # view -- forget any publish/pending state from before the reconnect so the first delta
        # after resuming gets the same leading-edge treatment as the very first delta ever.
        self._last_publish = None
        self._pending_deadline = None
        self._pending_touched = set()

        q: "queue.Queue[tuple[list, int] | tuple[str, BaseException] | None]" = queue.Queue()
        gen = self._transport.subscribe(self._table_name)

        def reader() -> None:
            try:
                for deltas, seq in gen:
                    q.put((deltas, seq))
                    if self._closed.is_set():
                        return
            except BaseException as exc:  # noqa: BLE001 -- forwarded to the owning thread, not swallowed
                q.put(("__error__", exc))
            finally:
                q.put(None)

        reader_thread = threading.Thread(target=reader, daemon=True)
        reader_thread.start()
        try:
            self._do_snapshot_and_replay(q)
            self._live_loop(q)
        finally:
            # `gen` is being iterated by `reader_thread`, a DIFFERENT thread than this one -- a
            # plain generator.close() from here would raise "generator already executing" and
            # silently leak the subscription. Transports attach a thread-safe `.cancel` hook to
            # the generator they return (see _grpc.py/_hub.py's subscribe()) for exactly this;
            # gen.close() is only the fallback for a hypothetical Transport that doesn't.
            cancel = getattr(gen, "cancel", None)
            try:
                (cancel or gen.close)()
            except Exception:
                pass

    def _do_snapshot_and_replay(self, q: "queue.Queue") -> None:
        snap_rows, snap_seq = self._transport.snapshot(self._table_name)
        buffered = _drain_nowait(q)

        with self._lock:
            self._zset.seed(snap_rows)
            self._seq = snap_seq
            for item in buffered:
                if item is None:
                    raise StreamsForgeError("subscription ended before the initial snapshot")
                if isinstance(item, tuple) and item[0] == "__error__":
                    raise item[1]
                deltas, seq = item
                if self._zset.already_reflected(deltas):
                    continue
                self._zset.apply(deltas)
                self._seq = seq

        if self._closed.is_set():
            raise _Stopped()
        self._ready.set()

    def _live_loop(self, q: "queue.Queue") -> None:
        # The reader must never stall draining the queue to wait out a coalescing window: every
        # iteration first fires an already-due trailing publish (a cheap timestamp check, not a
        # wait), THEN blocks on q.get() for at most whatever time is left until the next thing
        # that needs attention -- a queued item, the 1s liveness poll, or the pending deadline,
        # whichever is soonest. A burst of batches keeps flowing straight through q.get() without
        # ever hitting that timeout, so the flush fires as soon as the loop next comes around
        # after the deadline passes, not only when the queue happens to run dry.
        while not self._closed.is_set():
            self._flush_if_due()

            timeout = 1.0
            if self._pending_deadline is not None:
                timeout = max(0.0, min(timeout, self._pending_deadline - time.monotonic()))
            try:
                item = q.get(timeout=timeout)
            except queue.Empty:
                continue

            if item is None:
                raise StreamsForgeError(f"'{self._table_name}' subscription stream ended")
            if isinstance(item, tuple) and item[0] == "__error__":
                raise item[1]

            deltas, seq = item
            with self._lock:
                touched = self._zset.apply(deltas)
                self._seq = seq
            self._schedule(touched)

        raise _Stopped()

    def _schedule(self, touched: list[str]) -> None:
        """Leading-edge + trailing-coalesce: publish immediately if the window has already
        elapsed since the last publish (or there hasn't been one yet); otherwise merge into the
        single pending publish, due at last_publish + flush_s regardless of how many further
        batches land before then."""
        if not touched:
            return
        if self._flush_s <= 0:
            self._publish_now(touched)
            return
        now = time.monotonic()
        if self._pending_deadline is None and (
            self._last_publish is None or now - self._last_publish >= self._flush_s
        ):
            self._publish_now(touched)
            return
        self._pending_touched.update(touched)
        if self._pending_deadline is None:
            self._pending_deadline = (self._last_publish or now) + self._flush_s

    def _flush_if_due(self) -> None:
        if self._pending_deadline is not None and time.monotonic() >= self._pending_deadline:
            touched, self._pending_touched = self._pending_touched, set()
            self._pending_deadline = None
            self._emit(touched)
            self._last_publish = time.monotonic()

    def _publish_now(self, touched: list[str]) -> None:
        self._pending_deadline = None
        self._pending_touched = set()
        self._emit(touched)
        self._last_publish = time.monotonic()

    def _emit(self, touched: list[str]) -> None:
        if not touched:
            return
        with self._lock:
            cbs = list(self._callbacks)
        if not cbs:
            return
        df = self.df
        for cb in cbs:
            try:
                cb(df)
            except Exception:
                logger.exception("streamsforge: on_change callback for '%s' raised", self._table_name)


class _Stopped(Exception):
    """Internal sentinel: close() was called mid-cycle. Never escapes _run()."""


def _drain_nowait(q: "queue.Queue") -> list:
    items = []
    while True:
        try:
            items.append(q.get_nowait())
        except queue.Empty:
            break
    return items
