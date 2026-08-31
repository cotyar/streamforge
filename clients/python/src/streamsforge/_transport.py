"""The one interface every live transport implements: gRPC (_grpc.py) and SignalR in its three
wire modes (_hub.py). `live.py`, the reducer and every test are written against THIS, and do not
know which concrete transport is underneath -- that is the whole point of the contract test suite
running once per transport (design doc §3.6/§8): three implementations that agree on every
assertion are interchangeable, and one that drifts fails on the same line the others pass.
"""

from __future__ import annotations

from typing import Callable, Iterator, Protocol

from ._zset import Delta


class CancellableIterator:
    """Wraps a plain generator with a `.cancel` attribute -- generator objects do NOT support
    arbitrary attribute assignment in CPython (`'generator' object has no attribute 'cancel'`),
    so `_grpc.py`/`_hub.py`'s subscribe() return one of these instead of a bare generator when
    they want to hand live.py a thread-safe way to interrupt a blocked reader thread (see their
    subscribe() docstrings, and live.py's use of `getattr(gen, "cancel", None)`)."""

    __slots__ = ("_gen", "cancel")

    def __init__(self, gen: Iterator, cancel: Callable[[], None]) -> None:
        self._gen = gen
        self.cancel = cancel

    def __iter__(self) -> "CancellableIterator":
        return self

    def __next__(self):
        return next(self._gen)


class Transport(Protocol):
    name: str

    def subscribe(self, table_name: str) -> Iterator[tuple[list[Delta], int]]:
        """Yield (deltas, seq) batches for `table_name` until the subscription ends (error or a
        clean server-initiated close). No backfill: the first item is whatever arrives after the
        subscription is live, not the table's current contents -- callers pair this with
        snapshot() and buffer/replay (live.py), never rely on subscribe() alone."""
        ...

    def snapshot(self, table_name: str, limit: int = 500) -> tuple[list[Delta], int]:
        """One-shot read of the table's current consolidated rows (weight already summed
        server-side) plus the read's own sequence number. Not comparable to subscribe()'s seq --
        see _zset.py's module docstring."""
        ...

    def close(self) -> None: ...
