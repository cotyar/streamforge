"""The Z-set reducer -- ported literally from lib/streamforge/live-table.ts (otc-terms), itself a
port of crates-foundation's own web/src/hooks/useTableRows.ts. Pure functions/state, no I/O:
this module never touches a socket, so it is testable with handcrafted fixtures alone.

Hazards, and why the code below looks the way it does:

- A row's identity for retract/assert matching is the WHOLE row's content, not any one column:
  `canonical_key` is a stable serialization of every (key, value) pair, sorted by key. Weight is
  SUMMED per canonical identity across every delta seen; a summed weight <= 0 removes the row
  outright rather than going negative. This is what makes retract-then-assert and
  assert-then-retract (arrival order is not guaranteed) both converge to the same state.

- A logical key ("group", `key_fields`) can SUPERSEDE: two different canonical rows sharing the
  same key_fields values are the same logical entity at different times (an updated MTM tick, a
  LATEST BY row). When a new row for a group is asserted, the group's PREVIOUS canonical row is
  deleted even though its own weight was never explicitly retracted on the wire -- the retraction
  is implied by the new assert superseding it. `group_key_of` returns `"*"` for `key_fields=[]`
  (a global aggregate: one row, one group), and `None` for `key_fields=None` (unknown table, no
  override given) -- see its own docstring for why that case does NOT fall back to "first column"
  the way the browser console does.

- Subscribe races the initial snapshot: deltas can arrive before, during or after the snapshot
  read lands. The caller (live.py) buffers everything until the snapshot is in hand, seeds state
  from it (dropping weight<=0 rows and resolving any supersession the snapshot itself straddled --
  a snapshot read mid-update can carry both the old and new row of a group), and then replays the
  buffered batches -- except ones the snapshot has ALREADY reflected. There is no shared sequence
  counter between the snapshot read (`/rows`' `seq`) and the delta stream (`TableDeltaBatch.seq`
  is a per-subscription batch counter on a completely different scale -- measured ~860 vs ~15,000
  at the same instant in useTableRows.ts) so "already reflected" cannot be seq-based. Instead:
  `already_reflected` is a CONTENT heuristic -- a buffered batch is skipped only when EVERY one of
  its retractions targets a row the snapshot does not contain (i.e. the snapshot already dropped
  it). Replaying it anyway would double-apply a retraction the snapshot already reflects, which
  for a plain reduce-by-weight is harmless, but for a LATEST BY group it can delete the WRONG
  (newer) row out from under the group index. wishlist #20 (a shared epoch on both the snapshot
  and the delta stream) is the fix that would make this exact instead of a heuristic.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

Row = dict[str, Any]
Delta = tuple[Row, int]  # (row, weight)

_MISSING = object()


def canonical_key(row: Row) -> str:
    """Stable identity for a row's exact content: sorted (key, value) pairs, JSON-serialized."""
    keys = sorted(row.keys())
    return json.dumps([[k, row[k]] for k in keys], separators=(",", ":"), default=str)


def group_key_of(row: Row, key_fields: list[str] | None) -> str | None:
    """The row's logical-identity ("group") key, or None when supersession does not apply.

    - `key_fields` is a real (possibly empty) list: `[]` is a global aggregate -- exactly one row,
      one constant group `"*"`. A non-empty list joins each field's value.
    - `key_fields is None`: the table's key is unknown and none was given. Deliberately NOT the
      browser console's fallback (live-table.ts's `groupKeyOf` uses the row's first column, which
      is right for a LATEST BY table that leads with its key and silently wrong for a composite
      key or a global aggregate). This client instead falls back to whole-row identity -- no
      column is ever guessed, and simply never superseding is the safe failure mode (design doc
      "Key fields" section: "never guess the first column"). Returning None here means
      apply()/seed() skip all group bookkeeping for the row, so canonical_key alone is its
      identity, which is exactly whole-row identity.
    """
    if key_fields is None:
        return None
    if len(key_fields) == 0:
        return "*"
    parts = []
    for f in key_fields:
        value = row[f] if f in row else _MISSING
        parts.append(f"{f}={json.dumps(value, default=str) if value is not _MISSING else 'undefined'}")
    return "|".join(parts)


@dataclass
class ZSet:
    """Live reduced state for one table: canonical_key -> (row, weight), plus a group_key ->
    canonical_key index for supersession. `key_fields=None` disables supersession entirely
    (whole-row identity); `key_fields=[]` is a global aggregate."""

    key_fields: list[str] | None
    _map: dict[str, Delta] = field(default_factory=dict)
    _group_index: dict[str, str] = field(default_factory=dict)

    def rows(self) -> list[Row]:
        """Current live rows. Collapses to one row per group when key_fields is known -- a
        defensive step mirroring live-table.ts's flushToState: apply()/seed() already maintain
        the one-canonical-key-per-group invariant, but a consumer must never see a group surface
        twice regardless."""
        if self.key_fields is None:
            return [row for row, _weight in self._map.values()]
        by_group: dict[str, Row] = {}
        for row, _weight in self._map.values():
            gk = group_key_of(row, self.key_fields) or canonical_key(row)
            by_group[gk] = row
        return list(by_group.values())

    def apply(self, deltas: list[Delta]) -> list[str]:
        """Apply one batch of (row, weight) deltas in place. Returns the canonical keys that were
        newly asserted (weight summed > 0 after this batch) -- for on_change/flash tracking."""
        touched: list[str] = []
        for row, weight in deltas:
            key = canonical_key(row)
            group_key = group_key_of(row, self.key_fields)
            _prev_row, prev_weight = self._map.get(key, (row, 0))
            next_weight = prev_weight + weight
            if next_weight <= 0:
                self._map.pop(key, None)
                if group_key is not None and self._group_index.get(group_key) == key:
                    del self._group_index[group_key]
            else:
                if group_key is not None:
                    stale_key = self._group_index.get(group_key)
                    if stale_key is not None and stale_key != key:
                        self._map.pop(stale_key, None)
                    self._group_index[group_key] = key
                self._map[key] = (row, next_weight)
                touched.append(key)
        return touched

    def seed(self, snapshot_rows: list[Delta]) -> None:
        """Reset state and seed from a snapshot read (GET /rows). Mirrors apply()'s rules: a
        weight<=0 row is not part of the snapshot at all, and a group keeps only its newest row --
        a snapshot read mid-update can carry both sides of a supersession."""
        self._map = {}
        self._group_index = {}
        for row, weight in snapshot_rows:
            if weight <= 0:
                continue
            key = canonical_key(row)
            group_key = group_key_of(row, self.key_fields)
            if group_key is not None:
                stale_key = self._group_index.get(group_key)
                if stale_key is not None and stale_key != key:
                    self._map.pop(stale_key, None)
                self._group_index[group_key] = key
            self._map[key] = (row, weight)

    def already_reflected(self, deltas: list[Delta]) -> bool:
        """True when this buffered batch's effect is already visible in the (just-seeded)
        current state -- see the module docstring's "Subscribe races the initial snapshot"
        paragraph. A batch with no retractions is never considered reflected (an assert-only
        batch is always safe, and possibly necessary, to replay)."""
        retractions = [(row, w) for row, w in deltas if w < 0]
        if not retractions:
            return False
        for row, _weight in retractions:
            key = canonical_key(row)
            _row, weight = self._map.get(key, (row, 0))
            if weight > 0:
                return False
        return True
