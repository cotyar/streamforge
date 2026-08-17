"""Pure unit tests for the Z-set reducer -- no engine, no network. Fixtures are handcrafted to
exercise every hazard named in _zset.py's module docstring."""

from streamforge._zset import ZSet, canonical_key, group_key_of


def test_retract_assert_pair():
    z = ZSet(key_fields=["id"])
    z.apply([({"id": 1, "v": "a"}, 1)])
    assert z.rows() == [{"id": 1, "v": "a"}]
    z.apply([({"id": 1, "v": "a"}, -1)])
    assert z.rows() == []


def test_assert_before_retract_order_independent():
    z = ZSet(key_fields=["id"])
    # A batch that both retracts the old value and asserts the new one, summed within one call --
    # weight is summed per canonical identity regardless of arrival order.
    z.apply([({"id": 1, "v": "a"}, 1)])
    z.apply([({"id": 1, "v": "a"}, -1), ({"id": 1, "v": "b"}, 1)])
    assert z.rows() == [{"id": 1, "v": "b"}]


def test_coalesced_multi_delta_batch():
    z = ZSet(key_fields=["id"])
    # One batch carrying several updates to the SAME group -- only the last survives via
    # supersession, matching what a 100ms-epoch-flushed hub batch actually contains.
    z.apply(
        [
            ({"id": 1, "v": 1}, 1),
            ({"id": 1, "v": 1}, -1),
            ({"id": 1, "v": 2}, 1),
        ]
    )
    assert z.rows() == [{"id": 1, "v": 2}]


def test_composite_key_supersession():
    z = ZSet(key_fields=["desk", "strategy"])
    z.apply([({"desk": "Rates", "strategy": "A", "exposure": 100}, 1)])
    z.apply([({"desk": "Rates", "strategy": "B", "exposure": 200}, 1)])
    # Two distinct groups (composite key differs on `strategy`) must coexist, not collapse --
    # this is exactly what a first-column-only fallback would get wrong.
    assert sorted(z.rows(), key=lambda r: r["strategy"]) == [
        {"desk": "Rates", "strategy": "A", "exposure": 100},
        {"desk": "Rates", "strategy": "B", "exposure": 200},
    ]
    # A new row for group ("Rates","A") supersedes the old one, even with no explicit retraction.
    z.apply([({"desk": "Rates", "strategy": "A", "exposure": 150}, 1)])
    rows = {r["strategy"]: r["exposure"] for r in z.rows()}
    assert rows == {"A": 150, "B": 200}


def test_weight_le_zero_removes_row():
    z = ZSet(key_fields=["id"])
    z.apply([({"id": 1}, 3)])
    assert z.rows() == [{"id": 1}]
    z.apply([({"id": 1}, -3)])  # summed weight hits exactly 0 -> removed, not "0-weight present"
    assert z.rows() == []

    z.apply([({"id": 2}, 1)])
    z.apply([({"id": 2}, -5)])  # goes negative -> still just removed, never a negative-weight row
    assert z.rows() == []


def test_global_aggregate_key_fields_empty():
    z = ZSet(key_fields=[])
    assert group_key_of({"total": 1}, []) == "*"
    z.apply([({"total_usd": 100}, 1)])
    assert z.rows() == [{"total_usd": 100}]
    # A fresh row supersedes the ONE row of the aggregate, even though the content is unrelated --
    # there is exactly one group, "*", regardless of what the row looks like.
    z.apply([({"total_usd": 250}, 1)])
    assert z.rows() == [{"total_usd": 250}]


def test_unknown_key_fields_falls_back_to_whole_row_identity_not_first_column():
    # key_fields=None (unknown table, no override given) must NOT behave like live-table.ts's
    # browser fallback (first column as the group key) -- two rows sharing a first-column value
    # but differing elsewhere must NOT collapse into one.
    z = ZSet(key_fields=None)
    assert group_key_of({"a": 1, "b": "x"}, None) is None
    z.apply([({"a": 1, "b": "x"}, 1), ({"a": 1, "b": "y"}, 1)])
    assert sorted(z.rows(), key=lambda r: r["b"]) == [{"a": 1, "b": "x"}, {"a": 1, "b": "y"}]


def test_buffered_batch_replay_filter_already_reflected():
    z = ZSet(key_fields=["id"])
    # Seed from a snapshot that no longer contains id=1 (it was already retracted server-side by
    # the time the snapshot was taken).
    z.seed([({"id": 2}, 1)])
    # A buffered batch whose only content is a retraction of the row the snapshot never had --
    # replaying it would be a no-op at best, but for a LATEST BY group it could delete state the
    # snapshot legitimately re-asserted. It must be recognized as already reflected.
    buffered_retract_only = [({"id": 1}, -1)]
    assert z.already_reflected(buffered_retract_only) is True

    # Ported literally from live-table.ts's isBatchAlreadyReflected: it inspects ONLY the
    # retractions in a batch, not any asserts alongside them. A batch mixing an already-reflected
    # retraction with a genuinely new assert is therefore ALSO treated as reflected and skipped
    # whole -- a known, documented limitation of the content-based heuristic (design doc's
    # wishlist #20 is the fix: a shared epoch would make this exact instead of approximate).
    mixed = [({"id": 1}, -1), ({"id": 3}, 1)]
    assert z.already_reflected(mixed) is True

    # A batch retracting a row the snapshot DOES still contain is not reflected -- replaying it is
    # required, or that row would incorrectly linger forever.
    still_present = [({"id": 2}, -1)]
    assert z.already_reflected(still_present) is False

    # An assert-only batch is never "reflected" by definition -- there's nothing to skip.
    assert z.already_reflected([({"id": 4}, 1)]) is False


def test_seed_drops_weight_le_zero_and_resolves_supersession_mid_snapshot():
    z = ZSet(key_fields=["id"])
    # A snapshot read mid-update can carry both the old (weight<=0, i.e. already gone) and the new
    # row of the same group; and any row at weight<=0 at all is not part of the live state.
    z.seed(
        [
            ({"id": 1, "v": "old"}, 0),
            ({"id": 1, "v": "new"}, 1),
            ({"id": 2, "v": "gone"}, -1),
        ]
    )
    assert z.rows() == [{"id": 1, "v": "new"}]


def test_canonical_key_stable_regardless_of_field_order():
    assert canonical_key({"a": 1, "b": 2}) == canonical_key({"b": 2, "a": 1})
    assert canonical_key({"a": 1, "b": 2}) != canonical_key({"a": 1, "b": 3})
