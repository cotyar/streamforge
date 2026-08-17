# Z-set reducer conformance suite

`zset-cases.json` is the shared, language-neutral test set for the one piece of logic every
StreamForge client must get right: reducing a Z-set delta stream into live rows, across the race
between `SubscribeTable` and the initial `GET /rows` snapshot.

It exists because that logic is currently reimplemented per language — `clients/python`,
`clients/dotnet`, `clients/typescript`, `clients/kotlin`, plus two in-app copies
(`web/src/hooks/useTableRows.ts` and the otc-terms Excel add-in). Independent hand-written ports of
the same semantics drift, and drift here is silent: a wrong row count in a risk table, not a crash.
One fixture, read by every language's own test runner, turns "these agree" into something that
fails on the same named case everywhere.

## The runner contract

Implement exactly this, then assert the resulting rows equal `expectedRows` **ignoring order**:

```
z = ZSet(case.keyFields)          # null = whole-row identity; [] = one global group
z.seed(case.snapshot)             # GET /rows; weight <= 0 rows are not part of the state
for b in case.bufferedBatches:    # arrived while the snapshot was still in flight
    if not z.alreadyReflected(b.deltas): z.apply(b.deltas)
for b in case.liveBatches:
    z.apply(b.deltas)
assert rows(z) ≡ case.expectedRows
```

Each batch carries `seq`, and it is deliberately **not** usable for resolving the snapshot race:
the snapshot's `seq` and the stream's `seq` are different counters on different scales (measured
~860 vs ~15,000 at the same instant). A client that reaches for it fails here rather than in
production. Wishlist #20 is the fix that would make replay exact instead of a content heuristic.

## Regenerating

```bash
clients/python/.venv/bin/python clients/conformance/generate.py
```

`expectedRows` is computed by the Python reducer — the one covered by contract tests against a
real engine — not typed by hand. Add or change a case in `generate.py` and re-run; hand-editing
`zset-cases.json` defeats the point.
