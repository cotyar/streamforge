#!/usr/bin/env python3
"""Regenerate zset-cases.json — the cross-language conformance suite for the Z-set reducer.

Every StreamsForge client (Python, .NET, TypeScript, Kotlin, and the two in-app copies in
web/src/hooks/useTableRows.ts and the otc-terms add-in) reduces the same delta stream, and they
have historically been independent hand-written ports of each other. This file is the single
place where "they agree" stops being a claim and becomes a test: each case is a snapshot, the
batches around it, and the exact rows that must be live afterwards.

`expectedRows` is COMPUTED here by the Python reducer rather than typed by hand — that reducer is
the one covered by contract tests against a real engine, so it is the closest thing to an oracle
we have. Editing a case means re-running this script; editing expectedRows by hand defeats it.

Runner contract (implement exactly this in every language):

    z = ZSet(keyFields)                 # null = whole-row identity, [] = one global group
    for b in bufferedBatches: buffer.append(b)      # arrived before the snapshot landed
    z.seed(snapshot)                                # GET /rows, weight<=0 rows dropped
    for b in buffer:
        if not z.alreadyReflected(b): z.apply(b)    # content heuristic, see _zset.py
    for b in liveBatches: z.apply(b)
    assert set(z.rows()) == set(expectedRows)       # ORDER-INSENSITIVE

`seq` is carried on each batch but is deliberately NOT used to resolve the snapshot race: the
snapshot's seq and the stream's seq are different counters on different scales. It is present so a
client that mishandles it fails here rather than in production (wishlist #20).
"""

import json, os, sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "python", "src"))
from streamsforge._zset import ZSet  # noqa: E402


def d(row, weight):
    return {"row": row, "weight": weight}


def batch(deltas, seq):
    return {"deltas": deltas, "seq": seq}


R1 = {"desk": "Rates", "exposure_usd": 100.0}
R2 = {"desk": "Rates", "exposure_usd": 250.0}
C1 = {"desk": "Credit", "exposure_usd": 40.0}

CASES = [
    {
        "name": "assert-only",
        "description": "Two independent asserts, no snapshot, no supersession.",
        "keyFields": ["desk"],
        "bufferedBatches": [], "snapshot": [],
        "liveBatches": [batch([d(R1, 1), d(C1, 1)], 1)],
    },
    {
        "name": "retract-then-assert",
        "description": "A LATEST BY update as the engine emits it: retract the old row, assert the new.",
        "keyFields": ["desk"],
        "bufferedBatches": [], "snapshot": [d(R1, 1)],
        "liveBatches": [batch([d(R1, -1), d(R2, 1)], 1)],
    },
    {
        "name": "assert-then-retract-out-of-order",
        "description": "Same update with the two sides swapped. Arrival order is not guaranteed; "
                       "the summed-weight rule must converge to the same state either way.",
        "keyFields": ["desk"],
        "bufferedBatches": [], "snapshot": [d(R1, 1)],
        "liveBatches": [batch([d(R2, 1)], 1), batch([d(R1, -1)], 2)],
    },
    {
        "name": "coalesced-batch",
        "description": "Both sides of a supersession plus an unrelated desk in ONE batch.",
        "keyFields": ["desk"],
        "bufferedBatches": [], "snapshot": [],
        "liveBatches": [batch([d(R1, 1), d(R1, -1), d(R2, 1), d(C1, 1)], 1)],
    },
    {
        "name": "composite-key-supersession",
        "description": "A two-column logical key: only the row matching BOTH fields is superseded.",
        "keyFields": ["scenario_id", "desk"],
        "bufferedBatches": [], "snapshot": [],
        "liveBatches": [batch([
            d({"scenario_id": "base", "desk": "Rates", "v": 1.0}, 1),
            d({"scenario_id": "shock", "desk": "Rates", "v": 2.0}, 1),
            d({"scenario_id": "base", "desk": "Rates", "v": 9.0}, 1),
        ], 1)],
    },
    {
        "name": "weight-sums-to-zero-removes",
        "description": "Weights sum per identity; a sum of zero removes the row outright rather "
                       "than leaving a zero-weight ghost.",
        "keyFields": None,
        "bufferedBatches": [], "snapshot": [],
        "liveBatches": [batch([d(R1, 1)], 1), batch([d(R1, -1)], 2)],
    },
    {
        "name": "weight-goes-negative-removes",
        "description": "An over-retraction removes the row and must not leave a negative weight.",
        "keyFields": None,
        "bufferedBatches": [], "snapshot": [],
        "liveBatches": [batch([d(R1, 1)], 1), batch([d(R1, -3)], 2)],
    },
    {
        "name": "global-aggregate",
        "description": "keyFields=[] is ONE group: every new row supersedes the previous one.",
        "keyFields": [],
        "bufferedBatches": [], "snapshot": [],
        "liveBatches": [batch([d({"total": 10.0}, 1)], 1), batch([d({"total": 11.0}, 1)], 2)],
    },
    {
        "name": "whole-row-identity-no-supersession",
        "description": "keyFields=null disables supersession: two rows differing in any column "
                       "coexist. This is the safe fallback for an unknown table -- never guess a key.",
        "keyFields": None,
        "bufferedBatches": [], "snapshot": [],
        "liveBatches": [batch([d(R1, 1), d(R2, 1)], 1)],
    },
    {
        "name": "snapshot-drops-nonpositive",
        "description": "A snapshot read can carry weight<=0 rows; they are not part of the state.",
        "keyFields": ["desk"],
        "bufferedBatches": [], "snapshot": [d(R1, 1), d(C1, 0), d(R2, -1)],
        "liveBatches": [],
    },
    {
        "name": "snapshot-straddles-supersession",
        "description": "A snapshot read mid-update can contain BOTH rows of one group. Seeding must "
                       "keep only the last one rather than surfacing the group twice.",
        "keyFields": ["desk"],
        "bufferedBatches": [], "snapshot": [d(R1, 1), d(R2, 1)],
        "liveBatches": [],
    },
    {
        "name": "buffered-batch-already-reflected-is-skipped",
        "description": "A batch buffered before the snapshot whose retraction targets a row the "
                       "snapshot does NOT contain: the snapshot already reflects it. Replaying it "
                       "would re-delete, and for a LATEST BY group that deletes the NEWER row.",
        "keyFields": ["desk"],
        "bufferedBatches": [batch([d(R1, -1), d(R2, 1)], 7)],
        "snapshot": [d(R2, 1)],
        "liveBatches": [],
    },
    {
        "name": "buffered-batch-not-reflected-is-replayed",
        "description": "Same shape, but the retraction's target IS still in the snapshot -- the "
                       "snapshot predates the batch, so it must be replayed.",
        "keyFields": ["desk"],
        "bufferedBatches": [batch([d(R1, -1), d(R2, 1)], 7)],
        "snapshot": [d(R1, 1)],
        "liveBatches": [],
    },
    {
        "name": "buffered-assert-only-always-replays",
        "description": "A batch with no retractions is never treated as already reflected: "
                       "replaying an assert is safe, dropping one is not.",
        "keyFields": ["desk"],
        "bufferedBatches": [batch([d(C1, 1)], 3)],
        "snapshot": [d(R1, 1)],
        "liveBatches": [],
    },
]


def run(case):
    z = ZSet(case["keyFields"])
    z.seed([(x["row"], x["weight"]) for x in case["snapshot"]])
    for b in case["bufferedBatches"]:
        deltas = [(x["row"], x["weight"]) for x in b["deltas"]]
        if not z.already_reflected(deltas):
            z.apply(deltas)
    for b in case["liveBatches"]:
        z.apply([(x["row"], x["weight"]) for x in b["deltas"]])
    return z.rows()


out = {"version": 1, "cases": []}
for c in CASES:
    out["cases"].append({**c, "expectedRows": run(c)})

path = os.path.join(os.path.dirname(__file__), "zset-cases.json")
with open(path, "w") as f:
    json.dump(out, f, indent=2)
    f.write("\n")
print(f"wrote {len(CASES)} cases -> {path}")
for c in out["cases"]:
    print(f"  {c['name']:45s} -> {len(c['expectedRows'])} row(s)")
