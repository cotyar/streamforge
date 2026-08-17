"""Runs the cross-language conformance suite (clients/conformance/zset-cases.json).

Python is the fixture's oracle, so passing here is not independent evidence -- it is the executable
statement of the runner contract that .NET, TypeScript and Kotlin copy, and it fails loudly if the
checked-in JSON ever drifts from the reducer that generated it."""

import json
import pathlib

import pytest

from streamforge._zset import ZSet

CASES_PATH = pathlib.Path(__file__).resolve().parents[2] / "conformance" / "zset-cases.json"
CASES = json.loads(CASES_PATH.read_text())["cases"]


def _deltas(batch):
    return [(d["row"], d["weight"]) for d in batch["deltas"]]


@pytest.mark.parametrize("case", CASES, ids=[c["name"] for c in CASES])
def test_conformance_case(case):
    z = ZSet(case["keyFields"])
    z.seed([(d["row"], d["weight"]) for d in case["snapshot"]])
    for b in case["bufferedBatches"]:
        deltas = _deltas(b)
        if not z.already_reflected(deltas):
            z.apply(deltas)
    for b in case["liveBatches"]:
        z.apply(_deltas(b))
    # Order-insensitive: the reducer's map order is an implementation detail in every language.
    actual = sorted(json.dumps(r, sort_keys=True) for r in z.rows())
    expected = sorted(json.dumps(r, sort_keys=True) for r in case["expectedRows"])
    assert actual == expected
