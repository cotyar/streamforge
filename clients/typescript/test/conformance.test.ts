/**
 * Runs the cross-language Z-set reducer conformance suite (../../conformance/zset-cases.json)
 * against zset.ts, implementing the runner contract from ../../conformance/README.md exactly:
 *
 *   z = ZSet(case.keyFields)
 *   z.seed(case.snapshot)
 *   for b in case.bufferedBatches: if not z.alreadyReflected(b.deltas): z.apply(b.deltas)
 *   for b in case.liveBatches: z.apply(b.deltas)
 *   assert rows(z) == case.expectedRows, order-insensitive
 *
 * All 14 cases must pass -- this is the one piece of logic every StreamsForge client (this one,
 * Python, the console's own useTableRows.ts, the Excel add-in) must agree on bit-for-bit.
 */

import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { canonicalKey, ZSet, type Delta, type Row } from "../src/zset.js";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const CASES_PATH = path.join(HERE, "..", "..", "conformance", "zset-cases.json");

interface DeltaDto {
  row: Row;
  weight: number;
}
interface BatchDto {
  deltas: DeltaDto[];
  seq: number;
}
interface CaseDto {
  name: string;
  description: string;
  keyFields: string[] | null;
  bufferedBatches: BatchDto[];
  snapshot: DeltaDto[];
  liveBatches: BatchDto[];
  expectedRows: Row[];
}

function toDeltas(dtos: DeltaDto[]): Delta[] {
  return dtos.map((d) => [d.row, d.weight] as const);
}

/** Order-insensitive row-set comparison via the same canonical-key serialization the reducer
 * itself uses for identity -- two rows are "the same" iff their sorted-key JSON matches. */
function assertRowsEqual(actual: Row[], expected: Row[]): void {
  const actualKeys = actual.map(canonicalKey).sort();
  const expectedKeys = expected.map(canonicalKey).sort();
  expect(actualKeys).toEqual(expectedKeys);
}

const raw = readFileSync(CASES_PATH, "utf-8");
const suite = JSON.parse(raw) as { version: number; cases: CaseDto[] };

describe(`zset conformance (${suite.cases.length} cases)`, () => {
  for (const c of suite.cases) {
    test(`${c.name} -- ${c.description}`, () => {
      const z = new ZSet(c.keyFields);
      z.seed(toDeltas(c.snapshot));
      for (const b of c.bufferedBatches) {
        const deltas = toDeltas(b.deltas);
        if (!z.alreadyReflected(deltas)) z.apply(deltas);
      }
      for (const b of c.liveBatches) {
        z.apply(toDeltas(b.deltas));
      }
      assertRowsEqual(z.rows(), c.expectedRows);
    });
  }
});
