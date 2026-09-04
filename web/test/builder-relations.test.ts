// Pins the two PURE helpers the visual SQL builder was generalized onto when a table gained the
// ability to read a PIPELINE as one of its relations: the builder no longer takes
// `SourceDefinition[]` but `BuilderRelation[]` (name + kind + fields), so both "what does the
// FROM/JOIN picker show" and "which columns are addressable" had to stop being source-specific.
//
// WHY ONLY THE PURE FUNCTIONS (same reasoning as environment.test.ts): bun's runner has no DOM, so
// PipelineBuilder.tsx itself — a React component — is exercised only by `bun run build`'s typecheck.
// Extracting these two out of the component is what makes the interesting logic testable at all.
import { describe, expect, test } from 'bun:test'
import type { BuilderRelation, BuilderState } from '../src/builder/types'
import { columnOptionsFor, emptyBuilderState, groupRelationsByKind, newJoin } from '../src/builder/types'

const trades: BuilderRelation = {
  name: 'trades',
  kind: 'source',
  fields: [
    { name: 'symbol', type: 'String' },
    { name: 'price', type: 'Double' },
  ],
}
const vwap: BuilderRelation = {
  name: 'vwap',
  kind: 'pipeline',
  fields: [
    { name: 'symbol', type: 'String' },
    { name: 'vwap', type: 'Double' },
  ],
}
const positions: BuilderRelation = { name: 'positions', kind: 'table', fields: [{ name: 'qty', type: 'Long' }] }

describe('groupRelationsByKind', () => {
  test('emits Sources → Pipelines → Tables regardless of input order', () => {
    const groups = groupRelationsByKind([positions, vwap, trades])
    expect(groups.map((g) => g.kind)).toEqual(['source', 'pipeline', 'table'])
    expect(groups.map((g) => g.label)).toEqual(['Sources', 'Pipelines', 'Tables'])
  })

  test('drops empty buckets — the pipeline page passes sources only and must show one group', () => {
    const groups = groupRelationsByKind([trades])
    expect(groups).toHaveLength(1)
    expect(groups[0]!.kind).toBe('source')
    expect(groups[0]!.relations).toEqual([trades])
  })

  test('no relations at all is an empty list, not a group of empty groups', () => {
    expect(groupRelationsByKind([])).toEqual([])
  })

  test('preserves the incoming order within a bucket', () => {
    const b: BuilderRelation = { name: 'b', kind: 'source', fields: [] }
    const a: BuilderRelation = { name: 'a', kind: 'source', fields: [] }
    expect(groupRelationsByKind([b, a])[0]!.relations.map((r) => r.name)).toEqual(['b', 'a'])
  })
})

function stateWith(patch: Partial<BuilderState>): BuilderState {
  return { ...emptyBuilderState(), ...patch }
}

describe('columnOptionsFor', () => {
  test('a single FROM relation yields BARE column names — nothing to be ambiguous with', () => {
    const state = stateWith({ from: { source: 'trades', alias: '' } })
    expect(columnOptionsFor(state, [trades])).toEqual(['symbol', 'price'])
  })

  test('kind is irrelevant: a pipeline relation addresses exactly like a source', () => {
    const state = stateWith({ from: { source: 'vwap', alias: '' } })
    expect(columnOptionsFor(state, [trades, vwap])).toEqual(['symbol', 'vwap'])
  })

  test('one join qualifies BOTH sides — including the FROM clause, which was bare before', () => {
    const state = stateWith({
      from: { source: 'trades', alias: 't' },
      joins: [{ ...newJoin(), source: 'vwap', alias: 'v' }],
    })
    expect(columnOptionsFor(state, [trades, vwap])).toEqual(['t.symbol', 't.price', 'v.symbol', 'v.vwap'])
  })

  test('an empty alias falls back to the relation name as the qualifier', () => {
    const state = stateWith({
      from: { source: 'trades', alias: '   ' },
      joins: [{ ...newJoin(), source: 'positions', alias: '' }],
    })
    expect(columnOptionsFor(state, [trades, positions])).toEqual(['trades.symbol', 'trades.price', 'positions.qty'])
  })

  test('a clause naming an unknown relation contributes nothing rather than inventing columns', () => {
    const state = stateWith({
      from: { source: 'gone', alias: '' },
      joins: [{ ...newJoin(), source: 'vwap', alias: 'v' }],
    })
    expect(columnOptionsFor(state, [trades, vwap])).toEqual(['v.symbol', 'v.vwap'])
  })

  test('nothing selected yet is an empty list (the builder renders its "select a relation" hint)', () => {
    expect(columnOptionsFor(emptyBuilderState(), [trades, vwap, positions])).toEqual([])
  })
})
