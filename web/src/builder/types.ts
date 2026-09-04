import type { FieldDef } from '../api/types'

export type DurationUnit = 'MILLISECONDS' | 'SECONDS' | 'MINUTES' | 'HOURS'
export type JoinType = 'INNER' | 'LEFT' | 'RIGHT' | 'FULL' | 'CROSS'
export type WindowKind = 'NONE' | 'TUMBLING' | 'HOPPING' | 'SESSION'
export type AggFn = 'NONE' | 'COUNT' | 'SUM' | 'AVG' | 'MIN' | 'MAX'
export type EmitMode = 'DEFAULT' | 'CHANGES' | 'FINAL'
export type ConjunctionOp = 'AND' | 'OR'
export type CompareOp = '=' | '!=' | '<' | '<=' | '>' | '>='

export const DURATION_UNITS: DurationUnit[] = ['MILLISECONDS', 'SECONDS', 'MINUTES', 'HOURS']
export const JOIN_TYPES: JoinType[] = ['INNER', 'LEFT', 'RIGHT', 'FULL', 'CROSS']
export const AGG_FNS: AggFn[] = ['NONE', 'COUNT', 'SUM', 'AVG', 'MIN', 'MAX']
export const COMPARE_OPS: CompareOp[] = ['=', '!=', '<', '<=', '>', '>=']

/** What the builder's FROM/JOIN pickers offer. The builder used to take `SourceDefinition[]` and so
 * could only ever express "read a source"; a table may now also read a PIPELINE (by name, via
 * `TableDefinition.pipelineInputs`) or another table, and all three are just "a named thing with a
 * column list" as far as clause construction is concerned. The CALLER decides what is legal to offer —
 * the pipeline page passes sources only, the table page passes all three — so this type carries no
 * eligibility rules of its own, only the `kind` needed to label the picker's groups. */
export type RelationKind = 'source' | 'pipeline' | 'table'

export interface BuilderRelation {
  name: string
  kind: RelationKind
  fields: FieldDef[]
}

/** Stable render order for the picker's groups; also the order `groupRelationsByKind` emits. */
export const RELATION_KINDS: RelationKind[] = ['source', 'pipeline', 'table']
export const RELATION_KIND_LABELS: Record<RelationKind, string> = {
  source: 'Sources',
  pipeline: 'Pipelines',
  table: 'Tables',
}

export interface RelationGroup {
  kind: RelationKind
  label: string
  relations: BuilderRelation[]
}

/** Buckets relations into the labelled groups the FROM/JOIN `<select>`s render, in RELATION_KINDS
 * order, preserving each bucket's incoming order and DROPPING empty buckets — so the pipeline page
 * (sources only) shows a single unadorned "Sources" group rather than two empty headings. Pure. */
export function groupRelationsByKind(relations: BuilderRelation[]): RelationGroup[] {
  const groups: RelationGroup[] = []
  for (const kind of RELATION_KINDS) {
    const bucket = relations.filter((r) => r.kind === kind)
    if (bucket.length > 0) groups.push({ kind, label: RELATION_KIND_LABELS[kind], relations: bucket })
  }
  return groups
}

export interface FromClause {
  source: string
  alias: string
}

export interface JoinClause {
  type: JoinType
  source: string
  alias: string
  withinValue: number
  withinUnit: DurationUnit
  onLeft: string
  onRight: string
}

export interface WhereCondition {
  left: string
  op: CompareOp
  right: string
  conjunction: ConjunctionOp
}

export interface WindowSpec {
  kind: WindowKind
  size: number
  sizeUnit: DurationUnit
  advance?: number
  advanceUnit?: DurationUnit
  gap?: number
  gapUnit?: DurationUnit
}

export interface SelectItem {
  expr: string
  agg?: AggFn
  alias?: string
}

export interface BuilderState {
  from: FromClause
  joins: JoinClause[]
  where: WhereCondition[]
  groupBy: string[]
  window: WindowSpec
  select: SelectItem[]
  emit: EmitMode
}

export function emptyBuilderState(): BuilderState {
  return {
    from: { source: '', alias: '' },
    joins: [],
    where: [],
    groupBy: [],
    window: { kind: 'NONE', size: 5, sizeUnit: 'SECONDS' },
    select: [{ expr: '*', agg: 'NONE' }],
    emit: 'DEFAULT',
  }
}

export function newJoin(): JoinClause {
  return { type: 'INNER', source: '', alias: '', withinValue: 5, withinUnit: 'SECONDS', onLeft: '', onRight: '' }
}

export function newWhereCondition(): WhereCondition {
  return { left: '', op: '=', right: '', conjunction: 'AND' }
}

export function newSelectItem(): SelectItem {
  return { expr: '', agg: 'NONE', alias: '' }
}

/** Every column the current FROM/JOIN choices make addressable, in clause order: the FROM relation
 * first, then each join in order. Names are qualified with the clause's alias (falling back to the
 * relation's own name) as soon as there is at least one join, because an unqualified column is
 * ambiguous the moment two relations are in scope — with no joins they stay bare, which is what the
 * single-relation SQL the builder emits actually wants. A clause naming a relation that isn't in
 * `relations` (a stale pick, or a catalog fetch that failed) contributes nothing rather than
 * inventing column names. Kind-agnostic on purpose: a pipeline's columns are addressed exactly like
 * a source's. Pure — unit-tested in web/test/builder-relations.test.ts. */
export function columnOptionsFor(state: BuilderState, relations: BuilderRelation[]): string[] {
  const byName = new Map(relations.map((r) => [r.name, r]))
  const qualify = state.joins.length > 0
  const options: string[] = []

  const fromRelation = byName.get(state.from.source)
  if (fromRelation) {
    const alias = state.from.alias.trim() || fromRelation.name
    for (const f of fromRelation.fields) {
      options.push(qualify ? `${alias}.${f.name}` : f.name)
    }
  }
  for (const join of state.joins) {
    const rel = byName.get(join.source)
    if (!rel) continue
    const alias = join.alias.trim() || rel.name
    for (const f of rel.fields) {
      options.push(`${alias}.${f.name}`)
    }
  }
  return options
}
