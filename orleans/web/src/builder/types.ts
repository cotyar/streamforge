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
