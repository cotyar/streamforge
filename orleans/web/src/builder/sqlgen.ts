// Pure builder-state -> SQL text generator. No side effects, no dependencies —
// keep it easily testable even though this project has no test runner wired up.
import type { BuilderState, JoinClause, SelectItem, WhereCondition } from './types'

function renderSelectItem(item: SelectItem): string {
  const expr = item.expr.trim() || '*'
  const base = item.agg && item.agg !== 'NONE' ? `${item.agg}(${expr})` : expr
  return item.alias?.trim() ? `${base} AS ${item.alias.trim()}` : base
}

function renderJoin(join: JoinClause): string {
  const alias = join.alias.trim() ? ` ${join.alias.trim()}` : ''
  const source = join.source.trim() || '<source>'
  let clause = `${join.type} JOIN ${source}${alias}`
  if (join.type !== 'CROSS') {
    clause += ` WITHIN ${join.withinValue} ${join.withinUnit}`
    if (join.onLeft.trim() && join.onRight.trim()) {
      clause += ` ON ${join.onLeft.trim()} = ${join.onRight.trim()}`
    }
  }
  return clause
}

function renderWhere(conditions: WhereCondition[]): string {
  return conditions
    .filter((c) => c.left.trim() && c.right.trim())
    .map((c, i) => {
      const cond = `${c.left.trim()} ${c.op} ${c.right.trim()}`
      return i === 0 ? cond : `${c.conjunction} ${cond}`
    })
    .join(' ')
}

function renderWindow(state: BuilderState): string | null {
  const w = state.window
  switch (w.kind) {
    case 'NONE':
      return null
    case 'TUMBLING':
      return `WINDOW TUMBLING(SIZE ${w.size} ${w.sizeUnit})`
    case 'HOPPING':
      return `WINDOW HOPPING(SIZE ${w.size} ${w.sizeUnit}, ADVANCE BY ${w.advance ?? w.size} ${w.advanceUnit ?? w.sizeUnit})`
    case 'SESSION':
      return `WINDOW SESSION(GAP ${w.gap ?? w.size} ${w.gapUnit ?? w.sizeUnit})`
    default:
      return null
  }
}

/** Renders a BuilderState into well-formatted SQL: uppercase keywords, one clause per line. */
export function builderStateToSql(state: BuilderState): string {
  const lines: string[] = []

  const selectItems = state.select.length > 0 ? state.select : [{ expr: '*' } satisfies SelectItem]
  lines.push(`SELECT ${selectItems.map(renderSelectItem).join(', ')}`)

  const fromSource = state.from.source.trim() || '<source>'
  const fromAlias = state.from.alias.trim() ? ` ${state.from.alias.trim()}` : ''
  lines.push(`FROM ${fromSource}${fromAlias}`)

  for (const join of state.joins) {
    lines.push(renderJoin(join))
  }

  const whereText = renderWhere(state.where)
  if (whereText) {
    lines.push(`WHERE ${whereText}`)
  }

  const groupBy = state.groupBy.filter((c) => c.trim())
  if (groupBy.length > 0) {
    lines.push(`GROUP BY ${groupBy.join(', ')}`)
  }

  const windowLine = renderWindow(state)
  if (windowLine) lines.push(windowLine)

  if (state.emit !== 'DEFAULT') {
    lines.push(`EMIT ${state.emit}`)
  }

  return lines.join('\n')
}
