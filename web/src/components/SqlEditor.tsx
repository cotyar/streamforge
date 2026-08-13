import { useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react'
import type { ChangeEvent, KeyboardEvent, ReactNode, SyntheticEvent, UIEvent } from 'react'
import { WandSparkles } from 'lucide-react'
import type { FieldDef, FieldType, SourceDefinition, SqlDiagnostic } from '../api/types'
import { cn } from '@/lib/utils'
import { Button } from '@/components/ui/button'
import { findCaretScope, innerProjection, parseCtes } from './sqlScope'
import type { CteDef, ScopeFromItem } from './sqlScope'

const KEYWORDS = new Set([
  'SELECT', 'FROM', 'WHERE', 'GROUP', 'BY', 'WINDOW', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 'FULL', 'OUTER',
  'CROSS', 'ON', 'WITHIN', 'AS', 'EMIT', 'CHANGES', 'FINAL', 'TUMBLING', 'HOPPING', 'SESSION', 'SIZE',
  'ADVANCE', 'GAP', 'AND', 'OR', 'NOT', 'TRUE', 'FALSE', 'NULL', 'SECONDS', 'MILLISECONDS', 'MINUTES', 'HOURS',
  'WITH',
  // Plan 008 W3: set operations + GROUP BY ALL.
  'UNION', 'ALL',
  // Plan 009 Round C wave C1: CAST(expr AS type) sugar.
  'CAST',
])

const AGGREGATE_FNS = new Set(['COUNT', 'SUM', 'AVG', 'MIN', 'MAX'])
const SCALAR_FNS = new Set([
  'ABS', 'ROUND', 'UPPER', 'LOWER', 'COALESCE',
  // Plan 009 Round C wave C1: type-conversion functions.
  'TO_LONG', 'TO_DOUBLE', 'TO_BOOL', 'TO_TIMESTAMP', 'TO_STRING',
])
const FUNCTIONS = new Set([...AGGREGATE_FNS, ...SCALAR_FNS])

// Keywords that put the caret in an "expression position" when they are the last significant
// token before the word being typed (SELECT list, WHERE/ON predicates, GROUP BY, AND/OR chains).
const EXPR_TRIGGER_KEYWORDS = new Set(['SELECT', 'WHERE', 'ON', 'AND', 'OR', 'BY'])
const COMPARISON_PUNCT = new Set(['=', '!=', '<>', '<', '<=', '>', '>='])

type TokenKind = 'comment' | 'string' | 'number' | 'keyword' | 'function' | 'identifier' | 'whitespace' | 'punct'

interface Token {
  text: string
  start: number
  end: number
  kind: TokenKind
}

const TOKEN_REGEX = /(--[^\n]*)|('(?:[^'\\]|\\.)*')|(\b\d+(?:\.\d+)?\b)|([A-Za-z_][A-Za-z0-9_]*)|(\s+)|([^\sA-Za-z0-9_]+)/g

function tokenize(sql: string): Token[] {
  const tokens: Token[] = []
  const re = new RegExp(TOKEN_REGEX)
  let m: RegExpExecArray | null
  while ((m = re.exec(sql))) {
    const start = m.index
    const text = m[0]
    const end = start + text.length
    let kind: TokenKind
    if (m[1] !== undefined) kind = 'comment'
    else if (m[2] !== undefined) kind = 'string'
    else if (m[3] !== undefined) kind = 'number'
    else if (m[4] !== undefined) {
      const upper = m[4].toUpperCase()
      kind = KEYWORDS.has(upper) ? 'keyword' : FUNCTIONS.has(upper) ? 'function' : 'identifier'
    } else if (m[5] !== undefined) kind = 'whitespace'
    else kind = 'punct' // covers plain operators as well as the JSON `->` / `->>` pair — rendered as-is, no special-casing needed
    tokens.push({ text, start, end, kind })
  }
  return tokens
}

const KIND_CLASS: Record<TokenKind, string> = {
  comment: 'text-muted-foreground italic',
  string: 'text-[var(--sql-string)]',
  number: 'text-[var(--sql-number)]',
  keyword: 'text-[var(--sql-keyword)]',
  function: 'text-[var(--sql-function)]',
  identifier: 'text-foreground',
  whitespace: '',
  punct: 'text-muted-foreground',
}

interface DiagnosticRange {
  start: number
  end: number
  severity: SqlDiagnostic['severity']
}

function diagnosticRanges(sql: string, diagnostics: SqlDiagnostic[]): DiagnosticRange[] {
  const lineStarts: number[] = []
  let acc = 0
  for (const line of sql.split('\n')) {
    lineStarts.push(acc)
    acc += line.length + 1
  }
  return diagnostics.map((d) => {
    const lineIdx = Math.min(Math.max(d.line - 1, 0), Math.max(lineStarts.length - 1, 0))
    const lineStart = lineStarts[lineIdx] ?? 0
    const start = Math.min(lineStart + Math.max(d.column - 1, 0), sql.length)
    const rest = sql.slice(start)
    const word = /^[A-Za-z0-9_.]+/.exec(rest)
    const len = word ? word[0].length : 1
    return { start, end: start + Math.max(len, 1), severity: d.severity }
  })
}

function rangesFor(token: Token, ranges: DiagnosticRange[]): SqlDiagnostic['severity'] | null {
  let hit: SqlDiagnostic['severity'] | null = null
  for (const r of ranges) {
    if (token.start < r.end && token.end > r.start) {
      if (r.severity === 'Error') return 'Error'
      hit = 'Warning'
    }
  }
  return hit
}

const UNDERLINE_CLASS: Record<'Error' | 'Warning', string> = {
  Error: 'underline decoration-wavy decoration-2 decoration-destructive underline-offset-4',
  Warning: 'underline decoration-wavy decoration-2 decoration-warning underline-offset-4',
}

// ============================================================================
// Autocomplete
// ============================================================================

type SuggestionKind = 'source' | 'column' | 'alias' | 'keyword' | 'function' | 'aggregate' | 'operator' | 'cte'

interface Suggestion {
  kind: SuggestionKind
  label: string
  insertText: string
  /** Muted descriptive text, e.g. "5 fields · trades" or "alias of trades". */
  secondary?: string
  /** Short right-aligned mono tag, e.g. a field type or "kw" / "fn" / "agg". */
  meta?: string
}

interface SourceRef {
  sourceName: string
  alias: string | null
}

function significantTokens(tokens: Token[]): Token[] {
  return tokens.filter((t) => t.kind !== 'whitespace' && t.kind !== 'comment')
}

/** Maps every usable identifier (bare source name or explicit alias) to its canonical source name. */
function buildAliasIndex(refs: SourceRef[]): Map<string, string> {
  const idx = new Map<string, string>()
  for (const r of refs) {
    idx.set(r.sourceName, r.sourceName)
    if (r.alias) idx.set(r.alias, r.sourceName)
  }
  return idx
}

/** A `FROM`/`JOIN` item, in the caret's current scope, that isn't a real source — either an
 *  earlier CTE or a derived-table subquery. Resolved lazily (via `resolveDerivedColumns`) because
 *  most keystrokes never need it. */
interface DerivedTarget {
  kind: 'cte' | 'derived'
  /** Raw text of the inner `SELECT …`, in the coordinate space of the *full document* — always
   *  sliced from `value`, since `ScopeInfo.fromItems` for the caret's own scope is built directly
   *  against the document. */
  bodyText: string
  /** CTE name / derived alias, for the suggestion list's secondary text. */
  label: string
}

/**
 * Splits a scope's raw `ScopeFromItem[]` (from sqlScope.ts, which knows nothing about real
 * sources) into: `refs`/`aliasIndex` for real-source items (feeding the pre-existing
 * alias/column-suggestion machinery unchanged), and `derivedTargets` for everything else that
 * resolves to an in-scope CTE. Names that match neither a real source nor a CTE (typos, or a
 * source not yet declared) are silently dropped — no suggestion beats a wrong one.
 */
function buildScopeIndexes(
  value: string,
  fromItems: ScopeFromItem[],
  ctes: CteDef[],
  byName: Map<string, SourceDefinition>,
) {
  const refs: SourceRef[] = []
  const derivedTargets = new Map<string, DerivedTarget>()
  for (const item of fromItems) {
    if (item.kind === 'named' && item.name) {
      if (byName.has(item.name)) {
        refs.push({ sourceName: item.name, alias: item.alias })
        continue
      }
      const cte = ctes.find((c) => c.name.toLowerCase() === item.name!.toLowerCase())
      if (cte) {
        derivedTargets.set(item.alias ?? item.name, { kind: 'cte', bodyText: value.slice(cte.bodyStart, cte.bodyEnd), label: cte.name })
      }
    } else if (item.kind === 'derived' && item.alias && item.derivedStart !== undefined && item.derivedEnd !== undefined) {
      derivedTargets.set(item.alias, { kind: 'derived', bodyText: value.slice(item.derivedStart, item.derivedEnd), label: item.alias })
    }
  }
  const aliasIndex = buildAliasIndex(refs)
  const referencedSourceNames = Array.from(new Set(refs.map((r) => r.sourceName)))
  return { refs, aliasIndex, referencedSourceNames, derivedTargets }
}

/** A resolved derived/CTE projection column — `field` is the real `FieldDef` it maps to 1:1 when
 *  determinable (drives the type meta tag and lets JSON `->`/`->>` chaining follow through). */
interface ResolvedColumn {
  name: string
  field: FieldDef | null
}

/**
 * Resolves a derived-table/CTE body's projected columns against real source fields where
 * possible. `depth` caps recursion at one extra level (a derived table's `SELECT *` that itself
 * reads from another derived table / CTE) — beyond that, columns from that nested item are simply
 * omitted rather than guessed. `value` is the full document text, needed only to look up a nested
 * CTE's body (CTEs are always declared at the top of the document, never inside a subquery).
 */
function resolveDerivedColumns(
  bodyText: string,
  value: string,
  byName: Map<string, SourceDefinition>,
  ctes: CteDef[],
  depth: number,
): ResolvedColumn[] | null {
  const proj = innerProjection(bodyText)
  if (!proj.ok) return null

  const findItem = (qualifier?: string): ScopeFromItem | undefined => {
    if (qualifier) return proj.fromItems.find((it) => (it.alias ?? (it.kind === 'named' ? it.name : undefined)) === qualifier)
    return proj.fromItems.length === 1 ? proj.fromItems[0] : undefined
  }

  const expandItemFields = (item: ScopeFromItem): ResolvedColumn[] | null => {
    if (item.kind === 'named' && item.name && byName.has(item.name)) {
      return byName.get(item.name)!.fields.map((f) => ({ name: f.name, field: f }))
    }
    if (depth >= 1) return null // one level of recursion only — give up gracefully beyond that
    if (item.kind === 'named' && item.name) {
      const cte = ctes.find((c) => c.name.toLowerCase() === item.name!.toLowerCase())
      if (!cte) return null
      return resolveDerivedColumns(value.slice(cte.bodyStart, cte.bodyEnd), value, byName, ctes, depth + 1)
    }
    if (item.kind === 'derived' && item.derivedStart !== undefined && item.derivedEnd !== undefined) {
      return resolveDerivedColumns(bodyText.slice(item.derivedStart, item.derivedEnd), value, byName, ctes, depth + 1)
    }
    return null
  }

  const resolveSingleField = (sourceField: string, qualifier?: string): FieldDef | null => {
    const item = findItem(qualifier)
    if (!item) return null
    const expanded = expandItemFields(item)
    return expanded?.find((c) => c.name === sourceField)?.field ?? null
  }

  const results: ResolvedColumn[] = []
  const seen = new Set<string>()
  const add = (name: string, field: FieldDef | null) => {
    if (seen.has(name)) return
    seen.add(name)
    results.push({ name, field })
  }

  for (const col of proj.columns) {
    if (col.kind === 'star' || col.kind === 'qualifiedStar') {
      const item = findItem(col.qualifier)
      if (!item) continue // ambiguous (multiple FROM items, no qualifier) or unresolved — skip
      const expanded = expandItemFields(item)
      if (!expanded) continue
      for (const e of expanded) add(e.name, e.field)
      continue
    }
    const field = col.sourceField ? resolveSingleField(col.sourceField, col.qualifier) : null
    add(col.name, field)
  }
  return results
}

/** The word token under/immediately before the caret, or an empty insertion point if there is none. */
function findWordAt(tokens: Token[], caret: number): { start: number; end: number; prefix: string } {
  for (const t of tokens) {
    if ((t.kind === 'identifier' || t.kind === 'keyword' || t.kind === 'function') && t.start <= caret && caret <= t.end) {
      return { start: t.start, end: t.end, prefix: t.text.slice(0, caret - t.start) }
    }
  }
  return { start: caret, end: caret, prefix: '' }
}

type ContextKind = 'fromJoin' | 'dot' | 'expr' | 'default'

interface Context {
  kind: ContextKind
  /** For `dot`: the alias/source identifier immediately before the dot. */
  identifier?: string
}

function resolveContext(sig: Token[], wordStart: number): Context {
  const before = sig.filter((t) => t.end <= wordStart)
  const last = before.at(-1)
  if (last && last.kind === 'punct' && last.text === '.') {
    const identTok = before.at(-2)
    if (identTok && identTok.kind === 'identifier') {
      return { kind: 'dot', identifier: identTok.text }
    }
  }
  if (last && last.kind === 'keyword') {
    const upper = last.text.toUpperCase()
    if (upper === 'FROM' || upper === 'JOIN') return { kind: 'fromJoin' }
    if (EXPR_TRIGGER_KEYWORDS.has(upper)) return { kind: 'expr' }
  }
  if (last && last.kind === 'punct' && (last.text === '(' || last.text === ',' || COMPARISON_PUNCT.has(last.text))) {
    return { kind: 'expr' }
  }
  return { kind: 'default' }
}

/** Resolves the FieldType of the column reference that ends immediately before the caret, if any.
 *  `resolve` is the shared alias/derived-aware field resolver built in `computeAutocomplete`. */
function resolveLastColumnType(sig: Token[], wordStart: number, resolve: (ref: string) => FieldDef | null): FieldType | null {
  const before = sig.filter((t) => t.end <= wordStart)
  const last = before.at(-1)
  if (!last || last.kind !== 'identifier') return null
  const dot = before.at(-2)
  const owner = before.at(-3)
  const ref = dot && dot.kind === 'punct' && dot.text === '.' && owner && owner.kind === 'identifier' ? `${owner.text}.${last.text}` : last.text
  return resolve(ref)?.type ?? null
}

/** Resolves a column reference (`alias.col` or bare `col`) to its FieldDef, following alias/source resolution. */
function resolveFieldRef(
  baseRef: string,
  aliasIndex: Map<string, string>,
  byName: Map<string, SourceDefinition>,
  referencedSourceNames: string[],
): FieldDef | null {
  const dot = baseRef.indexOf('.')
  if (dot >= 0) {
    const srcName = aliasIndex.get(baseRef.slice(0, dot))
    const src = srcName ? byName.get(srcName) : undefined
    return src?.fields.find((f) => f.name === baseRef.slice(dot + 1)) ?? null
  }
  for (const srcName of referencedSourceNames) {
    const f = byName.get(srcName)?.fields.find((x) => x.name === baseRef)
    if (f) return f
  }
  return null
}

/**
 * When the caret sits inside an open JSON-path string (`payload -> 'user' ->> 'ti…`), resolves the
 * declared nested schema at that depth and offers the child keys. An integer-index step
 * (`legs -> 0 -> …`) unwraps one array level instead of descending into a named child: the element of
 * an `isArray` field is shaped like that same field's declared `children`, so the FieldDef reference
 * doesn't change, only the "are we still inside the array" bookkeeping does. Returns null when not in
 * that context or when the field (after unwrapping) has no declared `children`.
 */
function jsonKeyContext(
  value: string,
  caret: number,
  resolve: (ref: string) => FieldDef | null,
): { candidates: Suggestion[]; wordStart: number; prefix: string } | null {
  // base column ref, then zero or more completed `-> 'key'` / `-> <index>` segments, then the open
  // quoted segment being typed.
  const m = /([A-Za-z_]\w*(?:\.[A-Za-z_]\w*)?)((?:\s*->>?\s*(?:'[^']*'|\d+))*)\s*->>?\s*'([^']*)$/.exec(value.slice(0, caret))
  if (!m) return null
  const [, baseRef, completed, partial] = m
  let field = resolve(baseRef)
  if (!field || field.type !== 'Json') return null
  // Descend through the already-completed steps to the current nesting level.
  const stepRe = /'([^']*)'|(\d+)/g
  let sm: RegExpExecArray | null
  while ((sm = stepRe.exec(completed))) {
    if (sm[2] !== undefined) {
      // Integer index: array-element access. Only valid on an array field; the element's shape is the
      // same FieldDef's `children`, so there's nothing to descend into — just consume the step.
      if (!field.isArray) return null
      continue
    }
    const child: FieldDef | undefined = field.children?.find((c) => c.name === sm![1])
    if (!child || child.type !== 'Json') return null
    field = child
  }
  const children = field.children ?? []
  if (children.length === 0) return null
  const candidates: Suggestion[] = children.map((c) => ({
    kind: 'column',
    label: c.name,
    insertText: `${c.name}'`, // close the quote so `-> 'user` becomes `-> 'user'`
    meta: c.type,
    secondary: 'JSON key',
  }))
  return { candidates, wordStart: caret - partial.length, prefix: partial }
}

function buildSourceSuggestions(sources: SourceDefinition[]): Suggestion[] {
  return sources.map((s) => ({
    kind: 'source',
    label: s.name,
    insertText: s.name,
    secondary: `${s.fields.length} field${s.fields.length === 1 ? '' : 's'} · ${s.generatorProfile}`,
  }))
}

function buildDotSuggestions(identifier: string, aliasIndex: Map<string, string>, byName: Map<string, SourceDefinition>): Suggestion[] {
  const srcName = aliasIndex.get(identifier)
  const src = srcName ? byName.get(srcName) : undefined
  if (!src) return []
  // Qualified star `alias.*` — expands to all columns of this input (valid in the SELECT list).
  const suggestions: Suggestion[] = [{ kind: 'column', label: '*', insertText: '*', secondary: 'all columns', meta: 'star' }]
  for (const f of src.fields) {
    suggestions.push({ kind: 'column', label: f.name, insertText: f.name, meta: f.type })
  }
  suggestions.push({ kind: 'column', label: '_ts', insertText: '_ts', meta: 'Timestamp' })
  suggestions.push({ kind: 'column', label: '_source', insertText: '_source', meta: 'String' })
  return suggestions
}

/** In-scope CTE names for a `FROM`/`JOIN` position — those declared before the word being typed. */
function buildCteSuggestions(ctes: CteDef[], wordStart: number): Suggestion[] {
  return ctes
    .filter((c) => c.nameStart < wordStart)
    .map((c) => ({ kind: 'cte', label: c.name, insertText: c.name, secondary: 'CTE' }))
}

/** Dot-completion for an alias that binds to a CTE or derived table rather than a real source —
 *  the columns come from `resolveDerivedColumns`' heuristic read of the inner SELECT list. */
function buildDerivedDotSuggestions(
  identifier: string,
  derivedTargets: Map<string, DerivedTarget>,
  value: string,
  byName: Map<string, SourceDefinition>,
  ctes: CteDef[],
): Suggestion[] {
  const target = derivedTargets.get(identifier)
  if (!target) return []
  const cols = resolveDerivedColumns(target.bodyText, value, byName, ctes, 0)
  if (!cols) return []
  return cols.map((c) => ({
    kind: 'column',
    label: c.name,
    insertText: c.name,
    meta: c.field?.type ?? (target.kind === 'cte' ? 'CTE' : 'derived'),
    secondary: target.kind === 'cte' ? `CTE · ${target.label}` : `derived · ${target.label}`,
  }))
}

function buildKeywordSuggestions(): Suggestion[] {
  return Array.from(KEYWORDS).map((k) => ({ kind: 'keyword', label: k, insertText: k, meta: 'kw' }))
}

function buildFunctionSuggestions(): Suggestion[] {
  const fns: Suggestion[] = []
  for (const f of AGGREGATE_FNS) fns.push({ kind: 'aggregate', label: f, insertText: f, meta: 'agg' })
  for (const f of SCALAR_FNS) fns.push({ kind: 'function', label: f, insertText: f, meta: 'fn' })
  return fns
}

function buildExprSuggestions(refs: SourceRef[], sources: SourceDefinition[], derivedTargets: Map<string, DerivedTarget>): Suggestion[] {
  const byName = new Map(sources.map((s) => [s.name, s]))
  const aliasesForSource = new Map<string, string[]>()
  for (const r of refs) {
    if (!byName.has(r.sourceName)) continue
    const arr = aliasesForSource.get(r.sourceName) ?? []
    if (r.alias) arr.push(r.alias)
    aliasesForSource.set(r.sourceName, arr)
  }
  const referencedSourceNames = Array.from(aliasesForSource.keys())

  const fieldCount = new Map<string, number>()
  for (const srcName of referencedSourceNames) {
    for (const f of byName.get(srcName)!.fields) {
      fieldCount.set(f.name, (fieldCount.get(f.name) ?? 0) + 1)
    }
  }

  const columns: Suggestion[] = []
  const seen = new Set<string>()
  for (const srcName of referencedSourceNames) {
    const src = byName.get(srcName)!
    const aliases = aliasesForSource.get(srcName) ?? []
    const qualifier = aliases[0] ?? srcName
    for (const f of src.fields) {
      const unambiguous = (fieldCount.get(f.name) ?? 0) <= 1
      const label = unambiguous ? f.name : `${qualifier}.${f.name}`
      if (seen.has(label)) continue
      seen.add(label)
      columns.push({ kind: 'column', label, insertText: label, meta: f.type })
    }
  }

  const aliasSuggestions: Suggestion[] = []
  const seenAlias = new Set<string>()
  for (const r of refs) {
    if (!r.alias || !byName.has(r.sourceName) || seenAlias.has(r.alias)) continue
    seenAlias.add(r.alias)
    aliasSuggestions.push({ kind: 'alias', label: r.alias, insertText: r.alias, secondary: `alias of ${r.sourceName}` })
  }
  for (const [alias, target] of derivedTargets) {
    aliasSuggestions.push({
      kind: 'alias',
      label: alias,
      insertText: alias,
      secondary: target.kind === 'cte' ? `CTE · ${target.label}` : 'derived table',
    })
  }

  return [...columns, ...aliasSuggestions, ...buildFunctionSuggestions(), ...buildKeywordSuggestions()]
}

interface AutocompleteResult {
  suggestions: Suggestion[]
  wordStart: number
  wordEnd: number
}

function computeAutocomplete(
  value: string,
  caret: number,
  sources: SourceDefinition[],
  opts: { ignorePrefix?: boolean } = {},
): AutocompleteResult | null {
  const tokens = tokenize(value)
  const sig = significantTokens(tokens)
  const { start: wordStart, end: wordEnd, prefix } = findWordAt(tokens, caret)
  const effectivePrefix = opts.ignorePrefix ? '' : prefix

  const byName = new Map(sources.map((s) => [s.name, s]))

  // Scope-aware source resolution: CTEs (from the whole document — their forward-reference
  // restriction is enforced by the grammar, not re-checked here) plus the FROM/JOIN items visible
  // from the caret's own `(SELECT …)` scope only. A derived table's own alias is invisible from
  // inside its own parens, and vice versa — see sqlScope.ts's `findCaretScope` doc.
  const ctes = parseCtes(value)
  const scope = findCaretScope(value, caret)
  const { refs, aliasIndex, referencedSourceNames, derivedTargets } = buildScopeIndexes(value, scope.fromItems, ctes, byName)

  // Shared field resolver: real source/alias first (existing behavior), then — only for qualified
  // `alias.col` refs — an in-scope CTE/derived-table alias, followed one level through
  // `resolveDerivedColumns`. Bare (unqualified) names never attempt derived resolution, since a
  // derived/CTE column reference is always written qualified in this grammar.
  const resolve = (ref: string): FieldDef | null => {
    const direct = resolveFieldRef(ref, aliasIndex, byName, referencedSourceNames)
    if (direct) return direct
    const dot = ref.indexOf('.')
    if (dot < 0) return null
    const target = derivedTargets.get(ref.slice(0, dot))
    if (!target) return null
    const cols = resolveDerivedColumns(target.bodyText, value, byName, ctes, 0)
    return cols?.find((c) => c.name === ref.slice(dot + 1))?.field ?? null
  }

  // JSON key completion: caret inside an open `-> '…'` path → suggest the declared nested keys.
  const jsonKey = jsonKeyContext(value, caret, resolve)
  if (jsonKey) {
    const needle = opts.ignorePrefix ? '' : jsonKey.prefix.toLowerCase()
    const filtered = needle === '' ? jsonKey.candidates : jsonKey.candidates.filter((c) => c.label.toLowerCase().startsWith(needle))
    if (filtered.length === 0) return null
    return { suggestions: filtered, wordStart: jsonKey.wordStart, wordEnd: caret }
  }

  const context = resolveContext(sig, wordStart)

  let candidates: Suggestion[]
  if (context.kind === 'fromJoin') {
    candidates = [...buildSourceSuggestions(sources), ...buildCteSuggestions(ctes, wordStart)]
  } else if (context.kind === 'dot') {
    candidates = buildDotSuggestions(context.identifier!, aliasIndex, byName)
    if (candidates.length === 0) {
      candidates = buildDerivedDotSuggestions(context.identifier!, derivedTargets, value, byName, ctes)
    }
  } else if (context.kind === 'expr') {
    candidates = buildExprSuggestions(refs, sources, derivedTargets)
  } else {
    candidates = buildKeywordSuggestions()
  }

  // JSON bonus: once the caret sits right after a reference to a `Json` column, offer the
  // Postgres-style path operators as extra insertable snippets. Works through a derived/CTE alias
  // too, via the shared `resolve`.
  if (context.kind === 'expr' || context.kind === 'default') {
    const lastType = resolveLastColumnType(sig, wordStart, resolve)
    if (lastType === 'Json') {
      candidates = [
        { kind: 'operator', label: "-> '…'", insertText: "-> '", secondary: 'JSON field (object)', meta: 'op' },
        { kind: 'operator', label: "->> '…'", insertText: "->> '", secondary: 'JSON field (text)', meta: 'op' },
        ...candidates,
      ]
    }
  }

  const needle = effectivePrefix.toLowerCase()
  const filtered = needle === '' ? candidates : candidates.filter((c) => c.label.toLowerCase().startsWith(needle))
  if (filtered.length === 0) return null
  return { suggestions: filtered, wordStart, wordEnd }
}

// ---- caret pixel position (hidden mirror-div technique) -------------------

function measureCaretOffset(mirror: HTMLDivElement, text: string, position: number): { top: number; left: number; height: number } {
  mirror.innerHTML = ''
  mirror.appendChild(document.createTextNode(text.slice(0, position)))
  const marker = document.createElement('span')
  marker.textContent = '​'
  mirror.appendChild(marker)
  mirror.appendChild(document.createTextNode(text.slice(position) || '​'))
  return { top: marker.offsetTop, left: marker.offsetLeft, height: marker.offsetHeight || 20 }
}

const SUGGESTION_KIND_LABEL: Record<SuggestionKind, string> = {
  source: 'source',
  column: 'column',
  alias: 'alias',
  keyword: 'keyword',
  function: 'function',
  aggregate: 'aggregate',
  operator: 'operator',
  cte: 'CTE',
}

export function SqlEditor({
  value,
  onChange,
  diagnostics = [],
  readOnly = false,
  minRows = 14,
  placeholder,
  sources = [],
  onFormat,
  toolbarEnd,
}: {
  value: string
  onChange: (value: string) => void
  diagnostics?: SqlDiagnostic[]
  readOnly?: boolean
  minRows?: number
  placeholder?: string
  sources?: SourceDefinition[]
  /** When set (and not readOnly), shows a "Format" button in a small toolbar above the editor —
   *  the host page owns the actual formatting (see `lib/sqlFormat.ts`) and calls `onChange` itself. */
  onFormat?: () => void
  /** Extra controls rendered in the same toolbar, left of Format (e.g. a page's Revert button) —
   *  kept in the SqlEditor's toolbar so every host page's editor-adjacent controls line up the same
   *  way. Absent entirely (and no toolbar rendered at all) when neither this nor onFormat is set —
   *  LineageDetailPanel's read-only usage is unaffected either way. */
  toolbarEnd?: ReactNode
}) {
  const containerRef = useRef<HTMLDivElement>(null)
  const preRef = useRef<HTMLPreElement>(null)
  const textareaRef = useRef<HTMLTextAreaElement>(null)
  const mirrorRef = useRef<HTMLDivElement>(null)

  const tokens = useMemo(() => tokenize(value), [value])
  const ranges = useMemo(() => diagnosticRanges(value, diagnostics), [value, diagnostics])

  const [caretPos, setCaretPos] = useState(0)
  const [popupOpen, setPopupOpen] = useState(false)
  const [forceFullList, setForceFullList] = useState(false)
  const [activeIndex, setActiveIndex] = useState(0)
  const [anchor, setAnchor] = useState<{ top: number; left: number; placement: 'below' | 'above' } | null>(null)

  const autocomplete = useMemo(() => {
    if (readOnly) return null
    return computeAutocomplete(value, caretPos, sources, { ignorePrefix: forceFullList })
  }, [value, caretPos, sources, readOnly, forceFullList])

  const showPopup = !readOnly && popupOpen && autocomplete !== null && autocomplete.suggestions.length > 0

  useEffect(() => {
    setActiveIndex(0)
  }, [autocomplete?.wordStart, autocomplete?.suggestions.length, autocomplete?.suggestions[0]?.label])

  useLayoutEffect(() => {
    if (!showPopup || !autocomplete) {
      setAnchor(null)
      return
    }
    const mirror = mirrorRef.current
    const textarea = textareaRef.current
    const container = containerRef.current
    if (!mirror || !textarea || !container) return
    const { top, left, height } = measureCaretOffset(mirror, value, autocomplete.wordStart)
    const rawTop = top - textarea.scrollTop
    const rawLeft = left - textarea.scrollLeft
    const containerHeight = container.clientHeight
    const containerWidth = container.clientWidth
    const popupWidthEstimate = 288
    const popupHeightEstimate = 260
    const spaceBelow = containerHeight - (rawTop + height)
    const placement: 'below' | 'above' = spaceBelow < popupHeightEstimate && rawTop > spaceBelow ? 'above' : 'below'
    const clampedLeft = Math.min(Math.max(rawLeft, 4), Math.max(containerWidth - popupWidthEstimate - 4, 4))
    const clampedTop = placement === 'below' ? Math.min(rawTop + height + 4, Math.max(containerHeight - 4, 0)) : Math.max(rawTop - 4, 4)
    setAnchor({ top: clampedTop, left: clampedLeft, placement })
  }, [showPopup, autocomplete, value])

  function closePopup() {
    setPopupOpen(false)
    setForceFullList(false)
  }

  function acceptSuggestion(s: Suggestion) {
    if (!autocomplete) return
    const before = value.slice(0, autocomplete.wordStart)
    const after = value.slice(autocomplete.wordEnd)
    const nextValue = before + s.insertText + after
    const newCaret = autocomplete.wordStart + s.insertText.length
    onChange(nextValue)
    closePopup()
    requestAnimationFrame(() => {
      const el = textareaRef.current
      if (el) {
        el.selectionStart = el.selectionEnd = newCaret
        el.focus()
      }
    })
  }

  function handleScroll(e: UIEvent<HTMLTextAreaElement>) {
    if (preRef.current) {
      preRef.current.scrollTop = e.currentTarget.scrollTop
      preRef.current.scrollLeft = e.currentTarget.scrollLeft
    }
    if (showPopup) closePopup()
  }

  function handleChange(e: ChangeEvent<HTMLTextAreaElement>) {
    onChange(e.target.value)
    setCaretPos(e.target.selectionStart)
    setForceFullList(false)
    if (!readOnly) setPopupOpen(true)
  }

  function handleSelect(e: SyntheticEvent<HTMLTextAreaElement>) {
    setCaretPos(e.currentTarget.selectionStart)
  }

  function handleBlur() {
    closePopup()
  }

  function handleKeyDown(e: KeyboardEvent<HTMLTextAreaElement>) {
    if (!readOnly && e.ctrlKey && e.key === ' ') {
      e.preventDefault()
      setForceFullList(true)
      setPopupOpen(true)
      setCaretPos(e.currentTarget.selectionStart)
      return
    }

    if (showPopup && autocomplete) {
      const { suggestions } = autocomplete
      if (e.key === 'ArrowDown') {
        e.preventDefault()
        setActiveIndex((i) => Math.min(i + 1, suggestions.length - 1))
        return
      }
      if (e.key === 'ArrowUp') {
        e.preventDefault()
        setActiveIndex((i) => Math.max(i - 1, 0))
        return
      }
      if (e.key === 'Enter' || e.key === 'Tab') {
        e.preventDefault()
        acceptSuggestion(suggestions[Math.min(activeIndex, suggestions.length - 1)])
        return
      }
      if (e.key === 'Escape') {
        e.preventDefault()
        closePopup()
        return
      }
      if (e.key === 'ArrowLeft' || e.key === 'ArrowRight' || e.key === 'Home' || e.key === 'End') {
        closePopup()
      }
    }

    if (e.key === 'Tab') {
      e.preventDefault()
      const el = e.currentTarget
      const { selectionStart, selectionEnd } = el
      const next = `${value.slice(0, selectionStart)}  ${value.slice(selectionEnd)}`
      onChange(next)
      requestAnimationFrame(() => {
        el.selectionStart = el.selectionEnd = selectionStart + 2
      })
    }
  }

  const activeOptionId = showPopup && autocomplete ? `sql-ac-opt-${Math.min(activeIndex, autocomplete.suggestions.length - 1)}` : undefined

  const showToolbar = !readOnly && (onFormat || toolbarEnd)

  return (
    <div className="flex flex-col gap-1.5">
      {showToolbar && (
        <div className="flex items-center justify-end gap-2">
          {toolbarEnd}
          {onFormat && (
            <Button type="button" variant="outline" size="sm" className="h-7 px-2 text-xs" onClick={onFormat}>
              <WandSparkles className="size-3.5" /> Format
            </Button>
          )}
        </div>
      )}
      <div
        ref={containerRef}
        className="relative overflow-hidden rounded-lg border border-input bg-input/20 font-mono text-[13px] leading-6 transition-colors focus-within:border-ring focus-within:ring-3 focus-within:ring-ring/50"
      >
      <pre
        ref={preRef}
        aria-hidden
        // overflow-HIDDEN, not auto: this layer is scrolled programmatically from the textarea's
        // onScroll (handleScroll syncs both axes), never by the user — it is pointer-events-none. With
        // overflow-auto the browser still paints its own scrollbars for the overflowing content, which
        // is what put TWO horizontal scrollbars under a long single-line query: this one and the real
        // textarea's, stacked and moving together.
        className="pointer-events-none absolute inset-0 m-0 overflow-hidden whitespace-pre p-3"
      >
        {tokens.length === 0 && <span className="text-muted-foreground">{placeholder}</span>}
        {tokens.map((t, i) => {
          const severity = rangesFor(t, ranges)
          const cls = `${KIND_CLASS[t.kind]} ${severity ? UNDERLINE_CLASS[severity] : ''}`
          return (
            <span key={i} className={cls}>
              {t.text}
            </span>
          )
        })}
        {'\n'}
      </pre>
      <div ref={mirrorRef} aria-hidden className="pointer-events-none invisible absolute inset-0 overflow-hidden whitespace-pre p-3" />
      <textarea
        ref={textareaRef}
        value={value}
        onChange={handleChange}
        onScroll={handleScroll}
        onKeyDown={handleKeyDown}
        onSelect={handleSelect}
        onBlur={handleBlur}
        readOnly={readOnly}
        spellCheck={false}
        placeholder={placeholder}
        rows={minRows}
        role={readOnly ? undefined : 'combobox'}
        aria-expanded={readOnly ? undefined : showPopup}
        aria-controls={readOnly ? undefined : 'sql-ac-listbox'}
        aria-activedescendant={activeOptionId}
        aria-autocomplete={readOnly ? undefined : 'list'}
        className="relative w-full resize-none overflow-auto whitespace-pre bg-transparent p-3 text-transparent caret-foreground outline-none placeholder:text-transparent"
        style={{ fontFamily: 'inherit' }}
      />

      {showPopup && autocomplete && anchor && (
        <div
          id="sql-ac-listbox"
          role="listbox"
          onMouseDown={(e) => e.preventDefault()}
          className={cn(
            'absolute z-10 w-72 max-h-72 overflow-y-auto rounded-md border border-border bg-popover py-1 text-popover-foreground shadow-md animate-in fade-in-0 duration-150',
          )}
          style={{
            left: anchor.left,
            ...(anchor.placement === 'below' ? { top: anchor.top } : { bottom: containerRef.current ? containerRef.current.clientHeight - anchor.top : undefined }),
          }}
        >
          {autocomplete.suggestions.slice(0, 200).map((s, i) => {
            const selected = i === activeIndex
            return (
              <div
                key={`${s.kind}-${s.label}-${i}`}
                id={`sql-ac-opt-${i}`}
                role="option"
                aria-selected={selected}
                title={SUGGESTION_KIND_LABEL[s.kind]}
                onMouseEnter={() => setActiveIndex(i)}
                onClick={() => acceptSuggestion(s)}
                className={cn(
                  'flex cursor-pointer items-center gap-2 px-2.5 py-1.5 text-xs',
                  selected ? 'bg-accent text-accent-foreground' : 'text-popover-foreground',
                )}
              >
                <span className="min-w-0 flex-1 truncate">{s.label}</span>
                {s.secondary && <span className="shrink-0 truncate text-[11px] text-muted-foreground">{s.secondary}</span>}
                {s.meta && <span className="shrink-0 text-right font-mono text-[10px] text-muted-foreground/80">{s.meta}</span>}
              </div>
            )
          })}
        </div>
      )}
      </div>
    </div>
  )
}
