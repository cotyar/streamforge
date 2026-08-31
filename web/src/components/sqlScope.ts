/**
 * Heuristic (non-parsing) scope analysis feeding SqlEditor's autocomplete, extended to understand
 * `WITH`/CTEs and derived-table (subquery) `FROM`/`JOIN` items from the nested-query grammar:
 *
 *   WITH name AS (SELECT …), name2 AS (SELECT …)
 *   SELECT … FROM (SELECT …) alias [JOIN (SELECT …) alias2 …]
 *
 * This module never attempts a real SQL parse — no AST, no grammar. It scans the raw text outside
 * string literals / line comments, tracking paren nesting depth, to answer three cheap questions:
 *
 *   1. What CTEs are declared, and at what text offset does each name appear? (`parseCtes`)
 *   2. Given a caret position, which "select scope" (top-level query, a CTE body, or a derived
 *      table body) is it inside, and what FROM/JOIN items does *that* scope see? (`findCaretScope`)
 *   3. For one inner `SELECT …` query's raw text, what columns does its select-list project?
 *      (`innerProjection`)
 *
 * SqlEditor.tsx combines these primitives with the live `SourceDefinition[]` list (which this
 * module deliberately has no knowledge of) to resolve aliases, expand `SELECT *`, and follow
 * derived/CTE columns back to a real field for JSON `->`/`->>` chaining.
 *
 * KNOWN LIMITS (heuristic by design — prefer no suggestion over a wrong one):
 *  - String/comment masking mirrors the tokenizer in SqlEditor.tsx exactly: single-quoted strings
 *    with backslash-escapes, and `--` line comments. No double-quoted identifiers, no block
 *    comments, no dollar-quoting.
 *  - A CTE is recognized only as a depth-0 `name AS (SELECT …)` immediately after `WITH` (or a
 *    peer separated by commas). `WITH RECURSIVE` and forward CTE references aren't things the
 *    engine supports, so they aren't specially detected or rejected — a forward reference simply
 *    won't be offered as in-scope, which is the correct outcome anyway.
 *  - Every scan tolerates unbalanced parens (an unterminated `(` is treated as extending to the
 *    end of the text; a stray `)` is ignored) so a query the user is still typing never throws.
 *  - `innerProjection` reads only the top-level select-list and the top-level FROM/JOIN items of
 *    one query; it does not recurse. Recursing into a nested derived table / CTE (for `SELECT *`
 *    fallback or JSON passthrough) is the caller's job, and the caller is expected to cap it at
 *    one extra level, per the feature spec ("recurse one level, then give up gracefully").
 *  - Projection items that are computed expressions without an `AS alias` are skipped entirely —
 *    there's no name to offer. Computed expressions *with* an alias still produce a column name,
 *    just without a `sourceField` mapping (so JSON chaining through them silently stops).
 */

// ---------------------------------------------------------------------------
// String/comment masking + paren-depth scanning
// ---------------------------------------------------------------------------

/**
 * Replaces the contents of string literals and `--` comments with spaces, preserving length and
 * every other character exactly. Downstream scans run regexes / char-walks against this masked
 * text (so keywords, parens, and commas *inside* a string never get mistaken for structure) while
 * still being able to slice the *original* text using the same indices for content extraction.
 */
export function maskLiteralsAndComments(text: string): string {
  let out = ''
  let i = 0
  const n = text.length
  while (i < n) {
    const ch = text[i]
    if (ch === '-' && text[i + 1] === '-') {
      while (i < n && text[i] !== '\n') {
        out += ' '
        i++
      }
      continue
    }
    if (ch === "'") {
      out += ' '
      i++
      while (i < n) {
        if (text[i] === '\\' && i + 1 < n) {
          out += '  '
          i += 2
          continue
        }
        if (text[i] === "'") {
          out += ' '
          i++
          break
        }
        out += ' '
        i++
      }
      continue
    }
    out += ch
    i++
  }
  return out
}

interface ParenSpan {
  open: number
  /** Index of the matching `)`, or `text.length` if it's never closed (mid-typing). */
  close: number
  /** Nesting depth of the paren itself — 0 for a top-level paren. */
  depth: number
}

/** Tolerates unmatched `(` (closes at end-of-text) and unmatched `)` (ignored) without throwing. */
function findParenSpans(masked: string): ParenSpan[] {
  const spans: ParenSpan[] = []
  const stack: ParenSpan[] = []
  for (let i = 0; i < masked.length; i++) {
    const ch = masked[i]
    if (ch === '(') {
      const span: ParenSpan = { open: i, close: masked.length, depth: stack.length }
      spans.push(span)
      stack.push(span)
    } else if (ch === ')') {
      const span = stack.pop()
      if (span) span.close = i
    }
  }
  return spans
}

/**
 * Per-character paren nesting depth. The `(`/`)` characters themselves are stamped with the
 * *outer* depth (the paren's own depth), matching `ParenSpan.depth`; every character strictly
 * inside a paren at depth D is stamped D+1. Used to tell whether a keyword found by a document-wide
 * regex search actually belongs to the scope being scanned, or to something nested inside it.
 */
function computeDepths(masked: string): number[] {
  const depths: number[] = new Array(masked.length)
  let d = 0
  for (let i = 0; i < masked.length; i++) {
    const ch = masked[i]
    if (ch === '(') {
      depths[i] = d
      d++
    } else if (ch === ')') {
      d = Math.max(d - 1, 0)
      depths[i] = d
    } else {
      depths[i] = d
    }
  }
  return depths
}

/** A paren "is a select scope" when its content opens with `SELECT` — distinguishes `(SELECT …)`
 *  CTE/derived-table bodies from ordinary grouping/function-call parens. */
function isSelectSpan(masked: string, span: ParenSpan): boolean {
  return /^\s*SELECT\b/i.test(masked.slice(span.open + 1))
}

// Kept in sync manually with the KEYWORDS set in SqlEditor.tsx. Used only to avoid mistaking the
// next clause's keyword (`WHERE`, `JOIN`, …) for a bare table/CTE alias when no `AS` or comma
// separates them, e.g. `FROM trades WHERE …` must not read `WHERE` as trades' alias.
const CLAUSE_KEYWORDS = new Set([
  'SELECT', 'FROM', 'WHERE', 'GROUP', 'BY', 'WINDOW', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 'FULL', 'OUTER',
  'CROSS', 'ON', 'WITHIN', 'AS', 'EMIT', 'CHANGES', 'FINAL', 'AND', 'OR', 'WITH',
  // Reserved in shared/StreamsForge.Engine/Sql/Parser.cs's ClauseKeywords for exactly this reason and
  // missing here, so `FROM trades LATEST BY (id) WHERE …` read LATEST as an AS-less alias for trades
  // and column completion after WHERE came back empty — on the shape every table-mode CDC mirror uses.
  'LATEST', 'UNNEST', 'UNION', 'IN', 'EXISTS',
])

// ---------------------------------------------------------------------------
// FROM/JOIN item scanning (shared by findCaretScope and innerProjection)
// ---------------------------------------------------------------------------

export interface ScopeFromItem {
  kind: 'named' | 'derived'
  /** `kind: 'named'` only — the bare identifier after FROM/JOIN. This is a real source name *or*
   *  a CTE name; disambiguating between the two is the caller's job (it has the source list and
   *  can call `parseCtes` itself). */
  name?: string
  /** Usable qualifier for `alias.col`. Null when there's no alias — for `kind: 'named'` the item
   *  is still referenceable via `name` itself; for `kind: 'derived'` the grammar requires an
   *  alias, so an alias-less derived item is dropped entirely (nothing to key it by yet). */
  alias: string | null
  /** `kind: 'derived'` only — the raw-text bounds of the inner `SELECT …`, exclusive of its
   *  parens, in the coordinate space of whichever string was scanned to find it (the full
   *  document for `findCaretScope`, or the `selectText` argument for `innerProjection`). */
  derivedStart?: number
  derivedEnd?: number
}

/** Scans `[rangeStart, rangeEnd)` of `masked` for FROM/JOIN items whose keyword sits exactly at
 *  `atDepth` (so a FROM/JOIN belonging to a *nested* derived table is correctly excluded). */
function scanFromItems(
  masked: string,
  depths: number[],
  spans: ParenSpan[],
  rangeStart: number,
  rangeEnd: number,
  atDepth: number,
): ScopeFromItem[] {
  const items: ScopeFromItem[] = []
  const kwRe = /\b(FROM|JOIN)\b/gi
  kwRe.lastIndex = rangeStart
  let m: RegExpExecArray | null
  while ((m = kwRe.exec(masked))) {
    if (m.index >= rangeEnd) break
    if (depths[m.index] !== atDepth) continue
    const wsLen = /^\s*/.exec(masked.slice(m.index + m[0].length))?.[0].length ?? 0
    const p = m.index + m[0].length + wsLen
    if (p >= rangeEnd || p >= masked.length) continue
    if (masked[p] === '(') {
      const span = spans.find((s) => s.open === p)
      if (!span) continue
      const afterClose = masked.slice(span.close + 1)
      const aliasMatch = /^\s*(?:AS\s+)?([A-Za-z_]\w*)\b/i.exec(afterClose)
      const alias = aliasMatch && !CLAUSE_KEYWORDS.has(aliasMatch[1].toUpperCase()) ? aliasMatch[1] : null
      if (!alias) continue // derived tables require an alias per grammar; nothing to reference yet
      items.push({ kind: 'derived', alias, derivedStart: span.open + 1, derivedEnd: span.close })
    } else {
      const identMatch = /^[A-Za-z_]\w*/.exec(masked.slice(p))
      if (!identMatch) continue
      const name = identMatch[0]
      const rest = masked.slice(p + name.length)
      let alias: string | null = null
      const asMatch = /^\s+AS\s+([A-Za-z_]\w*)/i.exec(rest)
      if (asMatch) {
        alias = asMatch[1]
      } else {
        const bareMatch = /^\s+([A-Za-z_]\w*)\b/.exec(rest)
        if (bareMatch && !CLAUSE_KEYWORDS.has(bareMatch[1].toUpperCase())) alias = bareMatch[1]
      }
      items.push({ kind: 'named', name, alias })
    }
  }
  return items
}

// ---------------------------------------------------------------------------
// 1. parseCtes
// ---------------------------------------------------------------------------

export interface CteDef {
  name: string
  /** Text offset of the CTE's name token — "declared before the caret" is `nameStart < caret`. */
  nameStart: number
  /** Bounds of the inner `SELECT …`, exclusive of the CTE's own parens. */
  bodyStart: number
  bodyEnd: number
}

/**
 * Finds every `name AS (SELECT …)` entry in a leading `WITH name AS (…), name2 AS (…) …` clause.
 * Returns `[]` when the text doesn't start with `WITH` (CTEs are only recognized at statement
 * start, matching the grammar). Tolerant of an unterminated final CTE body (mid-typing).
 */
export function parseCtes(text: string): CteDef[] {
  const masked = maskLiteralsAndComments(text)
  if (!/^\s*WITH\b/i.test(masked)) return []
  const spans = findParenSpans(masked)
  const ctes: CteDef[] = []
  for (const span of spans) {
    if (span.depth !== 0) continue
    if (!isSelectSpan(masked, span)) continue
    const head = masked.slice(0, span.open)
    const m = /([A-Za-z_]\w*)\s*AS\s*$/i.exec(head)
    if (!m) continue
    ctes.push({ name: m[1], nameStart: m.index, bodyStart: span.open + 1, bodyEnd: span.close })
  }
  return ctes
}

// ---------------------------------------------------------------------------
// 2. findCaretScope
// ---------------------------------------------------------------------------

export interface ScopeInfo {
  /** FROM/JOIN items visible for alias / bare-name resolution *in this scope only* — a derived
   *  table's own alias (assigned in the outer query) is intentionally absent when the caret is
   *  inside that derived table's own parens, and vice versa. */
  fromItems: ScopeFromItem[]
  /** True when the caret sits inside some `(SELECT …)` — a CTE body or a derived-table body. */
  nested: boolean
}

/**
 * Determines which `(SELECT …)` scope the caret is lexically inside (if any) by finding the
 * innermost select-paren whose range contains it — non-select parens (function calls, grouping)
 * are transparently skipped over, since containment is purely by index range. CTEs declared
 * anywhere before the document (per `parseCtes`) always bound the *main* query's start; the
 * top-level scope is whatever follows the last CTE body (or the whole text, if there's no WITH).
 */
export function findCaretScope(text: string, caret: number): ScopeInfo {
  const masked = maskLiteralsAndComments(text)
  const depths = computeDepths(masked)
  const spans = findParenSpans(masked)
  const ctes = parseCtes(text)

  let enclosing: ParenSpan | null = null
  for (const span of spans) {
    if (!isSelectSpan(masked, span)) continue
    if (caret > span.open && caret <= span.close) {
      if (!enclosing || span.depth > enclosing.depth) enclosing = span
    }
  }

  const scopeStart = enclosing ? enclosing.open + 1 : ctes.length > 0 ? ctes[ctes.length - 1].bodyEnd + 1 : 0
  const scopeEnd = enclosing ? enclosing.close : text.length
  const scopeDepth = enclosing ? enclosing.depth + 1 : 0

  const fromItems = scanFromItems(masked, depths, spans, scopeStart, scopeEnd, scopeDepth)
  return { fromItems, nested: enclosing !== null }
}

// ---------------------------------------------------------------------------
// 3. innerProjection
// ---------------------------------------------------------------------------

export interface ProjectedColumn {
  name: string
  kind: 'star' | 'qualifiedStar' | 'named'
  /** The right-hand identifier this column maps to 1:1 in its own FROM item — a bare `payload`
   *  item, or the `b` of `a.b [AS x]`. Absent for computed expressions (even if aliased): there's
   *  nothing to follow through for JSON chaining / star expansion. */
  sourceField?: string
  /** The qualifier of a qualified ref/star (`a.b`, `a.*`) — an alias into this query's own
   *  `fromItems`, resolved by the caller. */
  qualifier?: string
}

export interface InnerProjectionResult {
  /** False when no top-level `SELECT` could even be found — caller should offer nothing. */
  ok: boolean
  columns: ProjectedColumn[]
  /** This query's own top-level FROM/JOIN items (same shape `findCaretScope` uses), for resolving
   *  `sourceField`/`qualifier` and expanding `SELECT *`. */
  fromItems: ScopeFromItem[]
}

function splitTopLevelCommas(masked: string, original: string): string[] {
  const parts: string[] = []
  let depth = 0
  let start = 0
  for (let i = 0; i < masked.length; i++) {
    const ch = masked[i]
    if (ch === '(') depth++
    else if (ch === ')') depth = Math.max(depth - 1, 0)
    else if (ch === ',' && depth === 0) {
      parts.push(original.slice(start, i))
      start = i + 1
    }
  }
  parts.push(original.slice(start))
  return parts.map((s) => s.trim()).filter((s) => s.length > 0)
}

function parseProjectionItem(raw: string): ProjectedColumn | null {
  const item = raw.trim()
  if (item === '') return null
  const masked = maskLiteralsAndComments(item)

  if (/^\*$/.test(masked)) return { name: '*', kind: 'star' }

  const qStar = /^([A-Za-z_]\w*)\s*\.\s*\*$/.exec(masked)
  if (qStar) return { name: '*', kind: 'qualifiedStar', qualifier: qStar[1] }

  const asMatch = /\bAS\s+([A-Za-z_]\w*)\s*$/i.exec(masked)
  if (asMatch) {
    const name = asMatch[1]
    const lhs = masked.slice(0, asMatch.index).trim()
    const simple = /^([A-Za-z_]\w*)(?:\s*\.\s*([A-Za-z_]\w*))?$/.exec(lhs)
    if (simple) {
      const sourceField = simple[2] ?? simple[1]
      const qualifier = simple[2] ? simple[1] : undefined
      return { name, kind: 'named', sourceField, qualifier }
    }
    return { name, kind: 'named' } // aliased computed expression — name only, no field mapping
  }

  const bare = /^[A-Za-z_]\w*$/.exec(masked)
  if (bare) return { name: masked, kind: 'named', sourceField: masked }

  const qualified = /^([A-Za-z_]\w*)\s*\.\s*([A-Za-z_]\w*)$/.exec(masked)
  if (qualified) return { name: qualified[2], kind: 'named', sourceField: qualified[2], qualifier: qualified[1] }

  return null // computed expression without an alias — nothing to name it, skip
}

/**
 * Parses one `SELECT …` query's top-level select-list into named columns, plus its own top-level
 * FROM/JOIN items. `selectText` should start with `SELECT` (a CTE or derived-table body, as
 * produced by `parseCtes`/`findCaretScope`); leading whitespace is fine.
 *
 * Rules (see module doc for the "give up" cases):
 *  - Split the select-list on top-level commas (paren nesting respected).
 *  - `AS name` → that name; a bare `payload` item stays `payload`; a qualified `a.b` (no alias)
 *    → `b`; a computed expression with no `AS` is skipped.
 *  - `*` / `alias.*` are surfaced as `star`/`qualifiedStar` columns for the caller to expand.
 */
export function innerProjection(selectText: string): InnerProjectionResult {
  const masked = maskLiteralsAndComments(selectText)
  const selMatch = /^\s*SELECT\b/i.exec(masked)
  if (!selMatch) return { ok: false, columns: [], fromItems: [] }
  const listStart = selMatch[0].length
  const depths = computeDepths(masked)
  const spans = findParenSpans(masked)

  const fromRe = /\bFROM\b/gi
  fromRe.lastIndex = listStart
  let fromIdx = -1
  let m: RegExpExecArray | null
  while ((m = fromRe.exec(masked))) {
    if (depths[m.index] === 0) {
      fromIdx = m.index
      break
    }
  }
  const listEnd = fromIdx >= 0 ? fromIdx : masked.length
  const maskedList = masked.slice(listStart, listEnd)
  const originalList = selectText.slice(listStart, listEnd)
  const columns = splitTopLevelCommas(maskedList, originalList)
    .map(parseProjectionItem)
    .filter((c): c is ProjectedColumn => c !== null)

  const fromItems = fromIdx >= 0 ? scanFromItems(masked, depths, spans, fromIdx, masked.length, 0) : []
  return { ok: true, columns, fromItems }
}
