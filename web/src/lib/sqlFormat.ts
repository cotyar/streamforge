/**
 * Dialect-aware SQL formatter for StreamsForge's streaming-SQL grammar (see `.claude/skills/sf-sql`
 * for the full grammar). Text-only and idempotent: it never adds/removes/reorders tokens, only
 * rewrites whitespace (collapsing runs, inserting line breaks + indentation) and the letter-case of
 * recognized dialect keywords/functions. String literals and `--` comments are never touched — see
 * the module doc on `maskLiteralsAndComments` (reused here from sqlScope.ts) for why that's safe:
 * everything this formatter does is driven by a real token stream in which a string/comment is
 * always one atomic token, so its interior is never separately visited.
 *
 * Approach: tokenize, then walk the tokens with a small recursive-descent-flavored line-builder.
 * Paren nesting is classified once up front (via `maskLiteralsAndComments` + a local paren-span
 * scan, mirroring sqlScope.ts's own `findParenSpans`/`isSelectSpan` — not exported there, so
 * duplicated here in ~15 lines) into "select spans" — `(SELECT …)` bodies: CTEs, derived tables,
 * and IN/EXISTS/scalar subqueries — versus ordinary grouping/function-call parens (`SUM(...)`,
 * `TUMBLING(...)`, `UNNEST(...)`). Only select spans are exploded onto their own indented block;
 * everything else is rendered inline on the current line, so `SUM(price * qty)` is never split.
 *
 * Clause keywords (SELECT/FROM/WHERE/GROUP BY/LATEST BY/WINDOW/EMIT/UNION/WITH) each start a new
 * line at the current block's base indent; a JOIN phrase (optional INNER/LEFT/RIGHT/FULL/OUTER/
 * CROSS modifiers followed by JOIN) starts a line one indent deeper; ON one deeper again; and each
 * top-level AND/OR in a WHERE (or similar) clause gets its own line one indent under its clause.
 */
import { maskLiteralsAndComments } from '../components/sqlScope'

type TokKind = 'comment' | 'string' | 'number' | 'word' | 'ws' | 'punct'

interface Tok {
  kind: TokKind
  text: string
  start: number
  end: number
}

// Mirrors the comment/string branches of `maskLiteralsAndComments` exactly (so a literal or
// comment is always captured as one atomic token — its contents are never independently visited,
// let alone reflowed or case-changed) plus number/word/whitespace, a small set of multi-character
// operators (longest-first so `->>` wins over `->`), and a single-char fallback that isolates every
// other punctuation character (parens, comma, dot, `*`, `=`, …) as its own token.
const TOKEN_RE = /(--[^\n]*)|('(?:[^'\\]|\\.)*')|(\b\d+(?:\.\d+)?\b)|([A-Za-z_][A-Za-z0-9_]*)|(\s+)|(->>|->|<=|>=|!=|<>)|([^\sA-Za-z0-9_])/g

function tokenize(text: string): Tok[] {
  const toks: Tok[] = []
  const re = new RegExp(TOKEN_RE)
  let m: RegExpExecArray | null
  while ((m = re.exec(text))) {
    const start = m.index
    const raw = m[0]
    const end = start + raw.length
    let kind: TokKind
    if (m[1] !== undefined) kind = 'comment'
    else if (m[2] !== undefined) kind = 'string'
    else if (m[3] !== undefined) kind = 'number'
    else if (m[4] !== undefined) kind = 'word'
    else if (m[5] !== undefined) kind = 'ws'
    else kind = 'punct'
    toks.push({ kind, text: raw, start, end })
  }
  return toks
}

interface ParenSpan {
  open: number
  close: number
}

/** Tolerates unmatched `(` (closes at end-of-text) exactly like sqlScope.ts's own scanner. */
function findParenSpans(masked: string): ParenSpan[] {
  const spans: ParenSpan[] = []
  const stack: ParenSpan[] = []
  for (let i = 0; i < masked.length; i++) {
    const ch = masked[i]
    if (ch === '(') {
      const span: ParenSpan = { open: i, close: masked.length }
      spans.push(span)
      stack.push(span)
    } else if (ch === ')') {
      const span = stack.pop()
      if (span) span.close = i
    }
  }
  return spans
}

function isSelectSpan(masked: string, span: ParenSpan): boolean {
  return /^\s*SELECT\b/i.test(masked.slice(span.open + 1))
}

// Clause keywords that always start a fresh line at the current block's base indent. GROUP BY and
// LATEST BY are handled as their own two-token compounds (below) rather than listed here, since
// "GROUP"/"LATEST"/"BY" alone aren't clause boundaries.
const CLAUSE_STARTERS = new Set(['SELECT', 'FROM', 'WHERE', 'WINDOW', 'EMIT', 'UNION', 'WITH'])
const JOIN_MODIFIERS = new Set(['INNER', 'LEFT', 'RIGHT', 'FULL', 'OUTER', 'CROSS'])

// Every reserved word / function name in the dialect (sf-sql SKILL.md's grammar), uppercased on
// output wherever it appears — matching sqlScope.ts/SqlEditor.tsx's own precedent of keyword
// recognition by text rather than by proven grammatical position.
const UPPERCASE_WORDS = new Set([
  'WITH', 'SELECT', 'FROM', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 'FULL', 'OUTER', 'CROSS', 'WITHIN', 'ON', 'WHERE',
  'NOT', 'IN', 'EXISTS', 'GROUP', 'BY', 'LATEST', 'WINDOW', 'TUMBLING', 'HOPPING', 'SESSION', 'SIZE', 'ADVANCE', 'GAP',
  'EMIT', 'CHANGES', 'FINAL', 'UNION', 'ALL', 'DISTINCT', 'AS', 'AND', 'OR', 'TRUE', 'FALSE', 'NULL',
  'MILLISECONDS', 'SECONDS', 'MINUTES', 'HOURS', 'CAST', 'UNNEST',
  'STRING', 'DOUBLE', 'LONG', 'BOOL', 'TIMESTAMP', 'TEXT', 'BIGINT', 'INT', 'BOOLEAN', 'PRECISION',
  'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'ABS', 'ROUND', 'UPPER', 'LOWER', 'COALESCE',
  'TO_LONG', 'TO_DOUBLE', 'TO_BOOL', 'TO_TIMESTAMP', 'TO_STRING',
  'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'IF',
  'VAR_SAMP', 'VAR_POP', 'STDDEV_SAMP', 'STDDEV_POP', 'VARIANCE', 'STDDEV', 'MEDIAN', 'PERCENTILE_CONT',
])

const INDENT_UNIT = '  '

/** Formats one SQL statement per the module doc. Never throws on malformed/mid-typing input (an
 *  unterminated paren degrades to "copy the rest inline" rather than crashing). */
export function formatSql(sql: string): string {
  const trimmed = sql.trim()
  if (!trimmed) return sql

  const masked = maskLiteralsAndComments(sql)
  const spans = findParenSpans(masked)
  const closeByOpen = new Map(spans.map((s) => [s.open, s.close]))
  const selectOpens = new Set(spans.filter((s) => isSelectSpan(masked, s)).map((s) => s.open))
  const tokens = tokenize(sql).filter((t) => t.kind !== 'ws')

  /** Formats tokens[startIdx, endIdx). `multiline` false = single-line rendering (used inside
   *  ordinary grouping/function-call parens, recursively, so nested parens there stay inline too). */
  function formatRange(startIdx: number, endIdx: number, indent: number, multiline: boolean): string {
    const lines: string[] = []
    let cur = ''
    let curIndent = indent
    let prevTok: Tok | null = null

    const flush = () => {
      const t = cur.trim()
      if (t !== '') lines.push(INDENT_UNIT.repeat(curIndent) + t)
      cur = ''
    }

    /** Appends already-rendered text for `tok`, inserting a single space unless an adjacency rule
     *  (no space around `.`/`,`/`(`/`)`, no space before a function-call `(`) says otherwise. */
    const append = (renderedText: string, tok: Tok, adjacentToPrev: boolean) => {
      let needSpace = cur.length > 0
      if (renderedText === ',' || renderedText === '.' || renderedText === ')') needSpace = false
      if (prevTok && (prevTok.text === '(' || prevTok.text === '.')) needSpace = false
      if (renderedText === '(' && adjacentToPrev) needSpace = false
      cur += (needSpace ? ' ' : '') + renderedText
      prevTok = tok
    }

    let i = startIdx
    while (i < endIdx) {
      const tok = tokens[i]

      if (tok.kind === 'comment') {
        // `--` runs to end-of-line by construction, so whatever follows must start a new line
        // anyway — emit the comment on the current line, then force a break.
        append(tok.text, tok, false)
        flush()
        curIndent = indent
        i++
        continue
      }

      if (tok.kind === 'punct' && tok.text === '(') {
        const close = closeByOpen.get(tok.start) ?? masked.length
        const isSelect = multiline && selectOpens.has(tok.start)
        let j = i + 1
        while (j < tokens.length && tokens[j].start < close) j++
        const innerText = formatRange(i + 1, j, isSelect ? indent + 1 : 0, isSelect)
        const closeTok = tokens[j] && tokens[j].start === close ? tokens[j] : null
        const adjacent = !!prevTok && prevTok.kind === 'word' && prevTok.end === tok.start

        if (isSelect) {
          append('(', tok, adjacent)
          flush()
          if (innerText) lines.push(innerText)
          curIndent = indent
          cur = ')'
          prevTok = closeTok ?? tok
        } else {
          const needSpace = cur.length > 0 && !adjacent && !(prevTok && (prevTok.text === '(' || prevTok.text === '.'))
          cur += (needSpace ? ' ' : '') + '(' + innerText + ')'
          prevTok = closeTok ?? tok
        }
        i = closeTok ? j + 1 : j
        continue
      }

      if (tok.kind === 'word') {
        const upper = tok.text.toUpperCase()
        const rendered = UPPERCASE_WORDS.has(upper) ? upper : tok.text

        if (multiline) {
          const next = tokens[i + 1]
          const nextUpper = next && next.kind === 'word' ? next.text.toUpperCase() : null

          if ((upper === 'GROUP' || upper === 'LATEST') && nextUpper === 'BY') {
            flush()
            curIndent = indent
            append(upper, tok, false)
            append('BY', next, false)
            i += 2
            continue
          }

          if (CLAUSE_STARTERS.has(upper)) {
            flush()
            curIndent = indent
            append(upper, tok, false)
            i++
            continue
          }

          if (upper === 'JOIN' || JOIN_MODIFIERS.has(upper)) {
            // Gather the whole join-type phrase (zero or more modifiers, then JOIN) so e.g. "FULL
            // OUTER JOIN" lands on one line rather than being split token-by-token.
            let end = i
            let sawJoin = upper === 'JOIN'
            while (!sawJoin && end + 1 < endIdx) {
              const nt = tokens[end + 1]
              if (nt.kind !== 'word') break
              const nu = nt.text.toUpperCase()
              end++
              if (nu === 'JOIN') {
                sawJoin = true
                break
              }
              if (!JOIN_MODIFIERS.has(nu)) break
            }
            if (sawJoin) {
              flush()
              curIndent = indent + 1
              for (let p = i; p <= end; p++) append(tokens[p].text.toUpperCase(), tokens[p], false)
              i = end + 1
              continue
            }
          }

          if (upper === 'ON') {
            flush()
            curIndent = indent + 2
            append('ON', tok, false)
            i++
            continue
          }

          if (upper === 'AND' || upper === 'OR') {
            flush()
            curIndent = indent + 1
            append(upper, tok, false)
            i++
            continue
          }
        }

        append(rendered, tok, false)
        i++
        continue
      }

      append(tok.text, tok, false)
      i++
    }

    flush()
    return lines.join('\n')
  }

  return formatRange(0, tokens.length, 0, true)
}
