import { useMemo, useRef } from 'react'
import type { ChangeEvent, KeyboardEvent, UIEvent } from 'react'
import type { SqlDiagnostic } from '../api/types'

const KEYWORDS = new Set([
  'SELECT', 'FROM', 'WHERE', 'GROUP', 'BY', 'WINDOW', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 'FULL', 'OUTER',
  'CROSS', 'ON', 'WITHIN', 'AS', 'EMIT', 'CHANGES', 'FINAL', 'TUMBLING', 'HOPPING', 'SESSION', 'SIZE',
  'ADVANCE', 'GAP', 'AND', 'OR', 'NOT', 'TRUE', 'FALSE', 'NULL', 'SECONDS', 'MILLISECONDS', 'MINUTES', 'HOURS',
])

const FUNCTIONS = new Set(['COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'ABS', 'ROUND', 'UPPER', 'LOWER', 'COALESCE'])

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
    else kind = 'punct'
    tokens.push({ text, start, end, kind })
  }
  return tokens
}

const KIND_CLASS: Record<TokenKind, string> = {
  comment: 'text-gray-500 italic',
  string: 'text-amber-400',
  number: 'text-violet-400',
  keyword: 'text-sky-400',
  function: 'text-teal-400',
  identifier: 'text-gray-200',
  whitespace: '',
  punct: 'text-gray-500',
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
  Error: 'underline decoration-wavy decoration-2 decoration-[var(--sf-bad)] underline-offset-4',
  Warning: 'underline decoration-wavy decoration-2 decoration-[var(--sf-warn)] underline-offset-4',
}

export function SqlEditor({
  value,
  onChange,
  diagnostics = [],
  readOnly = false,
  minRows = 14,
  placeholder,
}: {
  value: string
  onChange: (value: string) => void
  diagnostics?: SqlDiagnostic[]
  readOnly?: boolean
  minRows?: number
  placeholder?: string
}) {
  const preRef = useRef<HTMLPreElement>(null)
  const textareaRef = useRef<HTMLTextAreaElement>(null)

  const tokens = useMemo(() => tokenize(value), [value])
  const ranges = useMemo(() => diagnosticRanges(value, diagnostics), [value, diagnostics])

  function handleScroll(e: UIEvent<HTMLTextAreaElement>) {
    if (preRef.current) {
      preRef.current.scrollTop = e.currentTarget.scrollTop
      preRef.current.scrollLeft = e.currentTarget.scrollLeft
    }
  }

  function handleChange(e: ChangeEvent<HTMLTextAreaElement>) {
    onChange(e.target.value)
  }

  function handleKeyDown(e: KeyboardEvent<HTMLTextAreaElement>) {
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

  return (
    <div className="relative overflow-hidden rounded-lg border border-[var(--sf-border)] bg-[#0d1420] font-mono text-[13px] leading-6">
      <pre
        ref={preRef}
        aria-hidden
        className="pointer-events-none absolute inset-0 m-0 overflow-auto whitespace-pre p-3"
      >
        {tokens.length === 0 && <span className="text-gray-600">{placeholder}</span>}
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
      <textarea
        ref={textareaRef}
        value={value}
        onChange={handleChange}
        onScroll={handleScroll}
        onKeyDown={handleKeyDown}
        readOnly={readOnly}
        spellCheck={false}
        placeholder={placeholder}
        rows={minRows}
        className="relative w-full resize-none overflow-auto whitespace-pre bg-transparent p-3 text-transparent caret-white outline-none placeholder:text-transparent"
        style={{ fontFamily: 'inherit' }}
      />
    </div>
  )
}
