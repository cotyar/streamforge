import { useEffect, useRef, useState } from 'react'
import type { KeyboardEvent } from 'react'
import { ChevronRight, RotateCcw, Send, Sparkles, TriangleAlert, Wrench } from 'lucide-react'
import { chatApi } from '../api/chat'
import { ApiError } from '../api/client'
import type { ChatMessage, ChatToolCallDto } from '../api/types'
import { Topbar } from '../components/Topbar'
import { cn } from '@/lib/utils'
import { Button } from '@/components/ui/button'
import { Textarea } from '@/components/ui/textarea'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Spinner } from '@/components/ui/spinner'

// ============================================================================
// AI control chat (plan 007 D-D/W2A) — SPA over the shared POST /api/chat backend (Editor
// policy). Server is stateless: on every send we resend the FULL text history (role/content
// only — tool traces never round-trip). Conversation lives in component state only; navigating
// away loses it, by design (no persistence requested).
// ============================================================================

const EXAMPLE_PROMPTS = [
  'List all sources',
  'Create a demo source at 5 events/sec',
  'Which tables are running?',
  'Validate a SQL pipeline that counts trades per symbol',
]

interface Turn {
  role: 'user' | 'assistant'
  content: string
  toolCalls?: ChatToolCallDto[]
  model?: string
}

/** Pretty-print + visually truncate a tool call's opaque JSON payload — the trace is the audit
 * trail for a control surface over live streaming jobs, so it must stay legible even when a
 * result is large (the server already hard-truncates results to ~2KB; this just keeps very long
 * JSON from blowing out the page before the user asks to see it all). */
function JsonBlock({ value }: { value: unknown }) {
  const [expanded, setExpanded] = useState(false)
  const text = (() => {
    try {
      return JSON.stringify(value, null, 2)
    } catch {
      return String(value)
    }
  })()
  const lines = text.split('\n')
  const isLong = lines.length > 12
  const shown = expanded || !isLong ? text : lines.slice(0, 12).join('\n') + '\n…'

  return (
    <div className="rounded-md border border-border bg-input/20">
      <pre className="max-w-full overflow-x-auto whitespace-pre-wrap break-words p-2 font-mono text-[11px] leading-5 text-foreground">
        {shown}
      </pre>
      {isLong && (
        <button
          type="button"
          onClick={() => setExpanded((e) => !e)}
          className="w-full border-t border-border px-2 py-1 text-left text-[10px] font-medium text-muted-foreground hover:text-foreground"
        >
          {expanded ? 'Show less' : `Show all ${lines.length} lines`}
        </button>
      )}
    </div>
  )
}

function ToolCallTrace({ toolCalls }: { toolCalls: ChatToolCallDto[] }) {
  const [open, setOpen] = useState(false)
  if (toolCalls.length === 0) return null
  return (
    <div className="mt-1.5 max-w-[85%] rounded-lg border border-border bg-card">
      <button
        type="button"
        onClick={() => setOpen((o) => !o)}
        className="flex w-full items-center gap-1.5 px-2.5 py-1.5 text-left text-[11px] font-medium text-muted-foreground hover:text-foreground"
      >
        <ChevronRight className={cn('size-3 shrink-0 transition-transform', open && 'rotate-90')} />
        <Wrench className="size-3 shrink-0" />
        {toolCalls.length} tool call{toolCalls.length === 1 ? '' : 's'}
      </button>
      {open && (
        <div className="flex flex-col gap-2 border-t border-border px-2.5 py-2">
          {toolCalls.map((tc, i) => (
            <div key={i} className="flex flex-col gap-1">
              <span className="font-mono text-[11px] font-semibold text-foreground">{tc.name}</span>
              <div>
                <p className="mb-0.5 text-[10px] font-medium uppercase tracking-wide text-muted-foreground">Input</p>
                <JsonBlock value={tc.input} />
              </div>
              <div>
                <p className="mb-0.5 text-[10px] font-medium uppercase tracking-wide text-muted-foreground">Result</p>
                <JsonBlock value={tc.result} />
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

function Bubble({ turn }: { turn: Turn }) {
  const isUser = turn.role === 'user'
  return (
    <div className={cn('flex flex-col', isUser ? 'items-end' : 'items-start')}>
      <div
        className={cn(
          'max-w-[85%] whitespace-pre-wrap rounded-2xl px-3.5 py-2 text-sm',
          isUser ? 'bg-primary text-primary-foreground' : 'bg-muted text-foreground',
        )}
      >
        {turn.content}
      </div>
      {!isUser && turn.toolCalls && <ToolCallTrace toolCalls={turn.toolCalls} />}
    </div>
  )
}

function ThinkingBubble() {
  return (
    <div className="flex items-center gap-2 rounded-2xl bg-muted px-3.5 py-2 text-sm text-muted-foreground">
      <Spinner className="size-3.5" />
      Thinking…
    </div>
  )
}

export function ChatPage() {
  const [history, setHistory] = useState<Turn[]>([])
  const [input, setInput] = useState('')
  const [sending, setSending] = useState(false)
  const [lastError, setLastError] = useState<string | null>(null)
  const [notConfigured, setNotConfigured] = useState<string | null>(null)
  const bottomRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth', block: 'end' })
  }, [history, sending, lastError, notConfigured])

  async function runTurn(nextHistory: Turn[]) {
    setSending(true)
    setLastError(null)
    try {
      const wire: ChatMessage[] = nextHistory.map(({ role, content }) => ({ role, content }))
      const res = await chatApi.send(wire)
      setHistory([...nextHistory, { role: 'assistant', content: res.reply, toolCalls: res.toolCalls, model: res.model }])
      setNotConfigured(null)
    } catch (err) {
      if (err instanceof ApiError && err.status === 503) {
        setNotConfigured(err.message)
      } else {
        setLastError(err instanceof Error ? err.message : 'Failed to reach the AI chat backend.')
      }
    } finally {
      setSending(false)
    }
  }

  function send(text: string) {
    const trimmed = text.trim()
    if (!trimmed || sending) return
    setInput('')
    const nextHistory: Turn[] = [...history, { role: 'user', content: trimmed }]
    setHistory(nextHistory)
    void runTurn(nextHistory)
  }

  function retry() {
    if (sending || history.length === 0) return
    void runTurn(history)
  }

  function handleKeyDown(e: KeyboardEvent<HTMLTextAreaElement>) {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      send(input)
    }
  }

  return (
    <div className="flex flex-col">
      <Topbar title="AI Control Chat" subtitle="Ask Gemini to inspect and manage sources, pipelines, and tables — every tool call is logged below the reply." />

      <div className="mx-auto flex w-full max-w-3xl flex-1 flex-col gap-4 p-8">
        {history.length === 0 && (
          <div className="flex flex-col items-center gap-4 rounded-xl border border-dashed border-border p-8 text-center">
            <Sparkles className="size-6 text-muted-foreground" />
            <div>
              <p className="text-sm font-medium text-foreground">Start a conversation</p>
              <p className="mt-1 text-xs text-muted-foreground">
                The assistant can list, create, and manage sources/pipelines/tables on your behalf.
              </p>
            </div>
            <div className="flex flex-wrap justify-center gap-2">
              {EXAMPLE_PROMPTS.map((p) => (
                <Button key={p} type="button" variant="outline" size="sm" onClick={() => send(p)} disabled={sending}>
                  {p}
                </Button>
              ))}
            </div>
          </div>
        )}

        <div className="flex flex-col gap-3">
          {history.map((turn, i) => (
            <Bubble key={i} turn={turn} />
          ))}
          {sending && <ThinkingBubble />}
          {lastError && (
            <Alert variant="destructive">
              <TriangleAlert />
              <AlertTitle>The request failed</AlertTitle>
              <AlertDescription className="flex flex-col gap-2">
                <span>{lastError}</span>
                <Button type="button" variant="outline" size="sm" className="self-start" onClick={retry} disabled={sending}>
                  <RotateCcw data-icon="inline-start" /> Retry
                </Button>
              </AlertDescription>
            </Alert>
          )}
          {notConfigured && (
            <Alert>
              <TriangleAlert />
              <AlertTitle>AI chat needs setup</AlertTitle>
              <AlertDescription className="flex flex-col gap-2">
                <span>{notConfigured}</span>
                <span>
                  Run <code className="rounded bg-muted px-1 py-0.5 font-mono text-foreground">export GEMINI_API_KEY=&lt;key&gt;</code> (or
                  set <code className="rounded bg-muted px-1 py-0.5 font-mono text-foreground">Gemini:ApiKey</code>) and restart the host,
                  then try again.
                </span>
                <Button type="button" variant="outline" size="sm" className="self-start" onClick={retry} disabled={sending}>
                  <RotateCcw data-icon="inline-start" /> Try again
                </Button>
              </AlertDescription>
            </Alert>
          )}
          <div ref={bottomRef} />
        </div>
      </div>

      <div className="sticky bottom-0 border-t border-border bg-background/95 px-8 py-4 backdrop-blur">
        <div className="mx-auto flex w-full max-w-3xl items-end gap-2">
          <Textarea
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Ask about sources, pipelines, or tables… (Enter to send, Shift+Enter for a new line)"
            disabled={sending}
            className="min-h-11 flex-1 resize-none"
            rows={1}
          />
          <Button type="button" size="icon" onClick={() => send(input)} disabled={sending || !input.trim()} aria-label="Send">
            {sending ? <Spinner /> : <Send />}
          </Button>
        </div>
      </div>
    </div>
  )
}
