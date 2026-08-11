import { useState } from 'react'
import type { FormEvent } from 'react'
import { ingestApi } from '@/api/ingest'
import type { IngestPushResult } from '@/api/ingest'
import type { SourceDefinition } from '@/api/types'
import { useIngestStatus } from '@/hooks/useIngestStatus'
import { relativeFromNow } from './ConnectorStatusBadge'
import { IngestKeysPanel } from './IngestKeysPanel'
import { RoleGate } from '@/components/RoleGate'
import { cn } from '@/lib/utils'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Textarea } from '@/components/ui/textarea'
import { Alert, AlertDescription } from '@/components/ui/alert'

function Stat({ label, value, className }: { label: string; value: string; className?: string }) {
  return (
    <div className={cn('flex flex-col gap-0.5 rounded-lg border border-border bg-background/60 px-2.5 py-1.5', className)}>
      <span className="text-[10px] font-medium uppercase tracking-wide text-muted-foreground">{label}</span>
      <span className="font-mono text-sm font-semibold text-foreground">{value}</span>
    </div>
  )
}

const SAMPLE_PLACEHOLDER = '{ "field": "value" }\n\nor an array: [{ "field": "value" }, { "field": "value2" }]'

/**
 * Test-push panel: a JSON textarea + Send button against POST /api/sources/{name}/events, with
 * honest rendering of whatever comes back — 202 accepted/dropped/invalid counts, or the 400/409/
 * 413/429 error body (retryAfterMs + rowErrors on the ones that carry them). Editor-gated like the
 * rest of the SPA's write actions.
 */
function TestPushPanel({ name }: { name: string }) {
  const [text, setText] = useState('')
  const [partial, setPartial] = useState(false)
  const [sending, setSending] = useState(false)
  const [result, setResult] = useState<IngestPushResult | null>(null)
  const [error, setError] = useState<string | null>(null)

  async function handleSend(e: FormEvent) {
    e.preventDefault()
    setError(null)
    setResult(null)

    let parsed: unknown
    try {
      parsed = JSON.parse(text)
    } catch {
      setError('Not valid JSON — paste a single event object or an array of event objects.')
      return
    }
    const events = Array.isArray(parsed) ? parsed : [parsed]
    if (events.some((item) => typeof item !== 'object' || item === null)) {
      setError('Every event must be a JSON object.')
      return
    }

    setSending(true)
    try {
      const r = await ingestApi.pushEvents(name, { events: events as Record<string, unknown>[], partial })
      setResult(r)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to send events.')
    } finally {
      setSending(false)
    }
  }

  return (
    <RoleGate min="Editor">
      <form onSubmit={handleSend} className="flex flex-col gap-2 border-t border-border pt-3">
        <p className="text-[10px] font-medium uppercase tracking-wide text-muted-foreground">Send a test event</p>
        <Textarea
          value={text}
          onChange={(e) => setText(e.target.value)}
          placeholder={SAMPLE_PLACEHOLDER}
          rows={4}
          className="font-mono text-xs"
          disabled={sending}
        />
        <div className="flex items-center justify-between gap-2">
          <label className="flex items-center gap-1.5 text-[11px] text-muted-foreground">
            <input type="checkbox" checked={partial} onChange={(e) => setPartial(e.target.checked)} disabled={sending} />
            Admit valid rows even if some are invalid
          </label>
          <Button type="submit" size="sm" disabled={sending || !text.trim()}>
            {sending ? 'Sending…' : 'Send'}
          </Button>
        </div>

        {error && (
          <Alert variant="destructive">
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        )}

        {result && result.accepted && (
          <div className="flex flex-wrap items-center gap-2 rounded-lg border border-border bg-background/60 px-2.5 py-1.5 text-[11px]">
            <Badge>202 accepted</Badge>
            <span>
              <span className="font-mono text-foreground">{result.body.accepted}</span> accepted
            </span>
            <span>
              <span className="font-mono text-foreground">{result.body.dropped}</span> dropped
            </span>
            <span>
              <span className="font-mono text-foreground">{result.body.invalid}</span> invalid
            </span>
            <span className="text-muted-foreground">
              buffer <span className="font-mono text-foreground">{result.body.depthRows}</span>/
              <span className="font-mono text-foreground">{result.body.capacityRows}</span>
            </span>
          </div>
        )}

        {result && !result.accepted && (
          <Alert variant="destructive">
            <AlertDescription className="flex flex-col gap-1">
              <span>
                {result.status} — {result.body.error}
              </span>
              {result.status === 429 && result.body.retryAfterMs > 0 && (
                <span>Retry after {result.body.retryAfterMs}ms.</span>
              )}
              {result.body.rowErrors.length > 0 && (
                <ul className="flex flex-col gap-0.5">
                  {result.body.rowErrors.map((e, i) => (
                    <li key={i}>• {e}</li>
                  ))}
                </ul>
              )}
            </AlertDescription>
          </Alert>
        )}
      </form>
    </RoleGate>
  )
}

/**
 * Ingress card for an ingest-kind source: buffer depth against capacity, the active policy, and
 * the push counters from IngestStatusResponse — 2 s polled (useIngestStatus), the page's existing
 * convention; no new SignalR group. `downstreamDropped` is called out on its own — a SECOND loss
 * point (rows the transport dropped after we already published them), never merged into the
 * pre-publish drop/reject counters above it.
 */
export function IngestCard({ source }: { source: SourceDefinition }) {
  const status = useIngestStatus(source.name)

  if (!status) {
    return <p className="text-xs text-muted-foreground">Ingress status unavailable — the source may not be running yet.</p>
  }

  const depthPct = status.capacityRows > 0 ? Math.min(100, Math.round((status.depthRows / status.capacityRows) * 100)) : 0
  const nearCapacity = depthPct >= 90
  const aggregated = status.aggregated ?? false

  return (
    <div className="flex flex-col gap-3">
      <div className="flex items-center justify-between gap-2">
        <div className="flex items-center gap-2">
          <Badge variant="outline">{status.policy}</Badge>
          <span className="text-xs text-muted-foreground">
            buffer <span className="font-mono text-foreground">{status.depthRows}</span>/
            <span className="font-mono text-foreground">{status.capacityRows}</span> rows
          </span>
        </div>
        {status.lastPushMs > 0 && (
          <span className="text-[11px] text-muted-foreground">last push {relativeFromNow(status.lastPushMs)}</span>
        )}
      </div>

      <div className="h-1.5 w-full overflow-hidden rounded-full bg-muted">
        <div
          className={cn('h-full rounded-full transition-all', nearCapacity ? 'bg-destructive' : 'bg-primary')}
          style={{ width: `${depthPct}%` }}
        />
      </div>

      {/* Plan 009 A1.3: the buffer is process memory, so under more than one replica every counter
       * below is a per-replica view — say so plainly rather than presenting it as a global total. */}
      <div className="flex items-center justify-between gap-2 text-[11px] text-muted-foreground">
        <span>{aggregated ? 'Aggregated across all replicas.' : 'This replica only — not aggregated cluster-wide.'}</span>
        {status.instanceId && <span className="truncate font-mono" title={status.instanceId}>{status.instanceId}</span>}
      </div>

      <div className="grid grid-cols-2 gap-2 sm:grid-cols-5">
        <Stat label="Accepted" value={status.totalAccepted.toLocaleString()} />
        <Stat label="Rejected" value={status.totalRejected.toLocaleString()} />
        <Stat label="Dropped" value={status.totalDropped.toLocaleString()} />
        <Stat label="Invalid" value={status.totalInvalid.toLocaleString()} />
        <Stat label="Duplicate" value={(status.totalDuplicate ?? 0).toLocaleString()} />
      </div>
      <p className="-mt-2 text-[11px] text-muted-foreground">
        Three distinct reasons a row didn't land: dropped is capacity, invalid is coercion, duplicate is row-level
        dedup — none share a counter.
      </p>

      <div className="flex items-center justify-between gap-2 rounded-lg border border-warning/40 bg-warning/10 px-2.5 py-1.5">
        <div className="flex flex-col">
          <span className="text-[10px] font-medium uppercase tracking-wide text-warning">Downstream dropped</span>
          <span className="text-[11px] text-muted-foreground">
            A second, separate loss point — rows already published that the transport then dropped. Not counted above.
          </span>
        </div>
        <span className="font-mono text-sm font-semibold text-warning">{status.downstreamDropped.toLocaleString()}</span>
      </div>

      <p className="text-[11px] text-muted-foreground">
        <span className="font-mono text-foreground">{status.totalPublished.toLocaleString()}</span> published downstream ·{' '}
        max batch <span className="font-mono text-foreground">{status.maxBatchRows}</span> rows
      </p>

      <IngestKeysPanel sourceName={source.name} />

      <TestPushPanel name={source.name} />
    </div>
  )
}
