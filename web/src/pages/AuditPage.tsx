import { useCallback, useEffect, useState } from 'react'
import type { FormEvent } from 'react'
import { AlertTriangle, ChevronLeft, ChevronRight, EyeOff, RefreshCw, ScrollText, Search } from 'lucide-react'
import { toast } from 'sonner'
import { auditApi } from '../api/audit'
import type { AuditEntry, AuditPageResponse } from '../api/types'
import { useAuth } from '../api/auth'
import { Topbar } from '../components/Topbar'
import { ChangeDiff } from '../components/audit/ChangeDiff'
import { Card } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import { Switch } from '@/components/ui/switch'
import { Label } from '@/components/ui/label'
import { Skeleton } from '@/components/ui/skeleton'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { Select, SelectContent, SelectGroup, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from '@/components/ui/empty'
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog'

const PAGE_SIZES = [50, 200, 500]

/** `yyyyMMdd` (UTC) — a storage key, not a filter, which is why the server rejects anything else. */
function formatDay(day: string): string {
  if (day.length !== 8) return day
  const iso = `${day.slice(0, 4)}-${day.slice(4, 6)}-${day.slice(6, 8)}`
  const d = new Date(`${iso}T00:00:00Z`)
  return Number.isNaN(d.getTime())
    ? iso
    : `${iso} · ${d.toLocaleDateString(undefined, { weekday: 'short', timeZone: 'UTC' })}`
}

function formatTime(ms: number): string {
  return new Date(ms).toLocaleTimeString(undefined, { hour12: false }) + '.' + String(ms % 1000).padStart(3, '0')
}

function outcomeVariant(outcome: string): 'default' | 'secondary' | 'destructive' | 'outline' {
  switch (outcome) {
    case 'denied':
    case 'failed':
      return 'destructive'
    case 'requires-approval':
      return 'default'
    case 'executed':
      return 'secondary'
    default:
      return 'outline'
  }
}

/** `actor` alone is a lie when the chat acted — the model is the actor, the human whose token it
 *  carried is `onBehalfOf`, and this field exists precisely so the two never collapse. */
function Actor({ entry }: { entry: AuditEntry }) {
  return (
    <div className="flex flex-col">
      <span className="font-medium text-foreground">{entry.actor}</span>
      {entry.onBehalfOf && (
        <span className="text-xs text-muted-foreground">on behalf of {entry.onBehalfOf}</span>
      )}
    </div>
  )
}

function EntryDialog({
  entry,
  page,
  onClose,
}: {
  entry: AuditEntry
  page: AuditPageResponse
  onClose: () => void
}) {
  return (
    <Dialog open onOpenChange={(open) => !open && onClose()}>
      <DialogContent className="sm:max-w-3xl">
        <DialogHeader>
          <DialogTitle className="font-mono text-sm">{entry.action}</DialogTitle>
        </DialogHeader>

        <dl className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-1 text-sm">
          <dt className="text-muted-foreground">When</dt>
          <dd className="text-foreground">{new Date(entry.atMs).toLocaleString()}</dd>
          <dt className="text-muted-foreground">Actor</dt>
          <dd className="text-foreground">
            {entry.actor}
            {entry.onBehalfOf && <span className="text-muted-foreground"> on behalf of {entry.onBehalfOf}</span>}
          </dd>
          <dt className="text-muted-foreground">Scope</dt>
          <dd className="font-mono text-foreground">{entry.scope}</dd>
          <dt className="text-muted-foreground">Outcome</dt>
          <dd>
            <Badge variant={outcomeVariant(entry.outcome)}>{entry.outcome}</Badge>
          </dd>
          <dt className="text-muted-foreground">Origin</dt>
          <dd className="text-foreground">{entry.origin}</dd>
          {entry.approvalId && (
            <>
              <dt className="text-muted-foreground">Approval</dt>
              <dd className="font-mono text-xs text-foreground">{entry.approvalId}</dd>
            </>
          )}
          {entry.detail && (
            <>
              <dt className="text-muted-foreground">Detail</dt>
              <dd className="text-foreground">{entry.detail}</dd>
            </>
          )}
          <dt className="text-muted-foreground">Id</dt>
          <dd className="font-mono text-xs text-muted-foreground">{entry.id}</dd>
        </dl>

        <div className="border-t border-border pt-4">
          <ChangeDiff entry={entry} included={page.changesIncluded} withheld={page.changesWithheld} />
        </div>
      </DialogContent>
    </Dialog>
  )
}

export function AuditPage() {
  const { can } = useAuth()
  // The opt-in is two-sided: the request has to ask AND the caller has to hold access.read. Offering a
  // toggle that always answers "withheld" would be worse than not offering it, so it is gated here on
  // the same entitlement the server checks.
  const mayReadChanges = can('access.read')

  const [days, setDays] = useState<string[] | null>(null)
  const [day, setDay] = useState<string | null>(null)
  const [page, setPage] = useState<AuditPageResponse | null>(null)
  const [loading, setLoading] = useState(false)

  // Applied filters vs the boxes — the grammar is exact-actor + action-PREFIX and nothing more, so
  // there is nothing to debounce into a live search; the query runs when it is asked to.
  const [actorInput, setActorInput] = useState('')
  const [actionInput, setActionInput] = useState('')
  const [filters, setFilters] = useState<{ actor: string; action: string }>({ actor: '', action: '' })
  const [limit, setLimit] = useState(200)
  const [offset, setOffset] = useState(0)
  const [includeChanges, setIncludeChanges] = useState(false)
  const [selected, setSelected] = useState<AuditEntry | null>(null)

  useEffect(() => {
    auditApi
      .days()
      .then((d) => {
        setDays(d)
        setDay((current) => current ?? d[0] ?? null)
      })
      .catch((err: unknown) => {
        setDays([])
        toast.error(err instanceof Error ? err.message : 'Failed to list audit days.')
      })
  }, [])

  const load = useCallback(() => {
    if (!day) return
    setLoading(true)
    auditApi
      .page(day, {
        actor: filters.actor || undefined,
        action: filters.action || undefined,
        limit,
        offset,
        includeChanges: includeChanges && mayReadChanges,
      })
      .then(setPage)
      .catch((err: unknown) => {
        setPage(null)
        toast.error(err instanceof Error ? err.message : 'Failed to read the audit log.')
      })
      .finally(() => setLoading(false))
  }, [day, filters, limit, offset, includeChanges, mayReadChanges])

  useEffect(() => {
    load()
  }, [load])

  function applyFilters(e: FormEvent) {
    e.preventDefault()
    setOffset(0)
    setFilters({ actor: actorInput.trim(), action: actionInput.trim() })
  }

  const entries = page?.entries ?? []
  const total = page?.total ?? 0
  const hasPrev = offset > 0
  const hasNext = offset + entries.length < total

  return (
    <div>
      <Topbar
        title="Audit"
        subtitle="Every decision and mutation the platform recorded, one UTC day at a time"
        action={
          <Button variant="outline" onClick={load} disabled={!day || loading}>
            <RefreshCw data-icon="inline-start" /> Refresh
          </Button>
        }
      />

      <div className="flex flex-col gap-4 p-8">
        <form onSubmit={applyFilters} className="flex flex-wrap items-end gap-3">
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="audit-day">Day (UTC)</Label>
            <Select
              value={day ?? ''}
              onValueChange={(v) => {
                setOffset(0)
                setDay(v)
              }}
              disabled={!days || days.length === 0}
            >
              <SelectTrigger id="audit-day" className="w-56">
                <SelectValue placeholder={days === null ? 'Loading…' : 'No days recorded'} />
              </SelectTrigger>
              <SelectContent>
                <SelectGroup>
                  {(days ?? []).map((d) => (
                    <SelectItem key={d} value={d}>
                      {formatDay(d)}
                    </SelectItem>
                  ))}
                </SelectGroup>
              </SelectContent>
            </Select>
          </div>

          <div className="flex flex-col gap-1.5">
            <Label htmlFor="audit-actor">Actor (exact)</Label>
            <Input
              id="audit-actor"
              value={actorInput}
              onChange={(e) => setActorInput(e.target.value)}
              placeholder="alice"
              className="w-44"
            />
          </div>

          <div className="flex flex-col gap-1.5">
            <Label htmlFor="audit-action">Action (prefix)</Label>
            <Input
              id="audit-action"
              value={actionInput}
              onChange={(e) => setActionInput(e.target.value)}
              placeholder="source."
              className="w-44"
            />
          </div>

          <Button type="submit" variant="outline">
            <Search data-icon="inline-start" /> Apply
          </Button>

          <div className="flex flex-col gap-1.5">
            <Label htmlFor="audit-limit">Page size</Label>
            <Select
              value={String(limit)}
              onValueChange={(v) => {
                setOffset(0)
                setLimit(Number(v))
              }}
            >
              <SelectTrigger id="audit-limit" className="w-24">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectGroup>
                  {PAGE_SIZES.map((n) => (
                    <SelectItem key={n} value={String(n)}>
                      {n}
                    </SelectItem>
                  ))}
                </SelectGroup>
              </SelectContent>
            </Select>
          </div>

          {mayReadChanges && (
            <div className="flex items-center gap-2 pb-2">
              <Switch id="audit-changes" checked={includeChanges} onCheckedChange={setIncludeChanges} />
              <Label htmlFor="audit-changes" className="font-normal">
                Include before/after
              </Label>
            </div>
          )}
        </form>

        {/* The whole reason the day shard's drop-oldest cap is honest. It is persisted and never reset,
            so it is a standing statement about the day, not about this page — a footnote here would
            undo the mechanism it reports. */}
        {page && page.truncated > 0 && (
          <Alert variant="destructive">
            <AlertTriangle />
            <AlertTitle>
              {page.truncated.toLocaleString()} {page.truncated === 1 ? 'entry was' : 'entries were'} dropped from{' '}
              {page.day}
            </AlertTitle>
            <AlertDescription>
              This day hit the <code className="font-mono">Audit:MaxEntriesPerDay</code> cap and the oldest entries were
              discarded to make room. What you see below is not the whole day, and the missing rows are the earliest
              ones. Raise the cap, or export days you need to keep.
            </AlertDescription>
          </Alert>
        )}

        {page && !page.changesIncluded && page.changesWithheld > 0 && (
          <Alert>
            <EyeOff />
            <AlertTitle>
              {page.changesWithheld.toLocaleString()} {page.changesWithheld === 1 ? 'row carries' : 'rows carry'} a
              before/after payload you are not being shown
            </AlertTitle>
            <AlertDescription>
              {mayReadChanges
                ? 'Turn on "Include before/after" to request them. They can carry stored configuration, so they are opt-in.'
                : 'Releasing them needs the access.read entitlement in addition to audit.read.'}
            </AlertDescription>
          </Alert>
        )}

        {loading && page === null ? (
          <div className="flex flex-col gap-2">
            {Array.from({ length: 6 }).map((_, i) => (
              <Skeleton key={i} className="h-12 w-full" />
            ))}
          </div>
        ) : days !== null && days.length === 0 ? (
          <Empty className="border border-dashed">
            <EmptyHeader>
              <EmptyMedia variant="icon">
                <ScrollText />
              </EmptyMedia>
              <EmptyTitle>Nothing recorded yet</EmptyTitle>
              <EmptyDescription>
                The log holds refused decisions and catalog mutations. An instance nobody has changed has nothing to
                show.
              </EmptyDescription>
            </EmptyHeader>
          </Empty>
        ) : entries.length === 0 ? (
          <Empty className="border border-dashed">
            <EmptyHeader>
              <EmptyMedia variant="icon">
                <ScrollText />
              </EmptyMedia>
              <EmptyTitle>No entries match</EmptyTitle>
              <EmptyDescription>
                Actor is matched exactly and action by prefix — <code className="font-mono">source</code> finds{' '}
                <code className="font-mono">source.write</code>, <code className="font-mono">write</code> finds nothing.
              </EmptyDescription>
            </EmptyHeader>
          </Empty>
        ) : (
          <Card className="overflow-hidden py-0">
            <Table>
              <TableHeader>
                <TableRow className="hover:bg-transparent">
                  <TableHead className="w-32">Time</TableHead>
                  <TableHead className="w-44">Actor</TableHead>
                  <TableHead className="w-48">Action</TableHead>
                  <TableHead>Scope</TableHead>
                  <TableHead className="w-32">Outcome</TableHead>
                  <TableHead className="w-20">Origin</TableHead>
                  <TableHead>Detail</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {entries.map((e) => (
                  <TableRow key={e.id} className="cursor-pointer" onClick={() => setSelected(e)}>
                    <TableCell className="font-mono text-xs text-muted-foreground">{formatTime(e.atMs)}</TableCell>
                    <TableCell>
                      <Actor entry={e} />
                    </TableCell>
                    <TableCell className="font-mono text-xs text-foreground/90">{e.action}</TableCell>
                    <TableCell className="font-mono text-xs text-foreground/70">{e.scope}</TableCell>
                    <TableCell>
                      <Badge variant={outcomeVariant(e.outcome)}>{e.outcome}</Badge>
                    </TableCell>
                    <TableCell className="text-xs text-muted-foreground">{e.origin}</TableCell>
                    <TableCell className="max-w-md truncate text-xs text-muted-foreground" title={e.detail ?? ''}>
                      {e.detail}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </Card>
        )}

        {page && entries.length > 0 && (
          <div className="flex items-center justify-between text-sm text-muted-foreground">
            <span>
              {offset + 1}–{offset + entries.length} of {total.toLocaleString()}
              {page.changesIncluded && ' · before/after included'}
            </span>
            <div className="flex items-center gap-2">
              <Button
                variant="outline"
                size="sm"
                disabled={!hasPrev || loading}
                onClick={() => setOffset(Math.max(0, offset - limit))}
              >
                <ChevronLeft data-icon="inline-start" /> Newer
              </Button>
              <Button
                variant="outline"
                size="sm"
                disabled={!hasNext || loading}
                onClick={() => setOffset(offset + limit)}
              >
                Older <ChevronRight data-icon="inline-end" />
              </Button>
            </div>
          </div>
        )}
      </div>

      {selected && page && <EntryDialog entry={selected} page={page} onClose={() => setSelected(null)} />}
    </div>
  )
}
