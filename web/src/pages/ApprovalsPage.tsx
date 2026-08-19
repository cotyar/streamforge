import { useCallback, useEffect, useState } from 'react'
import type { FormEvent } from 'react'
import {
  AlarmClock,
  Ban,
  Bot,
  Check,
  Inbox,
  Plus,
  RefreshCw,
  TriangleAlert,
  X,
} from 'lucide-react'
import { toast } from 'sonner'
import { approvalsApi } from '../api/approvals'
import type { ApprovalRequest, ApprovalState } from '../api/types'
import { useAuth } from '../api/auth'
import { Topbar } from '../components/Topbar'
import { Card } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Input } from '@/components/ui/input'
import { Textarea } from '@/components/ui/textarea'
import { Skeleton } from '@/components/ui/skeleton'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Field, FieldDescription, FieldGroup, FieldLabel } from '@/components/ui/field'
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from '@/components/ui/empty'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Dialog, DialogClose, DialogContent, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'

// ------------------------------------------------------------------------------------------------
// helpers
// ------------------------------------------------------------------------------------------------

const STATES: ApprovalState[] = ['Pending', 'Approved', 'Rejected', 'Expired', 'Executed', 'Failed', 'Cancelled']

const STATE_VARIANT: Record<ApprovalState, 'default' | 'secondary' | 'destructive' | 'outline'> = {
  Pending: 'default',
  Approved: 'secondary',
  Executed: 'secondary',
  Rejected: 'destructive',
  Failed: 'destructive',
  Expired: 'outline',
  Cancelled: 'outline',
}

function message(err: unknown, fallback: string): string {
  return err instanceof Error && err.message ? err.message : fallback
}

function absolute(ms: number): string {
  return new Date(ms).toLocaleString(undefined, {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  })
}

const RELATIVE = new Intl.RelativeTimeFormat(undefined, { numeric: 'auto' })

function relative(ms: number, now: number): string {
  const delta = ms - now
  // `now` ticks every 30s and the timestamps are the server's clock, so a row filed a moment ago can
  // legitimately sit a few seconds in the future. "in 16 seconds" for something that just happened is
  // worse than an approximation.
  if (Math.abs(delta) < 45_000) return 'just now'
  const abs = Math.abs(delta)
  const units: [Intl.RelativeTimeFormatUnit, number][] = [
    ['day', 86_400_000],
    ['hour', 3_600_000],
    ['minute', 60_000],
    ['second', 1000],
  ]
  for (const [unit, size] of units) {
    if (abs >= size || unit === 'second') {
      return RELATIVE.format(Math.round(delta / size), unit)
    }
  }
  return 'now'
}

/** Origin is a first-class field precisely so an LLM-proposed action cannot hide among the ones a
 *  human typed. `chat` is called out; the other two are plain. */
function OriginBadge({ origin }: { origin: string }) {
  if (origin === 'chat') {
    return (
      <Badge variant="outline" className="border-destructive/40 text-destructive" title="Proposed by the AI control chat on a human's behalf — read the payload before approving">
        <Bot data-icon="inline-start" /> proposed by chat
      </Badge>
    )
  }
  return <Badge variant="outline" title={`Filed over ${origin}`}>{origin}</Badge>
}

// ------------------------------------------------------------------------------------------------
// File a request
// ------------------------------------------------------------------------------------------------

function FileDialog({ onClose, onFiled }: { onClose: () => void; onFiled: () => void }) {
  const [action, setAction] = useState('')
  const [scope, setScope] = useState('*')
  const [reason, setReason] = useState('')
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)

  async function submit(e: FormEvent) {
    e.preventDefault()
    setError(null)
    if (!action.trim()) return setError('Name the action you are asking for.')
    setSaving(true)
    try {
      // No `requestedBy`: the server stamps the authenticated principal, and the whole "you cannot
      // approve your own request" rule rests on it being unforgeable.
      await approvalsApi.file({ action: action.trim(), scope: scope.trim() || '*', reason: reason.trim() || null })
      onFiled()
    } catch (err) {
      // 409 here means no enabled template covers this action and scope — i.e. nobody is configured to
      // answer it, so nothing was filed. That sentence comes from the server and is the fix.
      setError(message(err, 'Failed to file the request.'))
    } finally {
      setSaving(false)
    }
  }

  return (
    <Dialog open onOpenChange={(open) => !open && onClose()}>
      <DialogContent className="sm:max-w-lg">
        <form onSubmit={submit} className="flex flex-col gap-4">
          <DialogHeader>
            <DialogTitle>Request approval</DialogTitle>
          </DialogHeader>

          <FieldGroup className="gap-3">
            <Field>
              <FieldLabel htmlFor="ap-action">Action</FieldLabel>
              <Input id="ap-action" value={action} onChange={(e) => setAction(e.target.value)} placeholder="pipeline.write" />
            </Field>
            <Field>
              <FieldLabel htmlFor="ap-scope">Scope</FieldLabel>
              <Input id="ap-scope" value={scope} onChange={(e) => setScope(e.target.value)} placeholder="prod-orders" />
              <FieldDescription>
                An enabled approval template has to match this action and scope, or there is nobody to answer it.
              </FieldDescription>
            </Field>
            <Field>
              <FieldLabel htmlFor="ap-reason">Reason</FieldLabel>
              <Textarea id="ap-reason" rows={3} value={reason} onChange={(e) => setReason(e.target.value)} placeholder="Why this needs doing, for whoever has to decide." />
            </Field>
          </FieldGroup>

          {error && (
            <Alert variant="destructive">
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          )}

          <DialogFooter>
            <DialogClose asChild>
              <Button type="button" variant="outline">Cancel</Button>
            </DialogClose>
            <Button type="submit" disabled={saving}>{saving ? 'Filing…' : 'File request'}</Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  )
}

// ------------------------------------------------------------------------------------------------
// Approve / reject, with the optional comment the API allows
// ------------------------------------------------------------------------------------------------

function DecisionDialog({
  request,
  approve,
  onClose,
  onDone,
}: {
  request: ApprovalRequest
  approve: boolean
  onClose: () => void
  onDone: () => void
}) {
  const [comment, setComment] = useState('')
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)

  async function submit(e: FormEvent) {
    e.preventDefault()
    setError(null)
    setSaving(true)
    try {
      const body = { comment: comment.trim() || null }
      const after = approve
        ? await approvalsApi.approve(request.id, body)
        : await approvalsApi.reject(request.id, body)
      toast.success(`Recorded — the request is now ${after.state.toLowerCase()}.`)
      onDone()
    } catch (err) {
      // The server explains a refused vote in a sentence: not pending any more (409), your own request
      // (403), not an approver (403, naming the groups that are). Show it as written.
      setError(message(err, 'The vote was not recorded.'))
    } finally {
      setSaving(false)
    }
  }

  return (
    <Dialog open onOpenChange={(open) => !open && onClose()}>
      <DialogContent className="sm:max-w-lg">
        <form onSubmit={submit} className="flex flex-col gap-4">
          <DialogHeader>
            <DialogTitle>{approve ? 'Approve' : 'Reject'} this request?</DialogTitle>
          </DialogHeader>

          <div className="rounded-md border border-border bg-muted/40 px-3 py-2 text-sm">
            <p className="font-mono text-xs text-foreground">
              {request.action} <span className="text-muted-foreground">@ {request.scope}</span>
            </p>
            <p className="mt-1 text-muted-foreground">
              filed by {request.requestedBy}
              {request.reason && ` — ${request.reason}`}
            </p>
          </div>

          {!approve && (
            <Alert>
              <AlertDescription>
                One rejection decides the whole request. Approvals need {request.requiredApprovals}; a single no is final.
              </AlertDescription>
            </Alert>
          )}

          <Field>
            <FieldLabel htmlFor="vote-comment">Comment (optional)</FieldLabel>
            <Textarea id="vote-comment" rows={3} value={comment} onChange={(e) => setComment(e.target.value)} />
          </Field>

          {error && (
            <Alert variant="destructive">
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          )}

          <DialogFooter>
            <DialogClose asChild>
              <Button type="button" variant="outline">Cancel</Button>
            </DialogClose>
            <Button type="submit" variant={approve ? 'default' : 'destructive'} disabled={saving}>
              {saving ? 'Recording…' : approve ? 'Approve' : 'Reject'}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  )
}

// ------------------------------------------------------------------------------------------------
// One request
// ------------------------------------------------------------------------------------------------

function RequestCard({
  request,
  now,
  isMine,
  mayDecide,
  onVote,
  onCancel,
}: {
  request: ApprovalRequest
  now: number
  isMine: boolean
  mayDecide: boolean
  onVote: (approve: boolean) => void
  onCancel: () => void
}) {
  const approvals = request.votes.filter((v) => v.approve).length
  const rejections = request.votes.filter((v) => !v.approve).length
  const pending = request.state === 'Pending'
  const expiringSoon = pending && request.expiresAtMs > 0 && request.expiresAtMs - now < 3_600_000
  const expired = pending && request.expiresAtMs > 0 && request.expiresAtMs <= now

  return (
    <Card className="gap-3 p-4">
      <div className="flex flex-wrap items-center gap-2">
        <Badge variant={STATE_VARIANT[request.state]}>{request.state}</Badge>
        <span className="font-mono text-sm text-foreground">
          {request.action} <span className="text-muted-foreground">@ {request.scope}</span>
        </span>
        <OriginBadge origin={request.origin} />
        {request.templateName && (
          <Badge variant="ghost" title="The approval template that routed this request">{request.templateName}</Badge>
        )}
        <span className="ml-auto text-xs text-muted-foreground" title={absolute(request.requestedAtMs)}>
          {request.requestedBy} · {relative(request.requestedAtMs, now)}
        </span>
      </div>

      {request.reason && <p className="text-sm text-foreground/80">{request.reason}</p>}

      {/* The two fields that make a pending request urgent. */}
      {pending && (
        <div className="flex flex-wrap items-center gap-3 text-xs">
          {request.expiresAtMs > 0 && (
            <span
              className={expired || expiringSoon ? 'flex items-center gap-1 text-destructive' : 'flex items-center gap-1 text-muted-foreground'}
              title={absolute(request.expiresAtMs)}
            >
              <AlarmClock className="size-3" />
              {expired ? 'past its deadline — the sweeper will expire it' : `expires ${relative(request.expiresAtMs, now)}`}
            </span>
          )}
          {request.escalatedAtMs != null && request.escalatedAtMs > 0 && (
            <span className="flex items-center gap-1 text-destructive" title={absolute(request.escalatedAtMs)}>
              <TriangleAlert className="size-3" />
              escalated {relative(request.escalatedAtMs, now)}
              {request.approverGroups.length > 0 && ` — now ${request.approverGroups.join(', ')}`}
            </span>
          )}
        </div>
      )}

      <div className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
        <span className={approvals >= request.requiredApprovals ? 'text-foreground' : undefined}>
          {approvals} of {request.requiredApprovals} approvals
        </span>
        {rejections > 0 && <span className="text-destructive">· {rejections} rejection{rejections > 1 ? 's' : ''}</span>}
        {request.approverGroups.length > 0 ? (
          <span>· decided by {request.approverGroups.join(', ')}</span>
        ) : (
          <span className="text-destructive">· no approver group — nobody can approve it, it can only expire</span>
        )}
      </div>

      {request.votes.length > 0 && (
        <ul className="flex flex-col gap-1 border-l-2 border-border pl-3 text-xs">
          {request.votes.map((v, i) => (
            <li key={i} className="text-muted-foreground">
              {v.approve ? (
                <Check className="mr-1 inline size-3 text-foreground" />
              ) : (
                <X className="mr-1 inline size-3 text-destructive" />
              )}
              <span className="text-foreground">{v.username}</span> {v.approve ? 'approved' : 'rejected'}{' '}
              <span title={absolute(v.atMs)}>{relative(v.atMs, now)}</span>
              {v.comment && <span className="italic"> — “{v.comment}”</span>}
            </li>
          ))}
        </ul>
      )}

      {request.payloadJson && (
        <details className="text-xs">
          <summary className="cursor-pointer text-muted-foreground">Payload — the request that would execute</summary>
          <pre className="mt-1 max-h-48 overflow-auto rounded-md bg-muted/50 p-2 font-mono text-[11px] text-foreground">
            {request.payloadJson}
          </pre>
        </details>
      )}

      {request.outcome && <p className="text-xs text-muted-foreground">Outcome: {request.outcome}</p>}

      {pending && (
        <div className="flex flex-wrap items-center gap-2 pt-1">
          {isMine ? (
            <>
              <p className="text-xs text-muted-foreground">
                You filed this, so you cannot vote on it — that is what the second pair of eyes is for.
              </p>
              <Button variant="outline" size="sm" className="ml-auto" onClick={onCancel}>
                <Ban data-icon="inline-start" /> Withdraw
              </Button>
            </>
          ) : mayDecide ? (
            <>
              <Button size="sm" onClick={() => onVote(true)}>
                <Check data-icon="inline-start" /> Approve
              </Button>
              <Button variant="destructive" size="sm" onClick={() => onVote(false)}>
                <X data-icon="inline-start" /> Reject
              </Button>
            </>
          ) : (
            <p className="text-xs text-muted-foreground">
              You are not entitled to decide this one — it is here because you can see it, not because you can act on it.
            </p>
          )}
        </div>
      )}
    </Card>
  )
}

// ------------------------------------------------------------------------------------------------
// The page
// ------------------------------------------------------------------------------------------------

export function ApprovalsPage() {
  // NO role gate. `GET /api/approvals` filters server-side to the administrator, the requester and the
  // entitled approver, so this page belongs to every logged-in user — a requester has to be able to
  // watch what they filed.
  const { user, can } = useAuth()
  const [rows, setRows] = useState<ApprovalRequest[] | null>(null)
  const [state, setState] = useState<ApprovalState | 'All'>('Pending')
  const [error, setError] = useState<string | null>(null)
  const [filing, setFiling] = useState(false)
  const [deciding, setDeciding] = useState<{ request: ApprovalRequest; approve: boolean } | null>(null)
  const [now, setNow] = useState(() => Date.now())

  // Expiry and escalation are the two things that make a row urgent, and both are clock-driven.
  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), 30_000)
    return () => clearInterval(t)
  }, [])

  const load = useCallback(() => {
    setError(null)
    setNow(Date.now())
    approvalsApi
      .list(state === 'All' ? null : state)
      .then((r) => setRows(r))
      .catch((err) => {
        setRows([])
        // Includes the 503 that `Approvals:Enabled=false` answers with — deliberately a sentence
        // naming the config key rather than an empty inbox, because "all clear" would be a lie.
        setError(message(err, 'Failed to load approvals.'))
      })
  }, [state])

  useEffect(() => load(), [load])

  async function cancel(request: ApprovalRequest) {
    try {
      const after = await approvalsApi.cancel(request.id)
      toast.success(`Withdrawn — the request is ${after.state.toLowerCase()}.`)
    } catch (err) {
      toast.error(message(err, 'Failed to withdraw the request.'))
    } finally {
      load()
    }
  }

  const mine = (r: ApprovalRequest) => r.requestedBy.toLowerCase() === (user?.username ?? '').toLowerCase()

  return (
    <div>
      <Topbar
        title="Approvals"
        subtitle="Requests you filed, and requests you can decide"
        action={
          <div className="flex items-center gap-2">
            <Select value={state} onValueChange={(v) => setState(v as ApprovalState | 'All')}>
              <SelectTrigger size="sm" className="w-36">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="All">All states</SelectItem>
                {STATES.map((s) => (
                  <SelectItem key={s} value={s}>{s}</SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Button variant="outline" size="sm" onClick={load}>
              <RefreshCw data-icon="inline-start" /> Refresh
            </Button>
            {can('approval.request') && (
              <Button size="sm" onClick={() => setFiling(true)}>
                <Plus data-icon="inline-start" /> Request approval
              </Button>
            )}
          </div>
        }
      />

      <div className="flex flex-col gap-3 p-8">
        {error && (
          <Alert variant="destructive">
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        )}

        {rows === null ? (
          Array.from({ length: 3 }).map((_, i) => <Skeleton key={i} className="h-32 w-full" />)
        ) : rows.length === 0 && !error ? (
          <Empty className="border border-dashed">
            <EmptyHeader>
              <EmptyMedia variant="icon"><Inbox /></EmptyMedia>
              <EmptyTitle>Nothing {state === 'All' ? 'here' : `in ${state.toLowerCase()}`}</EmptyTitle>
              <EmptyDescription>
                This inbox shows what you filed and what you are entitled to decide — not everything in the
                deployment.
              </EmptyDescription>
            </EmptyHeader>
          </Empty>
        ) : (
          rows.map((r) => (
            <RequestCard
              key={r.id}
              request={r}
              now={now}
              isMine={mine(r)}
              mayDecide={!mine(r) && can('approval.decide', r.scope)}
              onVote={(approve) => setDeciding({ request: r, approve })}
              onCancel={() => void cancel(r)}
            />
          ))
        )}
      </div>

      {filing && (
        <FileDialog
          onClose={() => setFiling(false)}
          onFiled={() => {
            setFiling(false)
            toast.success('Request filed.')
            load()
          }}
        />
      )}

      {deciding && (
        <DecisionDialog
          request={deciding.request}
          approve={deciding.approve}
          onClose={() => setDeciding(null)}
          onDone={() => {
            setDeciding(null)
            load()
          }}
        />
      )}
    </div>
  )
}
