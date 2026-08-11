import { useCallback, useEffect, useState } from 'react'
import type { FormEvent } from 'react'
import { toast } from 'sonner'
import { Check, Copy, KeyRound, Trash2, TriangleAlert } from 'lucide-react'
import { sourcesApi } from '@/api/sources'
import type { CreatedIngestKeyResponse, IngestKey } from '@/api/types'
import { relativeFromNow } from './ConnectorStatusBadge'
import { RoleGate } from '@/components/RoleGate'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Dialog, DialogClose, DialogContent, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '@/components/ui/alert-dialog'

function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false)
  async function handleCopy() {
    try {
      await navigator.clipboard.writeText(text)
      setCopied(true)
      setTimeout(() => setCopied(false), 1500)
    } catch {
      toast.error('Failed to copy to clipboard.')
    }
  }
  return (
    <Button type="button" variant="outline" size="icon-sm" onClick={() => void handleCopy()} aria-label="Copy secret" title="Copy secret">
      {copied ? <Check className="text-primary" /> : <Copy />}
    </Button>
  )
}

/**
 * Reveal-once dialog for a freshly generated ingest key's secret (plan 009 A1.2). There is no
 * second way to ever read this value back — GET only ever returns identity + usage — so the copy
 * affordance and the "won't see this again" warning are load-bearing, not decoration.
 */
function RevealSecretDialog({ created, onClose }: { created: CreatedIngestKeyResponse; onClose: () => void }) {
  return (
    <Dialog open onOpenChange={(open) => !open && onClose()}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>Key generated — copy it now</DialogTitle>
        </DialogHeader>
        <Alert variant="destructive">
          <TriangleAlert />
          <AlertDescription>
            This is the only time this secret is ever shown. Once you close this dialog it cannot be recovered — only
            revoked and regenerated.
          </AlertDescription>
        </Alert>
        <div className="flex items-center gap-1 rounded-lg border border-border bg-input/20 px-2.5 py-1.5">
          <code className="min-w-0 flex-1 overflow-x-auto whitespace-pre font-mono text-xs text-foreground">{created.secret}</code>
          <CopyButton text={created.secret} />
        </div>
        <p className="text-[11px] text-muted-foreground">
          Send it as the <span className="font-mono text-foreground">X-SF-Ingest-Key</span> header — this key authorizes
          push to this source only.
        </p>
        <DialogFooter>
          <DialogClose asChild>
            <Button type="button">Done</Button>
          </DialogClose>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

/**
 * Per-source push-key management (plan 009 A1.2): a machine pushing telemetry authenticates with a
 * key scoped to exactly this source, instead of an Editor JWT that could also rewrite SQL. Lists
 * identity + usage only (never the secret or its hash — the backend literally never returns them
 * again after generation), generates, and revokes. Self-contained: fetches its own list rather than
 * relying on the source object's `ingest.keys`, so it stays correct across generate/revoke without
 * needing the parent's full source reload.
 */
export function IngestKeysPanel({ sourceName }: { sourceName: string }) {
  const [keys, setKeys] = useState<IngestKey[] | null>(null)
  const [label, setLabel] = useState('')
  const [generating, setGenerating] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [revealed, setRevealed] = useState<CreatedIngestKeyResponse | null>(null)
  const [pendingRevoke, setPendingRevoke] = useState<IngestKey | null>(null)

  const load = useCallback(() => {
    sourcesApi
      .listIngestKeys(sourceName)
      .then(setKeys)
      .catch(() => setKeys([]))
  }, [sourceName])

  useEffect(() => {
    load()
  }, [load])

  async function handleGenerate(e: FormEvent) {
    e.preventDefault()
    setError(null)
    setGenerating(true)
    try {
      const created = await sourcesApi.generateIngestKey(sourceName, { label: label.trim() })
      setLabel('')
      setRevealed(created)
      load()
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to generate key.')
    } finally {
      setGenerating(false)
    }
  }

  async function confirmRevoke() {
    if (!pendingRevoke) return
    const id = pendingRevoke.id
    setPendingRevoke(null)
    try {
      await sourcesApi.revokeIngestKey(sourceName, id)
      load()
    } catch (err) {
      toast.error(err instanceof Error ? err.message : 'Failed to revoke key.')
    }
  }

  return (
    <RoleGate min="Editor">
      <div className="flex flex-col gap-2 border-t border-border pt-3">
        <p className="flex items-center gap-1.5 text-[10px] font-medium uppercase tracking-wide text-muted-foreground">
          <KeyRound className="size-3" /> Push keys
        </p>

        {keys === null ? (
          <p className="text-xs text-muted-foreground">Loading…</p>
        ) : keys.length === 0 ? (
          <p className="text-xs text-muted-foreground">No keys — this source only accepts pushes from an Editor JWT.</p>
        ) : (
          <div className="flex flex-col gap-1">
            {keys.map((k) => (
              <div key={k.id} className="flex items-center justify-between gap-2 rounded-lg border border-border bg-background/60 px-2.5 py-1.5 text-[11px]">
                <div className="flex min-w-0 flex-col">
                  <span className="truncate font-medium text-foreground">{k.label || k.id}</span>
                  <span className="text-muted-foreground">
                    created {relativeFromNow(k.createdAtMs)} · {k.lastUsedMs > 0 ? `last used ${relativeFromNow(k.lastUsedMs)}` : 'never used'}
                  </span>
                </div>
                <Button
                  type="button"
                  variant="ghost"
                  size="icon-sm"
                  className="shrink-0 hover:text-destructive"
                  onClick={() => setPendingRevoke(k)}
                  aria-label={`Revoke ${k.label || k.id}`}
                >
                  <Trash2 />
                </Button>
              </div>
            ))}
          </div>
        )}

        <form onSubmit={handleGenerate} className="flex items-center gap-2">
          <Input
            value={label}
            onChange={(e) => setLabel(e.target.value)}
            placeholder="Label (e.g. prod collector)"
            disabled={generating}
            className="h-8"
          />
          <Button type="submit" size="sm" disabled={generating}>
            {generating ? 'Generating…' : 'Generate key'}
          </Button>
        </form>
        {error && (
          <Alert variant="destructive">
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        )}
      </div>

      {revealed && <RevealSecretDialog created={revealed} onClose={() => setRevealed(null)} />}

      <AlertDialog open={pendingRevoke !== null} onOpenChange={(open) => !open && setPendingRevoke(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Revoke this key?</AlertDialogTitle>
            <AlertDialogDescription>
              <span className="font-medium text-foreground">{pendingRevoke?.label || pendingRevoke?.id}</span> immediately
              stops authorizing pushes to this source. This cannot be undone.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction variant="destructive" onClick={() => void confirmRevoke()}>
              Revoke
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </RoleGate>
  )
}
