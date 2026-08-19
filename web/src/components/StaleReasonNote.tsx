import { TriangleAlert } from 'lucide-react'
import { cn } from '@/lib/utils'
import { Alert, AlertDescription } from '@/components/ui/alert'

/** Plan 016 wave 2-B: the compact stale-pin note for a list row (TablesPage/PipelinesPage), placed the
 *  same way `t.error`/`p.error` already are — right under the tags. `staleReason` is a full sentence
 *  naming which dependency moved and from what (RegistryGrain/CatalogStore's RecomputeStaleReasons), so
 *  it is rendered as text, not reduced to a boolean icon; `title` carries the untruncated sentence for
 *  when the row is too narrow to show all of it. Renders nothing when absent — a pre-016 server, or
 *  every pin on this entity is still satisfied. */
export function StaleReasonNote({ reason, className }: { reason?: string | null; className?: string }) {
  if (!reason) return null
  return (
    <p className={cn('mt-0.5 flex max-w-xs items-start gap-1 text-xs text-warning', className)} title={reason}>
      <TriangleAlert className="mt-0.5 size-3 shrink-0" />
      <span className="truncate">{reason}</span>
    </p>
  )
}

/** Full-width variant for a detail page (TableDetailPage/PipelineDetailPage) — the sentence is never
 *  truncated here, which is the whole point of StaleReason being a string rather than a flag. */
export function StaleReasonBanner({ reason, className }: { reason?: string | null; className?: string }) {
  if (!reason) return null
  return (
    <Alert className={cn('border-warning/40 bg-warning/5', className)}>
      <TriangleAlert className="text-warning" />
      <AlertDescription className="text-foreground">{reason}</AlertDescription>
    </Alert>
  )
}
