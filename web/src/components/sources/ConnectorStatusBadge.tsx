import { useConnectorStatus } from '@/hooks/useConnectorStatus'
import { Badge } from '@/components/ui/badge'
import { cn } from '@/lib/utils'

/** "in 12s" / "3m ago" — deliberately coarse (no sub-second precision needed for a 2 s poll). */
function relativeFromNow(epochMs: number): string {
  const deltaMs = epochMs - Date.now()
  const future = deltaMs >= 0
  const abs = Math.abs(deltaMs)
  let value: number
  let unit: string
  if (abs < 1000) {
    return future ? 'now' : 'just now'
  } else if (abs < 60_000) {
    value = Math.round(abs / 1000)
    unit = 's'
  } else if (abs < 3_600_000) {
    value = Math.round(abs / 60_000)
    unit = 'm'
  } else {
    value = Math.round(abs / 3_600_000)
    unit = 'h'
  }
  return future ? `in ${value}${unit}` : `${value}${unit} ago`
}

/**
 * Connector runtime status line for a Sources list card — kind badge lives in the card header,
 * this renders below it. Self-contained: polls `useConnectorStatus(name)` itself so callers can
 * mount it unconditionally for connector-kind sources and get a `null` render (nothing shown) for
 * generator-kind ones or before the first status read lands.
 */
export function ConnectorStatusBadge({ name }: { name: string }) {
  const status = useConnectorStatus(name)
  if (!status) return null

  const dotClass =
    status.lastStatus === 'ok' ? 'bg-primary' : status.lastStatus === 'error' ? 'bg-destructive' : 'bg-muted-foreground'
  const badgeVariant = status.lastStatus === 'ok' ? 'default' : status.lastStatus === 'error' ? 'destructive' : 'secondary'

  return (
    <div className="flex flex-col gap-1 text-[11px] text-muted-foreground">
      <div className="flex flex-wrap items-center gap-x-2.5 gap-y-1">
        <Badge variant={badgeVariant} className="gap-1.5">
          <span className={cn('size-1.5 rounded-full', dotClass)} />
          {status.lastStatus}
        </Badge>
        {typeof status.nextRunMs === 'number' && <span>next run {relativeFromNow(status.nextRunMs)}</span>}
        {status.consecutiveFailures > 0 && (
          <span className="text-destructive">{status.consecutiveFailures} consecutive failures</span>
        )}
        <span>
          <span className="font-mono text-foreground">{status.eventsEmittedTotal}</span> events emitted
        </span>
      </div>
      {status.lastError && (
        <p className="truncate text-destructive" title={status.lastError}>
          {status.lastError}
        </p>
      )}
    </div>
  )
}
