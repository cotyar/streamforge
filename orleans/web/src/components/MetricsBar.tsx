import type { PipelineMetrics } from '../api/types'
import { Skeleton } from '@/components/ui/skeleton'

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex flex-col gap-0.5 rounded-lg border border-border bg-background/60 px-3 py-2">
      <span className="text-[10px] font-medium uppercase tracking-wide text-muted-foreground">{label}</span>
      <span className="font-mono text-sm font-semibold text-foreground">{value}</span>
    </div>
  )
}

export function MetricsBar({ metrics }: { metrics: PipelineMetrics | null }) {
  if (!metrics) {
    return (
      <div className="grid grid-cols-2 gap-2 sm:grid-cols-4">
        {Array.from({ length: 4 }).map((_, i) => (
          <Skeleton key={i} className="h-14" />
        ))}
      </div>
    )
  }

  return (
    <div className="grid grid-cols-2 gap-2 sm:grid-cols-4">
      <Stat label="Events in/s" value={metrics.eventsInPerSec.toFixed(1)} />
      <Stat label="Rows out/s" value={metrics.rowsOutPerSec.toFixed(1)} />
      <Stat label="Windows closed" value={metrics.windowsClosed.toLocaleString()} />
      <Stat label="Total in / out" value={`${metrics.totalEventsIn.toLocaleString()} / ${metrics.totalRowsOut.toLocaleString()}`} />
    </div>
  )
}
