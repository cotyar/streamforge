import type { PipelineStatus } from '../api/types'
import { Badge } from '@/components/ui/badge'
import { cn } from '@/lib/utils'

const STATUS_CONFIG: Record<
  PipelineStatus,
  { variant: 'default' | 'secondary' | 'destructive'; dot: string; pulse: boolean; label: string }
> = {
  Running: { variant: 'default', dot: 'bg-primary-foreground', pulse: true, label: 'Running' },
  Stopped: { variant: 'secondary', dot: 'bg-muted-foreground', pulse: false, label: 'Stopped' },
  Failed: { variant: 'destructive', dot: 'bg-destructive', pulse: false, label: 'Failed' },
}

export function StatusBadge({ status }: { status: PipelineStatus }) {
  const s = STATUS_CONFIG[status]
  return (
    <Badge variant={s.variant} className="gap-1.5">
      <span className="relative flex size-2">
        {s.pulse && <span className={cn('absolute inline-flex size-full animate-ping rounded-full opacity-60', s.dot)} />}
        <span className={cn('relative inline-flex size-2 rounded-full', s.dot)} />
      </span>
      {s.label}
    </Badge>
  )
}
