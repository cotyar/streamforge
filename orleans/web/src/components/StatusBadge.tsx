import type { PipelineStatus } from '../api/types'

const STYLES: Record<PipelineStatus, { dot: string; text: string; pulse: boolean; label: string }> = {
  Running: { dot: 'bg-[var(--sf-good)]', text: 'text-[var(--sf-good)]', pulse: true, label: 'Running' },
  Stopped: { dot: 'bg-gray-500', text: 'text-gray-400', pulse: false, label: 'Stopped' },
  Failed: { dot: 'bg-[var(--sf-bad)]', text: 'text-[var(--sf-bad)]', pulse: false, label: 'Failed' },
}

export function StatusBadge({ status }: { status: PipelineStatus }) {
  const s = STYLES[status]
  return (
    <span className={`inline-flex items-center gap-1.5 text-xs font-medium ${s.text}`}>
      <span className="relative flex h-2 w-2">
        {s.pulse && (
          <span className={`absolute inline-flex h-full w-full animate-ping rounded-full ${s.dot} opacity-60`} />
        )}
        <span className={`relative inline-flex h-2 w-2 rounded-full ${s.dot}`} />
      </span>
      {s.label}
    </span>
  )
}
