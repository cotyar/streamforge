import type { ReactNode } from 'react'

export function EmptyState({ title, hint, action }: { title: string; hint?: string; action?: ReactNode }) {
  return (
    <div className="flex flex-col items-center justify-center gap-3 rounded-xl border border-dashed border-[var(--sf-border)] bg-[var(--sf-panel)]/40 px-8 py-16 text-center">
      <p className="text-sm font-medium text-gray-300">{title}</p>
      {hint && <p className="max-w-sm text-sm text-gray-500">{hint}</p>}
      {action}
    </div>
  )
}
