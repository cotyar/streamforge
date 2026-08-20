import type { ReactNode } from 'react'
import { EnvironmentBadge } from './EnvironmentPicker'

/** Every authenticated page renders exactly one Topbar (Layout.tsx wraps them all), which makes it the
 * one place to put something that must be visible on every page without a per-page change — plan
 * 021's `EnvironmentBadge` (021-F: "make the current environment visible at all times, not only
 * inside the picker"). */
export function Topbar({ title, subtitle, action }: { title: string; subtitle?: string; action?: ReactNode }) {
  return (
    <div className="sticky top-0 z-10 flex items-center justify-between border-b border-border bg-background/85 px-8 py-5 backdrop-blur">
      <div>
        <div className="flex items-center gap-2.5">
          <h1 className="text-xl font-semibold text-foreground">{title}</h1>
          <EnvironmentBadge />
        </div>
        {subtitle && <p className="mt-0.5 text-sm text-muted-foreground">{subtitle}</p>}
      </div>
      {action}
    </div>
  )
}
