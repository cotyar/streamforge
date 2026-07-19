import { useEffect, useState } from 'react'
import type { ScheduleSpec } from '@/api/types'
import { Field, FieldLabel } from '@/components/ui/field'
import { Input } from '@/components/ui/input'
import { Select, SelectContent, SelectGroup, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'

type Mode = 'default' | 'interval' | 'cron'
type Unit = 's' | 'm' | 'h'

const UNIT_MS: Record<Unit, number> = { s: 1000, m: 60_000, h: 3_600_000 }

function unitAndValueFromMs(ms: number): { value: number; unit: Unit } {
  if (ms > 0 && ms % UNIT_MS.h === 0) return { value: ms / UNIT_MS.h, unit: 'h' }
  if (ms > 0 && ms % UNIT_MS.m === 0) return { value: ms / UNIT_MS.m, unit: 'm' }
  return { value: Math.max(1, Math.round(ms / UNIT_MS.s)), unit: 's' }
}

/**
 * Cron/interval schedule editor for url/file/folder connectors (D-E). Omitted (`mode: 'default'`)
 * means the server applies its documented 30 s default — said explicitly in the UI rather than
 * defaulting silently. Seeds its local editing state once from `initial` (MetadataEditor's
 * uncontrolled-with-callback pattern) — pass a remount `key` from the parent when switching entities.
 */
export function ScheduleEditor({
  initial,
  onChange,
  disabled = false,
}: {
  initial?: ScheduleSpec | null
  onChange: (schedule: ScheduleSpec | null) => void
  disabled?: boolean
}) {
  const [mode, setMode] = useState<Mode>(() => (initial?.cron ? 'cron' : initial?.intervalMs ? 'interval' : 'default'))
  const seeded = initial?.intervalMs ? unitAndValueFromMs(initial.intervalMs) : { value: 30, unit: 's' as Unit }
  const [intervalValue, setIntervalValue] = useState(seeded.value)
  const [intervalUnit, setIntervalUnit] = useState<Unit>(seeded.unit)
  const [cron, setCron] = useState(initial?.cron ?? '')

  useEffect(() => {
    if (mode === 'default') {
      onChange(null)
    } else if (mode === 'interval') {
      onChange({ intervalMs: Math.max(1000, Math.round(intervalValue * UNIT_MS[intervalUnit])), cron: null })
    } else {
      onChange({ cron: cron.trim(), intervalMs: null })
    }
    // onChange intentionally excluded — callers pass a fresh closure each render.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [mode, intervalValue, intervalUnit, cron])

  return (
    <Field>
      <div className="mb-1 flex items-center justify-between">
        <FieldLabel className="mb-0">Schedule</FieldLabel>
        <Select value={mode} onValueChange={(v) => setMode(v as Mode)} disabled={disabled}>
          <SelectTrigger size="sm" className="w-36">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectGroup>
              <SelectItem value="default">Default (30s)</SelectItem>
              <SelectItem value="interval">Interval</SelectItem>
              <SelectItem value="cron">Cron</SelectItem>
            </SelectGroup>
          </SelectContent>
        </Select>
      </div>

      {mode === 'default' && (
        <p className="text-[11px] text-muted-foreground">No schedule configured — polls every 30s by default.</p>
      )}

      {mode === 'interval' && (
        <div className="flex items-center gap-2">
          <Input
            type="number"
            min={1}
            value={intervalValue}
            disabled={disabled}
            onChange={(e) => setIntervalValue(Math.max(1, Number(e.target.value) || 1))}
            className="w-24"
          />
          <Select value={intervalUnit} onValueChange={(v) => setIntervalUnit(v as Unit)} disabled={disabled}>
            <SelectTrigger className="w-28">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectGroup>
                <SelectItem value="s">seconds</SelectItem>
                <SelectItem value="m">minutes</SelectItem>
                <SelectItem value="h">hours</SelectItem>
              </SelectGroup>
            </SelectContent>
          </Select>
          <span className="text-[11px] text-muted-foreground">min 1s</span>
        </div>
      )}

      {mode === 'cron' && (
        <>
          <Input
            value={cron}
            disabled={disabled}
            onChange={(e) => setCron(e.target.value)}
            placeholder="*/30 * * * * *"
            className="font-mono"
          />
          <p className="mt-1 text-[11px] text-muted-foreground">
            5-field (minute resolution) or 6-field (with seconds) cron, evaluated in UTC.
          </p>
        </>
      )}
    </Field>
  )
}
