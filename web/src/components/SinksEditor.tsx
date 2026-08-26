import { useEffect, useState } from 'react'
import type { SinkSpec, TransportDescriptor } from '@/api/types'
import { findDescriptor, transportsApi } from '@/api/transports'
import { Button } from '@/components/ui/button'
import { Switch } from '@/components/ui/switch'
import { Badge } from '@/components/ui/badge'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { FieldGroup, FieldLabel } from '@/components/ui/field'
import { Select, SelectContent, SelectGroup, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Plus, Trash2 } from 'lucide-react'
import { TransportConfigEditor, emptyTransportConfig, type TransportConfigValue } from '@/components/sources/TransportConfigEditor'

/**
 * Outbound sinks (plan 009 B2) — shared verbatim between PipelineDetailPage and TableDetailPage since a
 * `SinkSpec[]` means the same thing on both (republish result rows / table deltas).
 *
 * Plan 010: no longer NATS-shaped. The kind picker and every field come from `GET /api/transports`, so a
 * sink transport registered on the backend is configurable here with no change to this file — this used to
 * be one of the fourteen places a new transport had to touch.
 *
 * Credentials follow the same secrets-lite convention as every other editor: read back as "***", sending
 * "***" back keeps the stored value (SecretsMasker.MergeSinkSecrets, matched positionally by list index
 * since a sink has no stable id).
 */
export function SinksEditor({
  value,
  onChange,
  isEdit,
  disabled = false,
  declaredKeyColumns,
}: {
  value: SinkSpec[]
  onChange: (next: SinkSpec[]) => void
  isEdit: boolean
  disabled?: boolean
  /** Plan 014: the owning TABLE's row identity (`TableGroupKeyExtractor.Describe(sql).DeclaredKeys` on
   *  the backend) — undefined/empty on a pipeline (a pipeline emits results, not deltas, so upsert and
   *  "declared keys" are both meaningless there; see DbSinkModes.Upsert's own doc comment) or while the
   *  caller has not computed it yet. Purely a SUGGESTION rendered next to any sink whose config exposes a
   *  `mode`/`keyColumns` pair (shape-based, not a hardcoded "postgres"/"mssql" kind check) — see
   *  `KeyColumnsSuggestion` below for why it never writes the field on its own. */
  declaredKeyColumns?: string[]
}) {
  const [descriptors, setDescriptors] = useState<TransportDescriptor[]>([])
  const [addKind, setAddKind] = useState<string>('')

  useEffect(() => {
    let cancelled = false
    transportsApi
      .catalog()
      .then((catalog) => {
        if (cancelled) return
        setDescriptors(catalog.outbound)
        setAddKind((k) => k || (catalog.outbound[0]?.kind ?? ''))
      })
      .catch(() => {
        /* the page around this already surfaces API failures; a missing catalog just disables "add" */
      })
    return () => {
      cancelled = true
    }
  }, [])

  function update(i: number, patch: Partial<SinkSpec>) {
    onChange(value.map((s, idx) => (idx === i ? { ...s, ...patch } : s)))
  }

  function remove(i: number) {
    onChange(value.filter((_, idx) => idx !== i))
  }

  function add() {
    const descriptor = findDescriptor(descriptors, addKind)
    if (!descriptor) return
    onChange([...value, { kind: descriptor.kind, enabled: true, [descriptor.configProperty]: emptyTransportConfig(descriptor) }])
  }

  const canAdd = descriptors.length > 0 && !disabled

  return (
    <FieldGroup className="gap-3">
      <div className="flex items-center justify-between">
        <FieldLabel className="mb-0">Sinks</FieldLabel>
        <div className="flex items-center gap-2">
          {descriptors.length > 1 && (
            <Select value={addKind} onValueChange={setAddKind} disabled={disabled}>
              <SelectTrigger size="sm" className="w-36">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectGroup>
                  {descriptors.map((d) => (
                    <SelectItem key={d.kind} value={d.kind}>
                      {d.label}
                    </SelectItem>
                  ))}
                </SelectGroup>
              </SelectContent>
            </Select>
          )}
          <Button type="button" variant="ghost" size="sm" onClick={add} disabled={!canAdd}>
            <Plus data-icon="inline-start" /> Add sink
          </Button>
        </div>
      </div>
      <p className="-mt-2 text-[11px] text-muted-foreground">
        Delivery is fire-and-forget with no backpressure — a slow or absent broker drops messages rather than slowing
        this pipeline down.
      </p>

      {value.length === 0 ? (
        <p className="text-xs text-muted-foreground">No sinks — nothing is republished.</p>
      ) : (
        <div className="flex flex-col gap-3">
          {value.map((sink, i) => {
            const descriptor = findDescriptor(descriptors, sink.kind)
            const config = descriptor
              ? (((sink as unknown as Record<string, unknown>)[descriptor.configProperty] as TransportConfigValue | null) ?? {})
              : {}

            return (
              <div key={i} className="flex flex-col gap-2 rounded-lg border border-border p-3">
                <div className="flex items-center justify-between">
                  <Badge variant="outline">{descriptor?.label ?? sink.kind}</Badge>
                  <div className="flex items-center gap-2">
                    <label className="flex items-center gap-1.5 text-xs text-muted-foreground">
                      <Switch checked={sink.enabled} onCheckedChange={(checked) => update(i, { enabled: checked })} disabled={disabled} />
                      Enabled
                    </label>
                    <Button
                      type="button"
                      variant="ghost"
                      size="icon-sm"
                      className="hover:text-destructive"
                      onClick={() => remove(i)}
                      disabled={disabled}
                      aria-label="Remove sink"
                    >
                      <Trash2 />
                    </Button>
                  </div>
                </div>

                {descriptor ? (
                  <>
                    <TransportConfigEditor
                      descriptor={descriptor}
                      value={config}
                      onChange={(next) => update(i, { [descriptor.configProperty]: next } as Partial<SinkSpec>)}
                      isEdit={isEdit}
                      disabled={disabled}
                      idPrefix={`sink-${i}`}
                      direction="outbound"
                    />
                    <KeyColumnsSuggestion
                      descriptor={descriptor}
                      config={config}
                      declaredKeyColumns={declaredKeyColumns}
                      disabled={disabled}
                      onApply={(keyColumns) => update(i, { [descriptor.configProperty]: { ...config, keyColumns } } as Partial<SinkSpec>)}
                    />
                  </>
                ) : (
                  // A stored sink whose kind is no longer registered (the transport was removed, or the
                  // catalog has not loaded). Shown rather than hidden — silently dropping it on the next
                  // save would delete configuration the user never chose to delete.
                  <p className="text-[11px] text-muted-foreground">
                    No transport registered for kind <span className="font-mono">{sink.kind}</span> — its configuration is
                    preserved but cannot be edited here.
                  </p>
                )}
              </div>
            )
          })}
        </div>
      )}
    </FieldGroup>
  )
}

/**
 * Plan 014: "the console prefills [a database sink's upsert keyColumns] from the table's declared keys" —
 * DbSinkConfig's own doc comment. A VISIBLE, EDITABLE suggestion, never a silent derivation: when
 * `TableGroupKeyExtractor.Describe(sql)` fell back to whole-row identity (the exact case
 * `TableRowIdentityWarning` exists to call out), the "declared" keys are the ones that DIDN'T resolve —
 * blindly writing them into keyColumns would misconfigure the sink's unique-index requirement the same
 * way the fallback misconfigures history/sharding. So this never sets the field on mount or on data
 * change; it only offers a button, and only while the field is still empty (an operator's own edit is
 * never overwritten).
 *
 * Detected by CONFIG SHAPE (a `mode` + `keyColumns` field pair) rather than a hardcoded "postgres" /
 * "mssql" kind check, matching this file's existing descriptor-driven style — any future sink descriptor
 * with the same two fields gets the same suggestion for free.
 */
function KeyColumnsSuggestion({
  descriptor,
  config,
  declaredKeyColumns,
  disabled,
  onApply,
}: {
  descriptor: TransportDescriptor
  config: TransportConfigValue
  declaredKeyColumns?: string[]
  disabled: boolean
  onApply: (keyColumns: string) => void
}) {
  const hasUpsertShape = descriptor.fields.some((f) => f.key === 'mode') && descriptor.fields.some((f) => f.key === 'keyColumns')
  if (!hasUpsertShape) return null
  if (!declaredKeyColumns || declaredKeyColumns.length === 0) return null
  if (config.mode !== 'upsert') return null
  const current = typeof config.keyColumns === 'string' ? config.keyColumns.trim() : ''
  if (current !== '') return null

  const suggestion = declaredKeyColumns.join(', ')
  return (
    <Alert>
      <AlertDescription className="flex flex-wrap items-center gap-2">
        <span>
          This table's declared identity: <span className="font-mono">{suggestion}</span>
        </span>
        <Button type="button" variant="outline" size="sm" disabled={disabled} onClick={() => onApply(suggestion)}>
          Use these key columns
        </Button>
      </AlertDescription>
    </Alert>
  )
}
