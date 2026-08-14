import type { TransportDescriptor, TransportField, TransportGroup } from '@/api/types'
import { Input } from '@/components/ui/input'
import { Textarea } from '@/components/ui/textarea'
import { Switch } from '@/components/ui/switch'
import { Field, FieldGroup, FieldLabel } from '@/components/ui/field'
import { Select, SelectContent, SelectGroup, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'

/** The transport's own config object as it goes on the wire (a NatsSubConfig, a NatsPubConfig, …). This
 *  component never knows which — it reads and writes by descriptor key. */
export type TransportConfigValue = Record<string, unknown>

/**
 * Plan 010: ONE config editor for every transport, driven by the descriptor from `GET /api/transports`.
 * This file replaced a hand-written per-kind editor (`NatsConfigEditor.tsx`, 261 lines) and is the last
 * piece of "adding a transport touches nothing but the transport" — before it, a new kind meant a new React
 * component, a new form-state type, and wiring in two pages.
 *
 * What it deliberately does NOT do is decide anything: no conditional visibility, no cross-field rules, no
 * client-side required checks beyond a visual marker. The server validates and returns a list of messages
 * the modal already renders — duplicating those rules here is how the two would come to disagree.
 *
 * Secrets follow the same secrets-lite convention as every other editor in this console: a stored value
 * reads back as `***`, and sending `***` unchanged keeps it.
 */
export function TransportConfigEditor({
  descriptor,
  value,
  onChange,
  isEdit,
  disabled = false,
  idPrefix = 'tcfg',
}: {
  descriptor: TransportDescriptor
  value: TransportConfigValue
  onChange: (next: TransportConfigValue) => void
  isEdit: boolean
  disabled?: boolean
  idPrefix?: string
}) {
  const ungrouped = descriptor.fields.filter((f) => !f.group)
  const hasSecret = descriptor.fields.some((f) => f.type === 'secret')

  function set(field: TransportField, group: TransportGroup | undefined, next: unknown) {
    const objectKey = group?.objectKey
    if (!objectKey) {
      onChange({ ...value, [field.key]: next })
      return
    }
    const nested = (value[objectKey] as TransportConfigValue | null | undefined) ?? {}
    onChange({ ...value, [objectKey]: { ...nested, [field.key]: next } })
  }

  function read(field: TransportField, group: TransportGroup | undefined): unknown {
    const objectKey = group?.objectKey
    if (!objectKey) return value[field.key]
    return (value[objectKey] as TransportConfigValue | null | undefined)?.[field.key]
  }

  function toggleGroup(group: TransportGroup, on: boolean) {
    if (!group.objectKey) return
    onChange({
      ...value,
      // Off writes null, not an empty object: "absent entirely" is a distinct, meaningful state (core NATS
      // vs a JetStream consumer), and an empty object would fail server validation instead.
      [group.objectKey]: on ? defaultsFor(descriptor.fields.filter((f) => f.group === group.key)) : null,
    })
  }

  return (
    <FieldGroup className="gap-3">
      {descriptor.help && <p className="-mb-1 text-[11px] text-muted-foreground">{descriptor.help}</p>}

      {ungrouped.map((field) => (
        <FieldInput
          key={field.key}
          field={field}
          id={`${idPrefix}-${field.key}`}
          value={read(field, undefined)}
          onChange={(next) => set(field, undefined, next)}
          isEdit={isEdit}
          disabled={disabled}
        />
      ))}

      {descriptor.groups.map((group) => {
        const fields = descriptor.fields.filter((f) => f.group === group.key)
        if (fields.length === 0) return null
        const enabled = !group.optional || (group.objectKey ? value[group.objectKey] != null : true)

        return (
          <div key={group.key} className="flex flex-col gap-2 rounded-lg border border-border p-3">
            <div className="flex items-center justify-between">
              <p className="text-[11px] font-medium uppercase tracking-wide text-muted-foreground">
                {group.label}
                {group.optional && ' (optional)'}
              </p>
              {group.optional && (
                <Switch
                  id={`${idPrefix}-group-${group.key}`}
                  checked={enabled}
                  onCheckedChange={(checked) => toggleGroup(group, checked)}
                  disabled={disabled}
                  aria-label={group.label}
                />
              )}
            </div>
            {group.help && <p className="text-[11px] text-muted-foreground">{group.help}</p>}
            {enabled &&
              fields.map((field) => (
                <FieldInput
                  key={field.key}
                  field={field}
                  id={`${idPrefix}-${group.key}-${field.key}`}
                  value={read(field, group)}
                  onChange={(next) => set(field, group, next)}
                  isEdit={isEdit}
                  disabled={disabled}
                />
              ))}
          </div>
        )
      })}

      {isEdit && hasSecret && (
        <p className="-mt-1 text-[11px] text-muted-foreground">
          A credential field showing <span className="font-mono">***</span> is already stored — leave it as-is to keep it.
        </p>
      )}
    </FieldGroup>
  )
}

function FieldInput({
  field,
  id,
  value,
  onChange,
  isEdit,
  disabled,
}: {
  field: TransportField
  id: string
  value: unknown
  onChange: (next: unknown) => void
  isEdit: boolean
  disabled: boolean
}) {
  const label = (
    <FieldLabel htmlFor={id}>
      {field.label}
      {field.required && <span className="ml-0.5 text-muted-foreground">*</span>}
    </FieldLabel>
  )
  const help = field.help ? <p className="mt-1 text-[11px] text-muted-foreground">{field.help}</p> : null

  if (field.type === 'bool') {
    return (
      <Field>
        <label className="flex items-center gap-2 text-sm">
          <Switch id={id} checked={value === true} onCheckedChange={(checked) => onChange(checked)} disabled={disabled} />
          {field.label}
        </label>
        {help}
      </Field>
    )
  }

  if (field.type === 'select') {
    return (
      <Field>
        {label}
        <Select value={asString(value)} onValueChange={(v) => onChange(v)} disabled={disabled}>
          <SelectTrigger id={id} className="w-full">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectGroup>
              {(field.options ?? []).map((o) => (
                <SelectItem key={o} value={o}>
                  {o}
                </SelectItem>
              ))}
            </SelectGroup>
          </SelectContent>
        </Select>
        {help}
      </Field>
    )
  }

  if (field.type === 'text') {
    return (
      <Field>
        {label}
        <Textarea
          id={id}
          value={asString(value)}
          onChange={(e) => onChange(e.target.value)}
          disabled={disabled}
          placeholder={field.placeholder ?? undefined}
          rows={4}
          className={field.mono ? 'font-mono text-xs' : undefined}
        />
        {help}
      </Field>
    )
  }

  if (field.type === 'number') {
    return (
      <Field>
        {label}
        <Input
          id={id}
          type="number"
          value={typeof value === 'number' ? value : ''}
          onChange={(e) => onChange(e.target.value === '' ? null : Number(e.target.value))}
          disabled={disabled}
          className="w-40"
        />
        {help}
      </Field>
    )
  }

  const isSecret = field.type === 'secret'
  return (
    <Field>
      {label}
      <Input
        id={id}
        type={isSecret ? 'password' : 'text'}
        value={asString(value)}
        onChange={(e) => onChange(e.target.value)}
        disabled={disabled}
        placeholder={isSecret && isEdit ? '*** keeps the stored value' : (field.placeholder ?? undefined)}
        className={field.mono ? 'font-mono' : undefined}
      />
      {help}
    </Field>
  )
}

function asString(value: unknown): string {
  return typeof value === 'string' ? value : value == null ? '' : String(value)
}

/** Initial config for a NEW entity: every field's declared default, and every optional group off (its
 *  nested object null) — matching the server-side defaults so the first save round-trips unchanged. */
export function emptyTransportConfig(descriptor: TransportDescriptor): TransportConfigValue {
  const flat = descriptor.fields.filter((f) => {
    const group = descriptor.groups.find((g) => g.key === f.group)
    return !group?.objectKey
  })
  const config = defaultsFor(flat)
  for (const group of descriptor.groups) {
    if (group.objectKey) config[group.objectKey] = null
  }
  return config
}

function defaultsFor(fields: TransportField[]): TransportConfigValue {
  const out: TransportConfigValue = {}
  for (const f of fields) {
    out[f.key] = coerceDefault(f)
  }
  return out
}

function coerceDefault(field: TransportField): unknown {
  if (field.default == null) {
    return field.type === 'bool' ? false : field.type === 'number' ? null : ''
  }
  if (field.type === 'bool') return field.default === 'true'
  if (field.type === 'number') return Number(field.default)
  return field.default
}
