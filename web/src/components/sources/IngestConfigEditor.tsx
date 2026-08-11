import type { IngestConfig, IngressOverflowPolicy } from '@/api/types'
import { Input } from '@/components/ui/input'
import { Switch } from '@/components/ui/switch'
import { Field, FieldGroup, FieldLabel } from '@/components/ui/field'
import { Select, SelectContent, SelectGroup, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'

const POLICIES: IngressOverflowPolicy[] = ['Reject', 'Block', 'DropNewest', 'DropOldest', 'Inline']

const POLICY_HINT: Record<IngressOverflowPolicy, string> = {
  Reject: 'Refuse the whole batch (429 + Retry-After) when the buffer has no room — the default.',
  Block: 'Wait for space, up to the max wait below (server-capped at 30s).',
  DropNewest: 'Admit what fits, discard the overflow from the incoming batch.',
  DropOldest: 'Admit the batch, evicting the oldest buffered rows to make room.',
  Inline: 'No buffer — the request awaits the publish itself (not lossless: the transport can still drop downstream).',
}

export interface IngestFormState {
  policy: IngressOverflowPolicy
  capacityRows: number
  maxWaitMs: number
  maxBatchRows: number
  rejectUnknownFields: boolean
}

export function toIngestFormState(cfg?: IngestConfig | null): IngestFormState {
  return {
    policy: cfg?.policy ?? 'Reject',
    capacityRows: cfg?.capacityRows ?? 10_000,
    maxWaitMs: cfg?.maxWaitMs ?? 5_000,
    maxBatchRows: cfg?.maxBatchRows ?? 1_000,
    rejectUnknownFields: cfg?.rejectUnknownFields ?? false,
  }
}

export function buildIngestConfig(state: IngestFormState): IngestConfig {
  return {
    policy: state.policy,
    capacityRows: state.capacityRows,
    maxWaitMs: state.maxWaitMs,
    maxBatchRows: state.maxBatchRows,
    rejectUnknownFields: state.rejectUnknownFields,
  }
}

/** ingest-kind connector config: client-push admission control (plan 008 W4). No secrets, no
 * network-derived schema fetch — just the buffer/overflow knobs, so this stays flat next to
 * UrlConfigEditor/GrpcConfigEditor's shared idiom rather than growing their machinery. */
export function IngestConfigEditor({
  value,
  onChange,
  disabled = false,
}: {
  value: IngestFormState
  onChange: (patch: Partial<IngestFormState>) => void
  disabled?: boolean
}) {
  const isBlock = value.policy === 'Block'
  return (
    <FieldGroup className="gap-3">
      <Field>
        <FieldLabel htmlFor="ingest-cfg-policy">Overflow policy</FieldLabel>
        <Select value={value.policy} onValueChange={(v) => onChange({ policy: v as IngressOverflowPolicy })} disabled={disabled}>
          <SelectTrigger id="ingest-cfg-policy" className="w-full">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectGroup>
              {POLICIES.map((p) => (
                <SelectItem key={p} value={p}>
                  {p}
                </SelectItem>
              ))}
            </SelectGroup>
          </SelectContent>
        </Select>
        <p className="mt-1 text-[11px] text-muted-foreground">{POLICY_HINT[value.policy]}</p>
      </Field>

      <div className="grid grid-cols-2 gap-3">
        <Field>
          <FieldLabel htmlFor="ingest-cfg-capacity">Capacity (rows)</FieldLabel>
          <Input
            id="ingest-cfg-capacity"
            type="number"
            min={1}
            value={value.capacityRows}
            onChange={(e) => onChange({ capacityRows: Number(e.target.value) || 0 })}
            disabled={disabled}
          />
        </Field>
        <Field>
          <FieldLabel htmlFor="ingest-cfg-maxbatch">Max batch (rows)</FieldLabel>
          <Input
            id="ingest-cfg-maxbatch"
            type="number"
            min={1}
            value={value.maxBatchRows}
            onChange={(e) => onChange({ maxBatchRows: Number(e.target.value) || 0 })}
            disabled={disabled}
          />
          <p className="mt-1 text-[11px] text-muted-foreground">A bigger push is 413, never a partial admit.</p>
        </Field>
      </div>

      <Field>
        <FieldLabel htmlFor="ingest-cfg-maxwait">Max wait (ms)</FieldLabel>
        <Input
          id="ingest-cfg-maxwait"
          type="number"
          min={0}
          value={value.maxWaitMs}
          onChange={(e) => onChange({ maxWaitMs: Number(e.target.value) || 0 })}
          disabled={disabled || !isBlock}
        />
        <p className="mt-1 text-[11px] text-muted-foreground">
          {isBlock ? 'Server-capped at 30s.' : 'Only meaningful for the Block policy — ignored otherwise.'}
        </p>
      </Field>

      <Field orientation="horizontal" className="items-center pb-1.5">
        <Switch
          id="ingest-cfg-rejectunknown"
          checked={value.rejectUnknownFields}
          onCheckedChange={(checked) => onChange({ rejectUnknownFields: checked })}
          disabled={disabled}
        />
        <FieldLabel htmlFor="ingest-cfg-rejectunknown" className="font-normal">
          Reject rows with undeclared fields (otherwise dropped-and-counted silently)
        </FieldLabel>
      </Field>
    </FieldGroup>
  )
}
