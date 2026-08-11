import type { SinkSpec } from '@/api/types'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Switch } from '@/components/ui/switch'
import { Badge } from '@/components/ui/badge'
import { Field, FieldGroup, FieldLabel } from '@/components/ui/field'
import { Select, SelectContent, SelectGroup, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Plus, Trash2 } from 'lucide-react'

type AuthMode = 'none' | 'token' | 'password' | 'creds'

function authModeOf(nats: SinkSpec['nats']): AuthMode {
  if (nats?.token) return 'token'
  if (nats?.credentials) return 'creds'
  if (nats?.username) return 'password'
  return 'none'
}

function emptySink(): SinkSpec {
  return { kind: 'nats', enabled: true, nats: { url: '', subject: '', token: null, username: null, password: null, credentials: null } }
}

/**
 * Outbound sinks (plan 009 B2) — the platform's first outbound concept, shared verbatim between
 * PipelineDetailPage and TableDetailPage since a `SinkSpec[]` means the same thing on both
 * (republish result rows / table deltas to a NATS subject). Only kind 'nats' exists today, so this
 * doesn't offer a kind picker — every "add" appends a NATS sink. Credentials follow the same
 * secrets-lite convention as every other connector editor: read back as "***", sending "***" back
 * keeps the stored value (see SecretsMasker.MergeSinkSecrets on the backend, matched positionally by
 * list index since a sink has no stable id).
 */
export function SinksEditor({
  value,
  onChange,
  isEdit,
  disabled = false,
}: {
  value: SinkSpec[]
  onChange: (next: SinkSpec[]) => void
  isEdit: boolean
  disabled?: boolean
}) {
  function update(i: number, patch: Partial<SinkSpec>) {
    onChange(value.map((s, idx) => (idx === i ? { ...s, ...patch } : s)))
  }
  function updateNats(i: number, patch: Partial<NonNullable<SinkSpec['nats']>>) {
    const sink = value[i]
    update(i, { nats: { ...(sink.nats ?? { url: '', subject: '' }), ...patch } })
  }
  function remove(i: number) {
    onChange(value.filter((_, idx) => idx !== i))
  }

  return (
    <FieldGroup className="gap-3">
      <div className="flex items-center justify-between">
        <FieldLabel className="mb-0">Sinks</FieldLabel>
        <Button type="button" variant="ghost" size="sm" onClick={() => onChange([...value, emptySink()])} disabled={disabled}>
          <Plus data-icon="inline-start" /> Add NATS sink
        </Button>
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
            const authMode = authModeOf(sink.nats)
            return (
              <div key={i} className="flex flex-col gap-2 rounded-lg border border-border p-3">
                <div className="flex items-center justify-between">
                  <Badge variant="outline">nats</Badge>
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

                <div className="grid grid-cols-2 gap-3">
                  <Field>
                    <FieldLabel htmlFor={`sink-${i}-url`}>Server URL</FieldLabel>
                    <Input
                      id={`sink-${i}-url`}
                      value={sink.nats?.url ?? ''}
                      onChange={(e) => updateNats(i, { url: e.target.value })}
                      placeholder="nats://localhost:4222"
                      disabled={disabled}
                      className="font-mono"
                    />
                  </Field>
                  <Field>
                    <FieldLabel htmlFor={`sink-${i}-subject`}>Subject</FieldLabel>
                    <Input
                      id={`sink-${i}-subject`}
                      value={sink.nats?.subject ?? ''}
                      onChange={(e) => updateNats(i, { subject: e.target.value })}
                      placeholder="streamforge.{name}"
                      disabled={disabled}
                      className="font-mono"
                    />
                  </Field>
                </div>
                <p className="-mt-1 text-[11px] text-muted-foreground">
                  <span className="font-mono">{'{name}'}</span> in the subject is replaced with this pipeline/table's name.
                </p>

                <Field>
                  <FieldLabel htmlFor={`sink-${i}-auth`}>Authentication</FieldLabel>
                  <Select
                    value={authMode}
                    onValueChange={(v) => {
                      const mode = v as AuthMode
                      updateNats(i, {
                        token: mode === 'token' ? '' : null,
                        username: mode === 'password' ? '' : null,
                        password: mode === 'password' ? '' : null,
                        credentials: mode === 'creds' ? '' : null,
                      })
                    }}
                    disabled={disabled}
                  >
                    <SelectTrigger id={`sink-${i}-auth`} className="w-full">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectGroup>
                        <SelectItem value="none">None</SelectItem>
                        <SelectItem value="token">Static token</SelectItem>
                        <SelectItem value="password">Username / password</SelectItem>
                        <SelectItem value="creds">.creds file</SelectItem>
                      </SelectGroup>
                    </SelectContent>
                  </Select>
                </Field>

                {authMode === 'token' && (
                  <Field>
                    <FieldLabel htmlFor={`sink-${i}-token`}>Token</FieldLabel>
                    <Input
                      id={`sink-${i}-token`}
                      type="password"
                      value={sink.nats?.token ?? ''}
                      onChange={(e) => updateNats(i, { token: e.target.value })}
                      disabled={disabled}
                      placeholder={isEdit ? '*** keeps the stored value' : undefined}
                    />
                  </Field>
                )}
                {authMode === 'password' && (
                  <div className="grid grid-cols-2 gap-3">
                    <Field>
                      <FieldLabel htmlFor={`sink-${i}-username`}>Username</FieldLabel>
                      <Input
                        id={`sink-${i}-username`}
                        value={sink.nats?.username ?? ''}
                        onChange={(e) => updateNats(i, { username: e.target.value })}
                        disabled={disabled}
                      />
                    </Field>
                    <Field>
                      <FieldLabel htmlFor={`sink-${i}-password`}>Password</FieldLabel>
                      <Input
                        id={`sink-${i}-password`}
                        type="password"
                        value={sink.nats?.password ?? ''}
                        onChange={(e) => updateNats(i, { password: e.target.value })}
                        disabled={disabled}
                        placeholder={isEdit ? '*** keeps the stored value' : undefined}
                      />
                    </Field>
                  </div>
                )}
                {authMode === 'creds' && (
                  <Field>
                    <FieldLabel htmlFor={`sink-${i}-creds`}>.creds file contents</FieldLabel>
                    <Input
                      id={`sink-${i}-creds`}
                      type="password"
                      value={sink.nats?.credentials ?? ''}
                      onChange={(e) => updateNats(i, { credentials: e.target.value })}
                      disabled={disabled}
                      placeholder={isEdit ? '*** keeps the stored value' : 'Paste the contents of a NATS .creds file'}
                      className="font-mono"
                    />
                  </Field>
                )}
                {isEdit && authMode !== 'none' && (
                  <p className="text-[11px] text-muted-foreground">
                    A credential field showing <span className="font-mono">***</span> is already stored — leave it as-is
                    to keep it.
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
