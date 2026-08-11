import type { FileFormat, NatsSubConfig } from '@/api/types'
import { Input } from '@/components/ui/input'
import { Switch } from '@/components/ui/switch'
import { Field, FieldGroup, FieldLabel } from '@/components/ui/field'
import { Select, SelectContent, SelectGroup, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'

type AuthMode = 'none' | 'token' | 'password' | 'creds'
const FORMATS: FileFormat[] = ['ndjson', 'json', 'csv']

export interface NatsFormState {
  url: string
  subject: string
  queueGroup: string
  format: FileFormat
  authMode: AuthMode
  token: string
  username: string
  password: string
  credentials: string
  jetStreamEnabled: boolean
  jsStream: string
  jsDurable: string
  jsMaxAckPending: number
}

export function toNatsFormState(cfg?: NatsSubConfig | null): NatsFormState {
  return {
    url: cfg?.url ?? '',
    subject: cfg?.subject ?? '',
    queueGroup: cfg?.queueGroup ?? '',
    format: cfg?.format ?? 'ndjson',
    authMode: cfg?.token ? 'token' : cfg?.credentials ? 'creds' : cfg?.username ? 'password' : 'none',
    token: cfg?.token ?? '',
    username: cfg?.username ?? '',
    password: cfg?.password ?? '',
    credentials: cfg?.credentials ?? '',
    jetStreamEnabled: !!cfg?.jetStream,
    jsStream: cfg?.jetStream?.stream ?? '',
    jsDurable: cfg?.jetStream?.durable ?? '',
    jsMaxAckPending: cfg?.jetStream?.maxAckPending ?? 25,
  }
}

export function buildNatsConfig(state: NatsFormState): NatsSubConfig {
  return {
    url: state.url.trim(),
    subject: state.subject.trim(),
    queueGroup: state.queueGroup.trim(),
    format: state.format,
    token: state.authMode === 'token' ? state.token : null,
    username: state.authMode === 'password' ? state.username.trim() || null : null,
    password: state.authMode === 'password' ? state.password : null,
    credentials: state.authMode === 'creds' ? state.credentials : null,
    jetStream: state.jetStreamEnabled
      ? { stream: state.jsStream.trim(), durable: state.jsDurable.trim(), maxAckPending: state.jsMaxAckPending }
      : null,
  }
}

/**
 * nats-kind connector config (plan 009 B1): a persistent subscription, not a poll schedule — same
 * idiom as GrpcConfigEditor's credentials (secrets-lite masked "***"; sending "***" back keeps the
 * stored value). Core NATS subscribe (JetStream off, the default) is at-most-once with no cursor;
 * JetStream opts into a durable server-side consumer instead.
 */
export function NatsConfigEditor({
  value,
  onChange,
  isEdit,
  disabled = false,
}: {
  value: NatsFormState
  onChange: (patch: Partial<NatsFormState>) => void
  isEdit: boolean
  disabled?: boolean
}) {
  return (
    <FieldGroup className="gap-3">
      <Field>
        <FieldLabel htmlFor="nats-cfg-url">Server URL</FieldLabel>
        <Input
          id="nats-cfg-url"
          value={value.url}
          onChange={(e) => onChange({ url: e.target.value })}
          placeholder="nats://localhost:4222"
          disabled={disabled}
          className="font-mono"
        />
      </Field>

      <div className="grid grid-cols-2 gap-3">
        <Field>
          <FieldLabel htmlFor="nats-cfg-subject">Subject</FieldLabel>
          <Input
            id="nats-cfg-subject"
            value={value.subject}
            onChange={(e) => onChange({ subject: e.target.value })}
            placeholder="trades.>"
            disabled={disabled}
            className="font-mono"
          />
        </Field>
        <Field>
          <FieldLabel htmlFor="nats-cfg-format">Payload format</FieldLabel>
          <Select value={value.format} onValueChange={(v) => onChange({ format: v as FileFormat })} disabled={disabled}>
            <SelectTrigger id="nats-cfg-format" className="w-full">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectGroup>
                {FORMATS.map((f) => (
                  <SelectItem key={f} value={f}>
                    {f}
                  </SelectItem>
                ))}
              </SelectGroup>
            </SelectContent>
          </Select>
        </Field>
      </div>

      <Field>
        <FieldLabel htmlFor="nats-cfg-queuegroup">Queue group (optional)</FieldLabel>
        <Input
          id="nats-cfg-queuegroup"
          value={value.queueGroup}
          onChange={(e) => onChange({ queueGroup: e.target.value })}
          placeholder="streamforge-ingest"
          disabled={disabled}
          className="font-mono"
        />
        <p className="mt-1 text-[11px] text-muted-foreground">
          Two replicas sharing a queue group split the subject's messages between them instead of both ingesting every
          message — set this when this source runs on more than one host.
        </p>
      </Field>

      <Field>
        <FieldLabel htmlFor="nats-cfg-auth">Authentication</FieldLabel>
        <Select value={value.authMode} onValueChange={(v) => onChange({ authMode: v as AuthMode })} disabled={disabled}>
          <SelectTrigger id="nats-cfg-auth" className="w-full">
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

      {value.authMode === 'token' && (
        <Field>
          <FieldLabel htmlFor="nats-cfg-token">Token</FieldLabel>
          <Input
            id="nats-cfg-token"
            type="password"
            value={value.token}
            onChange={(e) => onChange({ token: e.target.value })}
            disabled={disabled}
            placeholder={isEdit ? '*** keeps the stored value' : undefined}
          />
        </Field>
      )}
      {value.authMode === 'password' && (
        <div className="grid grid-cols-2 gap-3">
          <Field>
            <FieldLabel htmlFor="nats-cfg-username">Username</FieldLabel>
            <Input id="nats-cfg-username" value={value.username} onChange={(e) => onChange({ username: e.target.value })} disabled={disabled} />
          </Field>
          <Field>
            <FieldLabel htmlFor="nats-cfg-password">Password</FieldLabel>
            <Input
              id="nats-cfg-password"
              type="password"
              value={value.password}
              onChange={(e) => onChange({ password: e.target.value })}
              disabled={disabled}
              placeholder={isEdit ? '*** keeps the stored value' : undefined}
            />
          </Field>
        </div>
      )}
      {value.authMode === 'creds' && (
        <Field>
          <FieldLabel htmlFor="nats-cfg-creds">.creds file contents</FieldLabel>
          <Input
            id="nats-cfg-creds"
            type="password"
            value={value.credentials}
            onChange={(e) => onChange({ credentials: e.target.value })}
            disabled={disabled}
            placeholder={isEdit ? '*** keeps the stored value' : 'Paste the contents of a NATS .creds file'}
            className="font-mono"
          />
        </Field>
      )}
      {isEdit && value.authMode !== 'none' && (
        <p className="-mt-2 text-[11px] text-muted-foreground">
          A credential field showing <span className="font-mono">***</span> is already stored — leave it as-is to keep it.
        </p>
      )}

      <div className="flex flex-col gap-2 rounded-lg border border-border p-3">
        <div className="flex items-center justify-between">
          <p className="text-[11px] font-medium uppercase tracking-wide text-muted-foreground">JetStream (optional)</p>
          <Switch
            id="nats-cfg-jetstream"
            checked={value.jetStreamEnabled}
            onCheckedChange={(checked) => onChange({ jetStreamEnabled: checked })}
            disabled={disabled}
          />
        </div>
        <p className="text-[11px] text-muted-foreground">
          Off (default) is core NATS: at-most-once, no cursor, nothing to clean up server-side. Turning this on trades
          that for a durable JetStream consumer — messages are redelivered until acked, at the cost of server-side
          state this platform now owns and must not leave orphaned.
        </p>
        {value.jetStreamEnabled && (
          <div className="grid grid-cols-2 gap-3 pt-1">
            <Field>
              <FieldLabel htmlFor="nats-cfg-jsstream">Stream</FieldLabel>
              <Input
                id="nats-cfg-jsstream"
                value={value.jsStream}
                onChange={(e) => onChange({ jsStream: e.target.value })}
                disabled={disabled}
                className="font-mono"
              />
            </Field>
            <Field>
              <FieldLabel htmlFor="nats-cfg-jsdurable">Durable consumer name</FieldLabel>
              <Input
                id="nats-cfg-jsdurable"
                value={value.jsDurable}
                onChange={(e) => onChange({ jsDurable: e.target.value })}
                disabled={disabled}
                className="font-mono"
              />
            </Field>
            <Field className="col-span-2">
              <FieldLabel htmlFor="nats-cfg-jsmaxack">Max ack pending</FieldLabel>
              <Input
                id="nats-cfg-jsmaxack"
                type="number"
                min={1}
                value={value.jsMaxAckPending}
                onChange={(e) => onChange({ jsMaxAckPending: Math.max(1, Number(e.target.value) || 1) })}
                disabled={disabled}
                className="w-32"
              />
            </Field>
          </div>
        )}
      </div>
    </FieldGroup>
  )
}
