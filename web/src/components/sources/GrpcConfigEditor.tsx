import { useState } from 'react'
import type { FieldDef, GrpcSubConfig } from '@/api/types'
import { sourcesApi } from '@/api/sources'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Textarea } from '@/components/ui/textarea'
import { Field, FieldGroup, FieldLabel } from '@/components/ui/field'
import { Select, SelectContent, SelectGroup, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Alert, AlertDescription } from '@/components/ui/alert'

type AuthMode = 'none' | 'password' | 'token'
type SchemaSource = 'reflection' | 'proto'

export interface GrpcFormState {
  address: string
  entityKey: string
  authMode: AuthMode
  username: string
  password: string
  token: string
  restAddress: string
  schemaSource: SchemaSource
  protoText: string
}

export function toGrpcFormState(cfg?: GrpcSubConfig | null): GrpcFormState {
  return {
    address: cfg?.address ?? '',
    entityKey: cfg?.entityKey ?? '',
    authMode: cfg?.token ? 'token' : cfg?.username ? 'password' : 'none',
    username: cfg?.username ?? '',
    password: cfg?.password ?? '',
    token: cfg?.token ?? '',
    restAddress: cfg?.restAddress ?? '',
    schemaSource: cfg?.schemaSource === 'proto' ? 'proto' : 'reflection',
    protoText: cfg?.protoText ?? '',
  }
}

export function buildGrpcConfig(state: GrpcFormState): GrpcSubConfig {
  return {
    address: state.address.trim(),
    entityKey: state.entityKey.trim(),
    username: state.authMode === 'password' ? state.username.trim() || null : null,
    password: state.authMode === 'password' ? state.password : null,
    token: state.authMode === 'token' ? state.token : null,
    restAddress: state.restAddress.trim() || null,
    schemaSource: state.schemaSource,
    protoText: state.schemaSource === 'proto' ? state.protoText : null,
  }
}

/**
 * grpc-kind connector config: the federation story (D-G) — subscribes a remote StreamForge
 * instance's source/pipeline/table over gRPC. Credential fields are secrets (D-H, masked "***" on
 * every read); schema is fetched once via "Fetch schema from remote" and replaces the source's
 * Fields editor content, same UX contract as the OpenAPI derive flow on url-kind sources.
 */
export function GrpcConfigEditor({
  value,
  onChange,
  isEdit,
  disabled = false,
  onFieldsFetched,
}: {
  value: GrpcFormState
  onChange: (patch: Partial<GrpcFormState>) => void
  isEdit: boolean
  disabled?: boolean
  onFieldsFetched: (fields: FieldDef[]) => void
}) {
  const [fetching, setFetching] = useState(false)
  const [diagnostics, setDiagnostics] = useState<string[] | null>(null)
  const [error, setError] = useState<string | null>(null)

  async function fetchSchema() {
    setError(null)
    setDiagnostics(null)
    setFetching(true)
    try {
      const result = await sourcesApi.fetchRemoteSchema({ grpc: buildGrpcConfig(value) })
      setDiagnostics(result.diagnostics)
      if (result.fields.length > 0) onFieldsFetched(result.fields)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch remote schema.')
    } finally {
      setFetching(false)
    }
  }

  return (
    <FieldGroup className="gap-3">
      <Field>
        <FieldLabel htmlFor="grpc-cfg-address">Remote gRPC address</FieldLabel>
        <Input
          id="grpc-cfg-address"
          value={value.address}
          onChange={(e) => onChange({ address: e.target.value })}
          placeholder="localhost:5299"
          disabled={disabled}
          className="font-mono"
        />
      </Field>
      <Field>
        <FieldLabel htmlFor="grpc-cfg-entitykey">Entity key</FieldLabel>
        <Input
          id="grpc-cfg-entitykey"
          value={value.entityKey}
          onChange={(e) => onChange({ entityKey: e.target.value })}
          placeholder="source:trades"
          disabled={disabled}
          className="font-mono"
        />
        <p className="mt-1 text-[11px] text-muted-foreground">
          One of <span className="font-mono">source:{'{name}'}</span>, <span className="font-mono">pipeline:{'{id}'}</span>,{' '}
          <span className="font-mono">table:{'{id}'}</span> on the remote instance.
        </p>
      </Field>

      <Field>
        <FieldLabel htmlFor="grpc-cfg-auth">Authentication</FieldLabel>
        <Select value={value.authMode} onValueChange={(v) => onChange({ authMode: v as AuthMode })} disabled={disabled}>
          <SelectTrigger id="grpc-cfg-auth" className="w-full">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectGroup>
              <SelectItem value="none">None</SelectItem>
              <SelectItem value="password">Username / password</SelectItem>
              <SelectItem value="token">Static token</SelectItem>
            </SelectGroup>
          </SelectContent>
        </Select>
      </Field>

      {value.authMode === 'password' && (
        <>
          <div className="grid grid-cols-2 gap-3">
            <Field>
              <FieldLabel htmlFor="grpc-cfg-username">Username</FieldLabel>
              <Input id="grpc-cfg-username" value={value.username} onChange={(e) => onChange({ username: e.target.value })} disabled={disabled} />
            </Field>
            <Field>
              <FieldLabel htmlFor="grpc-cfg-password">Password</FieldLabel>
              <Input
                id="grpc-cfg-password"
                type="password"
                value={value.password}
                onChange={(e) => onChange({ password: e.target.value })}
                disabled={disabled}
                placeholder={isEdit ? '*** keeps the stored value' : undefined}
              />
            </Field>
          </div>
          <Field>
            <FieldLabel htmlFor="grpc-cfg-restaddress">REST address (for login)</FieldLabel>
            <Input
              id="grpc-cfg-restaddress"
              value={value.restAddress}
              onChange={(e) => onChange({ restAddress: e.target.value })}
              placeholder="http://localhost:5199"
              disabled={disabled}
              className="font-mono"
            />
            <p className="mt-1 text-[11px] text-muted-foreground">
              Required with username/password — used to POST /api/auth/login on the remote instance.
            </p>
          </Field>
        </>
      )}
      {value.authMode === 'token' && (
        <Field>
          <FieldLabel htmlFor="grpc-cfg-token">Token</FieldLabel>
          <Input
            id="grpc-cfg-token"
            type="password"
            value={value.token}
            onChange={(e) => onChange({ token: e.target.value })}
            disabled={disabled}
            placeholder={isEdit ? '*** keeps the stored value' : undefined}
          />
        </Field>
      )}
      {isEdit && value.authMode !== 'none' && (
        <p className="-mt-2 text-[11px] text-muted-foreground">
          A credential field showing <span className="font-mono">***</span> is already stored — leave it as-is to keep it.
        </p>
      )}

      <Field>
        <FieldLabel htmlFor="grpc-cfg-schemasource">Schema source</FieldLabel>
        <Select
          value={value.schemaSource}
          onValueChange={(v) => onChange({ schemaSource: v as SchemaSource })}
          disabled={disabled}
        >
          <SelectTrigger id="grpc-cfg-schemasource" className="w-full">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectGroup>
              <SelectItem value="reflection">gRPC reflection</SelectItem>
              <SelectItem value="proto">Pasted .proto text</SelectItem>
            </SelectGroup>
          </SelectContent>
        </Select>
      </Field>
      {value.schemaSource === 'proto' && (
        <Field>
          <FieldLabel htmlFor="grpc-cfg-prototext">.proto text</FieldLabel>
          <Textarea
            id="grpc-cfg-prototext"
            value={value.protoText}
            onChange={(e) => onChange({ protoText: e.target.value })}
            rows={6}
            className="font-mono text-xs"
            disabled={disabled}
            placeholder={
              'Paste a StreamForge-generated .proto (from GET /api/{kind}/{key}/proto) — arbitrary third-party protos are rejected.'
            }
          />
        </Field>
      )}

      <Button
        type="button"
        variant="outline"
        size="sm"
        className="self-start"
        disabled={disabled || fetching || !value.address.trim() || !value.entityKey.trim()}
        onClick={() => void fetchSchema()}
      >
        {fetching ? 'Fetching…' : 'Fetch schema from remote'}
      </Button>
      {error && (
        <Alert variant="destructive">
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}
      {diagnostics && diagnostics.length > 0 && (
        <ul className="flex flex-col gap-0.5 text-[11px] text-muted-foreground">
          {diagnostics.map((d, i) => (
            <li key={i}>• {d}</li>
          ))}
        </ul>
      )}
      {diagnostics && diagnostics.length === 0 && <p className="text-[11px] text-primary">Fields fetched — check the Fields editor above.</p>}
    </FieldGroup>
  )
}
