import { useState } from 'react'
import type { FieldDef, FileFormat, UrlPollConfig } from '@/api/types'
import { sourcesApi } from '@/api/sources'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Textarea } from '@/components/ui/textarea'
import { Field, FieldGroup, FieldLabel } from '@/components/ui/field'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { EndpointRefHint } from '@/components/EndpointRefHint'
import { Plus, Trash2 } from 'lucide-react'

export interface UrlFormState {
  url: string
  format: FileFormat
  headers: { key: string; value: string }[]
  openApiDocUrl: string
  openApiDocInline: string
  openApiOperationId: string
  openApiSchemaPointer: string
}

export function toUrlFormState(cfg?: UrlPollConfig | null): UrlFormState {
  return {
    url: cfg?.url ?? '',
    format: cfg?.format ?? 'json',
    headers: Object.entries(cfg?.headers ?? {}).map(([key, value]) => ({ key, value })),
    openApiDocUrl: cfg?.openApi?.docUrl ?? '',
    openApiDocInline: cfg?.openApi?.docInline ?? '',
    openApiOperationId: cfg?.openApi?.operationId ?? '',
    openApiSchemaPointer: cfg?.openApi?.schemaPointer ?? '',
  }
}

export function buildUrlConfig(state: UrlFormState): UrlPollConfig {
  const headers: Record<string, string> = {}
  for (const h of state.headers) {
    const key = h.key.trim()
    if (key) headers[key] = h.value
  }
  const hasOpenApi = !!state.openApiDocUrl.trim() || !!state.openApiDocInline.trim()
  return {
    url: state.url.trim(),
    format: state.format,
    headers,
    openApi: hasOpenApi
      ? {
          docUrl: state.openApiDocUrl.trim() || null,
          docInline: state.openApiDocInline.trim() || null,
          operationId: state.openApiOperationId.trim() || null,
          schemaPointer: state.openApiSchemaPointer.trim() || null,
        }
      : null,
  }
}

/**
 * url-kind connector config: target URL + header editor (values are password-typed since they
 * commonly carry auth secrets — D-H masks them as "***" on every read) + an optional OpenAPI
 * derivation flow that replaces the source's Fields editor content on success (D-F).
 */
export function UrlConfigEditor({
  value,
  onChange,
  isEdit,
  disabled = false,
  onFieldsDerived,
}: {
  value: UrlFormState
  onChange: (patch: Partial<UrlFormState>) => void
  isEdit: boolean
  disabled?: boolean
  onFieldsDerived: (fields: FieldDef[]) => void
}) {
  const [deriving, setDeriving] = useState(false)
  const [diagnostics, setDiagnostics] = useState<string[] | null>(null)
  const [error, setError] = useState<string | null>(null)

  function updateHeader(i: number, patch: Partial<{ key: string; value: string }>) {
    onChange({ headers: value.headers.map((h, idx) => (idx === i ? { ...h, ...patch } : h)) })
  }
  function addHeader() {
    onChange({ headers: [...value.headers, { key: '', value: '' }] })
  }
  function removeHeader(i: number) {
    onChange({ headers: value.headers.filter((_, idx) => idx !== i) })
  }

  async function derive() {
    setError(null)
    setDiagnostics(null)
    setDeriving(true)
    try {
      const result = await sourcesApi.deriveOpenApi({
        openApi: {
          docUrl: value.openApiDocUrl.trim() || null,
          docInline: value.openApiDocInline.trim() || null,
          operationId: value.openApiOperationId.trim() || null,
          schemaPointer: value.openApiSchemaPointer.trim() || null,
        },
      })
      setDiagnostics(result.diagnostics)
      if (result.fields.length > 0) onFieldsDerived(result.fields)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to derive schema.')
    } finally {
      setDeriving(false)
    }
  }

  return (
    <FieldGroup className="gap-3">
      <Field>
        <FieldLabel htmlFor="url-cfg-url">URL</FieldLabel>
        <Input
          id="url-cfg-url"
          value={value.url}
          onChange={(e) => onChange({ url: e.target.value })}
          placeholder="https://api.example.com/trades"
          disabled={disabled}
          className="font-mono"
        />
        {/* Plan 016 wave 6: a value that is ENTIRELY "@name" (e.g. "@primary-oltp") resolves at
            connect time from this instance's Endpoints:<name> configuration instead of being read
            literally — see NamedEndpoints.cs. This is the one field in the source editor wired to
            say whether the name is known HERE; see EndpointRefHint's own doc for the rest. */}
        <EndpointRefHint value={value.url} />
      </Field>

      <Field>
        <FieldLabel htmlFor="url-cfg-format">Response format</FieldLabel>
        <Select value={value.format} onValueChange={(v) => onChange({ format: v as FileFormat })} disabled={disabled}>
          <SelectTrigger id="url-cfg-format" className="w-full">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="json">json — a JSON document (array or object)</SelectItem>
            <SelectItem value="ndjson">ndjson — one JSON value per line</SelectItem>
            <SelectItem value="csv">csv — header row; TSV / semicolon / pipe are detected too</SelectItem>
            <SelectItem value="fix">fix — FIX tag=value; SOH or pipe delimiter detected</SelectItem>
          </SelectContent>
        </Select>
      </Field>

      <Field>
        <div className="mb-1 flex items-center justify-between">
          <FieldLabel className="mb-0">Headers</FieldLabel>
          <Button type="button" variant="ghost" size="sm" onClick={addHeader} disabled={disabled}>
            <Plus data-icon="inline-start" /> Add header
          </Button>
        </div>
        {value.headers.length === 0 ? (
          <p className="text-xs text-muted-foreground">No headers.</p>
        ) : (
          <div className="flex flex-col gap-1.5">
            {value.headers.map((h, i) => (
              <div key={i} className="flex items-center gap-1.5">
                <Input value={h.key} onChange={(e) => updateHeader(i, { key: e.target.value })} placeholder="header name" disabled={disabled} />
                <Input
                  type="password"
                  value={h.value}
                  onChange={(e) => updateHeader(i, { value: e.target.value })}
                  placeholder="value"
                  disabled={disabled}
                />
                <Button
                  type="button"
                  variant="ghost"
                  size="icon-sm"
                  className="shrink-0 hover:text-destructive"
                  onClick={() => removeHeader(i)}
                  disabled={disabled}
                >
                  <Trash2 />
                </Button>
              </div>
            ))}
          </div>
        )}
        {isEdit && (
          <p className="mt-1 text-[11px] text-muted-foreground">
            A header value showing <span className="font-mono">***</span> is already stored — leave it as-is to keep it, or type a
            new value to change it.
          </p>
        )}
      </Field>

      <div className="flex flex-col gap-2 rounded-lg border border-border p-3">
        <p className="text-[11px] font-medium uppercase tracking-wide text-muted-foreground">OpenAPI schema derivation (optional)</p>
        <Field>
          <FieldLabel htmlFor="url-cfg-docurl">Doc URL</FieldLabel>
          <Input
            id="url-cfg-docurl"
            value={value.openApiDocUrl}
            onChange={(e) => onChange({ openApiDocUrl: e.target.value })}
            placeholder="https://api.example.com/openapi.json"
            disabled={disabled}
            className="font-mono"
          />
        </Field>
        <p className="text-center text-[11px] text-muted-foreground">or paste inline —</p>
        <Field>
          <FieldLabel htmlFor="url-cfg-docinline">Inline document (JSON or YAML)</FieldLabel>
          <Textarea
            id="url-cfg-docinline"
            value={value.openApiDocInline}
            onChange={(e) => onChange({ openApiDocInline: e.target.value })}
            rows={4}
            className="font-mono text-xs"
            disabled={disabled}
          />
        </Field>
        <div className="grid grid-cols-2 gap-2">
          <Field>
            <FieldLabel htmlFor="url-cfg-opid">Operation ID</FieldLabel>
            <Input
              id="url-cfg-opid"
              value={value.openApiOperationId}
              onChange={(e) => onChange({ openApiOperationId: e.target.value })}
              placeholder="listTrades"
              disabled={disabled}
            />
          </Field>
          <Field>
            <FieldLabel htmlFor="url-cfg-pointer">Schema pointer</FieldLabel>
            <Input
              id="url-cfg-pointer"
              value={value.openApiSchemaPointer}
              onChange={(e) => onChange({ openApiSchemaPointer: e.target.value })}
              placeholder="#/components/schemas/Trade"
              disabled={disabled}
              className="font-mono"
            />
          </Field>
        </div>
        <Button
          type="button"
          variant="outline"
          size="sm"
          className="self-start"
          disabled={disabled || deriving || (!value.openApiDocUrl.trim() && !value.openApiDocInline.trim())}
          onClick={() => void derive()}
        >
          {deriving ? 'Deriving…' : 'Derive schema'}
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
        {diagnostics && diagnostics.length === 0 && (
          <p className="text-[11px] text-primary">Fields derived — check the Fields editor above.</p>
        )}
      </div>
    </FieldGroup>
  )
}
