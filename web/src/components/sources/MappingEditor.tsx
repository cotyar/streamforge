import { useState } from 'react'
import type { FieldDef, FieldMapEntry, MappingSpec } from '@/api/types'
import { sourcesApi } from '@/api/sources'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Textarea } from '@/components/ui/textarea'
import { Field, FieldGroup, FieldLabel } from '@/components/ui/field'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'

export interface MappingFormState {
  itemsPath: string
  dedupKeyField: string
  timestampField: string
  /** Index-aligned with the source's `fields` array (D-B: "one schema, sourcePath decorates it") —
   * position-based, same characteristic FieldEditor's own index-keyed rows already have. */
  sourcePaths: string[]
}

export function toMappingFormState(fields: FieldDef[], mapping?: MappingSpec | null): MappingFormState {
  const byName = new Map((mapping?.fields ?? []).map((e) => [e.field.name, e.sourcePath ?? '']))
  return {
    itemsPath: mapping?.itemsPath ?? '$',
    dedupKeyField: mapping?.dedupKeyField ?? '',
    timestampField: mapping?.timestampField ?? '',
    sourcePaths: fields.map((f) => byName.get(f.name) ?? ''),
  }
}

/** Keeps `sourcePaths` the same length as `fields` after a row is added/removed — call whenever
 * the source's Fields editor changes while mapping mode is active. */
export function resyncSourcePaths(fields: FieldDef[], sourcePaths: string[]): string[] {
  if (sourcePaths.length === fields.length) return sourcePaths
  const next = sourcePaths.slice(0, fields.length)
  while (next.length < fields.length) next.push('')
  return next
}

/** GET/derive/fetch endpoints round-trip FieldDef with explicit `children: null`/`isArray: false`
 * (System.Text.Json writes nulls by default) — fine for the main SourceDefinition POST/PUT, but
 * MappingLoader.Parse (the strict document parser behind /schema/mapping-validate, and the same
 * model embedded in ConnectorConfig.mapping) rejects an explicit `null` for `children` where it
 * wants either an array or the property omitted entirely. Strip the falsy optionals recursively
 * before a field ever goes into a mapping document. */
function sanitizeFieldForMapping(f: FieldDef): FieldDef {
  const out: FieldDef = { name: f.name, type: f.type }
  if (f.type === 'Json' && f.children && f.children.length > 0) {
    out.children = f.children.map(sanitizeFieldForMapping)
  }
  if (f.isArray) out.isArray = true
  return out
}

export function buildMappingSpec(fields: FieldDef[], state: MappingFormState): MappingSpec {
  const entries: FieldMapEntry[] = []
  fields.forEach((f, i) => {
    if (!f.name.trim()) return
    const sourcePath = state.sourcePaths[i]?.trim()
    entries.push({ sourcePath: sourcePath || null, field: sanitizeFieldForMapping(f) })
  })
  return {
    itemsPath: state.itemsPath.trim() || '$',
    dedupKeyField: state.dedupKeyField.trim() || null,
    timestampField: state.timestampField.trim() || null,
    fields: entries,
  }
}

function formatPreviewCell(v: unknown): string {
  if (v === null || v === undefined) return '—'
  if (typeof v === 'object') return JSON.stringify(v)
  return String(v)
}

/**
 * Optional mapping editor for url/file/folder connectors: itemsPath/dedupKeyField/timestampField
 * plus a "Validate mapping" round-trip against POST /schema/mapping-validate. The per-field
 * sourcePath column lives on the source's own Fields editor (SourcesPage's FieldEditor, in mapping
 * mode) — this component only owns the extraction-level settings and the validate/preview flow, so
 * the mapping's Fields stay literally the same array as the source's schema (D-B).
 */
export function MappingEditor({
  fields,
  state,
  onChange,
  disabled = false,
}: {
  fields: FieldDef[]
  state: MappingFormState
  onChange: (patch: Partial<MappingFormState>) => void
  disabled?: boolean
}) {
  const [sample, setSample] = useState('')
  const [validating, setValidating] = useState(false)
  const [result, setResult] = useState<{ ok: boolean; diagnostics: string[]; previewRows: Record<string, unknown>[] } | null>(null)
  const [error, setError] = useState<string | null>(null)

  async function validate() {
    setError(null)
    setResult(null)
    setValidating(true)
    try {
      const mapping = buildMappingSpec(fields, state)
      const response = await sourcesApi.validateMapping({
        document: JSON.stringify(mapping),
        sample: sample.trim() || null,
      })
      setResult({ ok: response.ok, diagnostics: response.diagnostics, previewRows: response.previewRows })
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to validate mapping.')
    } finally {
      setValidating(false)
    }
  }

  const previewColumns = result && result.previewRows.length > 0 ? Object.keys(result.previewRows[0]) : []

  return (
    <FieldGroup className="gap-3 rounded-lg border border-border p-3">
      <p className="text-[11px] font-medium uppercase tracking-wide text-muted-foreground">Mapping</p>
      <Field>
        <FieldLabel htmlFor="mapping-itemspath">Items path</FieldLabel>
        <Input
          id="mapping-itemspath"
          value={state.itemsPath}
          onChange={(e) => onChange({ itemsPath: e.target.value })}
          placeholder="$.data.items[*]"
          disabled={disabled}
          className="font-mono"
        />
        <p className="mt-1 text-[11px] text-muted-foreground">
          JSONPath-lite subset only: <span className="font-mono">$ .name ['name'] [n] [*]</span>.
        </p>
      </Field>
      <div className="grid grid-cols-2 gap-3">
        <Field>
          <FieldLabel htmlFor="mapping-dedup">Dedup key field</FieldLabel>
          <Input
            id="mapping-dedup"
            value={state.dedupKeyField}
            onChange={(e) => onChange({ dedupKeyField: e.target.value })}
            placeholder="optional — a mapped field name"
            disabled={disabled}
          />
        </Field>
        <Field>
          <FieldLabel htmlFor="mapping-ts">Timestamp field</FieldLabel>
          <Input
            id="mapping-ts"
            value={state.timestampField}
            onChange={(e) => onChange({ timestampField: e.target.value })}
            placeholder="optional — else arrival time"
            disabled={disabled}
          />
        </Field>
      </div>

      <Field>
        <FieldLabel htmlFor="mapping-sample">Sample payload (optional)</FieldLabel>
        <Textarea
          id="mapping-sample"
          value={sample}
          onChange={(e) => setSample(e.target.value)}
          rows={4}
          className="font-mono text-xs"
          placeholder='{"data": {"items": [...]}}'
          disabled={disabled}
        />
      </Field>

      <Button type="button" variant="outline" size="sm" className="self-start" disabled={disabled || validating} onClick={() => void validate()}>
        {validating ? 'Validating…' : 'Validate mapping'}
      </Button>

      {error && (
        <Alert variant="destructive">
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}

      {result && (
        <div className="flex flex-col gap-2">
          {result.diagnostics.length > 0 ? (
            <ul className="flex flex-col gap-0.5 text-[11px] text-destructive">
              {result.diagnostics.map((d, i) => (
                <li key={i}>• {d}</li>
              ))}
            </ul>
          ) : (
            <p className="text-[11px] text-primary">Mapping is valid.</p>
          )}
          {result.previewRows.length > 0 && (
            <div className="overflow-hidden rounded-lg border border-border">
              <Table>
                <TableHeader>
                  <TableRow>
                    {previewColumns.map((c) => (
                      <TableHead key={c} className="font-mono text-[11px]">
                        {c}
                      </TableHead>
                    ))}
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {result.previewRows.map((row, i) => (
                    <TableRow key={i}>
                      {previewColumns.map((c) => (
                        <TableCell key={c} className="font-mono text-[11px]">
                          {formatPreviewCell(row[c])}
                        </TableCell>
                      ))}
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          )}
        </div>
      )}
    </FieldGroup>
  )
}
