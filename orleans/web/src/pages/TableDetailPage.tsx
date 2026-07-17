import { useEffect, useMemo, useState } from 'react'
import { useNavigate, useParams } from 'react-router-dom'
import { Check, CircleAlert, Play, Trash2, TriangleAlert } from 'lucide-react'
import { toast } from 'sonner'
import { tablesApi } from '../api/tables'
import { sourcesApi } from '../api/sources'
import type { RowValue, SourceDefinition, SqlDiagnostic, TableDefinition, TableOutputField } from '../api/types'
import { useAuth } from '../api/auth'
import { useTableRows } from '../hooks/useTableRows'
import { useTableMetrics } from '../hooks/useTableMetrics'
import { Topbar } from '../components/Topbar'
import { StatusBadge } from '../components/StatusBadge'
import { SqlEditor } from '../components/SqlEditor'
import { RoleGate } from '../components/RoleGate'
import { cn } from '@/lib/utils'
import { Card, CardContent } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Field, FieldLabel } from '@/components/ui/field'
import { Input } from '@/components/ui/input'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Skeleton } from '@/components/ui/skeleton'
import { Spinner } from '@/components/ui/spinner'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { Empty, EmptyDescription, EmptyHeader } from '@/components/ui/empty'
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '@/components/ui/alert-dialog'

function isJsonValue(v: RowValue): v is Record<string, RowValue> | RowValue[] {
  return typeof v === 'object' && v !== null
}

function formatCell(v: RowValue): string {
  if (v === undefined || v === null) return '—'
  if (typeof v === 'number') return Number.isInteger(v) ? v.toLocaleString() : v.toFixed(4)
  if (typeof v === 'boolean') return v ? 'true' : 'false'
  if (isJsonValue(v)) return JSON.stringify(v)
  return v
}

function formatClock(ms: number): string {
  return new Date(ms).toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit', second: '2-digit' })
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex flex-col gap-0.5 rounded-lg border border-border bg-background/60 px-3 py-2">
      <span className="text-[10px] font-medium uppercase tracking-wide text-muted-foreground">{label}</span>
      <span className="font-mono text-sm font-semibold text-foreground">{value}</span>
    </div>
  )
}

/** Live materialized-view grid: sorted by the first output column, mono numerics, sf-flash on rows
 * touched by the latest delta batch, and a weight column that only appears when some row's weight
 * is above 1 (rare — only visible mid-transition or on malformed dedupe). */
function MaterializedView({ table }: { table: TableDefinition }) {
  const { rows, live, flashKeys } = useTableRows(table.id, table.name)
  const { metrics, deltasInPerSec } = useTableMetrics(table.id)

  const sortedRows = useMemo(() => {
    const firstField = table.outputFields[0]
    if (!firstField) return rows
    const numeric = firstField.type === 'Double' || firstField.type === 'Long'
    return [...rows].sort((a, b) => {
      const av = a.row[firstField.name]
      const bv = b.row[firstField.name]
      if (numeric) return (Number(av) || 0) - (Number(bv) || 0)
      return String(av ?? '').localeCompare(String(bv ?? ''))
    })
  }, [rows, table.outputFields])

  const displayRows = sortedRows.slice(0, 500)
  const showWeightColumn = sortedRows.some((r) => r.weight > 1)

  return (
    <div className="flex flex-col gap-3">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div className="flex items-center gap-2 text-xs text-muted-foreground">
          <span className="relative flex size-2">
            {live && <span className="absolute inline-flex size-full animate-ping rounded-full bg-primary opacity-60" />}
            <span className={cn('relative inline-flex size-2 rounded-full', live ? 'bg-primary' : 'bg-muted-foreground')} />
          </span>
          <span className="font-medium text-foreground">{live ? 'Live' : 'Connecting…'}</span>
          <span>·</span>
          <span>
            {sortedRows.length.toLocaleString()} row{sortedRows.length === 1 ? '' : 's'}
          </span>
        </div>
        {metrics?.rebuilding && (
          <Badge variant="outline" className="border-warning/40 text-warning">
            Rebuilding
          </Badge>
        )}
      </div>

      <div className="grid grid-cols-2 gap-2 sm:grid-cols-4">
        <Stat label="Rows" value={(metrics?.rowCount ?? sortedRows.length).toLocaleString()} />
        <Stat label="Deltas in/s" value={deltasInPerSec.toFixed(1)} />
        <Stat label="Deltas out (total)" value={(metrics?.deltasOut ?? 0).toLocaleString()} />
        <Stat label="Last update" value={metrics ? formatClock(metrics.lastUpdateMs) : '—'} />
      </div>

      {sortedRows.length > 500 && (
        <p className="text-xs text-muted-foreground">
          Showing 500 of {sortedRows.length.toLocaleString()} rows.
        </p>
      )}

      <Card className="min-h-[16rem] flex-1 overflow-hidden py-0">
        {displayRows.length === 0 ? (
          <p className="px-4 py-10 text-center text-sm text-muted-foreground">Waiting for rows…</p>
        ) : (
          <div className="max-h-[28rem] overflow-auto">
            <Table className="min-w-max text-xs">
              <TableHeader className="sticky top-0 z-10 bg-card">
                <TableRow className="hover:bg-transparent">
                  {table.outputFields.map((f) => (
                    <TableHead key={f.name} className="uppercase tracking-wide text-muted-foreground">
                      {f.name}
                    </TableHead>
                  ))}
                  {showWeightColumn && (
                    <TableHead className="text-right uppercase tracking-wide text-muted-foreground">Weight</TableHead>
                  )}
                </TableRow>
              </TableHeader>
              <TableBody className="font-mono">
                {displayRows.map((r) => (
                  <TableRow key={r.key} className={cn(flashKeys.has(r.key) && 'sf-row-flash')}>
                    {table.outputFields.map((f) => {
                      const v: RowValue | undefined = r.row[f.name]
                      const json = v !== undefined && isJsonValue(v)
                      return (
                        <TableCell
                          key={f.name}
                          title={json ? formatCell(v) : undefined}
                          className={cn(
                            typeof v === 'number' ? 'text-right text-foreground' : 'text-foreground/80',
                            json && 'max-w-56 truncate font-mono',
                          )}
                        >
                          {formatCell(v)}
                        </TableCell>
                      )
                    })}
                    {showWeightColumn && <TableCell className="text-right text-foreground">{r.weight}</TableCell>}
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        )}
      </Card>
    </div>
  )
}

export function TableDetailPage() {
  const { id } = useParams<{ id: string }>()
  const navigate = useNavigate()
  const { hasRole } = useAuth()
  const isNew = id === 'new' || !id
  const canEdit = hasRole('Editor')

  const [table, setTable] = useState<TableDefinition | null>(null)
  const [loading, setLoading] = useState(!isNew)
  const [name, setName] = useState('')
  const [description, setDescription] = useState('')
  const [sql, setSql] = useState('')

  const [diagnostics, setDiagnostics] = useState<SqlDiagnostic[] | null>(null)
  const [planSummary, setPlanSummary] = useState<string | null>(null)
  const [outputSchema, setOutputSchema] = useState<TableOutputField[]>([])
  const [validatedStreamInputs, setValidatedStreamInputs] = useState<string[]>([])
  const [validatedTableInputs, setValidatedTableInputs] = useState<string[]>([])
  const [validating, setValidating] = useState(false)
  const [saving, setSaving] = useState(false)
  const [formError, setFormError] = useState<string | null>(null)
  const [confirmDelete, setConfirmDelete] = useState(false)

  const [sources, setSources] = useState<SourceDefinition[]>([])
  const [otherTables, setOtherTables] = useState<TableDefinition[]>([])

  // Real sources plus every other table exposed as a pseudo-source (its output schema mapped to
  // SourceDefinition-shaped fields) so the SQL editor's FROM/JOIN autocomplete suggests tables the
  // same way it suggests streams. The table currently being edited is excluded to avoid offering a
  // self-reference. Fetched once here so both the editor and any future consumer share one request.
  useEffect(() => {
    let cancelled = false
    sourcesApi
      .list()
      .then((list) => {
        if (!cancelled) setSources(list)
      })
      .catch(() => {
        if (!cancelled) setSources([])
      })
    tablesApi
      .list()
      .then((list) => {
        if (!cancelled) setOtherTables(list)
      })
      .catch(() => {
        if (!cancelled) setOtherTables([])
      })
    return () => {
      cancelled = true
    }
  }, [])

  const editorSources = useMemo<SourceDefinition[]>(() => {
    const pseudo: SourceDefinition[] = otherTables
      .filter((t) => t.id !== id)
      .map((t) => ({
        name: t.name,
        description: t.description,
        fields: t.outputFields,
        generatorProfile: 'generic',
        eventsPerSecond: 0,
        enabled: true,
      }))
    return [...sources, ...pseudo]
  }, [sources, otherTables, id])

  useEffect(() => {
    if (isNew) {
      setTable(null)
      setName('')
      setDescription('')
      setSql('')
      setLoading(false)
      return
    }
    setLoading(true)
    tablesApi
      .get(id!)
      .then((t) => {
        setTable(t)
        setName(t.name)
        setDescription(t.description)
        setSql(t.sql)
      })
      .finally(() => setLoading(false))
  }, [id, isNew])

  useEffect(() => {
    if (!sql.trim()) {
      setDiagnostics(null)
      setPlanSummary(null)
      setOutputSchema([])
      setValidatedStreamInputs([])
      setValidatedTableInputs([])
      return
    }
    setValidating(true)
    const timer = setTimeout(() => {
      tablesApi
        .validate({ sql })
        .then((res) => {
          setDiagnostics(res.diagnostics)
          if (res.ok) {
            setPlanSummary(res.planSummary)
            setOutputSchema(res.outputSchema)
            setValidatedStreamInputs(res.streamInputs)
            setValidatedTableInputs(res.tableInputs)
          } else {
            setPlanSummary(null)
            setOutputSchema([])
            setValidatedStreamInputs([])
            setValidatedTableInputs([])
          }
        })
        .catch(() => {
          setDiagnostics(null)
          setPlanSummary(null)
          setOutputSchema([])
          setValidatedStreamInputs([])
          setValidatedTableInputs([])
        })
        .finally(() => setValidating(false))
    }, 500)
    return () => clearTimeout(timer)
  }, [sql])

  async function handleSave(startAfter: boolean) {
    setFormError(null)
    if (!name.trim()) {
      setFormError('Name is required.')
      return
    }
    setSaving(true)
    try {
      let saved: TableDefinition
      if (isNew) {
        saved = await tablesApi.create({ name: name.trim(), description, sql })
      } else {
        saved = await tablesApi.update(id!, { name: name.trim(), description, sql })
      }
      if (startAfter && saved.status !== 'Running') {
        saved = await tablesApi.start(saved.id)
        if (saved.status === 'Failed' && saved.error) toast.error(saved.error)
      }
      setTable(saved)
      if (isNew) navigate(`/tables/${saved.id}`, { replace: true })
    } catch (err) {
      toast.error(err instanceof Error ? err.message : 'Failed to save table.')
    } finally {
      setSaving(false)
    }
  }

  async function handleDelete() {
    if (!table) return
    setSaving(true)
    try {
      await tablesApi.remove(table.id)
      navigate('/tables', { replace: true })
    } catch (err) {
      toast.error(err instanceof Error ? err.message : 'Failed to delete table.')
      setSaving(false)
    }
  }

  const currentStatus = table?.status ?? 'Stopped'

  if (loading) {
    return (
      <div className="p-8">
        <Skeleton className="h-8 w-64" />
        <Skeleton className="mt-4 h-96 w-full" />
      </div>
    )
  }

  return (
    <div>
      <Topbar
        title={isNew ? 'New table' : name || 'Table'}
        subtitle={isNew ? 'Define a windowless materialized view' : table?.id}
        action={!isNew && <StatusBadge status={currentStatus} />}
      />

      <div className="grid grid-cols-1 gap-6 p-8 xl:grid-cols-2">
        {/* LEFT */}
        <div className="flex flex-col gap-4">
          <Card>
            <CardContent className="grid grid-cols-1 gap-3 sm:grid-cols-2">
              <Field>
                <FieldLabel htmlFor="tbl-name">Name</FieldLabel>
                <Input
                  id="tbl-name"
                  value={name}
                  disabled={!canEdit}
                  onChange={(e) => setName(e.target.value)}
                  placeholder="buy_volume"
                />
              </Field>
              <Field>
                <FieldLabel htmlFor="tbl-description">Description</FieldLabel>
                <Input
                  id="tbl-description"
                  value={description}
                  disabled={!canEdit}
                  onChange={(e) => setDescription(e.target.value)}
                  placeholder="Running buy-side volume per symbol"
                />
              </Field>
            </CardContent>
          </Card>

          <SqlEditor value={sql} onChange={setSql} diagnostics={diagnostics ?? []} readOnly={!canEdit} sources={editorSources} />

          <Card>
            <CardContent>
              <h3 className="mb-2 text-xs font-medium uppercase tracking-wide text-muted-foreground">Validation</h3>
              {validating ? (
                <p className="text-sm text-muted-foreground">Validating…</p>
              ) : diagnostics === null ? (
                <p className="text-sm text-muted-foreground">Start typing SQL to validate.</p>
              ) : diagnostics.length === 0 ? (
                <div className="flex flex-col gap-3">
                  <Alert>
                    <Check className="text-primary" />
                    <AlertDescription>Valid{planSummary ? ` — ${planSummary}` : ''}</AlertDescription>
                  </Alert>

                  {outputSchema.length > 0 && (
                    <div>
                      <h4 className="mb-1.5 text-xs font-medium uppercase tracking-wide text-muted-foreground">Output schema</h4>
                      <div className="overflow-hidden rounded-lg border border-border">
                        <Table className="text-xs">
                          <TableHeader>
                            <TableRow className="hover:bg-transparent">
                              <TableHead>Field</TableHead>
                              <TableHead>Type</TableHead>
                            </TableRow>
                          </TableHeader>
                          <TableBody className="font-mono">
                            {outputSchema.map((f, i) => (
                              <TableRow key={`${f.name}-${i}`}>
                                <TableCell className="text-foreground">{f.name}</TableCell>
                                <TableCell className="text-muted-foreground">{f.kind}</TableCell>
                              </TableRow>
                            ))}
                          </TableBody>
                        </Table>
                      </div>
                    </div>
                  )}

                  {(validatedStreamInputs.length > 0 || validatedTableInputs.length > 0) && (
                    <div className="flex flex-wrap gap-1.5">
                      {validatedStreamInputs.map((s) => (
                        <Badge key={`s-${s}`} variant="outline" className="text-muted-foreground">
                          {s}
                        </Badge>
                      ))}
                      {validatedTableInputs.map((t) => (
                        <Badge key={`t-${t}`} variant="secondary">
                          {t}
                        </Badge>
                      ))}
                    </div>
                  )}
                </div>
              ) : (
                <Alert variant="destructive">
                  <CircleAlert />
                  <AlertDescription>
                    <ul className="flex flex-col gap-1.5">
                      {diagnostics.map((d, i) => (
                        <li key={i} className="flex items-start gap-2 text-xs">
                          {d.severity === 'Error' ? (
                            <CircleAlert className="mt-0.5 size-3.5 shrink-0" />
                          ) : (
                            <TriangleAlert className="mt-0.5 size-3.5 shrink-0 text-warning" />
                          )}
                          <span>
                            <span className="font-mono text-muted-foreground">
                              {d.line}:{d.column}
                            </span>{' '}
                            {d.message}
                          </span>
                        </li>
                      ))}
                    </ul>
                  </AlertDescription>
                </Alert>
              )}
            </CardContent>
          </Card>

          {formError && (
            <Alert variant="destructive">
              <AlertDescription>{formError}</AlertDescription>
            </Alert>
          )}

          <RoleGate min="Editor">
            <div className="flex flex-wrap gap-2">
              <Button onClick={() => void handleSave(false)} disabled={saving}>
                {saving && <Spinner data-icon="inline-start" />}
                {saving ? 'Saving…' : 'Save'}
              </Button>
              <Button variant="outline" onClick={() => void handleSave(true)} disabled={saving}>
                <Play data-icon="inline-start" /> Save & start
              </Button>
              {!isNew && (
                <Button
                  variant="outline"
                  onClick={() => setConfirmDelete(true)}
                  disabled={saving}
                  className="ml-auto hover:border-destructive/40 hover:text-destructive"
                >
                  <Trash2 data-icon="inline-start" /> Delete
                </Button>
              )}
            </div>
          </RoleGate>
        </div>

        {/* RIGHT */}
        <div className="flex flex-col gap-4">
          {isNew || !table ? (
            <Empty className="h-full border border-dashed">
              <EmptyHeader>
                <EmptyDescription>Save the table to see its materialized view here.</EmptyDescription>
              </EmptyHeader>
            </Empty>
          ) : (
            <MaterializedView table={table} />
          )}
        </div>
      </div>

      <AlertDialog open={confirmDelete} onOpenChange={setConfirmDelete}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete table?</AlertDialogTitle>
            <AlertDialogDescription>
              This permanently removes <span className="font-medium text-foreground">{name}</span> and its materialized rows. Tables
              that depend on it must be stopped first.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              variant="destructive"
              onClick={() => {
                setConfirmDelete(false)
                void handleDelete()
              }}
            >
              Delete
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  )
}
