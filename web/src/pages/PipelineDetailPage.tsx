import { useEffect, useState } from 'react'
import { useNavigate, useParams } from 'react-router-dom'
import { Check, CircleAlert, Play, Trash2, TriangleAlert } from 'lucide-react'
import { toast } from 'sonner'
import { pipelinesApi } from '../api/pipelines'
import { sourcesApi } from '../api/sources'
import type { Metadata, PipelineDefinition, SinkSpec, SourceDefinition, SqlDiagnostic, Tags } from '../api/types'
import { useAuth } from '../api/auth'
import { usePipelineResults } from '../hooks/usePipelineResults'
import { useMetricsStream } from '../hooks/useMetricsStream'
import { Topbar } from '../components/Topbar'
import { StatusBadge } from '../components/StatusBadge'
import { SqlEditor } from '../components/SqlEditor'
import { PipelineBuilder } from '../components/PipelineBuilder'
import { ResultsTable } from '../components/ResultsTable'
import { MetricsBar } from '../components/MetricsBar'
import { LiveChart } from '../components/LiveChart'
import { RoleGate } from '../components/RoleGate'
import { MetadataEditor } from '../components/MetadataEditor'
import { SinksEditor } from '../components/SinksEditor'
import { cn } from '@/lib/utils'
import { Card, CardContent } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Field, FieldLabel } from '@/components/ui/field'
import { Input } from '@/components/ui/input'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Skeleton } from '@/components/ui/skeleton'
import { Spinner } from '@/components/ui/spinner'
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
import type { BuilderState } from '../builder/types'
import { emptyBuilderState } from '../builder/types'
import { builderStateToSql } from '../builder/sqlgen'

type Mode = 'sql' | 'builder'

export function PipelineDetailPage() {
  const { id } = useParams<{ id: string }>()
  const navigate = useNavigate()
  const { hasRole } = useAuth()
  const isNew = id === 'new' || !id
  const canEdit = hasRole('Editor')

  const [pipeline, setPipeline] = useState<PipelineDefinition | null>(null)
  const [loading, setLoading] = useState(!isNew)
  const [name, setName] = useState('')
  const [description, setDescription] = useState('')
  const [sql, setSql] = useState('')
  const [mode, setMode] = useState<Mode>('sql')
  const [builderState, setBuilderState] = useState<BuilderState>(emptyBuilderState())
  const [tags, setTags] = useState<Tags>([])
  const [metadata, setMetadata] = useState<Metadata>({})
  const [sinks, setSinks] = useState<SinkSpec[]>([])

  const [diagnostics, setDiagnostics] = useState<SqlDiagnostic[] | null>(null)
  const [planSummary, setPlanSummary] = useState<string | null>(null)
  const [validating, setValidating] = useState(false)
  const [saving, setSaving] = useState(false)
  const [formError, setFormError] = useState<string | null>(null)
  const [confirmDelete, setConfirmDelete] = useState(false)
  const [sources, setSources] = useState<SourceDefinition[]>([])

  // Shared with the visual Builder tab and the SQL editor's autocomplete — fetched once here so
  // both consumers see the same list without duplicating the request.
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
    return () => {
      cancelled = true
    }
  }, [])

  useEffect(() => {
    if (isNew) {
      setPipeline(null)
      setName('')
      setDescription('')
      setSql('')
      setBuilderState(emptyBuilderState())
      setTags([])
      setMetadata({})
      setSinks([])
      setLoading(false)
      return
    }
    setLoading(true)
    pipelinesApi
      .get(id!)
      .then((p) => {
        setPipeline(p)
        setName(p.name)
        setDescription(p.description)
        setSql(p.sql)
        setTags(p.tags)
        setMetadata(p.metadata)
        setSinks(p.sinks ?? [])
      })
      .finally(() => setLoading(false))
  }, [id, isNew])

  const effectiveSql = mode === 'builder' ? builderStateToSql(builderState) : sql

  useEffect(() => {
    if (!effectiveSql.trim()) {
      setDiagnostics(null)
      setPlanSummary(null)
      return
    }
    setValidating(true)
    const timer = setTimeout(() => {
      pipelinesApi
        .validate({ sql: effectiveSql })
        .then((res) => {
          setDiagnostics(res.diagnostics)
          setPlanSummary(res.ok ? res.planSummary : null)
        })
        .catch(() => {
          setDiagnostics(null)
          setPlanSummary(null)
        })
        .finally(() => setValidating(false))
    }, 500)
    return () => clearTimeout(timer)
  }, [effectiveSql])

  function switchToSql() {
    if (mode === 'builder') setSql(builderStateToSql(builderState))
    setMode('sql')
  }

  async function handleSave(startAfter: boolean) {
    setFormError(null)
    if (!name.trim()) {
      setFormError('Name is required.')
      return
    }
    setSaving(true)
    try {
      const body = { name: name.trim(), description, sql: effectiveSql, tags, metadata, sinks }
      let saved: PipelineDefinition
      if (isNew) {
        saved = await pipelinesApi.create(body)
      } else {
        saved = await pipelinesApi.update(id!, body)
      }
      if (startAfter && saved.status !== 'Running') {
        saved = await pipelinesApi.start(saved.id)
      }
      setPipeline(saved)
      if (isNew) navigate(`/pipelines/${saved.id}`, { replace: true })
    } catch (err) {
      toast.error(err instanceof Error ? err.message : 'Failed to save pipeline.')
    } finally {
      setSaving(false)
    }
  }

  async function handleDelete() {
    if (!pipeline) return
    setSaving(true)
    try {
      await pipelinesApi.remove(pipeline.id)
      navigate('/pipelines', { replace: true })
    } catch (err) {
      toast.error(err instanceof Error ? err.message : 'Failed to delete pipeline.')
      setSaving(false)
    }
  }

  const { rows, status: liveStatus } = usePipelineResults(isNew ? undefined : id)
  const metrics = useMetricsStream()
  const currentMetrics = pipeline ? (metrics[pipeline.id] ?? null) : null
  const currentStatus = liveStatus ?? pipeline?.status ?? 'Stopped'

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
        title={isNew ? 'New pipeline' : name || 'Pipeline'}
        subtitle={isNew ? 'Define a streaming SQL job' : pipeline?.id}
        action={!isNew && <StatusBadge status={currentStatus} />}
      />

      <div className="grid grid-cols-1 gap-6 p-8 xl:grid-cols-2">
        {/* LEFT */}
        <div className="flex flex-col gap-4">
          <Card>
            <CardContent className="grid grid-cols-1 gap-3 sm:grid-cols-2">
              <Field>
                <FieldLabel htmlFor="pl-name">Name</FieldLabel>
                <Input
                  id="pl-name"
                  value={name}
                  disabled={!canEdit}
                  onChange={(e) => setName(e.target.value)}
                  placeholder="vwap-by-symbol"
                />
              </Field>
              <Field>
                <FieldLabel htmlFor="pl-description">Description</FieldLabel>
                <Input
                  id="pl-description"
                  value={description}
                  disabled={!canEdit}
                  onChange={(e) => setDescription(e.target.value)}
                  placeholder="Volume-weighted average price per symbol"
                />
              </Field>
            </CardContent>
          </Card>

          <MetadataEditor
            key={pipeline?.id ?? 'new'}
            initialTags={tags}
            initialMetadata={metadata}
            onChange={(t, m) => {
              setTags(t)
              setMetadata(m)
            }}
            readOnly={!canEdit}
          />

          <Tabs
            value={mode}
            onValueChange={(v) => {
              if (v === 'sql') switchToSql()
              else setMode('builder')
            }}
          >
            <TabsList>
              <TabsTrigger value="sql">SQL</TabsTrigger>
              <TabsTrigger value="builder">Builder</TabsTrigger>
            </TabsList>
            <TabsContent value="sql">
              <SqlEditor value={sql} onChange={setSql} diagnostics={diagnostics ?? []} readOnly={!canEdit} sources={sources} />
            </TabsContent>
            <TabsContent value="builder">
              <PipelineBuilder state={builderState} onChange={setBuilderState} sources={sources} />
            </TabsContent>
          </Tabs>

          <Card>
            <CardContent>
              <h3 className="mb-2 text-xs font-medium uppercase tracking-wide text-muted-foreground">Validation</h3>
              {validating ? (
                <p className="text-sm text-muted-foreground">Validating…</p>
              ) : diagnostics === null ? (
                <p className="text-sm text-muted-foreground">Start typing SQL to validate.</p>
              ) : diagnostics.length === 0 ? (
                <Alert>
                  <Check className="text-primary" />
                  <AlertDescription>Valid{planSummary ? ` — ${planSummary}` : ''}</AlertDescription>
                </Alert>
              ) : (
                <ul className="flex flex-col gap-1.5">
                  {diagnostics.map((d, i) => (
                    <li
                      key={i}
                      title={d.message}
                      className={cn('flex items-start gap-2 text-xs', d.severity === 'Error' ? 'text-destructive' : 'text-warning')}
                    >
                      {d.severity === 'Error' ? (
                        <CircleAlert className="mt-0.5 size-3.5 shrink-0" />
                      ) : (
                        <TriangleAlert className="mt-0.5 size-3.5 shrink-0" />
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
              )}
            </CardContent>
          </Card>

          <Card>
            <CardContent>
              <SinksEditor value={sinks} onChange={setSinks} isEdit={!isNew} disabled={!canEdit || saving} />
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
          {isNew ? (
            <Empty className="h-full border border-dashed">
              <EmptyHeader>
                <EmptyDescription>Save the pipeline to see live results here.</EmptyDescription>
              </EmptyHeader>
            </Empty>
          ) : (
            <>
              <Card>
                <CardContent>
                  <MetricsBar metrics={currentMetrics} />
                </CardContent>
              </Card>
              <Card>
                <CardContent>
                  <LiveChart rows={rows} />
                </CardContent>
              </Card>
              <Card className="min-h-[20rem] flex-1 overflow-hidden py-0">
                <ResultsTable rows={rows} />
              </Card>
            </>
          )}
        </div>
      </div>

      <AlertDialog open={confirmDelete} onOpenChange={setConfirmDelete}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete pipeline?</AlertDialogTitle>
            <AlertDialogDescription>
              This permanently removes <span className="font-medium text-foreground">{name}</span> and its results.
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
