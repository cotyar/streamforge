import { useMemo, useState } from 'react'
import type { ChangeEvent } from 'react'
import { Check, ClipboardList, Copy, Download, Upload } from 'lucide-react'
import { toast } from 'sonner'
import { useAuth } from '../api/auth'
import { exportConfig, importConfigFiles, importConfigText } from '../api/config'
import type { ConfigFormat, ImportMode } from '../api/config'
import type { ConfigImportReport, ConfigImportReportEntry } from '../api/types'
import { Topbar } from '../components/Topbar'
import { RoleGate } from '../components/RoleGate'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Textarea } from '@/components/ui/textarea'
import { Switch } from '@/components/ui/switch'
import { Badge } from '@/components/ui/badge'
import { Field, FieldDescription, FieldLabel } from '@/components/ui/field'
import { Select, SelectContent, SelectGroup, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from '@/components/ui/empty'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
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

// ============================================================================
// Shared bits (blob download) — same pattern as ApiExplorerPage's downloadText.
// ============================================================================

function downloadBlob(filename: string, blob: Blob) {
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = filename
  document.body.appendChild(a)
  a.click()
  a.remove()
  URL.revokeObjectURL(url)
}

// ============================================================================
// Export card.
// ============================================================================

function ExportCard() {
  const { hasRole } = useAuth()
  const isAdmin = hasRole('Admin')
  const [format, setFormat] = useState<ConfigFormat>('json')
  const [includeSecrets, setIncludeSecrets] = useState(false)
  const [busy, setBusy] = useState<'download' | 'copy' | null>(null)
  const [copied, setCopied] = useState(false)

  async function handleDownload() {
    setBusy('download')
    try {
      const { blob, filename } = await exportConfig(format, includeSecrets)
      downloadBlob(filename, blob)
    } catch (err) {
      toast.error(err instanceof Error ? err.message : 'Failed to export configuration.')
    } finally {
      setBusy(null)
    }
  }

  async function handleCopy() {
    setBusy('copy')
    try {
      const { blob } = await exportConfig(format, includeSecrets)
      await navigator.clipboard.writeText(await blob.text())
      setCopied(true)
      setTimeout(() => setCopied(false), 1500)
    } catch (err) {
      toast.error(err instanceof Error ? err.message : 'Failed to export configuration.')
    } finally {
      setBusy(null)
    }
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>Export</CardTitle>
      </CardHeader>
      <CardContent className="flex flex-col gap-4">
        <p className="text-sm text-muted-foreground">
          Download the current catalog — sources, tables, and pipelines — as a single canonical document. Export→reset→
          import→re-export is byte-equal for JSON.
        </p>

        <div className="grid grid-cols-2 gap-3">
          <Field>
            <FieldLabel htmlFor="cfg-export-format">Format</FieldLabel>
            <Select value={format} onValueChange={(v) => setFormat(v as ConfigFormat)}>
              <SelectTrigger id="cfg-export-format" className="w-full">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectGroup>
                  <SelectItem value="json">JSON (canonical)</SelectItem>
                  <SelectItem value="yaml">YAML</SelectItem>
                </SelectGroup>
              </SelectContent>
            </Select>
          </Field>

          {isAdmin && (
            <Field orientation="horizontal" className="items-center pb-1.5">
              <Switch id="cfg-include-secrets" checked={includeSecrets} onCheckedChange={setIncludeSecrets} />
              <FieldLabel htmlFor="cfg-include-secrets" className="font-normal">
                Include secrets
              </FieldLabel>
            </Field>
          )}
        </div>

        {includeSecrets && (
          <Alert variant="destructive">
            <AlertDescription>
              This export will contain real secret values (URL header values, gRPC credentials) in plain text — handle
              the downloaded file accordingly.
            </AlertDescription>
          </Alert>
        )}

        <div className="flex flex-wrap items-center gap-2">
          <Button onClick={() => void handleDownload()} disabled={busy !== null}>
            <Download data-icon="inline-start" /> {busy === 'download' ? 'Downloading…' : 'Download'}
          </Button>
          <Button type="button" variant="outline" onClick={() => void handleCopy()} disabled={busy !== null}>
            {copied ? <Check data-icon="inline-start" /> : <Copy data-icon="inline-start" />}
            {copied ? 'Copied' : busy === 'copy' ? 'Copying…' : 'Copy to clipboard'}
          </Button>
        </div>
      </CardContent>
    </Card>
  )
}

// ============================================================================
// Import report display.
// ============================================================================

const ACTION_BADGE_VARIANT: Record<ConfigImportReportEntry['action'], 'default' | 'secondary' | 'destructive' | 'outline'> = {
  created: 'default',
  updated: 'secondary',
  deleted: 'destructive',
  skipped: 'outline',
  error: 'destructive',
}

const REPORT_ACTIONS: ConfigImportReportEntry['action'][] = ['created', 'updated', 'deleted', 'skipped', 'error']

function reportCounts(report: ConfigImportReport): Record<ConfigImportReportEntry['action'], number> {
  const counts: Record<ConfigImportReportEntry['action'], number> = { created: 0, updated: 0, deleted: 0, skipped: 0, error: 0 }
  for (const entry of report.entries) counts[entry.action] += 1
  return counts
}

function ReportView({ report }: { report: ConfigImportReport }) {
  const counts = useMemo(() => reportCounts(report), [report])
  // ConfigImportReportEntry['kind'] in types.ts is 'source' | 'pipeline' | 'table', but the backend's
  // 400 document-level report (ConfigImportService.DocumentErrorReport) emits Kind="document" for
  // parse/compose failures — a real runtime value outside that frozen union. Compare as `string` rather
  // than widening the frozen contract.
  const isDocumentError = report.entries.length > 0 && report.entries.every((e) => (e.kind as string) === 'document')

  return (
    <div className="flex flex-col gap-3 border-t border-border pt-4">
      <div className="flex flex-wrap items-center gap-2">
        <Badge variant={report.ok ? 'default' : 'destructive'}>{report.ok ? 'OK' : 'Errors'}</Badge>
        <Badge variant="outline" className="uppercase">
          {report.mode}
        </Badge>
        {REPORT_ACTIONS.filter((a) => counts[a] > 0).map((action) => (
          <Badge key={action} variant={ACTION_BADGE_VARIANT[action]}>
            {counts[action]} {action}
          </Badge>
        ))}
      </div>

      {!report.ok && (
        <Alert variant="destructive">
          <AlertDescription>
            {isDocumentError
              ? 'The document could not be composed — see the diagnostics below.'
              : 'One or more entities failed to import. Entities that succeeded were still applied — whole-import atomicity is not promised, only per-entity.'}
          </AlertDescription>
        </Alert>
      )}

      <div className="overflow-hidden rounded-lg border border-border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Kind</TableHead>
              <TableHead>Name</TableHead>
              <TableHead>Action</TableHead>
              <TableHead>Diagnostics</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {report.entries.map((entry, i) => (
              <TableRow key={`${entry.kind}-${entry.name}-${i}`}>
                <TableCell className="text-muted-foreground">{entry.kind}</TableCell>
                <TableCell className="max-w-64 truncate font-medium" title={entry.name}>
                  {entry.name}
                </TableCell>
                <TableCell>
                  <Badge variant={ACTION_BADGE_VARIANT[entry.action]}>{entry.action}</Badge>
                </TableCell>
                <TableCell className="whitespace-normal">
                  {entry.diagnostics.length > 0 ? (
                    <ul className="flex flex-col gap-0.5 font-mono text-[11px] text-muted-foreground">
                      {entry.diagnostics.map((d, di) => (
                        <li key={di}>{d}</li>
                      ))}
                    </ul>
                  ) : (
                    <span className="text-muted-foreground/50">—</span>
                  )}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>
    </div>
  )
}

// ============================================================================
// Import card.
// ============================================================================

type ImportSource = 'file' | 'paste'

function ImportCard() {
  const { hasRole } = useAuth()
  const isAdmin = hasRole('Admin')

  const [source, setSource] = useState<ImportSource>('file')
  const [files, setFiles] = useState<File[]>([])
  const [pasteText, setPasteText] = useState('')
  const [mode, setMode] = useState<ImportMode>('validate')
  const [report, setReport] = useState<ConfigImportReport | null>(null)
  const [running, setRunning] = useState<ImportMode | null>(null)
  const [confirmReplace, setConfirmReplace] = useState(false)

  const canRun = source === 'file' ? files.length > 0 : pasteText.trim().length > 0

  async function runImport(target: ImportMode) {
    setRunning(target)
    try {
      const result = source === 'file' ? await importConfigFiles(target, files) : await importConfigText(target, pasteText)
      setReport(result)
      if (result.ok) {
        toast.success(`Import (${target}) completed — ${result.entries.length} ${result.entries.length === 1 ? 'entity' : 'entities'} reported.`)
      } else {
        toast.error(`Import (${target}) reported errors — see the report below.`)
      }
    } catch (err) {
      toast.error(err instanceof Error ? err.message : 'Import failed.')
    } finally {
      setRunning(null)
    }
  }

  function handlePrimary() {
    if (mode === 'replace') {
      setConfirmReplace(true)
      return
    }
    void runImport(mode)
  }

  function handleFileChange(e: ChangeEvent<HTMLInputElement>) {
    setFiles(e.target.files ? Array.from(e.target.files) : [])
  }

  const primaryLabel = mode === 'validate' ? 'Validate' : mode === 'merge' ? 'Import (merge)' : 'Import (replace)'

  return (
    <RoleGate min="Editor">
      <Card>
        <CardHeader>
          <CardTitle>Import</CardTitle>
        </CardHeader>
        <CardContent className="flex flex-col gap-4">
          <Tabs value={source} onValueChange={(v) => setSource(v as ImportSource)}>
            <TabsList>
              <TabsTrigger value="file">File(s)</TabsTrigger>
              <TabsTrigger value="paste">Paste</TabsTrigger>
            </TabsList>

            <TabsContent value="file" className="flex flex-col gap-2 pt-3">
              <Input type="file" accept=".json,.yaml,.yml,application/json,text/yaml" multiple onChange={handleFileChange} />
              {files.length > 0 && (
                <ul className="flex flex-col gap-0.5 font-mono text-[11px] text-muted-foreground">
                  {files.map((f, i) => (
                    <li key={f.name + i} className="flex items-center gap-1.5">
                      {i === 0 && (
                        <Badge variant="outline" className="h-4 px-1.5 text-[9px] uppercase">
                          root
                        </Badge>
                      )}
                      {f.name}
                    </li>
                  ))}
                </ul>
              )}
              <FieldDescription>
                Select multiple files to import an include set. The <span className="font-medium text-foreground">first</span>{' '}
                file selected is the root document; its <span className="font-mono">include</span> entries resolve by file name
                within the selected set.
              </FieldDescription>
            </TabsContent>

            <TabsContent value="paste" className="flex flex-col gap-2 pt-3">
              <Textarea
                value={pasteText}
                onChange={(e) => setPasteText(e.target.value)}
                placeholder={'{ "version": 1, "sources": [], "tables": [], "pipelines": [] }'}
                className="min-h-40 font-mono text-xs"
              />
              <FieldDescription>
                JSON or YAML. Text that parses as JSON is sent as <span className="font-mono">application/json</span>; anything
                else is sent as a raw <span className="font-mono">text/yaml</span> body.
              </FieldDescription>
            </TabsContent>
          </Tabs>

          <div className="grid grid-cols-[1fr_auto] items-end gap-3">
            <Field>
              <FieldLabel htmlFor="cfg-import-mode">Mode</FieldLabel>
              <Select value={mode} onValueChange={(v) => setMode(v as ImportMode)}>
                <SelectTrigger id="cfg-import-mode" className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectGroup>
                    <SelectItem value="validate">Validate (dry run)</SelectItem>
                    <SelectItem value="merge">Merge</SelectItem>
                    {isAdmin && <SelectItem value="replace">Replace</SelectItem>}
                  </SelectGroup>
                </SelectContent>
              </Select>
            </Field>
            <Button onClick={handlePrimary} disabled={!canRun || running !== null}>
              <Upload data-icon="inline-start" /> {running ? 'Running…' : primaryLabel}
            </Button>
          </div>

          {mode === 'replace' && (
            <FieldDescription className="text-destructive">
              Replace deletes every entity absent from the document — running entities are stopped first (pipelines, then
              tables, then sources).
            </FieldDescription>
          )}

          {report?.mode === 'validate' && (
            <div className="flex flex-wrap items-center gap-2 rounded-lg border border-dashed border-border bg-muted/30 px-3 py-2">
              <span className="text-xs text-muted-foreground">Apply this validated document:</span>
              <Button size="sm" variant="outline" onClick={() => void runImport('merge')} disabled={running !== null}>
                Apply as merge
              </Button>
              {isAdmin && (
                <Button
                  size="sm"
                  variant="outline"
                  className="hover:text-destructive"
                  onClick={() => setConfirmReplace(true)}
                  disabled={running !== null}
                >
                  Apply as replace
                </Button>
              )}
            </div>
          )}

          {report ? (
            <ReportView report={report} />
          ) : (
            <Empty className="border border-dashed">
              <EmptyHeader>
                <EmptyMedia variant="icon">
                  <ClipboardList />
                </EmptyMedia>
                <EmptyTitle>No import run yet</EmptyTitle>
                <EmptyDescription>Pick a source above and run Validate to see a dry-run report before applying anything.</EmptyDescription>
              </EmptyHeader>
            </Empty>
          )}
        </CardContent>
      </Card>

      <AlertDialog open={confirmReplace} onOpenChange={setConfirmReplace}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Replace the entire catalog?</AlertDialogTitle>
            <AlertDialogDescription>
              Replace deletes every entity absent from the document — running entities are stopped first (pipelines, then
              tables, then sources). This cannot be undone.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              variant="destructive"
              onClick={() => {
                setConfirmReplace(false)
                void runImport('replace')
              }}
            >
              Replace
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </RoleGate>
  )
}

// ============================================================================
// Page.
// ============================================================================

export function ConfigPage() {
  return (
    <div>
      <Topbar
        title="Configuration"
        subtitle="Export or import the catalog — sources, tables, and pipelines — as a versioned document."
      />
      <div className="p-8">
        <div className="mx-auto flex max-w-3xl flex-col gap-4">
          <ExportCard />
          <ImportCard />
        </div>
      </div>
    </div>
  )
}
