import { useCallback, useEffect, useMemo, useState } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { Pencil, Eye, Play, Plus, Search, Square, Trash2, Table2 } from 'lucide-react'
import { toast } from 'sonner'
import { tablesApi } from '../api/tables'
import type { TableDefinition, TableMetrics } from '../api/types'
import { Topbar } from '../components/Topbar'
import { StatusBadge } from '../components/StatusBadge'
import { RoleGate } from '../components/RoleGate'
import { TagList } from '../components/TagList'
import { RevisionBadge } from '../components/RevisionBadge'
import { StaleReasonNote } from '../components/StaleReasonNote'
import { Card } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Skeleton } from '@/components/ui/skeleton'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { Empty, EmptyContent, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from '@/components/ui/empty'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
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

function formatDate(ms: number): string {
  return new Date(ms).toLocaleString(undefined, {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  })
}

export function TablesPage() {
  const navigate = useNavigate()
  const [tables, setTables] = useState<TableDefinition[] | null>(null)
  const [metrics, setMetrics] = useState<Record<string, TableMetrics>>({})
  const [busyIds, setBusyIds] = useState<Set<string>>(new Set())
  const [pendingDelete, setPendingDelete] = useState<TableDefinition | null>(null)
  const [activeTags, setActiveTags] = useState<Set<string>>(new Set())

  const load = useCallback(() => {
    tablesApi.list().then(setTables).catch(() => setTables([]))
  }, [])

  useEffect(() => {
    load()
  }, [load])

  const allTags = useMemo(() => Array.from(new Set((tables ?? []).flatMap((t) => t.tags))).sort(), [tables])

  const visibleTables = useMemo(() => {
    if (!tables || activeTags.size === 0) return tables
    return tables.filter((t) => Array.from(activeTags).every((tag) => t.tags.includes(tag)))
  }, [tables, activeTags])

  function toggleTag(t: string) {
    setActiveTags((prev) => {
      const next = new Set(prev)
      if (next.has(t)) next.delete(t)
      else next.add(t)
      return next
    })
  }

  // Row counts + rebuilding state come from /metrics, fetched lazily (after the list itself
  // renders) so a slow metrics call never blocks the initial table listing.
  useEffect(() => {
    if (!tables || tables.length === 0) return
    let cancelled = false
    for (const t of tables) {
      tablesApi
        .metrics(t.id)
        .then((m) => {
          if (!cancelled) setMetrics((prev) => ({ ...prev, [t.id]: m }))
        })
        .catch(() => {
          // best-effort — leave the row count blank if metrics aren't available
        })
    }
    return () => {
      cancelled = true
    }
  }, [tables])

  // start()/stop() resolve with a 200 + updated TableDefinition even when the operation itself
  // "fails" (e.g. starting a table whose inputs aren't Running comes back as status: 'Failed' with
  // an error message) — only a dependency conflict (stopping/deleting a table another Running
  // table depends on) is a real 409 that lands in the catch block.
  async function toggleStartStop(t: TableDefinition) {
    setBusyIds((prev) => new Set(prev).add(t.id))
    try {
      const result = t.status === 'Running' ? await tablesApi.stop(t.id) : await tablesApi.start(t.id)
      if (result.status === 'Failed' && result.error) toast.error(result.error)
      load()
    } catch (err) {
      toast.error(err instanceof Error ? err.message : 'Action failed.')
    } finally {
      setBusyIds((prev) => {
        const next = new Set(prev)
        next.delete(t.id)
        return next
      })
    }
  }

  async function confirmDelete() {
    if (!pendingDelete) return
    const id = pendingDelete.id
    setPendingDelete(null)
    setBusyIds((prev) => new Set(prev).add(id))
    try {
      await tablesApi.remove(id)
      load()
    } catch (err) {
      toast.error(err instanceof Error ? err.message : 'Failed to delete table.')
    } finally {
      setBusyIds((prev) => {
        const next = new Set(prev)
        next.delete(id)
        return next
      })
    }
  }

  return (
    <div>
      <Topbar
        title="Tables"
        subtitle="Persistent materialized views — incremental Z-set aggregates over streams and other tables"
        action={
          <RoleGate min="Editor">
            <Button onClick={() => navigate('/tables/new')}>
              <Plus data-icon="inline-start" /> New table
            </Button>
          </RoleGate>
        }
      />

      <div className="p-8">
        {tables === null ? (
          <div className="flex flex-col gap-2">
            {Array.from({ length: 5 }).map((_, i) => (
              <Skeleton key={i} className="h-12 w-full" />
            ))}
          </div>
        ) : tables.length === 0 ? (
          <Empty className="border border-dashed">
            <EmptyHeader>
              <EmptyMedia variant="icon">
                <Table2 />
              </EmptyMedia>
              <EmptyTitle>Create your first materialized table</EmptyTitle>
              <EmptyDescription>
                Write a windowless SELECT with running aggregates to build a live, queryable view over your streams.
              </EmptyDescription>
            </EmptyHeader>
            <EmptyContent>
              <RoleGate min="Editor">
                <Button onClick={() => navigate('/tables/new')}>New table</Button>
              </RoleGate>
            </EmptyContent>
          </Empty>
        ) : (
          <>
            {allTags.length > 0 && (
              <div className="mb-4 flex flex-wrap items-center gap-1.5">
                <span className="text-xs text-muted-foreground">Filter by tag:</span>
                {allTags.map((t) => (
                  <Badge
                    key={t}
                    variant={activeTags.has(t) ? 'default' : 'secondary'}
                    className="cursor-pointer select-none"
                    onClick={() => toggleTag(t)}
                  >
                    {t}
                  </Badge>
                ))}
                {activeTags.size > 0 && (
                  <Button variant="ghost" size="sm" onClick={() => setActiveTags(new Set())}>
                    Clear
                  </Button>
                )}
              </div>
            )}

            {visibleTables && visibleTables.length === 0 ? (
              <Empty className="border border-dashed">
                <EmptyHeader>
                  <EmptyDescription>No tables match the selected tags.</EmptyDescription>
                </EmptyHeader>
              </Empty>
            ) : (
          <Card className="overflow-hidden py-0">
            <Table>
              <TableHeader>
                <TableRow className="hover:bg-transparent">
                  <TableHead>Name</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>Rows</TableHead>
                  <TableHead>Inputs</TableHead>
                  <TableHead>Description</TableHead>
                  <TableHead>Updated</TableHead>
                  <TableHead className="text-right">Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {(visibleTables ?? []).map((t) => {
                  const busy = busyIds.has(t.id)
                  const m = metrics[t.id]
                  const inputs = [...t.streamInputs, ...t.tableInputs]
                  return (
                    <TableRow key={t.id}>
                      <TableCell>
                        <div className="flex items-center gap-1.5">
                          <Link to={`/tables/${t.id}`} className="font-medium text-foreground hover:text-primary">
                            {t.name}
                          </Link>
                          {t.searchEnabled && (
                            <TooltipProvider>
                              <Tooltip>
                                <TooltipTrigger asChild>
                                  <Badge variant="outline" className="gap-1 px-1.5 text-muted-foreground">
                                    <Search className="size-3" />
                                  </Badge>
                                </TooltipTrigger>
                                <TooltipContent side="top">Search enabled — {t.searchMode}</TooltipContent>
                              </Tooltip>
                            </TooltipProvider>
                          )}
                          <RevisionBadge revision={t.revision} schemaRevision={t.schemaRevision} />
                        </div>
                        <TagList tags={t.tags} className="mt-1" />
                        {t.error && <p className="mt-0.5 max-w-xs truncate text-xs text-destructive">{t.error}</p>}
                        <StaleReasonNote reason={t.staleReason} />
                      </TableCell>
                      <TableCell>
                        <div className="flex items-center gap-1.5">
                          <StatusBadge status={t.status} />
                          {m?.rebuilding && (
                            <Badge variant="outline" className="border-warning/40 text-warning">
                              Rebuilding
                            </Badge>
                          )}
                        </div>
                      </TableCell>
                      <TableCell className="font-mono text-xs text-foreground">
                        {m ? m.rowCount.toLocaleString() : <span className="text-muted-foreground">—</span>}
                      </TableCell>
                      <TableCell>
                        {inputs.length > 0 ? (
                          <div className="flex flex-wrap gap-1">
                            {inputs.map((name) => (
                              <Badge key={name} variant="outline" className="text-muted-foreground">
                                {name}
                              </Badge>
                            ))}
                          </div>
                        ) : (
                          <span className="text-xs text-muted-foreground">—</span>
                        )}
                      </TableCell>
                      <TableCell className="max-w-xs truncate text-muted-foreground">{t.description || '—'}</TableCell>
                      <TableCell className="text-xs text-muted-foreground">{formatDate(t.updatedAtMs)}</TableCell>
                      <TableCell>
                        <div className="flex items-center justify-end gap-1">
                          <Button variant="ghost" size="icon-sm" asChild title="View">
                            <Link to={`/tables/${t.id}`}>
                              <Eye />
                            </Link>
                          </Button>
                          <RoleGate min="Editor">
                            <>
                              <Button
                                variant="ghost"
                                size="icon-sm"
                                title={t.status === 'Running' ? 'Stop' : 'Start'}
                                disabled={busy}
                                onClick={() => void toggleStartStop(t)}
                              >
                                {t.status === 'Running' ? <Square /> : <Play />}
                              </Button>
                              <Button variant="ghost" size="icon-sm" asChild title="Edit">
                                <Link to={`/tables/${t.id}`}>
                                  <Pencil />
                                </Link>
                              </Button>
                              <Button
                                variant="ghost"
                                size="icon-sm"
                                title="Delete"
                                disabled={busy}
                                onClick={() => setPendingDelete(t)}
                                className="hover:text-destructive"
                              >
                                <Trash2 />
                              </Button>
                            </>
                          </RoleGate>
                        </div>
                      </TableCell>
                    </TableRow>
                  )
                })}
              </TableBody>
            </Table>
          </Card>
            )}
          </>
        )}
      </div>

      <AlertDialog open={pendingDelete !== null} onOpenChange={(open) => !open && setPendingDelete(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete table?</AlertDialogTitle>
            <AlertDialogDescription>
              This permanently removes <span className="font-medium text-foreground">{pendingDelete?.name}</span> and its materialized
              rows. Tables that depend on it must be stopped first.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction variant="destructive" onClick={confirmDelete}>
              Delete
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  )
}
