import { useCallback, useEffect, useState } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { Pencil, Eye, Play, Plus, Square, Trash2, Workflow } from 'lucide-react'
import { toast } from 'sonner'
import { pipelinesApi } from '../api/pipelines'
import type { PipelineDefinition } from '../api/types'
import { extractSourcesFromSql } from '../lib/sqlSources'
import { Topbar } from '../components/Topbar'
import { StatusBadge } from '../components/StatusBadge'
import { RoleGate } from '../components/RoleGate'
import { Card } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { Empty, EmptyContent, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from '@/components/ui/empty'
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

export function PipelinesPage() {
  const navigate = useNavigate()
  const [pipelines, setPipelines] = useState<PipelineDefinition[] | null>(null)
  const [busyIds, setBusyIds] = useState<Set<string>>(new Set())
  const [pendingDelete, setPendingDelete] = useState<PipelineDefinition | null>(null)

  const load = useCallback(() => {
    pipelinesApi.list().then(setPipelines).catch(() => setPipelines([]))
  }, [])

  useEffect(() => {
    load()
  }, [load])

  function withBusy(id: string, fn: () => Promise<unknown>) {
    setBusyIds((prev) => new Set(prev).add(id))
    fn()
      .then(load)
      .catch((err: unknown) => {
        toast.error(err instanceof Error ? err.message : 'Action failed.')
      })
      .finally(() =>
        setBusyIds((prev) => {
          const next = new Set(prev)
          next.delete(id)
          return next
        }),
      )
  }

  async function confirmDelete() {
    if (!pendingDelete) return
    const id = pendingDelete.id
    setPendingDelete(null)
    setBusyIds((prev) => new Set(prev).add(id))
    try {
      await pipelinesApi.remove(id)
      load()
    } catch (err) {
      toast.error(err instanceof Error ? err.message : 'Failed to delete pipeline.')
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
        title="Pipelines"
        subtitle="Streaming SQL jobs running against your live sources"
        action={
          <RoleGate min="Editor">
            <Button onClick={() => navigate('/pipelines/new')}>
              <Plus data-icon="inline-start" /> New pipeline
            </Button>
          </RoleGate>
        }
      />

      <div className="p-8">
        {pipelines === null ? (
          <div className="flex flex-col gap-2">
            {Array.from({ length: 5 }).map((_, i) => (
              <Skeleton key={i} className="h-12 w-full" />
            ))}
          </div>
        ) : pipelines.length === 0 ? (
          <Empty className="border border-dashed">
            <EmptyHeader>
              <EmptyMedia variant="icon">
                <Workflow />
              </EmptyMedia>
              <EmptyTitle>Create your first streaming pipeline</EmptyTitle>
              <EmptyDescription>
                Write streaming SQL or use the visual builder to join, filter, and window your live sources.
              </EmptyDescription>
            </EmptyHeader>
            <EmptyContent>
              <RoleGate min="Editor">
                <Button onClick={() => navigate('/pipelines/new')}>New pipeline</Button>
              </RoleGate>
            </EmptyContent>
          </Empty>
        ) : (
          <Card className="overflow-hidden py-0">
            <Table>
              <TableHeader>
                <TableRow className="hover:bg-transparent">
                  <TableHead>Name</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>Sources</TableHead>
                  <TableHead>Description</TableHead>
                  <TableHead>Updated</TableHead>
                  <TableHead className="text-right">Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {pipelines.map((p) => {
                  const busy = busyIds.has(p.id)
                  const sources = extractSourcesFromSql(p.sql)
                  return (
                    <TableRow key={p.id}>
                      <TableCell>
                        <Link to={`/pipelines/${p.id}`} className="font-medium text-foreground hover:text-primary">
                          {p.name}
                        </Link>
                        {p.error && <p className="mt-0.5 max-w-xs truncate text-xs text-destructive">{p.error}</p>}
                      </TableCell>
                      <TableCell>
                        <StatusBadge status={p.status} />
                      </TableCell>
                      <TableCell className="text-xs text-muted-foreground">
                        {sources.length > 0 ? sources.join(', ') : '—'}
                      </TableCell>
                      <TableCell className="max-w-xs truncate text-muted-foreground">{p.description || '—'}</TableCell>
                      <TableCell className="text-xs text-muted-foreground">{formatDate(p.updatedAtMs)}</TableCell>
                      <TableCell>
                        <div className="flex items-center justify-end gap-1">
                          <Button variant="ghost" size="icon-sm" asChild title="View">
                            <Link to={`/pipelines/${p.id}`}>
                              <Eye />
                            </Link>
                          </Button>
                          <RoleGate min="Editor">
                            <>
                              <Button
                                variant="ghost"
                                size="icon-sm"
                                title={p.status === 'Running' ? 'Stop' : 'Start'}
                                disabled={busy}
                                onClick={() =>
                                  withBusy(p.id, () => (p.status === 'Running' ? pipelinesApi.stop(p.id) : pipelinesApi.start(p.id)))
                                }
                              >
                                {p.status === 'Running' ? <Square /> : <Play />}
                              </Button>
                              <Button variant="ghost" size="icon-sm" asChild title="Edit">
                                <Link to={`/pipelines/${p.id}`}>
                                  <Pencil />
                                </Link>
                              </Button>
                              <Button
                                variant="ghost"
                                size="icon-sm"
                                title="Delete"
                                disabled={busy}
                                onClick={() => setPendingDelete(p)}
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
      </div>

      <AlertDialog open={pendingDelete !== null} onOpenChange={(open) => !open && setPendingDelete(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete pipeline?</AlertDialogTitle>
            <AlertDialogDescription>
              This permanently removes <span className="font-medium text-foreground">{pendingDelete?.name}</span> and its results.
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
