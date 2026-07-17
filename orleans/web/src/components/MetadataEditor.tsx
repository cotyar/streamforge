import { useState } from 'react'
import type { KeyboardEvent } from 'react'
import { Plus, Trash2, X } from 'lucide-react'
import { Card, CardContent } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'

interface Row {
  key: string
  value: string
}

function metadataToRows(metadata: Record<string, string>): Row[] {
  return Object.entries(metadata).map(([key, value]) => ({ key, value }))
}

function rowsToMetadata(rows: Row[]): Record<string, string> {
  const out: Record<string, string> = {}
  for (const r of rows) {
    const key = r.key.trim()
    if (key) out[key] = r.value
  }
  return out
}

/**
 * Feature A metadata editor: tag chips (add/remove, Enter to add) + key-value rows (add/edit/delete).
 *
 * Deliberately uncontrolled-with-callback (like a form's local draft state elsewhere in this app,
 * e.g. TableDetailPage's own name/sql fields): `initialTags`/`initialMetadata` seed local state ONCE
 * on mount, every edit fires `onChange(tags, metadata)` so the caller can include the latest value in
 * its own save payload, but this component does NOT re-sync from props afterward — a blank-key draft
 * row being typed would otherwise vanish/reflow every keystroke if rows were derived fresh from a
 * metadata Record each render (empty keys can't round-trip through a Record). Callers that load a
 * different entity into the same page (switching /tables/:id without a full remount) should pass a
 * `key={entityId}` prop on this component to force a fresh mount — see TableDetailPage/PipelineDetailPage.
 */
export function MetadataEditor({
  initialTags,
  initialMetadata,
  onChange,
  readOnly = false,
  title = 'Metadata',
}: {
  initialTags: string[]
  initialMetadata: Record<string, string>
  onChange: (tags: string[], metadata: Record<string, string>) => void
  readOnly?: boolean
  title?: string
}) {
  const [tags, setTags] = useState<string[]>(initialTags)
  const [rows, setRows] = useState<Row[]>(() => (metadataToRows(initialMetadata).length > 0 ? metadataToRows(initialMetadata) : []))
  const [tagDraft, setTagDraft] = useState('')

  function commit(nextTags: string[], nextRows: Row[]) {
    setTags(nextTags)
    setRows(nextRows)
    onChange(nextTags, rowsToMetadata(nextRows))
  }

  function addTag() {
    const t = tagDraft.trim()
    if (!t || tags.includes(t)) {
      setTagDraft('')
      return
    }
    commit([...tags, t], rows)
    setTagDraft('')
  }

  function handleTagKeyDown(e: KeyboardEvent<HTMLInputElement>) {
    if (e.key === 'Enter') {
      e.preventDefault()
      addTag()
    }
  }

  function removeTag(t: string) {
    commit(
      tags.filter((x) => x !== t),
      rows,
    )
  }

  function updateRow(i: number, patch: Partial<Row>) {
    commit(
      tags,
      rows.map((r, idx) => (idx === i ? { ...r, ...patch } : r)),
    )
  }

  function removeRow(i: number) {
    commit(
      tags,
      rows.filter((_, idx) => idx !== i),
    )
  }

  function addRow() {
    commit(tags, [...rows, { key: '', value: '' }])
  }

  const persistedMetadata = rowsToMetadata(rows)

  if (readOnly) {
    return (
      <Card>
        <CardContent className="flex flex-col gap-3">
          <h3 className="text-xs font-medium uppercase tracking-wide text-muted-foreground">{title}</h3>
          {tags.length === 0 && Object.keys(persistedMetadata).length === 0 ? (
            <p className="text-xs text-muted-foreground">No metadata.</p>
          ) : (
            <>
              {tags.length > 0 && (
                <div className="flex flex-wrap gap-1.5">
                  {tags.map((t) => (
                    <Badge key={t} variant="secondary">
                      {t}
                    </Badge>
                  ))}
                </div>
              )}
              {Object.keys(persistedMetadata).length > 0 && (
                <dl className="flex flex-col gap-1 text-xs">
                  {Object.entries(persistedMetadata).map(([k, v]) => (
                    <div key={k} className="flex gap-1.5 font-mono">
                      <dt className="text-muted-foreground">{k}:</dt>
                      <dd className="text-foreground">{v}</dd>
                    </div>
                  ))}
                </dl>
              )}
            </>
          )}
        </CardContent>
      </Card>
    )
  }

  return (
    <Card>
      <CardContent className="flex flex-col gap-3">
        <h3 className="text-xs font-medium uppercase tracking-wide text-muted-foreground">{title}</h3>

        <div className="flex flex-col gap-1.5">
          <span className="text-[11px] font-medium text-muted-foreground">Tags</span>
          <div className="flex flex-wrap items-center gap-1.5">
            {tags.map((t) => (
              <Badge key={t} variant="secondary" className="gap-1 pr-1">
                {t}
                <button
                  type="button"
                  aria-label={`Remove tag ${t}`}
                  onClick={() => removeTag(t)}
                  className="rounded-sm text-muted-foreground hover:text-destructive"
                >
                  <X className="size-3" />
                </button>
              </Badge>
            ))}
            <Input
              value={tagDraft}
              onChange={(e) => setTagDraft(e.target.value)}
              onKeyDown={handleTagKeyDown}
              onBlur={addTag}
              placeholder="Add tag, Enter to confirm"
              className="h-7 max-w-[10rem]"
            />
          </div>
        </div>

        <div className="flex flex-col gap-1.5">
          <div className="flex items-center justify-between">
            <span className="text-[11px] font-medium text-muted-foreground">Key / value</span>
            <Button type="button" variant="ghost" size="sm" onClick={addRow}>
              <Plus data-icon="inline-start" /> Add
            </Button>
          </div>
          {rows.length === 0 ? (
            <p className="text-xs text-muted-foreground">No key-value pairs.</p>
          ) : (
            <div className="flex flex-col gap-1.5">
              {rows.map((r, i) => (
                <div key={i} className="flex items-center gap-1.5">
                  <Input
                    value={r.key}
                    onChange={(e) => updateRow(i, { key: e.target.value })}
                    placeholder="key"
                    className="h-8"
                  />
                  <Input
                    value={r.value}
                    onChange={(e) => updateRow(i, { value: e.target.value })}
                    placeholder="value"
                    className="h-8"
                  />
                  <Button
                    type="button"
                    variant="ghost"
                    size="icon-sm"
                    className="shrink-0 hover:text-destructive"
                    onClick={() => removeRow(i)}
                  >
                    <Trash2 />
                  </Button>
                </div>
              ))}
            </div>
          )}
        </div>
      </CardContent>
    </Card>
  )
}
