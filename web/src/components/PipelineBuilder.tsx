import { useMemo } from 'react'
import { Plus, Trash2 } from 'lucide-react'
import type { SourceDefinition } from '../api/types'
import type { BuilderState, JoinClause, SelectItem, WhereCondition } from '../builder/types'
import { AGG_FNS, COMPARE_OPS, DURATION_UNITS, JOIN_TYPES, newJoin, newSelectItem, newWhereCondition } from '../builder/types'
import { cn } from '@/lib/utils'
import { Card, CardAction, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Select, SelectContent, SelectGroup, SelectItem as SelectOption, SelectTrigger, SelectValue } from '@/components/ui/select'
import { ToggleGroup, ToggleGroupItem } from '@/components/ui/toggle-group'

const labelCls = 'mb-1 block text-xs font-medium uppercase tracking-wide text-muted-foreground'

function useColumnOptions(state: BuilderState, sources: SourceDefinition[]) {
  return useMemo(() => {
    const byName = new Map(sources.map((s) => [s.name, s]))
    const qualify = state.joins.length > 0
    const options: string[] = []

    const fromSource = byName.get(state.from.source)
    if (fromSource) {
      const alias = state.from.alias.trim() || fromSource.name
      for (const f of fromSource.fields) {
        options.push(qualify ? `${alias}.${f.name}` : f.name)
      }
    }
    for (const join of state.joins) {
      const src = byName.get(join.source)
      if (!src) continue
      const alias = join.alias.trim() || src.name
      for (const f of src.fields) {
        options.push(`${alias}.${f.name}`)
      }
    }
    return options
  }, [state.from, state.joins, sources])
}

function SourceSelect({
  value,
  onChange,
  sources,
  placeholder = 'Select…',
}: {
  value: string
  onChange: (value: string) => void
  sources: SourceDefinition[]
  placeholder?: string
}) {
  return (
    <Select value={value || undefined} onValueChange={onChange}>
      <SelectTrigger className="w-full">
        <SelectValue placeholder={placeholder} />
      </SelectTrigger>
      <SelectContent>
        <SelectGroup>
          {sources.map((s) => (
            <SelectOption key={s.name} value={s.name}>
              {s.name}
            </SelectOption>
          ))}
        </SelectGroup>
      </SelectContent>
    </Select>
  )
}

export function PipelineBuilder({
  state,
  onChange,
  sources,
}: {
  state: BuilderState
  onChange: (next: BuilderState) => void
  sources: SourceDefinition[]
}) {
  const columnOptions = useColumnOptions(state, sources)

  function patch(partial: Partial<BuilderState>) {
    onChange({ ...state, ...partial })
  }

  function updateJoin(i: number, patchJoin: Partial<JoinClause>) {
    const joins = state.joins.map((j, idx) => (idx === i ? { ...j, ...patchJoin } : j))
    patch({ joins })
  }

  function updateWhere(i: number, patchC: Partial<WhereCondition>) {
    const where = state.where.map((c, idx) => (idx === i ? { ...c, ...patchC } : c))
    patch({ where })
  }

  function updateSelect(i: number, patchS: Partial<SelectItem>) {
    const select = state.select.map((s, idx) => (idx === i ? { ...s, ...patchS } : s))
    patch({ select })
  }

  return (
    <div className="flex flex-col gap-4">
      {/* FROM */}
      <Card>
        <CardHeader>
          <CardTitle>From</CardTitle>
        </CardHeader>
        <CardContent className="grid grid-cols-2 gap-3">
          <div>
            <label className={labelCls}>Source</label>
            <SourceSelect
              value={state.from.source}
              onChange={(v) => patch({ from: { ...state.from, source: v } })}
              sources={sources}
              placeholder="Select source…"
            />
          </div>
          <div>
            <label className={labelCls}>Alias (optional)</label>
            <Input placeholder="t" value={state.from.alias} onChange={(e) => patch({ from: { ...state.from, alias: e.target.value } })} />
          </div>
        </CardContent>
      </Card>

      {/* JOINS */}
      <Card>
        <CardHeader>
          <CardTitle>Joins</CardTitle>
          <CardAction>
            <Button variant="outline" size="sm" onClick={() => patch({ joins: [...state.joins, newJoin()] })}>
              <Plus data-icon="inline-start" /> Add join
            </Button>
          </CardAction>
        </CardHeader>
        <CardContent className="flex flex-col gap-3">
          {state.joins.length === 0 && <p className="text-xs text-muted-foreground">No joins — single-source pipeline.</p>}
          {state.joins.map((join, i) => (
            <div key={i} className="rounded-lg border border-border p-3">
              <div className="mb-2 grid grid-cols-4 gap-2">
                <div>
                  <label className={labelCls}>Type</label>
                  <Select value={join.type} onValueChange={(v) => updateJoin(i, { type: v as JoinClause['type'] })}>
                    <SelectTrigger className="w-full">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectGroup>
                        {JOIN_TYPES.map((t) => (
                          <SelectOption key={t} value={t}>
                            {t}
                          </SelectOption>
                        ))}
                      </SelectGroup>
                    </SelectContent>
                  </Select>
                </div>
                <div>
                  <label className={labelCls}>Source</label>
                  <SourceSelect value={join.source} onChange={(v) => updateJoin(i, { source: v })} sources={sources} />
                </div>
                <div>
                  <label className={labelCls}>Alias</label>
                  <Input value={join.alias} onChange={(e) => updateJoin(i, { alias: e.target.value })} />
                </div>
                <div className="flex items-end justify-end">
                  <Button
                    variant="ghost"
                    size="icon-sm"
                    className="hover:text-destructive"
                    onClick={() => patch({ joins: state.joins.filter((_, idx) => idx !== i) })}
                    title="Remove join"
                  >
                    <Trash2 />
                  </Button>
                </div>
              </div>
              {join.type !== 'CROSS' && (
                <div className="grid grid-cols-4 gap-2">
                  <div>
                    <label className={labelCls}>Within</label>
                    <Input
                      type="number"
                      min={1}
                      value={join.withinValue}
                      onChange={(e) => updateJoin(i, { withinValue: Number(e.target.value) || 1 })}
                    />
                  </div>
                  <div>
                    <label className={labelCls}>Unit</label>
                    <Select value={join.withinUnit} onValueChange={(v) => updateJoin(i, { withinUnit: v as JoinClause['withinUnit'] })}>
                      <SelectTrigger className="w-full">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectGroup>
                          {DURATION_UNITS.map((u) => (
                            <SelectOption key={u} value={u}>
                              {u}
                            </SelectOption>
                          ))}
                        </SelectGroup>
                      </SelectContent>
                    </Select>
                  </div>
                  <div>
                    <label className={labelCls}>On (left)</label>
                    <Input list="sf-columns" value={join.onLeft} onChange={(e) => updateJoin(i, { onLeft: e.target.value })} />
                  </div>
                  <div>
                    <label className={labelCls}>On (right)</label>
                    <Input list="sf-columns" value={join.onRight} onChange={(e) => updateJoin(i, { onRight: e.target.value })} />
                  </div>
                </div>
              )}
            </div>
          ))}
        </CardContent>
      </Card>

      {/* WHERE */}
      <Card>
        <CardHeader>
          <CardTitle>Where</CardTitle>
          <CardAction>
            <Button variant="outline" size="sm" onClick={() => patch({ where: [...state.where, newWhereCondition()] })}>
              <Plus data-icon="inline-start" /> Add condition
            </Button>
          </CardAction>
        </CardHeader>
        <CardContent className="flex flex-col gap-2">
          {state.where.length === 0 && <p className="text-xs text-muted-foreground">No filters applied.</p>}
          {state.where.map((c, i) => (
            <div key={i} className="flex items-center gap-2">
              {i > 0 ? (
                <Select value={c.conjunction} onValueChange={(v) => updateWhere(i, { conjunction: v as WhereCondition['conjunction'] })}>
                  <SelectTrigger className="w-20 shrink-0">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectGroup>
                      <SelectOption value="AND">AND</SelectOption>
                      <SelectOption value="OR">OR</SelectOption>
                    </SelectGroup>
                  </SelectContent>
                </Select>
              ) : (
                <span className="w-20 shrink-0 text-center text-xs text-muted-foreground">WHERE</span>
              )}
              <Input list="sf-columns" placeholder="column" value={c.left} onChange={(e) => updateWhere(i, { left: e.target.value })} />
              <Select value={c.op} onValueChange={(v) => updateWhere(i, { op: v as WhereCondition['op'] })}>
                <SelectTrigger className="w-20 shrink-0">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectGroup>
                    {COMPARE_OPS.map((op) => (
                      <SelectOption key={op} value={op}>
                        {op}
                      </SelectOption>
                    ))}
                  </SelectGroup>
                </SelectContent>
              </Select>
              <Input placeholder="value" value={c.right} onChange={(e) => updateWhere(i, { right: e.target.value })} />
              <Button
                variant="ghost"
                size="icon-sm"
                className="hover:text-destructive"
                onClick={() => patch({ where: state.where.filter((_, idx) => idx !== i) })}
                title="Remove condition"
              >
                <Trash2 />
              </Button>
            </div>
          ))}
        </CardContent>
      </Card>

      {/* GROUP BY */}
      <Card>
        <CardHeader>
          <CardTitle>Group by</CardTitle>
        </CardHeader>
        <CardContent className="flex flex-wrap gap-2">
          {columnOptions.length === 0 && <p className="text-xs text-muted-foreground">Select a source to see available columns.</p>}
          {columnOptions.map((col) => {
            const active = state.groupBy.includes(col)
            return (
              <button
                key={col}
                onClick={() =>
                  patch({
                    groupBy: active ? state.groupBy.filter((c) => c !== col) : [...state.groupBy, col],
                  })
                }
                className={cn(
                  'cursor-pointer rounded-full border px-3 py-1 text-xs font-medium transition-colors',
                  active
                    ? 'border-primary bg-primary/15 text-primary'
                    : 'border-border text-muted-foreground hover:border-foreground/40 hover:text-foreground',
                )}
              >
                {col}
              </button>
            )
          })}
        </CardContent>
      </Card>

      {/* WINDOW */}
      <Card>
        <CardHeader>
          <CardTitle>Window</CardTitle>
        </CardHeader>
        <CardContent className="grid grid-cols-4 gap-2">
          <div>
            <label className={labelCls}>Kind</label>
            <Select
              value={state.window.kind}
              onValueChange={(v) => patch({ window: { ...state.window, kind: v as BuilderState['window']['kind'] } })}
            >
              <SelectTrigger className="w-full">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectGroup>
                  <SelectOption value="NONE">NONE</SelectOption>
                  <SelectOption value="TUMBLING">TUMBLING</SelectOption>
                  <SelectOption value="HOPPING">HOPPING</SelectOption>
                  <SelectOption value="SESSION">SESSION</SelectOption>
                </SelectGroup>
              </SelectContent>
            </Select>
          </div>
          {state.window.kind !== 'NONE' && state.window.kind !== 'SESSION' && (
            <>
              <div>
                <label className={labelCls}>Size</label>
                <Input
                  type="number"
                  min={1}
                  value={state.window.size}
                  onChange={(e) => patch({ window: { ...state.window, size: Number(e.target.value) || 1 } })}
                />
              </div>
              <div>
                <label className={labelCls}>Size unit</label>
                <Select
                  value={state.window.sizeUnit}
                  onValueChange={(v) => patch({ window: { ...state.window, sizeUnit: v as BuilderState['window']['sizeUnit'] } })}
                >
                  <SelectTrigger className="w-full">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectGroup>
                      {DURATION_UNITS.map((u) => (
                        <SelectOption key={u} value={u}>
                          {u}
                        </SelectOption>
                      ))}
                    </SelectGroup>
                  </SelectContent>
                </Select>
              </div>
            </>
          )}
          {state.window.kind === 'HOPPING' && (
            <>
              <div>
                <label className={labelCls}>Advance</label>
                <Input
                  type="number"
                  min={1}
                  value={state.window.advance ?? state.window.size}
                  onChange={(e) => patch({ window: { ...state.window, advance: Number(e.target.value) || 1 } })}
                />
              </div>
              <div>
                <label className={labelCls}>Advance unit</label>
                <Select
                  value={state.window.advanceUnit ?? state.window.sizeUnit}
                  onValueChange={(v) => patch({ window: { ...state.window, advanceUnit: v as BuilderState['window']['sizeUnit'] } })}
                >
                  <SelectTrigger className="w-full">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectGroup>
                      {DURATION_UNITS.map((u) => (
                        <SelectOption key={u} value={u}>
                          {u}
                        </SelectOption>
                      ))}
                    </SelectGroup>
                  </SelectContent>
                </Select>
              </div>
            </>
          )}
          {state.window.kind === 'SESSION' && (
            <>
              <div>
                <label className={labelCls}>Gap</label>
                <Input
                  type="number"
                  min={1}
                  value={state.window.gap ?? state.window.size}
                  onChange={(e) => patch({ window: { ...state.window, gap: Number(e.target.value) || 1 } })}
                />
              </div>
              <div>
                <label className={labelCls}>Gap unit</label>
                <Select
                  value={state.window.gapUnit ?? state.window.sizeUnit}
                  onValueChange={(v) => patch({ window: { ...state.window, gapUnit: v as BuilderState['window']['sizeUnit'] } })}
                >
                  <SelectTrigger className="w-full">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectGroup>
                      {DURATION_UNITS.map((u) => (
                        <SelectOption key={u} value={u}>
                          {u}
                        </SelectOption>
                      ))}
                    </SelectGroup>
                  </SelectContent>
                </Select>
              </div>
            </>
          )}
        </CardContent>
      </Card>

      {/* SELECT */}
      <Card>
        <CardHeader>
          <CardTitle>Select</CardTitle>
          <CardAction>
            <Button variant="outline" size="sm" onClick={() => patch({ select: [...state.select, newSelectItem()] })}>
              <Plus data-icon="inline-start" /> Add column
            </Button>
          </CardAction>
        </CardHeader>
        <CardContent className="flex flex-col gap-2">
          {state.select.map((s, i) => (
            <div key={i} className="flex items-center gap-2">
              <Select value={s.agg ?? 'NONE'} onValueChange={(v) => updateSelect(i, { agg: v as SelectItem['agg'] })}>
                <SelectTrigger className="w-24 shrink-0">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectGroup>
                    {AGG_FNS.map((a) => (
                      <SelectOption key={a} value={a}>
                        {a}
                      </SelectOption>
                    ))}
                  </SelectGroup>
                </SelectContent>
              </Select>
              <Input
                list="sf-columns"
                placeholder="expression or *"
                value={s.expr}
                onChange={(e) => updateSelect(i, { expr: e.target.value })}
              />
              <span className="text-xs text-muted-foreground">AS</span>
              <Input placeholder="alias" value={s.alias ?? ''} onChange={(e) => updateSelect(i, { alias: e.target.value })} />
              <Button
                variant="ghost"
                size="icon-sm"
                className="hover:text-destructive"
                onClick={() => patch({ select: state.select.filter((_, idx) => idx !== i) })}
                title="Remove column"
              >
                <Trash2 />
              </Button>
            </div>
          ))}
        </CardContent>
      </Card>

      {/* EMIT */}
      <Card>
        <CardHeader>
          <CardTitle>Emit</CardTitle>
        </CardHeader>
        <CardContent>
          <ToggleGroup
            type="single"
            value={state.emit}
            onValueChange={(v) => v && patch({ emit: v as BuilderState['emit'] })}
            spacing={2}
          >
            {(['DEFAULT', 'CHANGES', 'FINAL'] as const).map((mode) => (
              <ToggleGroupItem key={mode} value={mode} variant="outline">
                {mode === 'DEFAULT' ? 'DEFAULT' : `EMIT ${mode}`}
              </ToggleGroupItem>
            ))}
          </ToggleGroup>
        </CardContent>
      </Card>

      <datalist id="sf-columns">
        {columnOptions.map((col) => (
          <option key={col} value={col} />
        ))}
      </datalist>
    </div>
  )
}
