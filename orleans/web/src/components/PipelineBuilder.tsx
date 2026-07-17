import { useEffect, useMemo, useState } from 'react'
import type { SourceDefinition } from '../api/types'
import { sourcesApi } from '../api/sources'
import type { BuilderState, JoinClause, SelectItem, WhereCondition } from '../builder/types'
import { AGG_FNS, COMPARE_OPS, DURATION_UNITS, JOIN_TYPES, newJoin, newSelectItem, newWhereCondition } from '../builder/types'
import { PlusIcon, TrashIcon } from './icons'

const inputCls =
  'w-full rounded-md border border-[var(--sf-border)] bg-[var(--sf-bg)] px-2 py-1.5 text-sm text-gray-200 focus:border-[var(--sf-accent)] focus:outline-none'
const labelCls = 'mb-1 block text-xs font-medium uppercase tracking-wide text-gray-500'
const cardCls = 'rounded-xl border border-[var(--sf-border)] bg-[var(--sf-panel)] p-4'
const addBtnCls =
  'inline-flex items-center gap-1.5 rounded-md border border-dashed border-[var(--sf-border)] px-2.5 py-1.5 text-xs font-medium text-gray-400 transition-colors hover:border-[var(--sf-accent)] hover:text-[var(--sf-accent)]'
const removeBtnCls = 'rounded-md p-1.5 text-gray-500 transition-colors hover:bg-white/5 hover:text-[var(--sf-bad)]'

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

export function PipelineBuilder({ state, onChange }: { state: BuilderState; onChange: (next: BuilderState) => void }) {
  const [sources, setSources] = useState<SourceDefinition[]>([])
  const [loadError, setLoadError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    sourcesApi
      .list()
      .then((list) => {
        if (!cancelled) setSources(list)
      })
      .catch((err: unknown) => {
        if (!cancelled) setLoadError(err instanceof Error ? err.message : 'Failed to load sources')
      })
    return () => {
      cancelled = true
    }
  }, [])

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
      {loadError && (
        <p className="rounded-md border border-[var(--sf-bad)]/30 bg-[var(--sf-bad)]/10 px-3 py-2 text-xs text-[var(--sf-bad)]">
          {loadError}
        </p>
      )}

      {/* FROM */}
      <div className={cardCls}>
        <h3 className="mb-3 text-sm font-semibold text-gray-200">From</h3>
        <div className="grid grid-cols-2 gap-3">
          <div>
            <label className={labelCls}>Source</label>
            <select
              className={inputCls}
              value={state.from.source}
              onChange={(e) => patch({ from: { ...state.from, source: e.target.value } })}
            >
              <option value="">Select source…</option>
              {sources.map((s) => (
                <option key={s.name} value={s.name}>
                  {s.name}
                </option>
              ))}
            </select>
          </div>
          <div>
            <label className={labelCls}>Alias (optional)</label>
            <input
              className={inputCls}
              placeholder="t"
              value={state.from.alias}
              onChange={(e) => patch({ from: { ...state.from, alias: e.target.value } })}
            />
          </div>
        </div>
      </div>

      {/* JOINS */}
      <div className={cardCls}>
        <div className="mb-3 flex items-center justify-between">
          <h3 className="text-sm font-semibold text-gray-200">Joins</h3>
          <button className={addBtnCls} onClick={() => patch({ joins: [...state.joins, newJoin()] })}>
            <PlusIcon className="h-3.5 w-3.5" /> Add join
          </button>
        </div>
        {state.joins.length === 0 && <p className="text-xs text-gray-500">No joins — single-source pipeline.</p>}
        <div className="flex flex-col gap-3">
          {state.joins.map((join, i) => (
            <div key={i} className="rounded-lg border border-[var(--sf-border)] p-3">
              <div className="mb-2 grid grid-cols-4 gap-2">
                <div>
                  <label className={labelCls}>Type</label>
                  <select className={inputCls} value={join.type} onChange={(e) => updateJoin(i, { type: e.target.value as JoinClause['type'] })}>
                    {JOIN_TYPES.map((t) => (
                      <option key={t} value={t}>
                        {t}
                      </option>
                    ))}
                  </select>
                </div>
                <div>
                  <label className={labelCls}>Source</label>
                  <select className={inputCls} value={join.source} onChange={(e) => updateJoin(i, { source: e.target.value })}>
                    <option value="">Select…</option>
                    {sources.map((s) => (
                      <option key={s.name} value={s.name}>
                        {s.name}
                      </option>
                    ))}
                  </select>
                </div>
                <div>
                  <label className={labelCls}>Alias</label>
                  <input className={inputCls} value={join.alias} onChange={(e) => updateJoin(i, { alias: e.target.value })} />
                </div>
                <div className="flex items-end justify-end">
                  <button className={removeBtnCls} onClick={() => patch({ joins: state.joins.filter((_, idx) => idx !== i) })} title="Remove join">
                    <TrashIcon className="h-4 w-4" />
                  </button>
                </div>
              </div>
              {join.type !== 'CROSS' && (
                <div className="grid grid-cols-4 gap-2">
                  <div>
                    <label className={labelCls}>Within</label>
                    <input
                      type="number"
                      min={1}
                      className={inputCls}
                      value={join.withinValue}
                      onChange={(e) => updateJoin(i, { withinValue: Number(e.target.value) || 1 })}
                    />
                  </div>
                  <div>
                    <label className={labelCls}>Unit</label>
                    <select className={inputCls} value={join.withinUnit} onChange={(e) => updateJoin(i, { withinUnit: e.target.value as JoinClause['withinUnit'] })}>
                      {DURATION_UNITS.map((u) => (
                        <option key={u} value={u}>
                          {u}
                        </option>
                      ))}
                    </select>
                  </div>
                  <div>
                    <label className={labelCls}>On (left)</label>
                    <input list="sf-columns" className={inputCls} value={join.onLeft} onChange={(e) => updateJoin(i, { onLeft: e.target.value })} />
                  </div>
                  <div>
                    <label className={labelCls}>On (right)</label>
                    <input list="sf-columns" className={inputCls} value={join.onRight} onChange={(e) => updateJoin(i, { onRight: e.target.value })} />
                  </div>
                </div>
              )}
            </div>
          ))}
        </div>
      </div>

      {/* WHERE */}
      <div className={cardCls}>
        <div className="mb-3 flex items-center justify-between">
          <h3 className="text-sm font-semibold text-gray-200">Where</h3>
          <button className={addBtnCls} onClick={() => patch({ where: [...state.where, newWhereCondition()] })}>
            <PlusIcon className="h-3.5 w-3.5" /> Add condition
          </button>
        </div>
        {state.where.length === 0 && <p className="text-xs text-gray-500">No filters applied.</p>}
        <div className="flex flex-col gap-2">
          {state.where.map((c, i) => (
            <div key={i} className="flex items-center gap-2">
              {i > 0 ? (
                <select
                  className={`${inputCls} w-20 shrink-0`}
                  value={c.conjunction}
                  onChange={(e) => updateWhere(i, { conjunction: e.target.value as WhereCondition['conjunction'] })}
                >
                  <option value="AND">AND</option>
                  <option value="OR">OR</option>
                </select>
              ) : (
                <span className="w-20 shrink-0 text-center text-xs text-gray-600">WHERE</span>
              )}
              <input list="sf-columns" className={inputCls} placeholder="column" value={c.left} onChange={(e) => updateWhere(i, { left: e.target.value })} />
              <select className={`${inputCls} w-20 shrink-0`} value={c.op} onChange={(e) => updateWhere(i, { op: e.target.value as WhereCondition['op'] })}>
                {COMPARE_OPS.map((op) => (
                  <option key={op} value={op}>
                    {op}
                  </option>
                ))}
              </select>
              <input className={inputCls} placeholder="value" value={c.right} onChange={(e) => updateWhere(i, { right: e.target.value })} />
              <button className={removeBtnCls} onClick={() => patch({ where: state.where.filter((_, idx) => idx !== i) })} title="Remove condition">
                <TrashIcon className="h-4 w-4" />
              </button>
            </div>
          ))}
        </div>
      </div>

      {/* GROUP BY */}
      <div className={cardCls}>
        <h3 className="mb-3 text-sm font-semibold text-gray-200">Group by</h3>
        <div className="flex flex-wrap gap-2">
          {columnOptions.length === 0 && <p className="text-xs text-gray-500">Select a source to see available columns.</p>}
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
                className={`rounded-full border px-3 py-1 text-xs font-medium transition-colors ${
                  active
                    ? 'border-[var(--sf-accent)] bg-[var(--sf-accent)]/15 text-[var(--sf-accent)]'
                    : 'border-[var(--sf-border)] text-gray-400 hover:border-gray-500 hover:text-gray-200'
                }`}
              >
                {col}
              </button>
            )
          })}
        </div>
      </div>

      {/* WINDOW */}
      <div className={cardCls}>
        <h3 className="mb-3 text-sm font-semibold text-gray-200">Window</h3>
        <div className="grid grid-cols-4 gap-2">
          <div>
            <label className={labelCls}>Kind</label>
            <select
              className={inputCls}
              value={state.window.kind}
              onChange={(e) => patch({ window: { ...state.window, kind: e.target.value as BuilderState['window']['kind'] } })}
            >
              <option value="NONE">NONE</option>
              <option value="TUMBLING">TUMBLING</option>
              <option value="HOPPING">HOPPING</option>
              <option value="SESSION">SESSION</option>
            </select>
          </div>
          {state.window.kind !== 'NONE' && state.window.kind !== 'SESSION' && (
            <>
              <div>
                <label className={labelCls}>Size</label>
                <input
                  type="number"
                  min={1}
                  className={inputCls}
                  value={state.window.size}
                  onChange={(e) => patch({ window: { ...state.window, size: Number(e.target.value) || 1 } })}
                />
              </div>
              <div>
                <label className={labelCls}>Size unit</label>
                <select className={inputCls} value={state.window.sizeUnit} onChange={(e) => patch({ window: { ...state.window, sizeUnit: e.target.value as BuilderState['window']['sizeUnit'] } })}>
                  {DURATION_UNITS.map((u) => (
                    <option key={u} value={u}>
                      {u}
                    </option>
                  ))}
                </select>
              </div>
            </>
          )}
          {state.window.kind === 'HOPPING' && (
            <>
              <div>
                <label className={labelCls}>Advance</label>
                <input
                  type="number"
                  min={1}
                  className={inputCls}
                  value={state.window.advance ?? state.window.size}
                  onChange={(e) => patch({ window: { ...state.window, advance: Number(e.target.value) || 1 } })}
                />
              </div>
              <div>
                <label className={labelCls}>Advance unit</label>
                <select
                  className={inputCls}
                  value={state.window.advanceUnit ?? state.window.sizeUnit}
                  onChange={(e) => patch({ window: { ...state.window, advanceUnit: e.target.value as BuilderState['window']['sizeUnit'] } })}
                >
                  {DURATION_UNITS.map((u) => (
                    <option key={u} value={u}>
                      {u}
                    </option>
                  ))}
                </select>
              </div>
            </>
          )}
          {state.window.kind === 'SESSION' && (
            <>
              <div>
                <label className={labelCls}>Gap</label>
                <input
                  type="number"
                  min={1}
                  className={inputCls}
                  value={state.window.gap ?? state.window.size}
                  onChange={(e) => patch({ window: { ...state.window, gap: Number(e.target.value) || 1 } })}
                />
              </div>
              <div>
                <label className={labelCls}>Gap unit</label>
                <select
                  className={inputCls}
                  value={state.window.gapUnit ?? state.window.sizeUnit}
                  onChange={(e) => patch({ window: { ...state.window, gapUnit: e.target.value as BuilderState['window']['sizeUnit'] } })}
                >
                  {DURATION_UNITS.map((u) => (
                    <option key={u} value={u}>
                      {u}
                    </option>
                  ))}
                </select>
              </div>
            </>
          )}
        </div>
      </div>

      {/* SELECT */}
      <div className={cardCls}>
        <div className="mb-3 flex items-center justify-between">
          <h3 className="text-sm font-semibold text-gray-200">Select</h3>
          <button className={addBtnCls} onClick={() => patch({ select: [...state.select, newSelectItem()] })}>
            <PlusIcon className="h-3.5 w-3.5" /> Add column
          </button>
        </div>
        <div className="flex flex-col gap-2">
          {state.select.map((s, i) => (
            <div key={i} className="flex items-center gap-2">
              <select className={`${inputCls} w-24 shrink-0`} value={s.agg ?? 'NONE'} onChange={(e) => updateSelect(i, { agg: e.target.value as SelectItem['agg'] })}>
                {AGG_FNS.map((a) => (
                  <option key={a} value={a}>
                    {a}
                  </option>
                ))}
              </select>
              <input
                list="sf-columns"
                className={inputCls}
                placeholder="expression or *"
                value={s.expr}
                onChange={(e) => updateSelect(i, { expr: e.target.value })}
              />
              <span className="text-xs text-gray-600">AS</span>
              <input className={inputCls} placeholder="alias" value={s.alias ?? ''} onChange={(e) => updateSelect(i, { alias: e.target.value })} />
              <button
                className={removeBtnCls}
                onClick={() => patch({ select: state.select.filter((_, idx) => idx !== i) })}
                title="Remove column"
              >
                <TrashIcon className="h-4 w-4" />
              </button>
            </div>
          ))}
        </div>
      </div>

      {/* EMIT */}
      <div className={cardCls}>
        <h3 className="mb-3 text-sm font-semibold text-gray-200">Emit</h3>
        <div className="flex gap-2">
          {(['DEFAULT', 'CHANGES', 'FINAL'] as const).map((mode) => (
            <button
              key={mode}
              onClick={() => patch({ emit: mode })}
              className={`rounded-md border px-3 py-1.5 text-xs font-medium transition-colors ${
                state.emit === mode
                  ? 'border-[var(--sf-accent)] bg-[var(--sf-accent)]/15 text-[var(--sf-accent)]'
                  : 'border-[var(--sf-border)] text-gray-400 hover:border-gray-500 hover:text-gray-200'
              }`}
            >
              {mode === 'DEFAULT' ? 'DEFAULT' : `EMIT ${mode}`}
            </button>
          ))}
        </div>
      </div>

      <datalist id="sf-columns">
        {columnOptions.map((col) => (
          <option key={col} value={col} />
        ))}
      </datalist>
    </div>
  )
}
