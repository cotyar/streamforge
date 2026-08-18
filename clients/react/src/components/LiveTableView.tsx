/**
 * LiveTableView -- pure presentation over a Z-set's rows, no data fetching.
 *
 * Deliberately unstyled: the console's shadcn/Tailwind version (`web/src/components/ResultsTable.tsx`)
 * stays in `web/` and will ship separately as a shadcn registry entry, complete with flash-on-change
 * highlighting and a sticky header. This component's whole job is to drop into ANY host site with
 * zero styling assumptions -- semantic `<table>`/`<thead>`/`<tbody>` markup plus a documented,
 * stable set of class hooks a host stylesheet can target:
 *
 *   sf-table                  the <table> root (className is appended here, never replaces it)
 *   sf-table__head             the <thead>
 *   sf-table__row               each data <tr>
 *   sf-table__row--flash         a <tr> whose tuple changed in the last ~900ms (flashKeys)
 *   sf-table__cell               each data <td>
 *   sf-table__empty              the <td> shown for the loading/empty states (role="status")
 *   sf-table__error               the <td> shown for the error state (role="alert")
 *   sf-table__head--sortable       a sortable <th> (sortable=true)
 *   sf-table__head--sorted-asc/-desc  the currently-sorted <th>
 *   sf-table__sort-button           the <button> inside a sortable <th>
 *   sf-table__filter-row           the <thead> row of per-column filter inputs (columnFilters=true)
 *   sf-table__filter               each per-column filter <input>
 *   sf-table__head--dragging        the <th> currently being dragged (reorderable=true)
 *   sf-table__scroll               the virtualized scroll box (virtual=true only)
 *
 * No CSS ships with this package -- not a stylesheet, not a CSS-in-JS rule, not a Tailwind class.
 *
 * Grid features (sort/filter/reorder/virtualize) are built on TanStack Table + TanStack Virtual +
 * match-sorter-utils -- all three headless, all three optional (every new prop defaults to today's
 * plain-table behaviour). This file does not reimplement any of their algorithms; it wires them to
 * this package's existing data shape (`Row = Record<string, unknown>`, dynamic column derivation)
 * and to the `sf-*` class-hook vocabulary above. See README.md's "Grid features" section for the
 * split against `client.search()` (server-side) and against `web/`'s ResultsTable (styled).
 */
import { useEffect, useState } from "react";
import type { ReactElement, ReactNode } from "react";
import { canonicalKey } from "@streamforge/client";
import type { Row } from "@streamforge/client";
import { flexRender, getCoreRowModel, getFilteredRowModel, getSortedRowModel, useReactTable } from "@tanstack/react-table";
import type {
  ColumnDef,
  ColumnFiltersState,
  ColumnOrderState,
  FilterFn,
  Row as TanStackRow,
  SortingState,
  VisibilityState,
} from "@tanstack/react-table";
import { compareItems, rankItem } from "@tanstack/match-sorter-utils";
import type { RankingInfo } from "@tanstack/match-sorter-utils";
import { useVirtualizer } from "@tanstack/react-virtual";

// TanStack's `FilterMeta` is an empty interface meant for exactly this: the documented fuzzy-filter
// pattern (tanstack.com/table's filters-fuzzy example) augments it so addMeta()'s payload is typed
// instead of `unknown`, and so `row.columnFiltersMeta[id]?.itemRank` below is typed too.
declare module "@tanstack/table-core" {
  interface FilterMeta {
    itemRank?: RankingInfo;
  }
}

// Ported from web/src/lib/format.ts's isEpochMsColumn/formatEpochMs: epoch-ms longs travel as
// plain numbers on the wire, so a column is only treated as a timestamp when BOTH its name reads
// as one (`_ts`, `..._at`, `..._start`/`_end`, `...time`, `...timestamp`) AND the value lands in a
// plausible epoch-ms range (2001..2096) -- a numeric id or count never accidentally becomes a date.
const TS_COLUMN = /(^|_)ts$|time$|timestamp$|_at$|_(start|end)$/i;

function isEpochMsColumn(name: string, v: unknown): v is number {
  return typeof v === "number" && Number.isInteger(v) && v >= 1e12 && v < 4e12 && TS_COLUMN.test(name);
}

function formatEpochMs(ms: number): string {
  const d = new Date(ms);
  const clock = `${d.toLocaleTimeString(undefined, { hour12: false })}.${String(d.getMilliseconds()).padStart(3, "0")}`;
  return d.toDateString() === new Date().toDateString() ? clock : `${d.toLocaleDateString()} ${clock}`;
}

function defaultFormatCell(value: unknown, column: string): ReactNode {
  if (value === null || value === undefined) return "";
  if (isEpochMsColumn(column, value)) return formatEpochMs(value);
  if (typeof value === "boolean") return value ? "true" : "false"; // React renders bare booleans as nothing
  if (typeof value === "number" || typeof value === "string") return value;
  return JSON.stringify(value); // objects/arrays (and anything else exotic) -- compact JSON
}

function deriveColumns(rows: readonly Row[]): string[] {
  const seen = new Set<string>();
  for (const row of rows) for (const k of Object.keys(row)) seen.add(k);
  return Array.from(seen); // Object.keys preserves insertion order -- first-seen wins across rows
}

// A row's own content is a more stable React key than its bare array index: the Z-set consolidates
// by retract/assert, so the row at index N this render is not necessarily the row at index N last
// render (rows reorder, or drop out from the middle). Keying by index alone would make React
// reuse/mutate the wrong DOM node instead of remounting it; the index is kept only as a tiebreaker
// for genuinely duplicate first-column values.
function rowKey(row: Row, index: number): string {
  const firstColumn = Object.keys(row)[0];
  return firstColumn !== undefined ? `${JSON.stringify(row[firstColumn])}#${index}` : String(index);
}

// The documented TanStack fuzzy-filter pattern verbatim (not a hand-rolled scorer): rank the raw
// cell value against the query string and stash the rank on the row via addMeta so it can be read
// back for sorting below. Used as `globalFilterFn` -- TanStack calls this once per globally-
// filterable column per row, OR-ing the results (see getFilteredRowModel's source: it breaks on the
// first column that passes), which is also why bestRank() below can't assume every column was tested.
const fuzzyFilter: FilterFn<Row> = (row, columnId, value, addMeta) => {
  const itemRank = rankItem(row.getValue(columnId), value);
  addMeta({ itemRank });
  return itemRank.passed;
};

// The best (lowest via compareItems -- match-sorter ranks "better" as "smaller") rank a row earned
// across whichever columns the global filter actually evaluated. Not every leaf column necessarily
// has a rank recorded -- TanStack's filtered-row-model stops evaluating columns for a row the moment
// one of them passes -- but the column that DID make the row pass always does, so a passing row
// always has at least one rank to compare here.
function bestRank(row: TanStackRow<Row>): RankingInfo | null {
  let best: RankingInfo | null = null;
  for (const meta of Object.values(row.columnFiltersMeta)) {
    const rank = meta?.itemRank;
    if (rank && (!best || compareItems(rank, best) < 0)) best = rank;
  }
  return best;
}

function rankSort(rows: readonly TanStackRow<Row>[]): TanStackRow<Row>[] {
  return [...rows].sort((a, b) => {
    const ra = bestRank(a);
    const rb = bestRank(b);
    return ra && rb ? compareItems(ra, rb) : 0;
  });
}

export interface LiveTableViewProps {
  rows: readonly Row[];
  /** Column order. Default: union of the rows' own key order (first-seen wins). */
  columns?: string[];
  loading?: boolean;
  error?: Error | null;
  emptyText?: string;
  /** Render at most N rows (default: all), applied after any filtering/sorting below. */
  maxRows?: number;
  className?: string;
  formatCell?: (value: unknown, column: string, row: Row) => ReactNode;
  /** Click a header to cycle asc -> desc -> none. Default false. */
  sortable?: boolean;
  /**
   * Fuzzy, RANKED (match-sorter `rankItem`) filter over the rows currently in memory --
   * NOT the server-side search `@streamforge/client`'s `client.search(name, query, limit)` runs
   * (a per-table opt-in index, `Exact`/`Fuzzy` mode, maintained by the engine over the FULL table).
   * The two are deliberately separate: silently swapping one for the other would make rows "exist
   * but not be findable" whenever this view only holds a page/window of the real table. undefined
   * or "" means no filtering.
   */
  globalFilter?: string;
  /** Render a per-column substring-filter input row under the headers. Default false. */
  columnFilters?: boolean;
  /**
   * Drag headers to reorder columns, via native HTML5 `draggable`. Default false.
   * ponytail: pointer-only -- there is no keyboard equivalent for the drag gesture itself. The
   * keyboard-accessible path is `onColumnStateChange`: a host can render its own reorder controls
   * (buttons, a select) that call back into whatever owns `initialColumnOrder`.
   */
  reorderable?: boolean;
  initialColumnOrder?: string[];
  initialHiddenColumns?: string[];
  onColumnStateChange?: (state: { order: string[]; hidden: string[] }) => void;
  /**
   * Windowed rendering via TanStack Virtual. Default false. At the console's real row counts
   * (200-500) this is unnecessary -- it earns its place in the tens of thousands. When on, this
   * component renders its OWN scroll container (`sf-table__scroll`, capped at `maxHeight`) --
   * do not also wrap it in `<StreamView>`, two scroll containers fight over the same content.
   */
  virtual?: boolean | { rowHeight?: number; overscan?: number };
  /** Scroll-box height cap; only meaningful when `virtual` is on. Default 320. */
  maxHeight?: number | string;
  /**
   * Canonical Z-set keys whose tuples just changed -- rows matching them get
   * `sf-table__row--flash` so a host stylesheet can animate them. Feed it `useLiveTable`'s own
   * `flashKeys` (LiveTablePanel does this for you); it is a plain set, so a caller driving this
   * view from some other source can compute it however it likes.
   */
  flashKeys?: ReadonlySet<string>;
}

export function LiveTableView(props: LiveTableViewProps): ReactElement {
  const {
    rows,
    columns,
    loading = false,
    error = null,
    emptyText = "No rows yet.",
    maxRows,
    formatCell,
    className,
    sortable = false,
    globalFilter,
    columnFilters: showColumnFilters = false,
    reorderable = false,
    initialColumnOrder,
    initialHiddenColumns,
    onColumnStateChange,
    virtual = false,
    maxHeight = 320,
    flashKeys,
  } = props;

  const cols = columns ?? deriveColumns(rows);
  const format = formatCell ?? defaultFormatCell;

  const [sorting, setSorting] = useState<SortingState>([]);
  const [columnFiltersState, setColumnFiltersState] = useState<ColumnFiltersState>([]);
  const [columnOrder, setColumnOrder] = useState<ColumnOrderState>(initialColumnOrder ?? []);
  const [columnVisibility, setColumnVisibility] = useState<VisibilityState>(() =>
    Object.fromEntries((initialHiddenColumns ?? []).map((id) => [id, false])),
  );
  const [draggingId, setDraggingId] = useState<string | null>(null);
  const [scrollEl, setScrollEl] = useState<HTMLDivElement | null>(null);

  const tableColumns: ColumnDef<Row>[] = cols.map((c) => ({
    id: c,
    accessorFn: (row) => row[c],
    header: c,
    filterFn: "includesString", // per-column filters: plain substring, not the fuzzy global one
    cell: (ctx) => format(ctx.row.original[c], c, ctx.row.original),
  }));

  const table = useReactTable<Row>({
    data: rows as Row[],
    columns: tableColumns,
    state: {
      sorting,
      columnFilters: columnFiltersState,
      columnVisibility,
      columnOrder,
      globalFilter: globalFilter ?? "",
    },
    onSortingChange: setSorting,
    onColumnFiltersChange: setColumnFiltersState,
    onColumnVisibilityChange: setColumnVisibility,
    onColumnOrderChange: setColumnOrder,
    globalFilterFn: fuzzyFilter,
    enableSortingRemoval: true, // asc -> desc -> none, never stuck sorted
    sortDescFirst: false, // ...and always ascending first, regardless of a column's value types
    getCoreRowModel: getCoreRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    getSortedRowModel: getSortedRowModel(),
  });

  // An explicit column sort (the user clicked a header) wins over rank order -- that's a direct
  // request. Otherwise, an active global filter re-sorts the (still-filtered) rows by match quality;
  // with neither, rows stay in the table's natural order, exactly like the plain-table path always
  // has.
  const sortingActive = sortable && sorting.length > 0;
  const globalFilterActive = Boolean(globalFilter);
  const baseRows = sortingActive ? table.getSortedRowModel().rows : table.getFilteredRowModel().rows;
  const orderedRows = !sortingActive && globalFilterActive ? rankSort(baseRows) : baseRows;
  const finalRows = maxRows !== undefined ? orderedRows.slice(0, maxRows) : orderedRows;

  const visibleLeafColumns = table.getVisibleLeafColumns();
  const colSpan = Math.max(visibleLeafColumns.length, 1);
  const rootClass = className ? `sf-table ${className}` : "sf-table";
  const headerGroup = table.getHeaderGroups()[0];

  // Report the FULL effective column state whenever it actually changes -- "order" always lists
  // every real column (TanStack appends anything `initialColumnOrder` didn't mention), not just
  // what was explicitly reordered, so a host doesn't have to reconstruct the rest itself.
  const effectiveOrder = table.getAllLeafColumns().map((c) => c.id);
  const hiddenIds = table
    .getAllLeafColumns()
    .filter((c) => !c.getIsVisible())
    .map((c) => c.id);
  useEffect(() => {
    onColumnStateChange?.({ order: effectiveOrder, hidden: hiddenIds });
    // eslint-disable-next-line react-hooks/exhaustive-deps -- fire on the effective state's own
    // change (joined into a stable string), not on onColumnStateChange's identity or the fresh
    // array references table.getAllLeafColumns() returns every render.
  }, [effectiveOrder.join(" "), hiddenIds.join(" ")]);

  function handleDrop(targetId: string): void {
    if (draggingId !== null && draggingId !== targetId) {
      const order = table.getAllLeafColumns().map((c) => c.id);
      const from = order.indexOf(draggingId);
      const to = order.indexOf(targetId);
      if (from !== -1 && to !== -1) {
        order.splice(from, 1);
        order.splice(to, 0, draggingId);
        setColumnOrder(order);
      }
    }
    setDraggingId(null);
  }

  const virtualOn = Boolean(virtual);
  const virtualOpts = typeof virtual === "object" ? virtual : {};
  const rowHeightEstimate = virtualOpts.rowHeight ?? 28;
  const overscan = virtualOpts.overscan ?? 8;

  // Always called (rules of hooks) but `enabled: virtualOn` makes it a true no-op otherwise -- see
  // virtual-core's `_willUpdate`, which skips measuring/observing entirely when disabled.
  const rowVirtualizer = useVirtualizer({
    count: finalRows.length,
    getScrollElement: () => scrollEl,
    estimateSize: () => rowHeightEstimate,
    overscan,
    enabled: virtualOn,
  });

  const virtualItems = virtualOn ? rowVirtualizer.getVirtualItems() : [];
  const totalSize = virtualOn ? rowVirtualizer.getTotalSize() : 0;
  const paddingTop = virtualItems.length > 0 ? (virtualItems[0]?.start ?? 0) : 0;
  const paddingBottom = virtualItems.length > 0 ? totalSize - (virtualItems[virtualItems.length - 1]?.end ?? 0) : 0;
  const displayRows = virtualOn
    ? virtualItems.map((vi) => finalRows[vi.index]).filter((r): r is TanStackRow<Row> => r !== undefined)
    : finalRows;

  // `flashKeys` speaks the Z-set's canonical key, not this file's rowKey() (which is a React
  // reconciliation key, deliberately index-tiebroken and therefore NOT an identity). canonicalKey()
  // is the same function the reducer keys its map with, so this is a lookup, not a re-derivation.
  // ponytail: canonicalKey() JSON-stringifies each rendered row, so this is O(rendered rows) work
  // per render -- gated on there actually being something to flash, which is the ~900ms after a
  // batch. If that ever shows up in a profile, have the hook hand back rows already paired with
  // their keys instead of re-deriving them here.
  const flashing = flashKeys !== undefined && flashKeys.size > 0 ? flashKeys : null;
  function rowClass(row: Row): string {
    return flashing?.has(canonicalKey(row)) ? "sf-table__row sf-table__row--flash" : "sf-table__row";
  }

  function renderCells(row: TanStackRow<Row>): ReactElement[] {
    return row.getVisibleCells().map((cell) => (
      <td key={cell.column.id} className="sf-table__cell">
        {flexRender(cell.column.columnDef.cell, cell.getContext())}
      </td>
    ));
  }

  const tableEl = (
    <table className={rootClass}>
      {cols.length > 0 && (
        <thead className="sf-table__head">
          <tr>
            {headerGroup?.headers.map((header) => {
              const col = header.column;
              const sortDir = sortable ? col.getIsSorted() : false;
              const ariaSort = !sortable ? undefined : sortDir === "asc" ? "ascending" : sortDir === "desc" ? "descending" : "none";
              const headClass =
                [
                  sortable && "sf-table__head--sortable",
                  sortDir === "asc" && "sf-table__head--sorted-asc",
                  sortDir === "desc" && "sf-table__head--sorted-desc",
                  reorderable && draggingId === col.id && "sf-table__head--dragging",
                ]
                  .filter(Boolean)
                  .join(" ") || undefined;
              return (
                <th
                  key={header.id}
                  scope="col"
                  className={headClass}
                  aria-sort={ariaSort}
                  draggable={reorderable || undefined}
                  onDragStart={reorderable ? () => setDraggingId(col.id) : undefined}
                  onDragOver={reorderable ? (e) => e.preventDefault() : undefined}
                  onDrop={reorderable ? () => handleDrop(col.id) : undefined}
                  onDragEnd={reorderable ? () => setDraggingId(null) : undefined}
                >
                  {sortable ? (
                    <button type="button" className="sf-table__sort-button" onClick={col.getToggleSortingHandler()}>
                      {flexRender(col.columnDef.header, header.getContext())}
                    </button>
                  ) : (
                    flexRender(col.columnDef.header, header.getContext())
                  )}
                </th>
              );
            })}
          </tr>
          {showColumnFilters && (
            <tr className="sf-table__filter-row">
              {headerGroup?.headers.map((header) => (
                <th key={header.id}>
                  <input
                    type="text"
                    className="sf-table__filter"
                    aria-label={`Filter ${header.column.id}`}
                    value={(header.column.getFilterValue() as string | undefined) ?? ""}
                    onChange={(e) => header.column.setFilterValue(e.target.value || undefined)}
                  />
                </th>
              ))}
            </tr>
          )}
        </thead>
      )}
      <tbody>
        {error ? (
          <tr>
            <td className="sf-table__error" role="alert" colSpan={colSpan}>
              {error.message}
            </td>
          </tr>
        ) : loading && finalRows.length === 0 ? (
          <tr>
            <td className="sf-table__empty" role="status" colSpan={colSpan}>
              Loading…
            </td>
          </tr>
        ) : finalRows.length === 0 ? (
          <tr>
            <td className="sf-table__empty" role="status" colSpan={colSpan}>
              {emptyText}
            </td>
          </tr>
        ) : virtualOn ? (
          <>
            {paddingTop > 0 && (
              <tr style={{ height: paddingTop }}>
                <td colSpan={colSpan} />
              </tr>
            )}
            {displayRows.map((row) => (
              <tr key={rowKey(row.original, row.index)} className={rowClass(row.original)}>
                {renderCells(row)}
              </tr>
            ))}
            {paddingBottom > 0 && (
              <tr style={{ height: paddingBottom }}>
                <td colSpan={colSpan} />
              </tr>
            )}
          </>
        ) : (
          finalRows.map((row) => (
            <tr key={rowKey(row.original, row.index)} className={rowClass(row.original)}>
              {renderCells(row)}
            </tr>
          ))
        )}
      </tbody>
    </table>
  );

  if (!virtualOn) return tableEl;

  return (
    <div
      ref={setScrollEl}
      className="sf-table__scroll"
      style={{ maxHeight: typeof maxHeight === "number" ? `${maxHeight}px` : maxHeight, overflow: "auto" }}
    >
      {tableEl}
    </div>
  );
}
