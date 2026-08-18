# @streamforge/react

React bindings for `@streamforge/client`: a provider that owns one `Client` per subtree, hooks
(`useLiveTable`, `useLiveSql`, `useTables`) that mirror a `LiveTable`'s state into React state, and
unstyled components (`LiveTableView`, `LiveTablePanel`, `Sparkline`) over the rows those hooks
return. This package does **not** re-implement the wire protocol -- subscribe -> snapshot ->
replay, the Z-set reduction, reconnect/backoff, all of it lives in `@streamforge/client`
(`clients/typescript`), and this package's whole job is gluing that class's lifecycle to React's.
See that package's own README for the client itself; this one covers what's React-specific.

## What this is not

- Not a second implementation of the protocol -- one `LiveTable` per hook, doing exactly what
  `clients/typescript/src/live-table.ts` already does.
- Not styled. Every component ships bare semantic markup (`<table>`, `<svg>`) plus a documented set
  of class hooks (below) for a host stylesheet to target -- no CSS, no CSS-in-JS, no Tailwind
  classes ship with this package. The console's polished, Tailwind/shadcn version of the same idea
  stays in `web/` (`web/src/components/ResultsTable.tsx`) and will ship separately as a shadcn
  registry entry, complete with flash-on-change highlighting and a sticky header -- reach for that
  instead if that's what you actually want.

## Install

```bash
bun install
bun run build      # emits dist/
```

## Quick start

```tsx
import { StreamForgeProvider, LiveTablePanel } from "@streamforge/react";

function App() {
  return (
    <StreamForgeProvider url="http://localhost:5199" user="admin" password="admin123!">
      <LiveTablePanel name="trigger_monitor" />
    </StreamForgeProvider>
  );
}
```

One `StreamForgeProvider` per app (or per subtree that should share a connection) `connect()`s
exactly one `Client` and hands it to every `useLiveTable`/`useLiveSql`/`useTables` call below it --
each `connect()` handshakes a transport (auth, then a gRPC channel or SignalR hub connection), so
sharing one `Client` means that handshake happens once per subtree instead of once per hook. Each
hook still opens its own `LiveTable` subscription over that shared `Client` -- what's shared is the
connection underneath, not the per-table state. Props are `ConnectOptions` (`url`, `grpc`, `user`,
`password`, `token`, `ingestKey`, `transport`, `verify` -- see `@streamforge/client`'s README) plus
`children`.

Already have a `Client` (e.g. one instantiated outside React, or shared with non-React code)? Pass
it directly and the provider connects nothing and owns nothing:

```tsx
<StreamForgeProvider client={existingClient}>...</StreamForgeProvider>
```

### Hooks-only path

Reach for `useLiveTable` directly (instead of `<LiveTablePanel>`) when you need to interleave other
UI with the fetch -- a toolbar, a row-click handler, a second view over the same rows:

```tsx
import { useLiveTable, LiveTableView } from "@streamforge/react";

function DeskExposure() {
  const { rows, loading, error } = useLiveTable("desk_exposure", { key: ["desk"] });
  return (
    <>
      <Toolbar onRowClick={(row) => ...} />
      <LiveTableView rows={rows} loading={loading} error={error} />
    </>
  );
}
```

### StreamView: the height-capped, auto-following scroll box

Every live view in the console is "a stream in a box with a height cap and a scrollbar" -- a
`ResultsTable`, a table's row grid, a source's live event tape. `StreamView` is that box, extracted
once instead of re-typed at each call site: it stays pinned to the newest edge while content streams
in, but stops the instant the user scrolls away, so new rows never yank their view out from under
them. Scrolling back to the edge resumes following. Put a `<LiveTableView>` (or your own markup)
inside it:

```tsx
import { useLiveTable, LiveTableView, StreamView } from "@streamforge/react";

function OrderLog() {
  const { rows, loading, error } = useLiveTable("orders");
  return (
    <StreamView maxHeight={320}>
      <LiveTableView rows={rows} loading={loading} error={error} />
    </StreamView>
  );
}
```

`newest="top"` flips it for a newest-first tape (the console's `useSourceTape` pattern -- prepend
new items, pin at `scrollTop === 0`) instead of the default log-style bottom edge. `follow={false}`
disables the auto-scroll behavior entirely, leaving a plain height-capped scroll box.

Like `LiveTableView` and `Sparkline`, this ships no CSS -- only the height cap and `overflow: auto`
are inline (the component's function, not decoration). `sf-stream--following` is present on the root
while it's actively pinned to the edge, so a host stylesheet can use it to show a "jump to latest"
affordance if it wants one; this package doesn't provide that chrome itself.

## Grid features: sort, filter, reorder, virtualize

`LiveTableView` (and therefore `LiveTablePanel`, which forwards everything it doesn't own itself) can
grow from a plain table into a real data grid, one independent, opt-in prop at a time -- every one of
them defaults to today's plain-table behaviour, so adding this section changed nothing for existing
callers. Built on [TanStack Table](https://tanstack.com/table), [TanStack
Virtual](https://tanstack.com/virtual) and `@tanstack/match-sorter-utils` -- all three headless, all
three optional dependencies of this package, none of them shipping any CSS of their own either.

```tsx
<LiveTableView
  rows={rows}
  sortable
  columnFilters
  reorderable
  globalFilter={query}
  initialHiddenColumns={["internal_id"]}
  onColumnStateChange={({ order, hidden }) => persist(order, hidden)}
  virtual={{ rowHeight: 28, overscan: 8 }}
  maxHeight={480}
/>
```

- **`sortable`** -- click a header to cycle ascending -> descending -> unsorted. Real `<button>`s
  inside each `<th>` (keyboard-operable for free) with `aria-sort` kept in sync (`"none"` while
  sortable but unsorted, not omitted -- the column IS sortable, it's just not the active one).
- **`globalFilter`** -- a controlled string, fuzzy-matched with match-sorter's `rankItem` (the
  [documented TanStack fuzzy-filter pattern](https://tanstack.com/table/latest/docs/framework/react/examples/filters-fuzzy),
  not a hand-rolled scorer) and RANKED: matching rows are re-sorted best-match-first via
  `compareItems`, unless an explicit column sort (`sortable`, clicked) is active, which wins instead.
  **This is a client-side filter over the rows already in memory, not a query.** It is a completely
  different feature from `@streamforge/client`'s `client.search(name, query, limit)` -- a per-table
  opt-in server-side index (`Exact` or `Fuzzy` mode, maintained by the engine over the table's FULL
  contents, see that package's README and `web/src/pages/TableDetailPage.tsx`'s `SearchAndView` for
  the console's own use of it). Swapping one for the other silently would make rows "exist but not be
  findable" the moment this view holds only a page or a capped live window rather than the whole
  table -- so this component never tries to guess which one a host meant; `globalFilter` is always
  the in-memory one, and reaching for `client.search()` directly is the host's job when a query needs
  to reach rows outside what's currently in `rows`.
- **`columnFilters`** -- a filter-input row under the headers, one plain substring match
  (case-insensitive, TanStack's built-in `includesString`) per column, independent of `globalFilter`
  and of each other.
- **`reorderable`** -- drag a header to reorder columns, via native HTML5 `draggable` (no
  drag-and-drop library). ponytail: this is pointer-only -- there's no keyboard equivalent for the
  drag gesture itself. `onColumnStateChange` is the keyboard-accessible path: it fires with the full,
  current `{ order, hidden }` on every change (drag OR programmatic), so a host can render its own
  reorder controls (buttons, a `<select>`) that write back through `initialColumnOrder` instead.
- **`initialColumnOrder`** / **`initialHiddenColumns`** -- seed column order/visibility once at
  mount, like `defaultValue` -- not resynced on every re-render. This component owns no column-picker
  or reorder UI itself (`onColumnStateChange` is the wire-out for a host that wants one); state lives
  here only because *something* has to hold it between a drag and the next `initialX` the host might
  pass, and putting it in the host would mean every host reimplements the same bookkeeping.
- **`virtual`** -- windowed rendering via `useVirtualizer`, `true` or `{ rowHeight?, overscan? }`
  (defaults ~28px / 8). Renders its OWN scroll container (`sf-table__scroll`, capped at `maxHeight`,
  default 320) -- **do not also wrap it in `<StreamView>`**, two scroll containers fighting over the
  same content is worse than either alone. At the console's real row counts (a few hundred, per
  `TableDetailPage.tsx`'s own 500-row cap) virtualization is unnecessary overhead; it earns its place
  once a table view is regularly in the tens of thousands of rows, which nothing in this repo's own
  UI currently is.

## Public surface

```ts
<StreamForgeProvider url? grpc? user? password? token? ingestKey? transport? verify? client? children>
useStreamForge() -> Client | null                    // null until connected; throws outside a provider
useStreamForgeStatus() -> { client, connecting, error }

useLiveTable(name: string | undefined, opts?: { key?: string[]; timeoutMs?: number }) -> {
  rows: readonly Row[]; loading: boolean; error: Error | null; table: LiveTable | null;
  flashKeys: ReadonlySet<string>;                    // canonical keys touched in the last ~900ms
}
useLiveSql(sql: string | undefined, opts: { name: string; key?: string[]; timeoutMs?: number }) -> same shape as useLiveTable
useTables() -> { tables: TableDefinitionDto[]; loading: boolean; error: Error | null }

<LiveTableView
  rows columns? loading? error? emptyText? maxRows? className? formatCell?
  sortable? globalFilter? columnFilters? reorderable?
  initialColumnOrder? initialHiddenColumns? onColumnStateChange?
  virtual? maxHeight? flashKeys?
/>
<LiveTablePanel name tableKey? timeoutMs? {...LiveTableView props except rows/loading/error} />
<Sparkline values width? height? className? stroke? />
<StreamView maxHeight? follow? newest? className? children />
```

`name`/`sql` passed as `undefined` puts the corresponding hook in an idle state (`rows: []`,
`loading: false`, `error: null`) rather than fetching -- useful for a "no table selected yet" UI
that shouldn't spin.

`opts.key` is the row-identity columns used for LATEST BY-style supersession (an updated row for
the same key replaces the old one rather than appearing twice) -- forwarded straight to
`client.table()`'s own `key` option; see `@streamforge/client`'s README and
`clients/typescript/src/zset.ts`'s module doc comment for what "identity" means here and why no
column is ever guessed when it's omitted.

`table` on the hook's return value is the underlying `LiveTable` once connected -- an escape hatch
for `waitFor()`, `.seq`, `.reconnects`, or anything else the hook's own state doesn't surface.

## Styling: the class hooks

No CSS ships with this package. `LiveTableView` and `Sparkline` emit a small, stable set of class
names for a host stylesheet to target:

| Class              | Element                                          |
| ------------------ | ------------------------------------------------- |
| `sf-table`          | the `<table>` root (`className` prop is appended, never replaces it) |
| `sf-table__head`     | the `<thead>`                                     |
| `sf-table__row`      | each data `<tr>`                                  |
| `sf-table__row--flash` | a `<tr>` whose tuple changed in the last ~900ms (`flashKeys`) |
| `sf-table__cell`     | each data `<td>`                                  |
| `sf-table__empty`    | the loading/empty-state `<td>` (`role="status"`)  |
| `sf-table__error`    | the error-state `<td>` (`role="alert"`)           |
| `sf-table__head--sortable` | a sortable `<th>` (`sortable` prop on)      |
| `sf-table__head--sorted-asc` / `--sorted-desc` | the `<th>` currently sorted, matching direction |
| `sf-table__sort-button` | the `<button>` inside a sortable `<th>`         |
| `sf-table__filter-row` | the `<thead>` row of per-column filter inputs (`columnFilters` prop on) |
| `sf-table__filter`   | each per-column filter `<input>`                  |
| `sf-table__head--dragging` | the `<th>` currently being dragged (`reorderable` prop on) |
| `sf-table__scroll`   | the virtualized scroll box root (`virtual` prop on only) |
| `sf-sparkline`       | the `<svg>` root                                  |
| `sf-stream`          | the `StreamView` scroll box root                  |
| `sf-stream--following` | present on the root while pinned to the newest edge |

```css
.sf-table { border-collapse: collapse; width: 100%; }
.sf-table__head th { text-align: left; border-bottom: 1px solid #ddd; }
.sf-table__cell { padding: 4px 8px; }
.sf-table__error { color: #b00020; }
.sf-table__head--sortable { cursor: pointer; }
.sf-table__sort-button { background: none; border: none; font: inherit; cursor: pointer; }
.sf-table__filter { width: 100%; box-sizing: border-box; }
.sf-table__head--dragging { opacity: 0.5; }
.sf-table__scroll { border: 1px solid #ddd; }
.sf-sparkline { color: #2b6cb0; } /* Sparkline's stroke defaults to currentColor */
.sf-stream { border: 1px solid #ddd; }
```

## Browser support: SignalR only, gRPC is Node-only

This package runs entirely on top of `@streamforge/client`'s `connect()`, so the same rule applies
unchanged: a browser cannot speak h2c gRPC at all, so in a browser build `transport: "auto"`
(the default) silently skips the gRPC attempt and goes straight to SignalR. See
`clients/typescript/README.md`'s "gRPC is Node-only" and "Transports" sections for the full
explanation (why, how `"auto"` picks ws -> sse -> lp, and what a browser bundler does with the
dynamically-imported gRPC module) -- nothing about it changes here, so it isn't repeated.

## Not included, and why

- **No flash ANIMATION.** The flash *state* is here: `useLiveTable` returns `flashKeys` (the
  canonical Z-set keys touched in the last ~900ms, straight from `LiveTable.onChange`'s `touched`
  argument -- no second reducer), `LiveTablePanel` feeds it to the view automatically, and matching
  rows get `sf-table__row--flash`. What that class *looks* like is a host stylesheet's business, like
  every other class here. Note that the first emission after a reconnect flashes the whole table:
  the client marks a post-reconnect reseed as "everything changed", which is honest -- deltas
  emitted while the connection was down are gone, not buffered.
- **No column-picker menu, no search box, no "jump to page" chrome.** Sorting, filtering, reordering
  and virtualizing are all here now (see "Grid features" above) -- what's still deliberately absent
  is any UI for DRIVING them beyond a sortable header and a drag handle. This component exposes
  state and callbacks (`onColumnStateChange`, the controlled `globalFilter` string); a host owns
  whatever chrome sits around that (a search input, a column-visibility dropdown, pagination
  controls). Shipping that chrome here would mean styling it, which contradicts the whole point of
  this package.
- **No TanStack DB in this package**, deliberately. StreamForge is already the
  incremental-view-maintenance engine (the Z-set reducer in `clients/typescript/src/zset.ts` **is**
  that layer -- retract/assert, weight summation, group-key supersession), so stacking TanStack DB's
  own IVM under these hooks would mean two systems independently deciding what a row's identity is
  and when it changed: a correctness risk (they can disagree) for no gain, since `LiveTable.rows`
  already IS the materialized, deduped view. `useLiveTable`'s plain `Row[]` is the integration point
  for state managers that just need data.
  What TanStack DB *does* buy -- client-side live queries JOINING several tables, and optimistic
  mutations -- is real, and lives in its own optional package, `@streamforge/tanstack-db`
  (`clients/tanstack-db`): a bridge that feeds `LiveTable`'s touched-key deltas into a collection
  keyed by the Z-set's own canonical key, so there is still exactly one notion of row identity.
  Reach for it when you need those two things; reach for these hooks when you don't.
- **No CSS.** See "Styling" above -- deliberate, not an oversight.

## Testing

```bash
bun test    # test/react.test.tsx: hooks through a fake Transport + real LiveTable, pure component rendering
            # test/stream-view.test.tsx: StreamView's auto-follow/scroll-pinning behavior
            # test/grid.test.tsx: sort/filter/reorder/virtualize, layered onto LiveTableView
```

`test/happydom.ts` (a bun test preload, wired via `bunfig.toml`) registers
`@happy-dom/global-registrator`'s DOM globals and sets `IS_REACT_ACT_ENVIRONMENT` before any test
file -- or the `@testing-library/react`/`react-dom` it imports -- loads; both must be in place
before those modules are first evaluated, which only a preload (not a same-file import) guarantees.
No live engine is needed: tests drive `useLiveTable` through a hand-rolled in-memory `Transport`
(implementing `clients/typescript`'s `Transport` interface directly, per its own doc comment) and a
real `LiveTable`, wrapped in a minimal object cast to `Client` and handed to
`<StreamForgeProvider client={...}>` -- the same pre-built-client escape hatch documented above,
not a mock of this package's own code.
