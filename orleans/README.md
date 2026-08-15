# StreamForge

An enterprise streaming-platform foundation on **Microsoft Orleans 10** (.NET 10): declarative streaming pipelines in a SQL-like dialect — joins, temporal windows, grouping, filtering, projections — managed through a React console with live results and role-based access control. Zero external infrastructure: localhost clustering, in-memory Orleans streams, JSON-file persistence.

## Run

```bash
# prerequisites: .NET 10 SDK (installed at ~/.dotnet), Node 22+
cd web && npm install && npm run build && cd ..
~/.dotnet/dotnet run --project src/StreamForge.Host        # http://localhost:5199
```

Open http://localhost:5199 and log in:

| user | password | role |
|---|---|---|
| `admin` | `admin123!` | Admin — everything + user management |
| `editor` | `editor123!` | Editor — create/edit/start/stop pipelines & sources |
| `viewer` | `viewer123!` | Viewer — read-only + live dashboards |

Frontend dev mode: `cd web && npm run dev` → http://localhost:5173 (proxies to :5199).

Tests: `~/.dotnet/dotnet test tests/StreamForge.Engine.Tests` (86 tests: parser, validator, expression semantics, all five join types, all three window kinds, aggregates, late events, plus an Orleans TestingHost end-to-end smoke test).

## What it does

Synthetic market-data generator grains publish **trades**, **quotes**, and **orders** onto Orleans streams (~8 symbols, random-walk prices). Each running pipeline is one `PipelineGrain` that compiles its SQL, subscribes to the referenced source streams, executes an interpreted operator chain (join → filter → window/aggregate → project), and publishes result rows to a per-pipeline output stream. A bridge service relays output, metrics, and raw source streams to SignalR groups for the browser. Definitions live in JSON files under `data/` — kill the host, restart it, and running pipelines resume on their own.

Four pipelines are seeded, two already running: a 5-second tumbling VWAP, a trades×quotes interval join, a session-window order-burst detector, and a LEFT JOIN unfilled-orders detector (demonstrates NULL padding on eviction).

## Streaming SQL dialect

```sql
SELECT expr [AS alias], ... | *
FROM source [alias]
  [ [INNER|LEFT [OUTER]|RIGHT [OUTER]|FULL [OUTER]] JOIN source [alias]
      WITHIN <dur> ON <expr>
  | CROSS JOIN source [alias] WITHIN <dur> ] ...
[WHERE expr]
[GROUP BY expr, ...]
[WINDOW TUMBLING(SIZE <dur>) | HOPPING(SIZE <dur>, ADVANCE BY <dur>) | SESSION(GAP <dur>)]
[EMIT CHANGES | FINAL]
```

- **Durations**: `N MILLISECONDS | SECONDS | MINUTES | HOURS` (singular accepted).
- **Joins** are interval (windowed) stream–stream joins in the Flink/ksqlDB model: each side buffers events for `WITHIN`, matched by the equi-key from `ON` (residual conditions become a post-match filter). `LEFT`/`RIGHT`/`FULL` emit NULL-padded rows when an unmatched event is evicted past the watermark; `CROSS` matches everything buffered (no `ON`).
- **Windows** close on the watermark: `max(event time, wall clock) − 1 s` allowed lateness, advanced every 500 ms. Late events are dropped and counted. `EMIT FINAL` (default) emits one row per window+group on close, with `window_start`/`window_end` columns; `EMIT CHANGES` also emits a running update per input event, flagged `_final = false`.
- **Aggregates**: `COUNT(*) | COUNT(x) | SUM | AVG | MIN | MAX`, plus `VAR_SAMP | VAR_POP | STDDEV_SAMP | STDDEV_POP | MEDIAN | PERCENTILE_CONT(p, x) | COUNT(DISTINCT x)` (`VARIANCE`/`STDDEV` alias the sample forms). All of them subtract, so they stay correct in a table where `LATEST BY` retracts a superseded row. **Functions**: `ABS, ROUND, UPPER, LOWER, COALESCE, IF`, plus searched `CASE WHEN … THEN … [ELSE …] END` (sugar for nested `IF`; branches must agree on type). Full expression grammar with `AND/OR/NOT`, comparisons, arithmetic, SQL three-valued NULL semantics; `/` always yields a double.
- With joins, `SELECT *` and result columns are alias-prefixed (`t_symbol`, `q_bid`). Reserved per-event fields: `_ts` (epoch ms), `_source`.

Examples:

```sql
-- per-symbol VWAP over 5-second tumbling windows
SELECT symbol, SUM(price * qty) / SUM(qty) AS vwap, COUNT(*) AS trades
FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 5 SECONDS)

-- enrich buys with the current quote
SELECT t.symbol, t.price, q.bid, q.ask, t.price - q.bid AS above_bid
FROM trades t JOIN quotes q WITHIN 5 SECONDS ON t.symbol = q.symbol
WHERE t.side = 'BUY'
```

The console's **visual builder** generates this dialect; the SQL editor validates as you type with positioned diagnostics from the real compiler (`POST /api/pipelines/validate`).

## Architecture

```
Browser SPA (React 19 + Tailwind 4)
   │ REST + JWT                │ SignalR /hubs/stream
   ▼                           ▼
ASP.NET Core minimal APIs ── StreamHub ◄── StreamBridgeService
   │ IClusterClient                              ▲ Orleans streams
   ▼                                             │
Orleans silo (same process, localhost clustering, memory streams)
   GeneratorGrain(trades|quotes|orders) ─► "sources"/{name}
   PipelineGrain(id): compile SQL → subscribe → execute → "pipeline-out"/{id}
   RegistryGrain("catalog"), UserStoreGrain("users") ─► JSON files in data/
```

| project | contents |
|---|---|
| `src/StreamForge.Engine` | Pure C# streaming-SQL engine: tokenizer → recursive-descent parser → validator (positioned diagnostics) → planner → interpreted operators. No Orleans dependency. |
| `src/StreamForge.Abstractions` | Grain interfaces + `[GenerateSerializer]` models. |
| `src/StreamForge.Host` | Co-hosted silo + API: grains, JSON-file grain storage, JWT auth (PBKDF2), REST endpoints, SignalR hub + stream bridge, market-data generators. |
| `tests/StreamForge.Engine.Tests` | 86 xunit tests incl. Orleans TestingHost smoke test. |
| `web/` | Vite + React SPA: dashboard, SQL editor with live diagnostics, visual pipeline builder, live results/chart, sources with live tape, user management. |

## RBAC

JWT bearer (HS256, 12 h), role claim enforced by ASP.NET policies and mirrored in the UI:
**Viewer** ⊂ **Editor** (pipeline/source CRUD, start/stop, validate) ⊂ **Admin** (user management). The SignalR hub authenticates via `access_token` query string, viewer-and-up.
