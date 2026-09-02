# @streamsforge/server

An embeddable JS/TS server that speaks the same `tableDelta` contract as the Orleans/Dapr hosts,
so `@streamsforge/client` (`live-table.ts`, TanStack DB, React) works against it unchanged — over
plain **SSE** (any runtime) or **WebSocket** (Bun), no SignalR.

It is the *dataset layer*: a registry of Z-set tables, snapshot + delta fan-out, the REST routes
the client needs, and an ingest route. There is **no SQL engine** — the "executor" is your own code
in a source handler.

```ts
import { createStreamsForge } from "@streamsforge/server";

const sf = createStreamsForge();                              // open; see `auth` below
const trades = sf.table("trades", { keyFields: ["trade_id"] }); // LATEST BY trade_id
const totals = sf.table("desk_totals", { keyFields: ["desk"] });

sf.source("trade_feed", (rows) => {                           // the executor
  for (const r of rows) trades.upsert(r);
  const byDesk = new Map<string, number>();
  for (const t of trades.rows()) byDesk.set(String(t.desk), (byDesk.get(String(t.desk)) ?? 0) + Number(t.notional));
  for (const [desk, notional] of byDesk) totals.upsert({ desk, notional });
});
```

Then the client side is the usual thing, with `transport: "ws"` or `"sse"`:

```ts
const client = await connect({ url: "http://localhost:3000", user: "x", password: "y", transport: "sse" });
const t = await client.table("trades");           // subscribes, snapshots, replays
await client.push("trade_feed", [{ trade_id: "T1", desk: "Rates", notional: 100 }]);
```

## Embedding

**Bun** (SSE + WebSocket):

```ts
Bun.serve({ port: 3000, fetch: sf.fetch, websocket: sf.websocket });
```

**Hono** (SSE; add WebSocket by routing through Bun.serve):

```ts
const app = new Hono();
app.all("/api/*", (c) => sf.fetch(c.req.raw));
// Bun + WS:  Bun.serve({ fetch: (req, srv) => sf.matches(req) ? sf.fetch(req, srv) : app.fetch(req, srv), websocket: sf.websocket })
```

**Next.js** (App Router, SSE only — route handlers cannot upgrade to WebSocket):

```ts
// lib/sf.ts — keep one instance across dev HMR reloads
export const sf = (globalThis as any).__sf ??= createStreamsForge();

// app/api/[...sf]/route.ts
import { sf } from "@/lib/sf";
export const dynamic = "force-dynamic";
export const GET = (req: Request) => sf.fetch(req);
export const POST = GET;
```

`prefix: "/sf"` if the routes live under a sub-path; the client's `url` then ends with `/sf`.

## Table API

| call | keyed table (`keyFields: [...]`) | unkeyed (`null`) |
|---|---|---|
| `upsert(row)` | retracts the group's previous row, asserts this one (identical row = no-op) | asserts `+1` |
| `remove(row)` | retracts whatever the group holds; only key columns needed | retracts `-1` |
| `apply(deltas)` | raw `[row, weight]` batch, broadcast as one `seq` | same |
| `rows()` | current rows | current rows |

`keyFields: []` is a single global row (one group `"*"`).

## Auth

Omitted = open (login returns a dummy token, nothing checked; put your own middleware in front).
Provided = every route needs a Bearer token (`?access_token=` for WebSocket):

```ts
createStreamsForge({
  auth: {
    login: (u, p) => (ok(u, p) ? { token: mint(u), role: "Editor" } : null),
    verify: (token) => check(token),
  },
});
```

## Wire contract

Identical payload to the SignalR hub's `tableDelta(name, [{row, weight}], seq)`.

```
POST {prefix}/api/auth/login                  {username,password} -> {token, username, displayName, role}
GET  {prefix}/api/tables                      -> [{id, name, keyFields}]
GET  {prefix}/api/tables/{name}/rows?limit=   -> {rows:[{row,weight}], totalRows, seq}
POST {prefix}/api/sources/{name}/events       {events:[...]} -> 202 {accepted, dropped, invalid, ...}

GET  {prefix}/api/tables/{name}/live          SSE, one connection per subscription
       event: subscribed   data: {"name":"trades"}
       event: tableDelta   data: {"name":"trades","deltas":[{"row":{...},"weight":1}],"seq":12}
       : ping                                 keepalive comment every `keepaliveMs` (15s)

GET  {prefix}/api/live?access_token=…         WebSocket (Bun only), multiplexed
       -> {"type":"subscribe","table":"trades"}      -> {"type":"unsubscribe","table":"trades"}
       <- {"type":"subscribed","table":"trades"}
       <- {"type":"tableDelta","name":"trades","deltas":[...],"seq":12}
       <- {"type":"error","table":"trades","message":"..."}
```

`seq` is a per-table batch counter; as with the .NET hosts it is not comparable to the snapshot's
`seq` (the client's replay heuristic is content-based, see `zset.ts`).

Not served: search, history, SQL validate, config import, ad-hoc SQL (`client.sql()`), gRPC. The
client's `search()`/`history()`/`sql()` return 404 here.

## Test

```bash
bun test          # boots Bun.serve on a random port, drives it with @streamsforge/client over ws and sse
```
