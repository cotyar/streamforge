# Plan 013 — `sf` admin CLI + MCP server

Status: **IN PROGRESS**. Baseline `1d4b782` (Orleans 1676 tests, Dapr 313, both green).

Request: *"Create me a CLI and MCP server to the MCP specification for dealing with the admin part
of this stuff."*

## What "the admin part" is here

Everything an operator does to a running StreamForge instance through the console, done from a
terminal or by an agent instead: health, the catalog (sources / pipelines / tables), entity
lifecycle (start / stop / delete), SQL validation before committing to a definition, reading rows
and results (including the plan 012 CSV), and catalog config export / import.

Both flavors expose the identical REST surface (`shared/StreamForge.Api`), so one client covers
Orleans on `:5199` and Dapr on `:5399` with nothing but a different base URL.

## Shape: three files, zero dependencies

`admin/` already holds the cluster-admin app and already sets the house style — a single
`Bun.serve()` process, **zero npm dependencies**. The CLI and the MCP server join it there and keep
that rule:

| File | What it is |
|---|---|
| `admin/sfclient.ts` | The REST client + token handling. Shared by both entry points; the only file that knows the API's shape. |
| `admin/sf.ts` | The CLI. `bun admin/sf.ts <command>` |
| `admin/mcp.ts` | The MCP server, stdio transport. `bun admin/mcp.ts` |

**Why no `@modelcontextprotocol/sdk`.** MCP's stdio transport is newline-delimited JSON-RPC 2.0 and
the server half of the tools flow is four methods (`initialize`, `notifications/initialized`,
`tools/list`, `tools/call`) plus `ping`. The SDK would add the repo's first npm dependency outside
`web/`, a lockfile and an install step to `admin/`, which has deliberately had none since plan 007
— for less code than it saves. The protocol handling is pinned by tests instead (see Verification).

## Auth

`SF_URL` (default `http://localhost:5199`), and a token from — in order — `--token`, `SF_TOKEN`,
`~/.streamforge/token.json` (written by `sf login`, mode 600), or a `SF_USER`/`SF_PASSWORD` login
performed on the spot. No credential is ever written to disk; only the JWT the server issued is,
and only by an explicit `sf login`. There are no default credentials in the code — the seeded
logins live in the repo docs, not in the tool.

## CLI surface

```
sf health                                  # /healthz + who am I
sf login [--user U]                         # password from SF_PASSWORD or a no-echo prompt
sf ls <sources|pipelines|tables>            # --json for the raw array
sf get <kind> <id>
sf start|stop <pipelines|tables> <id>
sf create <kind> -f def.json                # the JSON the REST API already takes
sf delete <kind> <id>                       # --yes to skip the confirmation
sf rows <table> [--csv] [--limit N]         # plan 012's rows.csv when --csv
sf results <pipeline> [--csv] [--limit N]
sf validate <pipelines|tables> "<sql>"
sf config export [--yaml] [--secrets] [-o file]
sf config import <file> [--mode validate|merge|replace]
sf api <METHOD> <path> [body.json]          # escape hatch: anything not above
```

## MCP tools

The same operations, minus the escape hatch — an unlabelled arbitrary-HTTP tool is exactly what an
agent should not be handed, since it defeats every per-tool annotation below.

`health`, `list_entities`, `get_entity`, `get_metrics`, `validate_sql`, `get_rows`, `get_results`,
`create_entity`, `start_entity`, `stop_entity`, `delete_entity`, `export_config`, `import_config`.

Read-only tools carry `readOnlyHint`; `delete_entity` and `import_config` carry `destructiveHint`.
Tool *execution* failures come back as `isError: true` with the server's message — an unreachable
instance is a result the model can reason about, not a transport error. Only protocol faults
(unknown tool, bad params) are JSON-RPC errors.

## Verification

- `bun test admin/` — protocol conformance (initialize handshake and version negotiation,
  `tools/list` schema shape, `tools/call` success + `isError` + unknown-tool JSON-RPC error, one
  JSON object per line and **nothing** else on stdout) and the client's URL/auth construction,
  driven against a stub `Bun.serve()` instance on an ephemeral port.
- Live against a real isolated instance (6xxx–9xxx ports, temp data dir, killed afterwards): the
  CLI drives a full create → validate → start → rows → CSV → stop → delete round trip, and the MCP
  server answers a hand-written stdio session doing the same.
- Both .NET suites stay green (this wave adds no C# — it consumes the existing API).

## Deliberately not done

- **Cluster start/stop** (`docker compose` / Cloud Run). `admin/main.ts` already owns that surface
  and has a UI for it; folding it in would mean the CLI shells out to docker, which is a different
  trust and failure story from "call a REST API".
- **Streaming subscriptions** (SignalR / gRPC). A CLI that tails a live table is a different tool
  with a different lifetime; `sf rows` polls, which is what an admin actually does.
- **An npm-published package.** `bun admin/sf.ts` from the repo is the delivery vehicle, same as
  every other tool in `tools/`.
