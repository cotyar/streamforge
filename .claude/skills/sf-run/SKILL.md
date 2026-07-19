---
name: sf-run
description: Run, restart, reseed, and health-check a StreamForge host — Orleans flavor (:5199) or Dapr flavor (:5399). Use when asked to start/restart the server, reseed demo data, or confirm the stack is healthy.
---

# sf-run — run / restart / reseed / health-check StreamForge

dotnet is at `~/.dotnet/dotnet` (NOT on PATH). Never bind or kill ports 5199/5299 unless you are
intentionally (re)starting the primary Orleans dev server; test instances go on 6xxx–9xxx. The Dapr
flavor's ports (5399 app, 3599/4599 sidecar) are separate — see its own section below.

## Start (Orleans flavor, primary dev server)

```bash
cd /Users/yuriyhabarov/work/crates-foundation/orleans
cd web && bun run build && cd ..                      # only if web/ changed
~/.dotnet/dotnet run --project src/StreamForge.Host   # run in background; :5199 + :5299
```

If a Browser-pane preview is available, prefer `preview_start` with launch config name
`"streamforge"` over raw Bash.

## Restart / reseed

1. Stop the old process (`kill $(lsof -tnP -iTCP:5199 -sTCP:LISTEN)`).
2. **Reseed only if wanted**: `rm -rf src/StreamForge.Host/data` — seeds apply solely to an empty
   data dir. This wipes user-created entities, field-number maps, and history.
3. Rebuild if code changed, then start as above. Running tables resume automatically (topo-sorted;
   `Rebuilding` flag while operator state warms from live traffic).

## Isolated test instance (never for the primary ports)

```bash
~/.dotnet/dotnet run --project src/StreamForge.Host \
  --Http:Port 9199 --Grpc:Port 9299 --DataDir "$(mktemp -d)"
# ...verify...; then kill it and confirm the port is free.
```

## Health check (after ~12 s warmup)

```bash
TOKEN=$(curl -s -X POST localhost:5199/api/auth/login -H 'Content-Type: application/json' \
  -d '{"username":"viewer","password":"viewer123!"}' | python3 -c "import sys,json;print(json.load(sys.stdin)['token'])")
curl -s -H "Authorization: Bearer $TOKEN" localhost:5199/api/tables      # expect Running seeds, error:null
curl -s -o /dev/null -w "%{http_code}\n" localhost:5199/docs             # 200
curl -s -o /dev/null -w "%{http_code}\n" localhost:5199/scalar           # 302 (redirect into UI)
```

Healthy = seeded tables Running with `error: null`, dashboard rows/s climbing, `/docs` and
`/scalar` and `/explorer` all served. Logins: admin/editor/viewer + `123!`.

## Dapr flavor

Prereqs: `dapr init` already run once (containers `dapr_redis`/`dapr_placement`/`dapr_scheduler`;
check `docker ps`). Ports: app `5399` (REST/SignalR/SPA), gRPC reserved `5499` (not served — phase 2),
sidecar HTTP `3599` / gRPC `4599`. Never touches 5199/5299.

```bash
cd /Users/yuriyhabarov/work/crates-foundation
./dapr/tools/run.sh                 # dapr run --app-id streamforge-dapr --app-port 5399 ...
# Ctrl-C, or from another shell: dapr stop --app-id streamforge-dapr
```

Restart / reseed:
```bash
./dapr/tools/reset.sh               # scoped SCAN + DEL of this app's Redis keys (keyPrefix=appid)
./dapr/tools/run.sh                 # next boot reseeds from empty state
```
Seeded sources/pipelines/tables resume Running automatically within ~15s of boot (periodic
supervisor sweeps — `GeneratorSupervisorService`/`PipelineSupervisorService`/
`TableSupervisorService`/`TableHistorySupervisorService` — not a one-shot boot gate, since the
sidecar needs a moment to become ready). Health check is otherwise identical to the Orleans flavor,
just against `:5399` and with a longer warmup (~20s, not ~12s) to account for that sweep lag:
```bash
TOKEN=$(curl -s -X POST localhost:5399/api/auth/login -H 'Content-Type: application/json' \
  -d '{"username":"viewer","password":"viewer123!"}' | python3 -c "import sys,json;print(json.load(sys.stdin)['token'])")
curl -s -H "Authorization: Bearer $TOKEN" localhost:5399/api/tables      # expect Running seeds
```
Note: `/api/tables/{id}` (and `/metrics`, `/rows`, etc.) resolve by the table's REST **id** (a GUID
from the list response), not its name — `localhost:5399/api/tables/order_states` 404s;
`localhost:5399/api/tables/<id>/metrics` is the correct shape. `/docs` is not mapped on this flavor
(stays Orleans-served); `orleans/docs/comparison.html` is the cross-flavor comparison page.

Isolated test instance for the Dapr flavor is not practical the same way (fixed ports, shared Redis
sidecar) — for a scratch run, prefer resetting the shared dev instance rather than trying to stand up
a second one on different ports.

Polyglot processors (each with its own sidecar/ports, see `dapr/processors/*/README.md`):
```bash
cd dapr/processors/python-enricher && dapr run --app-id sf-enricher --app-port 8399 \
  --dapr-http-port 3899 --dapr-grpc-port 4899 --resources-path ../../components -- python3 main.py
cd dapr/processors/ts-consumer && dapr run --app-id sf-ts-consumer --app-port 8499 \
  --dapr-http-port 3999 --dapr-grpc-port 4999 --resources-path ../../components -- bun run main.ts
cd dapr/processors/java-consumer && dapr run --app-id sf-java-consumer --app-port 8599 \
  --dapr-http-port 4099 --dapr-grpc-port 5099 --resources-path ../../components -- gradle --no-daemon run
```
