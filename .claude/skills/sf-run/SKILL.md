---
name: sf-run
description: Run, restart, reseed, and health-check a StreamForge host (Orleans flavor now; Dapr flavor once it lands). Use when asked to start/restart the server, reseed demo data, or confirm the stack is healthy.
---

# sf-run — run / restart / reseed / health-check StreamForge

dotnet is at `~/.dotnet/dotnet` (NOT on PATH). Never bind or kill ports 5199/5299 unless you are
intentionally (re)starting the primary dev server; test instances go on 6xxx–9xxx.

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
