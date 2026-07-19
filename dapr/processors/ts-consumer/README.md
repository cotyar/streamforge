# sf-ts-consumer

A standalone [bun](https://bun.sh) process proving polyglot reach over StreamForge's Dapr pub/sub
(plan 005, wave W8-B) — no Dapr SDK, no npm dependencies, `Bun.serve()` is enough.

It runs with its own Dapr sidecar (a separate `--app-id` from the main `streamforge-dapr` host) and
subscribes to two of the platform's frozen envelope topics (see `dapr/POLYGLOT.md`):

- `sf-table-delta` → `TableDeltaEnvelope` (`{ table, seq, deltas: [{ row, weight }] }`)
- `sf-pipeline-out` → `PipelineResultsEnvelope` (`{ pipelineId, results: [{ pipelineId, seq, timestampMs, row }] }`)

Dapr's sidecar discovers these via `GET /dapr/subscribe` (declarative HTTP subscription handshake)
and POSTs each message, CloudEvents-wrapped, to the declared route. This process unwraps `.data`,
prints a compact colored line per message, and prints running counters every 10 seconds.

## Run

Requires `bun` on `PATH` (never `npm` — this repo standardizes on bun for TypeScript tooling) and a
Dapr runtime + Redis already up (see `dapr/tools/` for the main host's own setup; this consumer only
needs the sidecar + the shared `pubsub` component, not the .NET host itself).

```bash
dapr run --app-id sf-ts-consumer --app-port 8499 --dapr-http-port 3999 --dapr-grpc-port 4999 \
  --resources-path ../../components -- bun run main.ts
```

Run from this directory (`dapr/processors/ts-consumer/`) — `--resources-path ../../components`
resolves to `dapr/components/` (the same `pubsub` component, type `pubsub.redis`, the main host
uses).

### Ports

All three ports default to the values in the command above and are overridable via env, so multiple
polyglot consumers can run side by side without clashing (see the repo root `AGENTS.md`'s port table
— this consumer must never bind the main host's `5399`/`3599`/`4599` or the python-enricher's ports):

| Env var | Default | Purpose |
|---|---|---|
| `APP_PORT` (or `PORT`) | `8499` | this process's own HTTP listener (`Bun.serve`) — must match `--app-port` |
| `DAPR_HTTP_PORT` | `3999` | informational — this sidecar's HTTP API port, for future outbound publish calls |
| `DAPR_GRPC_PORT` | `4999` | informational — this sidecar's gRPC API port |
| `PUBSUB_NAME` | `pubsub` | the Dapr pub/sub component name declared in `/dapr/subscribe` |

## Verify it's alive

```bash
curl -s localhost:8499/dapr/subscribe | jq
```

should return the two-topic subscription list. Publish a sample message from anywhere (see
`dapr/POLYGLOT.md`'s "Publishing from outside .NET" section for the exact envelope shapes) and watch
this process's stdout for a colored `[sf-table-delta]` or `[sf-pipeline-out]` line, plus a
`counters: ...` line every 10 seconds.

## Stop

`dapr stop --app-id sf-ts-consumer` (or Ctrl-C the foreground `dapr run`). Confirm `8499`/`3999`/`4999`
are free afterwards (`lsof -i :8499` etc.) before starting anything else on those ports.

## Note on the shared pubsub component

The shared `dapr/components/pubsub.yaml` was originally scoped to app-id `streamforge-dapr` only,
which blocked every polyglot processor's sidecar (`ERR_PUBSUB_NOT_FOUND`). The scope was removed
after this wave landed — all processors now run against `--resources-path ../../components`
directly, as documented above. (Verification during the wave used a temporary local component copy,
since removed.)
