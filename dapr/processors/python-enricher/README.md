# python-enricher

A Python trades enricher — plan 005 (Dapr sibling runtime), wave W8-A. This is the polyglot-reach
demo the Dapr flavor exists for: a non-.NET process participating in the platform through nothing
but its own Dapr sidecar's HTTP API, no shared assemblies, no StreamsForge code in the loop at all.

## What it does

1. Subscribes `sf-source-trades` — the egress copy of the seeded "trades" source that
   `GeneratorActor` publishes alongside `sf-sources` (one `SourceEventsEnvelope` batch per tick; see
   `dapr/POLYGLOT.md`).
2. Enriches every trade event with 3 derived fields:
   - `notional` (`Double`) = `price * qty`
   - `signedQty` (`Long`) = `+qty` on a `BUY`, `-qty` on a `SELL` — a side classification useful for
     downstream netting.
   - `avgPrice` (`Double`) = a rolling per-symbol mean price (see the `ponytail:` note in `main.py`
     for the deliberate in-memory-only ceiling).
3. Republishes the enriched batch into `sf-sources` (the polyglot door, decision D-D) as a
   brand-new first-class source, `trades-enriched` — `{"source": "trades-enriched", "events": [...]}`
   per `dapr/POLYGLOT.md`'s frozen envelope contract, so any pipeline/table/console tape can consume
   it exactly like a seeded source.
4. At startup, best-effort registers the `trades-enriched` `SourceDefinition` via
   `POST /api/sources/` on the Dapr host (retries forever in the background, non-fatal — the
   enricher works whether or not the host is up). `Enabled: false` is deliberate: this source is fed
   by this process, not a synthetic `GeneratorActor` — enabling it would make the host spin one up
   too.

No third-party dependencies — stdlib only (`http.server` for the Dapr app callbacks,
`urllib.request` for the sidecar publish call and the REST registration call).

## Prerequisites

- Python 3.10+ (uses `dict[str, tuple[int, float]]` built-in generics)
- Dapr CLI + a running Redis-backed `pubsub` component (`dapr/components/pubsub.yaml`) — same
  sidecar infra the main Dapr host uses. The main host does NOT need to be running for this
  process's pub/sub role to work; it's only needed for step 4's registration to actually land.

## Run

```bash
dapr run --app-id sf-enricher --app-port 8399 --dapr-http-port 3899 --dapr-grpc-port 4899 \
  --resources-path ../../components -- python3 main.py
```

Ports/app-id are all overridable via env: `APP_PORT` (8399), `DAPR_HTTP_PORT` (3899). The Dapr
CLI flags (`--app-port`/`--dapr-http-port`/`--dapr-grpc-port`) must match whatever you set. Never
reuse the main Dapr host's ports (5399 app / 3599 sidecar HTTP / 4599 sidecar gRPC / 5499 gRPC
reserved — see `AGENTS.md`).

REST registration target: `SF_API` (default `http://localhost:5399`), `SF_USER`/`SF_PASS` (default
`editor`/`editor123!`, a seeded demo user with the `Editor` role `POST /api/sources` requires).

## Try it

Publish a fake trade batch to the egress topic this process subscribes:

```bash
dapr publish --publish-app-id sf-enricher --pubsub pubsub --topic sf-source-trades --data \
  '{"source":"trades","events":[{"symbol":"AAPL","price":101.5,"qty":100,"side":"BUY","venue":"NASDAQ"}]}'
```

Watch the log for `enriched 1 trade(s) -> republished as 'trades-enriched'`, then check Redis for
the republished envelope on `sf-sources` (or run the full console/host stack and watch
`trades-enriched` show up in the source list once registration lands).

## Note on the shared pubsub component

The shared `dapr/components/pubsub.yaml` was originally scoped to app-id `streamsforge-dapr` only,
which blocked every polyglot processor's sidecar (`ERR_PUBSUB_NOT_FOUND`). The scope was removed
after this wave landed — all processors now run against `--resources-path ../../components`
directly, as documented above. (Verification during the wave used a temporary local component copy,
since removed.)
