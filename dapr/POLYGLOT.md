# StreamsForge — Dapr polyglot pub/sub contract (plan 005, wave W5-B)

This is the **frozen wire contract** any non-.NET process (a Python enricher, a bun consumer, a plain
`dapr publish` CLI call, or any other sidecar'd process) needs to speak to participate in the platform's
five fixed pub/sub topics (decision D-D, `plans/005-dapr-port.md`). Field numbers/shapes here are forever,
exactly like every other frozen DTO in this project — additive changes only.

## Pub/sub component

Every topic below is on the Dapr component named **`pubsub`** (`dapr/components/pubsub.yaml`, `type:
pubsub.redis`). Publish with `--pubsub pubsub`.

## Topics, route paths, and directionality

| Topic | Host route (what the sidecar POSTs to) | Direction | Envelope type |
|---|---|---|---|
| `sf-sources` | `POST /sf-sources` | **ingress** — the platform subscribes; a generator or any polyglot process may publish here | `SourceEventsEnvelope` |
| `sf-source-{name}` | *(not subscribed by the host)* | **egress only** — generators publish raw per-source events here for any sidecar to consume; the host never subscribes these dynamically-named topics (decision D-D: per-entity topics can't be subscribed without a restart) | *(one raw event object per message, source-specific shape)* |
| `sf-pipeline-out` | `POST /sf-pipeline-out` | produced by W6's PipelineActor, relayed to SignalR — not a polyglot ingress point in practice, but the endpoint accepts and normalizes any publish the same way | `PipelineResultsEnvelope` |
| `sf-table-delta` | `POST /sf-table-delta` | produced by W7's TableActor, relayed to SignalR | `TableDeltaEnvelope` |
| `sf-lifecycle` | `POST /sf-lifecycle` | produced by `DaprLifecycleOrchestrator`, relayed to SignalR as `pipelineStatus`/`tableStatus` | `LifecycleEvent` |
| `sf-metrics` | `POST /sf-metrics` | produced by W6's PipelineActor, relayed to SignalR as `pipelineMetrics` | `PipelineMetrics` |

The **polyglot door** (decision D-D) is `sf-sources`: any sidecar'd process may publish an
`SourceEventsEnvelope` here and the router treats it identically to a `GeneratorActor` tick — the derived/
enriched data shows up in the console exactly like any seeded source's events, once its name is
registered via the normal `POST /api/sources` REST call.

## Case-handling — what an external publisher must send

**Canonical form: camelCase property names**, matching every other wire contract in this platform (REST
responses, SignalR payloads, `web/src/api/types.ts`). Example: `source`/`events`/`table`/`seq`/`deltas`/
`row`/`weight`/`pipelineId`/`results`/`timestampMs`/`kind`/`status`.

**PascalCase is also accepted** (but not the documented/preferred form): every endpoint deserializes with
the app's ambient `Microsoft.AspNetCore.Http.Json.JsonOptions`
(`shared/StreamsForge.Api/StreamsForgeApiExtensions.cs`'s `ConfigureHttpJsonOptions` call only adds a
`JsonStringEnumConverter`; it does not touch `PropertyNameCaseInsensitive`, which stays at ASP.NET Core's
own default of `true` for `Http.Json.JsonOptions`) — so `"Source"` and `"source"` both bind to
`SourceEventsEnvelope.Source`. This is a byproduct of the shared JSON configuration, not a contract this
plan actively maintains; publish camelCase.

Enum fields (`PipelineStatus` on `LifecycleEvent`/`PipelineMetrics`) serialize/deserialize as their
**string names** (`"Running"`, `"Stopped"`, `"Failed"`, ...), matching the REST/SignalR contract's
`JsonStringEnumConverter` — not as raw ints. (This is the OPPOSITE of the Dapr actor-invocation wire,
which uses ints — see `dapr/ARCHITECTURE.md`'s serialization note. Actor wire and pub/sub wire are
independently configured.)

## Envelope shapes

### `sf-sources` → `SourceEventsEnvelope`

```json
{
  "source": "trades",
  "events": [
    { "symbol": "AAPL", "price": 101.5, "qty": 10, "side": "buy" }
  ]
}
```

- `source` (string): the source's registered name — must already exist via `POST /api/sources` (or be a
  name the console/registry knows about) for the SPA to have a group to relay into; the router itself
  doesn't validate this, it just relays whatever `source:{name}` group exists.
- `events` (array of objects): each object is a free-form field-name → value map — this is the row shape
  that ultimately reaches the Engine/table search index. Numbers without a fractional part are read back
  as 64-bit integers; numbers with one are read back as doubles (see Normalization below). Reserved keys
  `_ts`/`_source` are honored the same way `StreamsForge.Engine.EventRecord`'s own accessors read them
  elsewhere in the platform, if present — not required.

### `sf-table-delta` → `TableDeltaEnvelope`

```json
{
  "table": "positions",
  "seq": 42,
  "deltas": [
    { "row": { "symbol": "AAPL", "qty": 5 }, "weight": 1 }
  ]
}
```

- `weight` is a Z-set weight: positive = row entering, negative = row leaving, per the platform's
  standard Z-set delta semantics.
- `seq` is the table's own monotonic sequence number — the bridge relays it verbatim to SignalR's
  `tableDelta` event, it does not renumber it.

### `sf-pipeline-out` → `PipelineResultsEnvelope`

```json
{
  "pipelineId": "p1",
  "results": [
    { "pipelineId": "p1", "seq": 1, "timestampMs": 1737244800000, "row": { "total": 42.5 } }
  ]
}
```

### `sf-lifecycle` → `LifecycleEvent`

```json
{ "pipelineId": "p1", "kind": "started", "status": "Running", "timestampMs": 1737244800000 }
```

`kind` is one of `"created"|"updated"|"deleted"|"started"|"stopped"|"failed"` for pipelines, or the same
set prefixed `"table-"` (`"table-started"`, ...) for tables — in the table case, `pipelineId` actually
carries the table's **Name**, mirroring the Orleans flavor's own stream reuse (see
`orleans/src/StreamsForge.Host/Services/StreamBridgeService.cs`'s doc comment on `OnLifecycleEventAsync`).

### `sf-metrics` → `PipelineMetrics`

```json
{
  "pipelineId": "p1",
  "status": "Running",
  "eventsInPerSec": 12.5,
  "rowsOutPerSec": 3.1,
  "totalEventsIn": 10000,
  "totalRowsOut": 2500,
  "windowsClosed": 40,
  "lastEventTsMs": 1737244800000
}
```

## Normalization

Every `Dictionary<string, object?>` payload (`SourceEventsEnvelope.Events[*]`,
`TableDeltaEnvelope.Deltas[*].Row`, `PipelineResultsEnvelope.Results[*].Row`) is passed through
`StreamsForge.AppCore.Json.JsonValueNormalizer.NormalizeInPlace` immediately after deserialization, before
any consumer sees it — turning raw `JsonElement` wire values into plain CLR types (`string`/`long`/
`double`/`bool`/`null`/`Dictionary<string,object?>`/`List<object?>`). A publisher doesn't need to do
anything special to get this — it's applied unconditionally by the host, not something the wire format
itself encodes.

## Malformed-payload handling (poison-message loop protection)

Every endpoint above **always responds 200 OK**, whether or not the body parsed as valid JSON for its
envelope type. A malformed/unparseable/empty payload is logged as a warning and silently dropped —
deliberately, because Dapr's pub/sub delivery is at-least-once and a non-2xx response from the subscriber
is exactly the signal that triggers redelivery. Without this, a single permanently-malformed message (a
schema mismatch from a buggy polyglot publisher, say) would retry forever. There is no dead-letter topic
configured for `pubsub` today — a malformed message is simply lost, with a log line as the only trace.

## Publishing from outside .NET (examples)

Via the Dapr CLI (any language, or none at all — useful for manual smoke tests):

```bash
dapr publish --publish-app-id streamsforge-dapr --pubsub pubsub --topic sf-sources --data \
  '{"source":"trades","events":[{"symbol":"AAPL","price":101.5,"qty":10}]}'

dapr publish --publish-app-id streamsforge-dapr --pubsub pubsub --topic sf-table-delta --data \
  '{"table":"positions","seq":1,"deltas":[{"row":{"symbol":"AAPL","qty":5},"weight":1}]}'

dapr publish --publish-app-id streamsforge-dapr --pubsub pubsub --topic sf-lifecycle --data \
  '{"pipelineId":"p1","kind":"started","status":"Running","timestampMs":0}'

dapr publish --publish-app-id streamsforge-dapr --pubsub pubsub --topic sf-metrics --data \
  '{"pipelineId":"p1","status":"Running","eventsInPerSec":1,"rowsOutPerSec":1,"totalEventsIn":1,"totalRowsOut":1,"windowsClosed":0,"lastEventTsMs":0}'
```

From any sidecar'd process in any language: publish a CloudEvents-wrapped (or raw, with
`--metadata rawPayload=true` on the component/request) HTTP or gRPC pub/sub publish call to the sidecar,
topic `sf-sources`, with the JSON body shown above — no .NET-specific serialization involved anywhere on
the wire.

## SignalR relay (for reference — see `dapr/src/StreamsForge.Dapr.Host/Streaming/DaprStreamBridge.cs`)

| Topic | SignalR group | Event | Args |
|---|---|---|---|
| `sf-sources` | `source:{name}` | `sourceEvent` | `(name, eventDict)` — one send per surviving event, sampled to ~20 msg/s per source |
| `sf-table-delta` | `table:{name}` | `tableDelta` | `(name, deltas, seq)` |
| `sf-pipeline-out` | `pipeline:{id}` | `pipelineResult` | `(id, results)` |
| `sf-lifecycle` (pipeline kind) | `pipeline:{id}` | `pipelineStatus` | `(id, status)` |
| `sf-lifecycle` (table-* kind) | `table:{name}` | `tableStatus` | `(name, status)` |
| `sf-metrics` | `metrics` | `pipelineMetrics` | `(metrics)` |

This table is byte-for-byte the same group/event/arg shape the Orleans flavor's
`StreamBridgeService` uses — the console SPA (`web/src/realtime/hub.ts`) needs no runtime-specific
branching.
