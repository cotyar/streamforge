---
name: sf-client-gen
description: Generate and use a typed gRPC client library for any StreamsForge entity (source/pipeline/table) via the proto-download + codegen pipeline. Use when asked for a typed client, proto file, or programmatic consumption of an entity's stream.
---

# sf-client-gen — typed clients from live entities

Every source, table, and compiling pipeline exposes a **self-contained proto3 file**; the bundled
CLI turns it into a built .NET client library with a typed streaming wrapper. Field numbers are
registry-persisted (evolution-safe — schema edits keep old numbers, removed fields' numbers are
reserved forever), so generated clients survive schema changes.

## One command

```bash
cd /Users/yuriyhabarov/work/crates-foundation/orleans
PATH="$HOME/.dotnet:$PATH" ./tools/generate-client.sh \
  <source|pipeline|table> <name-or-id> \
  [--server http://localhost:5199] [--out <dir>] [--user editor --pass 'editor123!']
```

Output dir contains: `entity.proto`, `GeneratedClient.csproj` (Google.Protobuf + Grpc.Net.Client +
Grpc.Tools, already **built**), and `StreamsForgeClient.cs` with:
- `LoginAsync(httpBase, user, pass)` → JWT (login is REST-only).
- `SubscribeAsync(grpcBase, jwt)` → typed `IAsyncEnumerable<{Entity}Event|{Entity}Delta>`
  (sources/pipelines get `…Event` = row+seq+ts_ms; tables get `…Delta` = row+weight+seq, negative
  weight = Z-set retraction).

To run a quick probe, flip `<OutputType>` to `Exe`, add a `Program.cs` iterating
`SubscribeAsync("http://localhost:5299", jwt)`, and `~/.dotnet/dotnet run`.

## Raw endpoints (when the CLI is overkill)

- `GET /api/{sources|pipelines|tables}/{id-or-name}/proto` (Viewer JWT) — the proto file
  (404 unknown; 409 + diagnostics for non-compiling pipelines).
- gRPC `:5299`, cleartext h2c, JWT as `Authorization: Bearer` metadata. Reflection is live from
  the catalog: `grpcurl -plaintext localhost:5299 list` / `describe streamsforge.dynamic.v1.<Msg>`.
- Generic typed stream: `streamsforge.dynamic.v1.DynamicStreamService/SubscribeEntity` with
  `entity_key` = `source:{name}` | `pipeline:{id-or-unique-name}` | `table:{id-or-name}`;
  `DynamicFrame.payload` holds the typed message bytes (schema snapshotted at subscribe — re-subscribe
  after schema edits).
- Other languages: feed the downloaded `.proto` to protoc (Go/Python/TS/Java) — standard proto3,
  no custom options. Message naming: PascalCase of the entity name (`gold_tier_orders` →
  `GoldTierOrders`); array fields are `repeated` (element message named after the field, plural).

## Gotchas

- h2c: .NET clients need `AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true)`
  (the generated wrapper already does this).
- Two pipelines with identical *derived proto names* would collide in reflection (first wins) —
  not reachable with seeds, avoid punctuation-only name variants.
- Machinery lives in `orleans/src/StreamsForge.Host/Grpc/Dynamic/` (descriptor factory, wire
  encoder, proto builder — Orleans-free; slated for `shared/`).
