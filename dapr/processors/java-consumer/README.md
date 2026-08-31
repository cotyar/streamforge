# sf-java-consumer

A minimal buildable Java sample proving JVM-language polyglot reach over StreamsForge's Dapr pub/sub
(plan 005, wave W8-B). Deliberately small: `com.sun.net.httpserver.HttpServer` (JDK stdlib, no
Jetty/Spring/whatever) for the HTTP surface, plus exactly one small JSON dependency (Gson, via
Gradle) since the JDK has no built-in JSON parser.

It subscribes to the same two frozen envelope topics as the sibling `ts-consumer` (see
`dapr/POLYGLOT.md`):

- `sf-table-delta` → `TableDeltaEnvelope` (`{ table, seq, deltas: [{ row, weight }] }`)
- `sf-pipeline-out` → `PipelineResultsEnvelope` (`{ pipelineId, results: [{ pipelineId, seq, timestampMs, row }] }`)

## Build

```bash
gradle --no-daemon build
```

(or `/opt/homebrew/bin/gradle --no-daemon build` if `gradle` isn't the one on `PATH`). Requires JDK
23 — this project's `build.gradle` pins a `JavaLanguageVersion.of(23)` toolchain, which resolves
straight to an installed JDK 23 (no auto-provisioning needed) as long as one is discoverable by
Gradle's toolchain detection (Homebrew's `openjdk@23` at `/opt/homebrew/opt/openjdk` qualifies, and
is in fact the JVM Gradle itself launches with on this machine — `gradle --version` reports `Daemon
JVM: .../openjdk/23.0.2/.../Home`). No pin/override was needed in this environment; if a future
environment's Gradle can't resolve JDK 23 on its own, either install one where Gradle's toolchain
service can find it, or point `org.gradle.java.installations.paths` at it in `gradle.properties`.

This wave's acceptance is the clean build (`gradle build` producing `BUILD SUCCESSFUL`, verified
2026-07-19 with Gradle 8.14 / JDK 23.0.2) — running it live against a Dapr sidecar is optional here
(the `ts-consumer` carries the live-verification requirement for W8-B).

## Run (optional this wave)

```bash
dapr run --app-id sf-java-consumer --app-port 8599 --dapr-http-port 4099 --dapr-grpc-port 5099 \
  --resources-path ../../components -- java -jar build/libs/sf-java-consumer.jar
```

or, without building a jar first:

```bash
dapr run --app-id sf-java-consumer --app-port 8599 --dapr-http-port 4099 --dapr-grpc-port 5099 \
  --resources-path ../../components -- gradle --no-daemon run
```

Run from this directory (`dapr/processors/java-consumer/`) — `--resources-path ../../components`
resolves to `dapr/components/` (the same `pubsub` component, type `pubsub.redis`, the main host
uses).

### Ports

All three default to the values above and are overridable via env — chosen to avoid the main host's
`5399`/`3599`/`4599`, the `ts-consumer`'s `8499`/`3999`/`4999`, and the python-enricher's ports:

| Env var | Default | Purpose |
|---|---|---|
| `APP_PORT` (or `PORT`) | `8599` | this process's own HTTP listener — must match `--app-port` |
| `DAPR_HTTP_PORT` | `4099` | informational — this sidecar's HTTP API port |
| `DAPR_GRPC_PORT` | `5099` | informational — this sidecar's gRPC API port |
| `PUBSUB_NAME` | `pubsub` | the Dapr pub/sub component name declared in `/dapr/subscribe` |

## Behavior

Same shape as `ts-consumer`: `GET /dapr/subscribe` returns the two-topic subscription list, each
`POST` unwraps the CloudEvents `.data` (or base64-decodes `.data_base64`), prints one compact
colored line per message (table deltas: table/seq/±weight rows; pipeline results: pipelineId + row
summary), and always responds `200 {"status":"SUCCESS"}` — a non-2xx would trigger Dapr's
at-least-once redelivery, so a malformed payload is logged and dropped instead of retried forever.
Running counters print every 10 seconds. Field lookup tolerates both camelCase (canonical per
`dapr/POLYGLOT.md`) and PascalCase, mirroring the .NET host's own case-insensitive JSON binding.

## Note on the shared pubsub component

The shared `dapr/components/pubsub.yaml` was originally scoped to app-id `streamsforge-dapr` only,
which blocked every polyglot processor's sidecar (`ERR_PUBSUB_NOT_FOUND`). The scope was removed
after this wave landed — all processors now run against `--resources-path ../../components`
directly, as documented above. (Verification during the wave used a temporary local component copy,
since removed.)
