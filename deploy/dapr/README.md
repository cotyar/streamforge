# StreamsForge — Dapr flavor container deployment

Plan [`007-cloudrun-admin-aichat.md`](../../plans/007-cloudrun-admin-aichat.md), decision D-A, wave
W1B. Packages the Dapr flavor (`dapr/`) as a self-contained, four-container stack — app + `daprd` +
`placement` + `redis` — that runs **identically** under `docker compose` locally and as **one** Cloud
Run multi-container service. All four containers share a single network namespace (compose:
`network_mode: "service:app"` on the three sidecars; Cloud Run: its native sidecar-container feature),
so `localhost:6379`/`localhost:50005`/`localhost:3500` resolve the same way in both places.

Never bind or kill the local dev ports (5199/5299/5399) or the `dapr init` containers
(`dapr_redis`/`dapr_placement`/`dapr_scheduler`) — this stack is fully independent of them, on its own
compose project (`streamsforge-dapr`) and its own host port (**6399**).

## Local: docker compose

```bash
# from the repo root
docker compose -f deploy/dapr/compose.yaml up -d --build
docker compose -f deploy/dapr/compose.yaml ps
docker compose -f deploy/dapr/compose.yaml logs -f daprd     # watch for actor-placement errors
```

Once `app` reports healthy (`docker compose -f deploy/dapr/compose.yaml ps` shows `healthy` — the
image's own `HEALTHCHECK`, since the final `mcr.microsoft.com/dotnet/aspnet:10.0` image has neither
`curl` nor `wget`; `healthcheck.sh` speaks raw HTTP over bash's `/dev/tcp`). **This can take up to ~100s
on a cold `up --build`** — not a bug, see "Startup ordering" below.

```bash
curl -s http://localhost:6399/healthz
# {"status":"ok","flavor":"dapr","time":"..."}

TOKEN=$(curl -s -X POST http://localhost:6399/api/auth/login \
  -H 'content-type: application/json' \
  -d '{"username":"admin","password":"admin123!"}' | jq -r .token)

curl -s http://localhost:6399/api/sources -H "authorization: Bearer $TOKEN"

# actor round-trip: create + delete a source goes through RegistryActor, in-stack daprd + placement + redis
curl -s -X POST http://localhost:6399/api/sources -H "authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' -d '{"name":"smoke-test", ...}'
curl -s -X DELETE http://localhost:6399/api/sources/smoke-test -H "authorization: Bearer $TOKEN"

# generators -> pub/sub -> table actors, all inside the stack (allow ~15-20s after boot)
curl -s http://localhost:6399/api/tables -H "authorization: Bearer $TOKEN"
curl -s http://localhost:6399/api/tables/positions/rows -H "authorization: Bearer $TOKEN"
```

Tear down (also removes the anonymous Redis volume, if any — there isn't one by design, see below):

```bash
docker compose -f deploy/dapr/compose.yaml down -v
```

This never touches the local `dapr init` containers — it's a completely separate compose project/
network with no dependency on them.

### Files

| File | Role |
|---|---|
| `Dockerfile.app` | Multi-stage: `oven/bun` builds `web/dist`, `mcr.microsoft.com/dotnet/sdk:10.0` publishes `StreamsForge.Dapr.Host`, `mcr.microsoft.com/dotnet/aspnet:10.0` serves it. Bakes `web/dist` and `orleans/docs/` in, pointed at by `Web:Dist`/`Docs:File` (env `Web__Dist`/`Docs__File`). Binds `0.0.0.0:${PORT:-8080}`. |
| `Dockerfile.daprd` | `FROM daprio/daprd:1.18.1` (pinned to the local `dapr --version` runtime) with `deploy/dapr/components/` baked in at `/components` — no volumes needed anywhere this image runs. |
| `components/*.yaml` | Copies of `dapr/components/*` retargeted at `localhost:6379` (same instance's Redis container/sidecar) — see each file's own header comment for exactly what changed vs. the original. |
| `compose.yaml` | Local stack: `app` (only container with a published port, `6399:8080`), `redis`/`placement`/`daprd` all `network_mode: "service:app"`. |
| `service.yaml` | Cloud Run v1 Service manifest — same four containers, `app` is the ingress container (`containerPort: 8080` + `startupProbe`), the rest are sidecars ordered via `run.googleapis.com/container-dependencies`. Image refs are `${APP_IMAGE}`/`${DAPRD_IMAGE}` placeholders, rendered by `deploy.sh`. |
| `deploy.sh` | Builds+pushes both custom images to Artifact Registry, renders `service.yaml`, and `gcloud run services replace`s it. `--dry-run` prints every command without touching GCP or Docker Hub. |
| `healthcheck.sh` | Baked into `Dockerfile.app`'s `HEALTHCHECK` — raw HTTP GET over bash's `/dev/tcp` (no HTTP client binary in the final image). |
| `entrypoint.sh` | `Dockerfile.app`'s `ENTRYPOINT` — starts `dotnet` immediately, then restarts it once after confirming `daprd`'s own `/v1.0/healthz` is ready, to guarantee a clean catalog seed. Load-bearing, not cosmetic — see "Startup ordering" below. |

## Cloud Run: prepare (not execute)

```bash
deploy/dapr/deploy.sh --dry-run          # inspect every command + the rendered service.yaml first
deploy/dapr/deploy.sh                    # for real — builds, pushes, and deploys (bills your GCP project)
```

`PROJECT_ID` defaults to `gcloud config get-value project` (whatever project is currently configured —
nothing in this repo assumes or hardcodes one); override with `PROJECT_ID=...`. `REGION` defaults to
`europe-west1`. Actual execution is the user's call — this wave only prepares the scripts/manifest;
`deploy.sh` was never run for real during this wave's verification (only `--dry-run`, plus the equivalent
container set proven live via `compose.yaml`).

### Cloud Run YAML caveats — only verifiable at actual deploy time

- **`run.googleapis.com/container-dependencies` semantics** — the annotation's exact readiness
  contract (whether a startupProbe-less container is treated as instantly ready, retry/backoff
  behavior on a dependency that never becomes healthy, etc.) can only be confirmed by watching a real
  Cloud Run revision roll out; `startupProbe`s were added to `placement` (`httpGet /healthz` on 8081)
  and `redis` (`tcpSocket` on 6379) specifically so daprd's dependency has *something* concrete to wait
  on, mirroring compose's `depends_on: condition: service_started/service_healthy`, but Cloud Run's own
  scheduler ultimately decides the real ordering.
- **Combined per-container resource limits vs. instance size** — `service.yaml` sums to exactly 2Gi
  memory / 2 vCPU across the four containers (app 1Gi/1cpu, daprd 512Mi/0.5cpu, placement 256Mi/0.25cpu,
  redis 256Mi/0.25cpu); Cloud Run's multi-container feature requires the combined total to fit an
  allowed instance size (2/4/8/16 vCPU tiers) — this instance-size validation only happens at
  `gcloud run services replace` time.
- **gen2 execution environment requirement** — multi-container (sidecar) services require
  `run.googleapis.com/execution-environment: gen2`; if this project/region combination doesn't support
  gen2 for some reason, `services replace` will reject the manifest with a clear error at deploy time.
- **Secret handling** — `GEMINI_API_KEY` is passed as a **plain env var** placeholder
  (`${GEMINI_API_KEY}`, defaulting to empty), matching the compose file's
  `GEMINI_API_KEY: ${GEMINI_API_KEY:-}`. Wiring it through Secret Manager + a `valueFrom.secretKeyRef`
  in `service.yaml` is an obvious hardening follow-up, out of scope for this wave (no chat backend
  exists yet on this branch — see plan 007 wave W1C, running in parallel).

## Startup ordering — the app/daprd circular dependency, and how it's actually broken

`StreamsForge.Dapr.Host.Services.CatalogInitializationService` seeds the demo catalog/users **exactly
once**, on `ApplicationStarted`, with **no retry loop** (by design — see its own doc comment: "a
failure here just means the demo world isn't seeded yet"). That's correct for local dev
(`tools/run.sh` starts the Dapr sidecar, then execs the app as its child process), but this
container topology has a genuine circular readiness dependency, confirmed live from both directions:

- `daprd` cannot register actor types or connect to `placement` until it can reach the **app** on
  8080 — its own log is explicit: right after startup it prints `"application protocol: http.
  waiting on port 8080. This will block until the app is listening on that port."` and does not
  proceed to `Registering hosted actors` / `Connected to placement` until the app answers (daprd
  needs to call the app's own Dapr-SDK-mapped endpoints to learn its actor types/subscriptions first).
- `CatalogInitializationService`'s one-shot seed needs **daprd** up first, or it silently fails for
  that boot (no retry).

An earlier version of this Dockerfile/entrypoint tried to resolve this by having the app's
`ENTRYPOINT` **block, waiting for `daprd`'s own `/v1.0/healthz`, before ever launching `dotnet`.**
That is wrong and was proven wrong live: with both sides waiting on each other, `docker compose logs
daprd` sat on repeated `"waiting for application to listen on port 8080"` lines forever, with no
actor-registration or placement-connection log ever appearing — a genuine deadlock, not just a slow
race.

**The actual fix, in `deploy/dapr/entrypoint.sh`**: never make the app wait to start.
1. Launch `dotnet` **immediately** in the background — this is exactly what unblocks `daprd`'s own
   gate (the app answers on 8080 right away).
2. In parallel, poll `daprd`'s `/v1.0/healthz` (bounded by `DAPR_WAIT_TIMEOUT_S`, default 100s).
3. Once `daprd` reports ready, do **one clean restart** of the `dotnet` process (`SIGTERM`, wait, relaunch)
   so a fresh `ApplicationStarted` fires with `daprd` definitely up, guaranteeing the seed a working
   attempt. This restart runs unconditionally (even if the very first attempt already happened to
   succeed) because `EnsureInitializedAsync` (both `RegistryActor` and `UserStoreActor`) is
   idempotent — it checks `Count == 0` first, so a redundant call after a successful seed is a
   harmless no-op. `compose.yaml`'s `daprd` service depends on `app` at `service_started` only (NOT
   `service_healthy`) — gating on healthy would recreate the same deadlock this design avoids.

**Verified live, both ways it can play out**: in one run, the app's first boot won the race outright
(`daprd`'s full init — including placement connection — completed in 352ms once it detected the app
was up, which happened before `CatalogInitializationService`'s `ApplicationStarted` hook fired) —
`"catalog seeded (6 sources, 7 pipelines, 5 tables)"` logged on the very first attempt, and the
subsequent guaranteed restart logged `"catalog/users actors initialized"` again with **no** duplicate
`"catalog seeded"` line, confirming the idempotency guard. In another run, `daprd`'s connection to
`placement` took much longer before the app was ever reachable, and it was the entrypoint's own
restart that produced the only successful seed. Both outcomes converge on a seeded, healthy stack —
the mechanism doesn't depend on winning any particular race. No actor-placement errors appeared in
`docker compose logs daprd` in any run.

**Budgets**: `entrypoint.sh`'s `DAPR_WAIT_TIMEOUT_S` defaults to 100s, `Dockerfile.app`'s
`HEALTHCHECK` has a 110s `start-period`, and `service.yaml`'s `startupProbe` allows `periodSeconds: 5
* failureThreshold: 30` = 150s — generous enough to absorb a slow cold `daprd`/`placement` handshake
(observed once taking up tens of seconds when both start concurrently, which is exactly Cloud Run's
own sidecar startup model) plus the one guaranteed restart. In practice this wave's compose runs went
healthy anywhere from ~20s to just under the full budget, and every run ended with a correctly seeded
catalog and zero actor-placement errors.

## Timers, not reminders — why there's no scheduler container

Plan 005's actor design uses **Dapr timers**, never reminders, for every actor's periodic work
(`GeneratorActor`'s 200ms tick, `TableActor`'s 2s flush, the `*SupervisorService` ~15s sweeps are
regular .NET `IHostedService` loops, not actor reminders either) — see `dapr/ARCHITECTURE.md`. Only
actor **reminders** need the Dapr Scheduler service (Dapr 1.15+); timers are handled by `daprd` itself.
The local `dapr init` set does run a `dapr_scheduler` container (visible via `docker ps`), but this
stack deliberately has **no scheduler container** — `daprd`'s `--config` here never references one, and
no `--scheduler-host-address` flag is passed. This was verified live during this wave: the compose
stack's `daprd` starts cleanly with no scheduler present, actor placement/activation works (the
create-then-delete-source smoke test proves a real `RegistryActor` round trip through this stack's own
`daprd`+`placement`+`redis`), and `docker compose logs daprd` shows no actor-placement errors.

## Ephemeral Redis — a cold start reseeds the demo catalog

`redis` in `compose.yaml` (and the `redis:7-alpine` sidecar in `service.yaml`) has **no volume** — by
design, mirroring the Orleans flavor's own ephemeral-data-dir story (decision D-A) and Cloud Run's own
lack of persistent disk for a `minScale: 0` service. Every fresh `docker compose up` (or every Cloud Run
cold start after scale-to-zero) starts from an empty Redis, so `CatalogInitializationService` reseeds
the demo catalog from scratch — same behavior as deleting `orleans/src/StreamsForge.Host/data/` on that
flavor. This is acceptable for a demo platform (documented, not a bug); a persistent-Redis follow-up
(Cloud Memorystore, or a mounted volume for local-only use) is out of scope for this wave.

## Descoped: polyglot processors

`dapr/processors/{python-enricher,ts-consumer,java-consumer}/` are **not** part of this container stack
or the Cloud Run service — each would need its own sidecar'd deployment reaching the same Redis pub/sub,
which is out of scope here (see plan 007's non-goals). They remain local-only demo assets, run via their
own README's `dapr run` invocations against the *local dev* stack (`dapr/tools/run.sh`), never against
this containerized one.
