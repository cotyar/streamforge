# Plan 007 — Containers + Cloud Run prep, cluster admin app, AI control chat

**Status: DONE** (P + W0–W3 complete)

Results: both flavors containerized and compose-verified live (orleans single container :6199,
265MB, linux/amd64-pinned — grpc.tools protoc segfaults under native arm64 Docker; dapr as a
self-contained app+daprd:1.18.1+placement:1.18.1+redis:7 stack :6399, shared network namespace,
no scheduler — timers only, daprd merely warns). Cloud Run manifests + parameterized `deploy.sh
--dry-run` ready for both (deploying is the user's call). Admin app (`admin/`, :5599) drove both
stacks through full start→healthy→stop cycles live, local + cloudrun-mode graceful degradation
verified. AI control chat: `POST /api/chat` in the shared Api — **Gemini** function calling
(user's provider switch mid-plan; config `Gemini:ApiKey|Model|BaseUrl`, `GEMINI_API_KEY` env,
default `gemini-2.5-flash`), 16 tools over the catalog facades, confirmed-delete guard,
8-iteration cap; SPA "AI Control" page (Editor+) with per-turn tool-call audit trace; live-proven
on Orleans via stubbed Gemini (chat request → real `create_source` → source visible in catalog)
and DI-proven on the Dapr container (clean 503-unconfigured, not a 500). Suites: orleans 884
(393+491, +7 chat tests, existing test files untouched), dapr 181, `bun run build` green.
Notable fixes en route: seed-vs-daprd circular dependency in the container topology (entrypoint
does one idempotent restart once daprd is healthy); admin stop timeout 120s→300s (a killed
`compose down` strands half the dapr stack under Docker load — observed live).

Depends on: 005 (both flavors + shared core), 006 (connectors/config live on both).

## Problem

Three user asks:

1. **Pack everything into Docker and prepare Cloud Run deployments** — both flavors must be
   runnable as containers locally (compose) and be one command away from Cloud Run
   (**prepare**, not deploy: scripts are parameterized and ready; the actual `deploy.sh` run is
   the user's call since it bills their GCP project — `total-casing-445522-j8` is configured in
   gcloud but nothing here assumes it).
2. **Admin app** that fires up the cluster on command, checks health, shuts it down.
3. **AI control chat** managing streaming/financial jobs — missing on BOTH Orleans and Dapr.

## Decisions

- **D-A — self-contained Cloud Run services, one per flavor.**
  - *Orleans*: single container. Multi-stage Dockerfile: `oven/bun` stage builds `web/dist`,
    `dotnet/sdk:10.0` publishes `StreamForge.Host`, `dotnet/aspnet:10.0` runtime with SPA +
    `orleans/docs` baked in. Cloud Run injects `PORT` → `ASPNETCORE_URLS=http://0.0.0.0:$PORT`
    (both hosts already honor `urls`). Single-silo localhost clustering; `max-instances 1`;
    data dir ephemeral — a cold start reseeds the demo catalog (documented, acceptable for a
    demo platform; persistent volume/GCS is future work). gRPC :5299 not exposed on Cloud Run
    (one port per service) — documented in parity notes.
  - *Dapr*: ONE Cloud Run multi-container service — app + `daprd` + `placement` + `redis`, all
    talking over localhost inside the instance (the Cloud Run sidecar feature). Components
    (pubsub/statestore/config, retargeted at `localhost:6379`) are baked into a tiny custom
    daprd image so no volume choreography is needed. Actors use **timers, not reminders**
    (decision from 005), so no scheduler container — verified live in compose before the YAML
    is trusted. Polyglot processors are NOT deployed to Cloud Run (local demo assets; each
    would need its own sidecar'd service — descoped, documented).
  - Local parity: `docker compose` files per flavor are the same containers Cloud Run runs —
    and double as the admin app's "local" driver. Compose host ports: orleans **6199**, dapr
    app **6399** (never 5199/5299/5399 — dev servers own those).
- **D-B — `/healthz` in shared Api** (anonymous, `{status, flavor, time}`; flavor carried on
  `StreamForgeApiOptions`). One implementation → both hosts, the admin app, compose
  healthchecks, and Cloud Run startup probes all use it. Orchestrator does this pre-wave (W0)
  since every later wave leans on it.
- **D-C — admin app = `admin/`, bun, zero deps, port 5599.** One `main.ts` (Bun.serve) + one
  page. Two drivers behind the same start/status/stop/health API:
  `local` shells out to `docker compose -f deploy/<flavor>/compose.yaml up -d|down|ps`;
  `cloudrun` shells out to `gcloud run services update|describe` (scale-to-zero = stop).
  Health = polling each flavor's `/healthz`. It never binds or signals the dev servers on
  5199/5299/5399 — it manages containers and Cloud Run services only. StreamForge text wordmark only.
- **D-D — AI control chat implemented ONCE, in `shared/StreamForge.Api/Chat/`** — both flavors
  get it with zero host edits (registered inside `AddStreamForgeApi`/`MapStreamForgeApi`,
  exactly like every other shared endpoint). `POST /api/chat` (Editor policy): server-side
  Anthropic Messages API tool loop over the **existing facades** — list/create/update/pause/
  resume/delete sources, list/create pipelines + validate SQL, list tables + rows/search +
  history. Plain `HttpClient`, no new SDK dependency. Config `Anthropic:ApiKey|Model|BaseUrl`
  with `ANTHROPIC_API_KEY` env fallback; default model `claude-sonnet-5`; unconfigured → 503
  with a friendly "set ANTHROPIC_API_KEY" body the SPA surfaces verbatim. The BaseUrl override
  is also the verification seam: a local stub server proves the multi-turn tool loop
  end-to-end without spending the user's key.
- **D-E — frozen contracts hold.** `web/src/api/types.ts` extended additively (chat DTOs);
  no existing test file modified (new test files only — same rule as 005/006); orleans 512 +
  dapr suites stay green after every commit.

## Waves

Gates everywhere: `~/.dotnet/dotnet test orleans/StreamForge.sln` (512 green, existing test
files unmodified) + `dapr/StreamForge.Dapr.sln` green + `bun run build` green. Implementation
agents = Sonnet 5, high effort, parallel where ownership is disjoint. Orchestrator commits
between waves, pushes after stable waves.

| Wave | Agents | Owns | Work | Acceptance |
|---|---|---|---|---|
| **P** | orchestrator | `plans/007*`, `plans/README.md` | this plan, committed first | committed |
| **W0** | orchestrator | `shared/StreamForge.Api/StreamForgeApiExtensions.cs` + `StreamForgeApiOptions` + both `Program.cs` (flavor arg) | `/healthz` | both suites green; live 200 on both flavors (isolated ports) |
| **W1A** | 1 | `deploy/orleans/**`, `deploy/README.md`, root `.dockerignore` | Orleans image + compose + Cloud Run `service.yaml` + `deploy.sh` | `docker build` green; compose up on 6199 → healthz+login+SPA; down clean |
| **W1B** | 1 (parallel) | `deploy/dapr/**` | app image + components-baked daprd image + compose (app/daprd/placement/redis) + multi-container `service.yaml` + `deploy.sh` | compose up on 6399 → healthz+login+CRUD+live SignalR events; down clean |
| **W1C** | 1 (parallel) | `shared/StreamForge.Api/Chat/**` + registration lines in `StreamForgeApiExtensions.cs` + new test files | chat backend per D-D | suites green; stub-loop test proves ≥2 tool round-trips; live 503-when-unset + stub-driven create-source on isolated port |
| **W2A** | 1 | `web/**` | chat UI (DTOs pinned verbatim from W1C) | `bun run build`; live chat against stubbed backend |
| **W2B** | 1 (parallel) | `admin/**` | admin app per D-C | live: starts orleans compose stack, health turns green, stops it; cloudrun driver dry-run (`--help`/describe against no service → clean error surface) |
| **W3** | orchestrator (+1 agent if needed) | docs (`orleans/docs/index.html`, `AGENTS.md`, `plans/*`) | full sweep: both suites, both docker builds, both compose stacks live side by side, admin drives both, chat stub loop; docs synced; status → DONE; push | everything above green in one pass |

Sequencing: P → W0 → {W1A, W1B, W1C} → {W2A, W2B} → W3.

## Risks

1. **Cloud Run multi-container YAML can't be fully verified without deploying** — mitigate:
   compose runs the identical container set; YAML validated with `gcloud run services replace
   --dry-run`-style linting where available, else documented as deploy-time verification.
2. **daprd/placement image tags drift from local runtime 1.18.x** — pin exact tags.
3. **Chat tool loop cost/abuse** — Editor policy + hard cap on loop iterations (8) and
   max-tokens; destructive tools (delete) require the model to echo an explicit user
   confirmation phrase — cheap guardrail, documented.
4. **Compose port collisions** — 6199/6399/5599 chosen off the reserved map; checked at start.

## Non-goals

Actual Cloud Run deployment (user runs `deploy.sh` when ready); persistent storage for
containerized Orleans data dir; polyglot processors on Cloud Run; gRPC over Cloud Run;
chat streaming (SSE) — request/response first, SSE only if trivially cheap.
