# StreamForge — container images & Cloud Run prep

Two runtime flavors, one container each (plan [`007-cloudrun-admin-aichat.md`](../plans/007-cloudrun-admin-aichat.md),
decision D-A). Both are **self-contained**: SPA + docs + published host baked into one image, no
external volumes. Both are **prepared, not deployed** — `deploy.sh` in each folder only builds and
prints/applies gcloud commands when you run it yourself; nothing here runs automatically.

## Orleans flavor — `deploy/orleans/`

Single container: bun builds the SPA, the dotnet SDK publishes `StreamForge.Host`, a slim
`aspnet:10.0` runtime bakes the publish output + `web/dist` + `orleans/docs/` together. Single-silo
localhost clustering (Orleans membership, not a real cluster) — this is why Cloud Run
`max-instances` is pinned to **1** in `service.yaml`; do not raise it without first replacing
clustering with a real membership provider.

> **Apple Silicon note**: the image build is pinned to `linux/amd64` (see `compose.yaml` /
> `Dockerfile`) — `grpc.tools`' bundled `protoc` segfaults under native `linux/arm64` in the
> `dotnet/sdk:10.0` image as of `grpc.tools` 2.80.0. `amd64` builds fine via QEMU emulation
> locally and natively on Cloud Build/Cloud Run; it's just slower on an M-series Mac.

### Build + run locally (compose)

```bash
docker compose -f deploy/orleans/compose.yaml up -d --build
curl -s http://localhost:6199/healthz                 # {"status":"ok","flavor":"orleans",...}
curl -s -X POST http://localhost:6199/api/auth/login \
  -H 'content-type: application/json' \
  -d '{"username":"admin","password":"admin123!"}'    # → JWT
# GET /            → SPA index.html
# GET /docs        → docs/index.html (comparison.html too, under /docs/comparison.html)
docker compose -f deploy/orleans/compose.yaml down
```

Host port **6199** (never 5199/5299 — those are the live dev server; see `CLAUDE.md`). The
data dir is ephemeral inside the container (`./data`, relative to the working dir, resolves to
`/app/data`) — **every cold start reseeds the demo catalog** (admin/editor/viewer users, sample
sources/pipelines/tables). Acceptable for a demo platform; persistent storage (a mounted volume or
GCS-backed grain storage) is future work, not part of this wave.

`GEMINI_API_KEY` / `ANTHROPIC_API_KEY` pass through from the host environment if set (the AI
control chat, decision D-D, reads `ANTHROPIC_API_KEY` / `Anthropic__ApiKey`; unset means the chat
endpoint replies 503 with a friendly message — expected out of the box).

### Deploy to Cloud Run

```bash
deploy/orleans/deploy.sh --dry-run                    # prints every command, touches nothing
deploy/orleans/deploy.sh --project my-project --region europe-west1
```

Defaults: `PROJECT_ID` from `gcloud config get-value project`, `REGION=europe-west1`, `TAG`=git
short SHA. Builds via `gcloud builds submit` (repo-root context) to Artifact Registry at
`${REGION}-docker.pkg.dev/${PROJECT_ID}/streamforge/orleans`, then renders
`deploy/orleans/service.yaml` (envsubst `${IMAGE}` / `${ANTHROPIC_API_KEY}`) and applies it with
`gcloud run services replace`. Nothing in this repo assumes any particular GCP project — the
`total-casing-445522-j8` project configured in local `gcloud` is the user's own, and `deploy.sh`
only picks it up as a *default* you can override with `--project`.

Cloud Run injects `PORT`; the image's entrypoint binds `0.0.0.0:${PORT:-8080}` via `--urls`
(`ASPNETCORE_URLS`/`--urls` wins over the host's own `Http:Port`/`Grpc:Port` Kestrel setup — see
`Program.cs`). gRPC stays internal-only: Cloud Run is one-port-per-service, and setting `--urls`
means the host never opens its second (gRPC) Kestrel endpoint at all.

**Caveats only verifiable at actual deploy time** (not exercised by this wave — see plan 007
risk #1): real Cloud Run scheduling behavior for `startupProbe`/`livenessProbe` timing, cold-start
latency under Cloud Run's own sandboxing (vs. plain `docker run` locally), and whether
`containerConcurrency: 40` is a sane default for this workload — revisit with real traffic data.

## Dapr flavor — `deploy/dapr/`

See `deploy/dapr/` — documented by its own README (owned by a parallel wave agent; not duplicated
here to avoid two sources of truth drifting apart).

## Admin app

The admin app (`admin/`, plan 007 decision D-C) drives both flavors' compose stacks (`local`
driver) and, later, their Cloud Run services (`cloudrun` driver) from one small UI — see its own
docs once that wave lands.
