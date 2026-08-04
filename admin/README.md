# StreamForge — Cluster Admin

Fires up either flavor's containerized stack on command, shows live health, shuts it down. Plan
007 decision D-C. Single Bun.serve() process, **zero npm dependencies** — one `main.ts` + one
static `index.html`.

It **never** binds, signals, or health-probes the local dev servers on **5199/5299/5399**
(Orleans/Dapr dev instances — see repo `CLAUDE.md`). It only ever manages:

- the local docker-compose stacks (`deploy/orleans/compose.yaml` on host port **6199**,
  `deploy/dapr/compose.yaml` on host port **6399**), or
- the Cloud Run services `streamforge-orleans` / `streamforge-dapr` (via `gcloud`).

## Run

```bash
cd admin && bun main.ts
# → StreamForge admin listening on :5599 (mode=local)
```

Open <http://localhost:5599>.

## Modes

| MODE (env, default `local`) | start does | stop does | health comes from |
|---|---|---|---|
| `local` | `docker compose -f deploy/<flavor>/compose.yaml up -d --build` | `docker compose -f deploy/<flavor>/compose.yaml down` | `GET http://localhost:<6199\|6399>/healthz` |
| `cloudrun` | `gcloud run services update <svc> --region $REGION --min-instances=1` | `gcloud run services update <svc> --region $REGION --min-instances=0` (scale to zero) | `gcloud run services describe <svc> --format json` → `status.url` → `GET <url>/healthz` |

`<svc>` is `streamforge-orleans` or `streamforge-dapr` — must match (and does match)
`metadata.name` in `deploy/orleans/service.yaml` / `deploy/dapr/service.yaml`.

## Env vars

- `ADMIN_PORT` — default `5599`.
- `MODE` — `local` (default) or `cloudrun`.
- `PROJECT_ID` — cloudrun mode only; default `gcloud config get-value project`.
- `REGION` — cloudrun mode only; default `europe-west1`.
- `GEMINI_API_KEY` / `ANTHROPIC_API_KEY` — passed through unchanged to the local compose stacks
  (they read `${VAR:-}` from whatever environment `docker compose` itself runs in — i.e. this
  admin process's env — so the AI control chat works inside the containers). Not required; unset
  means the in-container chat endpoint 503s with a friendly message, same as running the compose
  files directly.

## API

- `GET /api/status` → `{ mode, orleans: Entry, dapr: Entry }` where
  `Entry = { mode, running: bool|"unknown", health: "ok"|"down"|"starting", url, detail }`.
  Polled by the page every 3s.
- `POST /api/start?flavor=orleans|dapr` → runs the start command above, returns
  `{ ok, detail }` (detail = captured stdout/stderr tail).
- `POST /api/stop?flavor=orleans|dapr` → runs the stop command above, returns `{ ok, detail }`.
- `GET /api/logs?flavor=orleans|dapr` → last ~100 lines. Local: `docker compose logs --no-color
  --tail 100`. Cloudrun: `gcloud logging read` (best effort — empty/expected if nothing is
  deployed).

`flavor` is validated against the exact two-value allowlist (`orleans`, `dapr`) on every endpoint
that takes it — anything else is `400`. Nothing is ever shell-interpolated from user input;
`Bun.spawn` always receives a fixed argv array.

## What the UI shows

Two flavor cards (Orleans, Dapr), each with a status pill (ok/starting/down/unknown), the
console/service URL, Start/Stop buttons (disabled while an operation is in flight for that
flavor; Stop asks `confirm()` first), and a collapsible logs panel. The header shows the active
mode. Neutral text wordmark only ("STREAMFORGE — CLUSTER ADMIN") — no client branding or logo
graphic, per repo brand rules.

## Notes / caveats

- `cloudrun` mode degrades gracefully when a service isn't deployed yet or `gcloud` isn't
  authenticated: `/api/status` reports `running: "unknown"`, `health: "down"`, and the raw
  `gcloud` error (truncated) in `detail` — it does not crash.
- Local `docker compose up -d --build` for the Dapr flavor can take ~60-120s cold (two custom
  image builds + daprd/placement dissemination); the UI keeps polling `/api/status` throughout.
- This app performs no destructive/mutating Cloud Run calls on its own initiative — `start`/`stop`
  in cloudrun mode only ever run `gcloud run services update --min-instances=...`; nothing here
  runs `deploy.sh` or provisions infrastructure.
