# StreamForge — Admin

Three admin surfaces, one folder, **zero npm dependencies** between them (the rule this folder has
kept since plan 007):

| Surface | Entry point | What it administers |
|---|---|---|
| Cluster app (plan 007) | `bun main.ts` → :5599 | The containerized stacks / Cloud Run services — start, health, stop, logs |
| `sf` CLI (plan 013) | `bun sf.ts <command>` | A **running instance**: catalog, lifecycle, SQL, rows, config, and (plan 015) access, approvals, audit |
| MCP server (plan 013) | `bun mcp.ts` (stdio) | The same, as tools an agent can call — minus the ones an agent should not hold (see below) |

The CLI and the MCP server share one REST client (`sfclient.ts`), so they cannot drift about what a
command means. Both work against either flavor — `SF_URL` is the only difference between
administering Orleans on :5199 and Dapr on :5399.

---

## Cluster app

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

---

## `sf` — the admin CLI (plan 013)

```bash
bun admin/sf.ts health
SF_URL=http://localhost:5399 bun admin/sf.ts ls tables    # the Dapr flavor, same commands
```

| Command | Does |
|---|---|
| `sf health` | Instance health + the identity this token carries |
| `sf login [--user U]` | Stores a token in `~/.streamforge/token.json` (mode 600). Password from `--password`, `SF_PASSWORD`, or a no-echo prompt |
| `sf ls <sources\|pipelines\|tables>` | One line per entity (`--json` for the raw array) |
| `sf get <kind> <id>` | One entity's full definition |
| `sf start\|stop <pipelines\|tables> <id>` | Lifecycle |
| `sf create <kind> -f def.json` | Create from the JSON the REST API already takes |
| `sf delete <kind> <id> [--yes]` | Delete — asks first unless `--yes` |
| `sf rows <table-id> [--csv] [--limit N]` | A table's rows; `--csv` is plan 012's `rows.csv` |
| `sf results <pipeline-id> [--csv] [--limit N]` | A pipeline's recent results |
| `sf validate <pipelines\|tables> "<sql>"` | Compiles without creating anything |
| `sf config export [--yaml] [--secrets] [-o file]` | Catalog export (`--secrets` needs the Admin role) |
| `sf config import <file> [--mode validate\|merge\|replace]` | Catalog import |
| `sf api <METHOD> <path> [body.json]` | Escape hatch for anything not above — users, shards, ingest keys |
| `sf mcp` | Runs the MCP server on stdio, same as `bun mcp.ts` |

### Entitlements, approvals and audit (plan 015)

| Command | Does |
|---|---|
| `sf access get` | The whole policy document — roles, groups, user entries, approval templates, version |
| `sf access effective <username>` | What that user can actually do, flattened as the decision flattens it |
| `sf access role\|group\|user\|template set <name> -f body.json` | Upsert one; the body is the same shape `GET /api/access` returns |
| `sf access role\|group\|user\|template rm <name> [--yes]` | Delete one — asks first unless `--yes` |
| `sf access disable\|enable <username>` | The one-field revocation route (`PUT …/users/{u}/disabled`) |
| `sf approvals ls [--state Pending] [--limit N]` | The inbox this token can see |
| `sf approvals get <id>` | One request, votes and approver groups included |
| `sf approvals file --action A [--scope S] [--reason R] [--payload body.json]` | Ask for a privileged action |
| `sf approvals approve\|reject\|cancel <id> [--comment C]` | Vote, or withdraw your own request |
| `sf audit days` | Which days hold entries (reads an index; wakes no day shard) |
| `sf audit day <yyyyMMdd> [--actor A] [--action prefix] [--limit N] [--offset N] [--changes]` | One day's page |

Five things that will otherwise cost you an afternoon:

- **Scope is the entity NAME, never its id.** Everywhere — grants, approval requests, templates.
- **`--state` is validated here, not by the server.** `GET /api/approvals?state=Bogus` is a 400 with an
  *empty body*, so a client that forwarded it would print nothing at all.
- **`sf access effective` on a disabled user is empty across the board** — the server short-circuits, so
  "disabled" and "configured with nothing" are the same picture apart from the flag. The printer says so
  in as many words; `sf access get` is where you see what the entry actually holds.
- **`truncated` on an audit page is what the day shard DROPPED**, persisted so silence is never read as
  absence. The printer shouts about it whenever it is non-zero.
- **`--changes` needs `access.read` as well as the flag.** The response's `changesIncluded` /
  `changesWithheld` say what you actually got; the printer reports the withheld count either way.

`sf approvals` is the only family here that is not Admin-gated: filing a request is not a privilege, and
the listing filters server-side to the requester, the entitled approver and the administrator. Every one
of these routes answers **503 with a sentence** while `Approvals:Enabled=false` (the shipped default).

**Auth resolution order**: `--token`, `SF_TOKEN`, `~/.streamforge/token.json` (only when its stored
URL matches the one being addressed — a token from another host is not silently sent), then a login
with `SF_USER`/`SF_PASSWORD`. Only the JWT is ever written to disk, and only by an explicit
`sf login`; no credential is. There are no default credentials in the code.

## MCP server (plan 013)

Stdio transport, written to the MCP specification (protocol `2025-06-18`, negotiating down to
`2025-03-26` / `2024-11-05`). No SDK dependency: the server half of a tools-only server is
`initialize`, `notifications/initialized`, `ping`, `tools/list` and `tools/call`, and the conformance
that matters is pinned by `mcp.test.ts` rather than assumed.

```jsonc
// Claude Code / Claude Desktop MCP config
{
  "mcpServers": {
    "streamforge": {
      "command": "bun",
      "args": ["/abs/path/to/crates-foundation/admin/mcp.ts"],
      "env": { "SF_URL": "http://localhost:5199", "SF_USER": "admin", "SF_PASSWORD": "…" }
    }
  }
}
```

**Tools** — `health`, `list_entities`, `get_entity`, `get_metrics`, `validate_sql`, `get_rows`,
`get_results`, `create_entity`, `start_entity`, `stop_entity`, `delete_entity`, `export_config`,
`import_config`, and plan 015's `get_access_policy`, `get_effective_permissions`, `list_approvals`,
`get_approval`, `request_approval`, `get_audit_days`, `get_audit_day`. Read-only tools carry
`readOnlyHint`; `delete_entity` and `import_config` carry `destructiveHint`.

Four deliberate omissions, all about what an agent should be handed:

- **No raw HTTP tool.** The CLI's `sf api` escape hatch has no MCP twin — an unlabelled
  arbitrary-request tool defeats every per-tool annotation above.
- **No secret export.** `export_config` never sets `includeSecrets`; the CLI's `--secrets` remains
  for a human who asked for it. (The server also gates it on the Admin role regardless.)
- **`request_approval`, and nothing that decides one.** An approval exists so a *second pair of eyes*
  sees a privileged action. An agent that can both propose and approve is not a second pair of eyes; it
  is the same pair twice, and shipping the approve tool would convert the mechanism into a formality
  that logs itself — with the log then reading as though a review had happened. Reject and cancel are
  out for the same reason through politer verbs. The server's own half of this rule (a requester cannot
  vote on their own request) does not substitute: an MCP server configured with an administrator's
  credentials and a human filing in the console are two identities, and the store cannot tell that one
  of them is a model. So the line is drawn at the tool list, the only place that knows.
- **No access writes, and no `includeChanges` on the audit tool.** An agent that can edit the policy
  governing it is ungoverned — one PUT to `/api/access/roles/Viewer` is the whole distance from
  "read-only tools" to "anything". Reading the policy stays in, and is genuinely useful: "why was I
  refused" is answerable from it. The audit before/after payloads stay out because they can carry stored
  credential fields, which is exactly why the server gates them twice.

The CLI carries all of it — `sf approvals approve`, `sf access role set`. That is a human at a terminal
with their own token, which is what the mechanism is asking for. Four assertions in `mcp.test.ts` keep
the boundary from eroding by accident.

One thing the tool list cannot fix, so configure around it: **give this server its own login, not an
administrator's.** With `SF_USER=admin` the request `request_approval` files is filed *as admin* — and if
that same account is in the template's approver group, the human who approves it in the console is
literally approving their own row. The tool list stops the agent voting; only the credentials stop the
proposal and the decision being the same identity. A dedicated account holding `approval.request` (and
whatever reads the agent genuinely needs) costs one `sf access user set`.

A tool that fails *executing* returns `isError: true` with the server's own message, so a model can
see a 404 or an unreachable host and adjust. Only protocol faults — unknown tool, bad params — are
JSON-RPC errors. **stdout carries nothing but JSON-RPC**; diagnostics go to stderr, and a test
asserts it.

## Tests

```bash
bun test admin/
```

30 tests: protocol conformance driven against the server as a real subprocess over pipes (handshake,
version negotiation, notifications never answered, parse errors survivable, batches refused, stdout
purity), the plan-015 tool boundary above, plus the shared client's auth and error handling against a
stub instance.
