---
name: sf-env
description: StreamForge environment isolation — curl recipes for creating/listing/selecting/force-deleting a named environment (a partition inside one running server, not a second deployment), the X-StreamForge-Environment header and ?env= override, and the rules that bite. Use when asked to set up, select, isolate, or tear down a StreamForge environment (e.g. staging vs prod inside one instance), or to explain why an entity in one environment can't see another's.
---

# sf-env — environment isolation (create, select, force-delete)

Full user docs: `orleans/docs/index.html` (§ Environments). Contributor-facing key composition:
`orleans/ARCHITECTURE.md` / `dapr/ARCHITECTURE.md` (§ Environment isolation), `TRANSPORTS.md`'s
"What environment isolation does to a transport". Plan: `plans/021-environment-isolation.md`.

**Do not confuse this with plan 016's `@name` endpoints or the informal "promote a config document
between deployed instances" sense of the word.** This skill is about ONE running server holding several
disjoint catalogs. `@name` (see `/sf-federate`) is about the same catalog document dialing a different
host per deployment; the two are unrelated features that happen to share the English word "environment".

## The rules that bite

- **`default` is not creatable, not deletable, and no environment — including `default` — can ever be
  renamed.** There is no rename route at all. The name is qualified into every grain key, actor id and
  stream id that environment's entities ever produced; a rename would silently orphan all of it.
- **An unknown environment is a 404 on every route except a short exclusion list**, checked BEFORE the
  request reaches its handler: `/healthz`, `/api/healthz`, `/api/meta/instance`, every `/api/auth/*`
  route, and `/api/environments` itself. That last exclusion is deliberate recovery, not an oversight —
  without it, a client whose selected environment gets force-deleted 404s on every route including the
  one it would use to discover its selection is gone.
- **`?env=` overrides `X-StreamForge-Environment` when both are present** — for a browser navigation, an
  `<img>`/download link, or a `curl` that would rather not set a header.
- **Naming `default` (or nothing) costs nothing**: no environment-registry lookup, no per-request ambient
  write. Naming anything else validates against the registry first — a typo'd header is a 404, never an
  implicit new empty environment.
- **Force-delete erases catalog AND runtime state** — every source/pipeline/table in the environment is
  torn down through the same code path a manual one-at-a-time delete uses, then the environment record
  itself is removed. It leaves the environment's own now-empty registry state file / Redis entry behind
  (nothing in either flavor's storage layer can delete a grain/actor's own persisted file, only empty the
  object living in it) — re-creating the same name later inherits that dead weight.
- **A source or table name may not contain `.`** — refused at create/rename. `.` is the qualification
  separator (`EnvKeys.Separator`), chosen specifically because Orleans' `JsonFileGrainStorage` sanitizes
  every other non-alphanumeric character to `_`, and `_` is legal in a name — `.` is the one separator
  that can't collide with a name a user might already have.
- **A namespace, not a security boundary, and not a resource boundary.** Any authenticated Editor can
  point the header at any environment and edit it — nothing in the entitlement model (`/sf-access`) scopes
  a grant to one yet. One process, one heap: a runaway pipeline in `staging` starves `default` exactly as
  much as it does today.
- **Seeding only ever happens in `default`.** A newly created environment starts with an empty catalog and
  stays that way — restarting the host does not backfill the demo catalog into it, and force-deleting an
  environment's contents does not restore them on the next boot either.
- **The `metrics` SignalR group is cluster-wide, on purpose, and unqualified.** A console (or any other
  client) holding `catalog.read` at `*` sees every environment's live pipeline throughput on that one
  channel — a stated, written-down limitation, not a bug — see `StreamHub.SubscribeMetrics`'s own doc
  comment for the full argument.

## Setup for every recipe

```bash
B=http://localhost:5199   # :5399 for the Dapr flavor
T=$(curl -s -X POST $B/api/auth/login -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"admin123!"}' | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')
```

## Creating and listing

```bash
curl -s $B/api/environments -H "Authorization: Bearer $T" | jq .
# default always first, synthesised — present even on an instance that predates this feature

curl -s -X POST $B/api/environments -H "Authorization: Bearer $T" \
  -d '{"name":"staging","description":"pre-prod"}' | jq .
# -> 200, entityCount -1 (not yet counted)

curl -s -X POST $B/api/environments -H "Authorization: Bearer $T" -d '{"name":"staging"}' -w '\n%{http_code}\n'
# -> 409 "Environment 'staging' already exists."
```

## Selecting one

```bash
curl -s $B/api/tables -H "Authorization: Bearer $T" -H 'X-StreamForge-Environment: staging' | jq .
# [] — staging's own, disjoint table list

curl -s $B/api/tables -H "Authorization: Bearer $T" -H 'X-StreamForge-Environment: nope' -w '\n%{http_code}\n'
# -> 404 "environment 'nope' does not exist" — refused before any catalog call

curl -s -X POST "$B/api/environments?env=staging" -H "Authorization: Bearer $T" \
     -H 'X-StreamForge-Environment: nope' -w '\n%{http_code}\n'
# ?env= wins — the bogus header is never consulted, and /api/environments ignores the selector entirely
```

Creating a table of the same name in two environments produces two distinct ids and two distinct on-disk
state files (`table.table_<name>.json` for `default`, `table.table_staging.<name>.json` for `staging`) —
verified live: the same SQL text against different source shapes in each environment produced
`rows.csv` headers `symbol,_weight` (default) and `symbol,price,_weight` (staging), from two independently
running tables.

## Force-delete

```bash
curl -s -X DELETE "$B/api/environments/staging" -H "Authorization: Bearer $T" -w '\n%{http_code}\n'
# -> 409 "Environment 'staging' is not empty (... ) — pass force=true to delete it and everything in it."

curl -s -X DELETE "$B/api/environments/staging?force=true" -H "Authorization: Bearer $T" -w '\n%{http_code}\n'
# -> 204 — default's own entities are untouched

curl -s -X DELETE "$B/api/environments/default?force=true" -H "Authorization: Bearer $T" -w '\n%{http_code}\n'
# -> 409 "The default environment cannot be deleted." — force does not change this, ever
```

## Gotchas

- **A `loopback` or `duplex` sink names a catalog entity, not an external endpoint, so it's read in the
  environment it was authored in** — resolved at sink-client construction, never written back to the
  catalog (same rule `/sf-federate`'s `@name` follows). Before this was fixed, a `staging` table's
  `loopback` sink to `feed` silently published into `default`'s `feed` generator and reported success.
  `nats`/`http`/`db`/`file` sinks are untouched — they name something outside the process, and an
  environment has no opinion about those.
- **The console's environment picker stores the selection in the browser**, not a server-side session — a
  stale selection pointing at a since-deleted environment falls back to `default` automatically rather
  than dead-ending on repeated 404s (this is exactly why `/api/environments` stays reachable regardless of
  the header).
- **Two-stage delete in the console**: a plain delete first; only after the server reports the environment
  is non-empty (409) does the console offer the explicit "force-delete erases catalog AND runtime state"
  confirmation.
