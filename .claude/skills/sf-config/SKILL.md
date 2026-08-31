---
name: sf-config
description: StreamsForge catalog config export/import workflow — curl recipes for export/import (JSON/YAML, includeSecrets, validate/merge/replace, multipart include sets), the compose/merge rule cheat-sheet, the report shape, and the gotchas. Use when asked to export, import, back up, promote, or move a StreamsForge catalog (sources/pipelines/tables) between instances or flavors.
---

# sf-config — catalog export/import quick reference

Full user docs: `orleans/docs/index.html` (§ Configuration import/export). Endpoints (shared,
byte-identical on both flavors): `shared/StreamsForge.Api/Endpoints/ConfigEndpoints.cs` +
`ConfigImportService.cs`. Engine: `shared/StreamsForge.AppCore/Config/**` (`ConfigSerializer`,
`ConfigComposer`, `ImportPlanner`, `SecretsMasker`). What travels: **source/pipeline/table
definitions only** — no ids, no users/credentials, no runtime state (connector dedup ledgers,
counters, row data never leave the process this way).

## Export

```bash
# Orleans :5199 or Dapr :5399 — same endpoint shape on both
curl -s "localhost:5199/api/config/export?format=json" \
  -H "Authorization: Bearer $VIEWER_TOKEN" -o export.json

curl -s "localhost:5199/api/config/export?format=yaml" \
  -H "Authorization: Bearer $VIEWER_TOKEN" -o export.yaml

# Admin-only: include real secret values (URL header values, gRPC password/token) instead of "***"
curl -s "localhost:5199/api/config/export?format=json&includeSecrets=true" \
  -H "Authorization: Bearer $ADMIN_TOKEN" -o export-with-secrets.json
```

Canonical JSON is deterministic (2-space indent, camelCase, entities sorted by name, fixed property
order, empty/null omitted) — `export → reset → import → re-export` is byte-identical for the same
catalog state (round-trip verified live, wave 3C and again in W6). YAML has no such byte-equality
contract, only JSON does.

## Import

```bash
# ALWAYS validate first — report-only, writes nothing
curl -s -X POST "localhost:5199/api/config/import?mode=validate" \
  -H "Authorization: Bearer $EDITOR_TOKEN" -H 'Content-Type: application/json' \
  --data @export.json | jq .

# merge (default if ?mode= omitted) — upsert by name, leaves everything else alone
curl -s -X POST "localhost:5199/api/config/import?mode=merge" \
  -H "Authorization: Bearer $EDITOR_TOKEN" -H 'Content-Type: application/json' \
  --data @export.json | jq .

# replace — Admin only, DESTRUCTIVE: entities absent from the doc are deleted
# (running ones stopped first: pipelines -> tables reverse-topo -> sources)
curl -s -X POST "localhost:5199/api/config/import?mode=replace" \
  -H "Authorization: Bearer $ADMIN_TOKEN" -H 'Content-Type: application/json' \
  --data @export.json | jq .

# an ORDERED JSON ARRAY of documents composes as a sequence — later element wins per entity
curl -s -X POST "localhost:5199/api/config/import?mode=merge" \
  -H "Authorization: Bearer $EDITOR_TOKEN" -H 'Content-Type: application/json' \
  --data '[{"version":1,"sources":[...]}, {"version":1,"sources":[...]}]'

# multipart file SET — includes resolve by exact file name WITHIN the uploaded set only
# (the server never reads its own filesystem for an include — no path traversal by construction);
# -F ORDER doesn't matter for resolution, but the FIRST file becomes the root document
curl -s -X POST "localhost:5199/api/config/import?mode=merge" \
  -H "Authorization: Bearer $EDITOR_TOKEN" \
  -F "base.json=@base.json" -F "overlay.json=@overlay.json"
```

A single JSON/YAML document with a non-empty `include` list is **rejected** outside a multipart
upload — there's nothing on the server to resolve it against; pre-compose an ordered array yourself,
or use multipart.

## Compose/merge rules (exact — `ConfigComposer`)

1. A document's own `include` list resolves **depth-first, in order**, before the document itself —
   the includer always wins over everything it includes, recursively.
2. Across the resulting ordered sequence, **later documents win per entity**, matched by
   `(kind, name)`.
3. The merge is a **shallow field override**: a property present in the later entity replaces the
   earlier value **entirely** — arrays/objects replace whole, never element- or deep-merged.
4. An explicit JSON `null` for an optional field **clears** it — distinct from the field being
   absent, which leaves the earlier value untouched.
5. An include **cycle** or a **missing** include is fatal (a diagnostic names the path/chain) — no
   partial composition.

## Apply order (exact — `ImportPlanner`)

- **Apply**: sources → tables (topo-sorted by input-table dependencies) → pipelines.
- **`replace`-mode deletions run last**, reverse dependency order: pipelines → tables
  (reverse-topo) → sources — a running entity is always stopped before it's deleted.
- Every imported pipeline/table SQL is recompiled through the **real Engine compiler** against the
  composed post-import catalog; a compile failure marks that one entity `error` and is skipped —
  the rest of the import still applies (`validate` reports every entity the same way, applies
  nothing).

## Report shape

```json
{ "mode": "merge", "ok": true,
  "entries": [
    { "kind": "source", "name": "weather", "action": "created", "diagnostics": [] },
    { "kind": "table", "name": "latest_by_city", "action": "updated", "diagnostics": [] },
    { "kind": "pipeline", "name": "bad_sql", "action": "error", "diagnostics": ["3:1 unknown column 'x'"] }
  ]
}
```

`action`: `created | updated | deleted | skipped | error`. `ok` is `false` iff any entry is `error`.
Whole-import atomicity is **not** promised — entities apply one at a time through the same
serialized catalog registry; a failure partway through leaves earlier entities in that run already
applied. Read the report, don't assume all-or-nothing.

## Auth

Export = Viewer (`includeSecrets=true` needs Admin). Import = Editor. `replace` mode additionally
needs Admin.

## Gotchas

- **`"***"` secrets keep the stored value on write.** Every export/GET masks non-empty URL header
  values and gRPC `password`/`token` as `"***"`; re-importing a doc with `"***"` in those fields
  merges in the *currently stored* real value rather than overwriting it with the literal string —
  this is what makes a plain (non-`includeSecrets`) export safe to round-trip. If the stored source
  doesn't exist yet (a `"***"` value on a brand-new source name), there's nothing to merge from —
  don't hand-author a source doc with `"***"` in it, it stays the literal string.
- **A source import is a FULL upsert, not a patch.** Like `PUT /api/sources/{name}`, an imported
  source entity replaces the whole stored definition — a doc that omits a field the stored source
  had (e.g. drops a header, drops a mapping field) **nulls it out**, it does not preserve it. Export
  first, edit the export, re-import — don't hand-write a partial source doc expecting a merge at the
  field level (only pipeline/table `running` desired-state and entity-level compose get that
  treatment, not a source's own fields).
- **Byte-equality is a same-instance-canonical-JSON guarantee only.** `export → reset → import →
  re-export` is byte-identical because the same catalog produces the same canonical bytes — it is
  NOT a promise that two *different* catalogs, or JSON vs. YAML output, compare byte-equal. YAML has
  no byte-equality contract at all (only correctness + stable ordering).
- **Connector runtime state never travels.** Dedup ledgers, file/folder mtime ledgers, connector
  status/counters (`ConnectorRuntimeStatus`) are per-flavor runtime state (Orleans grain
  `[PersistentState]` / Dapr actor state in Redis) — importing a source's *definition* on a new
  instance starts it fresh, with an empty ledger/dedup set, even if the source existed with history
  elsewhere. This is deliberate (D-I: "definitions only") and matches at-least-once ingestion
  semantics — expect some re-emission on a freshly-imported connector source.
- **`running: true` on a `Failed` pipeline/table is correct, not a bug.** Export maps `Running` from
  the desired-state boolean (`Status != Stopped`), so a pipeline that's currently `Failed` (bad SQL,
  say) still exports `running: true` — it was asked to run and never told to stop. Re-importing it
  recompiles the SQL; if it now compiles, it starts.
- **Pick `validate` before `merge`/`replace`, always.** It runs the identical diff + SQL-compile
  pass with zero side effects — the fastest way to see exactly what an import would do before
  committing to it, especially before a `replace`.
