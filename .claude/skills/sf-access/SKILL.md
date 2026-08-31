---
name: sf-access
description: StreamsForge entitlements, groups, approvals and audit — curl recipes for the /api/access document, the effective-permissions view, disabling a login, filing/voting on approvals, and reading the audit log, plus the rule cheat-sheet and the gotchas. Use when asked to grant, revoke, scope or audit access, disable a user, set up approvals, or explain why a caller got a 403.
---

# sf-access — entitlements, approvals, audit quick reference

Full user docs: `orleans/docs/index.html` (§ Roles, entitlements & approvals). Endpoints (shared,
identical on both flavours): `shared/StreamsForge.Api/Endpoints/{AccessEndpoints,ApprovalsEndpoints,
AuditEndpoints}.cs`. The decision itself: `shared/StreamsForge.AppCore/Access/PermissionEvaluator.cs`
(pure, unit-tested in both suites). Plan: `plans/015-rbac-entitlements-approvals.md`.

## The rules that bite

- **Permissions are resolved server-side per request, never read off the JWT.** The token still lives
  12h and still carries one role claim; the decision comes from the access document. A revoked grant
  lands within `Auth:PolicyCacheSeconds` (default 10) cluster-wide, and immediately on the replica that
  wrote it. Nobody has to log out.
- **`Auth:Mode` defaults to `entitlements`.** `legacy` is the one-flag rollback of the whole feature.
  Only the exact string `legacy` (case-insensitive) turns it off — a typo leaves enforcement **on**,
  deliberately.
- **Deny-overrides.** Any matching `Deny` anywhere in the flattened grant list (user + groups + roles)
  wins over every `Allow`. There is no specificity ordering: a `Deny` at `*` beats an `Allow` on
  `prod-orders` and vice versa — deny simply wins.
- **`requiresApproval: true` is not an override.** It applies only when *no* unconditional `Allow`
  matches. A user carrying the Editor role (`pipeline.delete` at `*`) is unaffected by a narrower
  `requiresApproval` grant — verified live. To make an action need a second pair of eyes you must take
  the plain Allow away (drop the role, or `Deny` it), not add a `requiresApproval` grant next to it.
- **Scope is the entity NAME, never its id.** Ids are `Guid("n")`, so a `prod-*` grant matches no id at
  all. REST, gRPC and the chat were reconciled onto the name in wave 3; sources were never affected
  (their route segment *is* the name).
- **Scope grammar**: `*` | exact name | prefix glob `prod-*` | `tag:finance` (matches the entity's
  `Tags`, glob allowed after `tag:`). `action` takes the same globs (`pipeline.*`, `*`).
- **`access.write` — not `user.write` — satisfies the coarse `Admin` policy.** Whatever satisfies that
  policy is the key to `/api/access` itself, so a narrow "user administrator" role must not be it. A
  role holding `user.write` alone is refused `/api/users` today; that is the deliberate direction.
- **Lists filter, they do not refuse.** A caller entitled to nothing gets `200 []`, not a 403. A
  single-entity GET is guarded *before* the 404, so 404-vs-403 cannot be used to enumerate names.
- **Approving does not perform the action.** An approved request is a record; nothing replays it.
  `Executed`/`Failed` are only reachable through `IApprovalFacade.RecordOutcomeAsync`, which no REST
  route calls. The requester still needs the entitlement, or an operator does the thing by hand.

## Setup for every recipe

```bash
B=http://localhost:5199                     # :5399 for the Dapr flavour
login(){ curl -s -X POST $B/api/auth/login -H 'Content-Type: application/json' \
  -d "{\"username\":\"$1\",\"password\":\"$2\"}" | sed -n 's/.*"token":"\([^"]*\)".*/\1/p'; }
A=$(login admin 'admin123!'); E=$(login editor 'editor123!')
```

## Reading access

```bash
# The whole document — roles, groups, user entries, approval templates, version. access.read + Admin.
curl -s $B/api/access -H "Authorization: Bearer $A" | jq .

# What one user can actually do, flattened the way a decision flattens it, from the RESOLVER's snapshot
curl -s $B/api/access/effective/editor -H "Authorization: Bearer $A" | jq .

# What the CALLER holds, on the route the console already polls (no Admin needed)
curl -s $B/api/auth/me -H "Authorization: Bearer $E" | jq '{role, roles, groups, policyVersion, n: (.permissions|length)}'
```

## Writing access

Every write is a **whole-object PUT** keyed by the path segment; the body's `name`/`username` is
overwritten from the path, `builtIn` is derived, `updatedBy`/`updatedAtMs` are stamped server-side.

```bash
# A role. Deleting a built-in (Admin/Editor/Viewer) is refused with 409, edit its grants instead.
curl -s -X PUT $B/api/access/roles/prod-operator -H "Authorization: Bearer $A" \
  -H 'Content-Type: application/json' -d '{
  "description": "runs prod pipelines, changes nothing",
  "grants": [
    {"action":"pipeline.read","scope":"prod-*"},
    {"action":"pipeline.control","scope":"prod-*"},
    {"action":"pipeline.delete","scope":"*","effect":"Deny","note":"never"}
  ]}' | jq .
curl -s -X DELETE $B/api/access/roles/prod-operator -H "Authorization: Bearer $A"   # 204

# A group. Membership lives HERE, not on the user entry.
curl -s -X PUT $B/api/access/groups/prod-approvers -H "Authorization: Bearer $A" \
  -H 'Content-Type: application/json' \
  -d '{"description":"decides prod changes","members":["admin"],"roles":[],"grants":[]}' | jq .

# A user's own entry: roles + direct grants. Roles here (not the credential record's Role) are what
# the evaluator reads. Sending "roles": [] takes every role away.
curl -s -X PUT $B/api/access/users/editor -H "Authorization: Bearer $A" \
  -H 'Content-Type: application/json' -d '{
  "roles": ["Editor"],
  "grants": [{"action":"pipeline.*","scope":"prod-*","effect":"Deny","note":"no prod writes"}]}' | jq .

# Deleting the ENTRY is not deleting the user — the login keeps working and falls back to its token
# role claim. DELETE /api/users/{u} is the route that removes the account.
curl -s -X DELETE $B/api/access/users/editor -H "Authorization: Bearer $A"          # 204
```

## Disabling a login (the cheap 90% of revocation)

```bash
curl -s -X PUT $B/api/access/users/viewer/disabled -H "Authorization: Bearer $A" \
  -H 'Content-Type: application/json' -d '{"disabled":true}' | jq .
# within Auth:PolicyCacheSeconds: every route 403s for that token, /api/auth/me included
curl -s -X PUT $B/api/access/users/viewer/disabled -H "Authorization: Bearer $A" \
  -H 'Content-Type: application/json' -d '{"disabled":false}' | jq .
```

Own route on purpose: flipping one boolean must never require re-sending an entry whose grants the
caller may not know about. When no entry exists yet it seeds `roles` from the credential record, so
disable→enable returns a working login rather than a silently demoted one (the wave-6 bug).

**It is not JTI revocation.** The token stays valid and stays signed; what changes is that the
resolver hands back an empty grant set. `Auth:StrictViewer` (default true) is what makes the coarse
`Viewer` policy refuse a disabled principal too — it fails *open* when the store is unreachable, when
the document has zero roles (pre-upgrade catalog), or when the user carries direct grants or group
membership but no role.

## Approvals

Ship **disabled** (`Approvals:Enabled=false`); every route answers 503 naming the key until you turn
them on. Filing needs a matching enabled template or you get a 409 — **no template means no approval
is required**, which is the direction that keeps a disabled deployment byte-identical and also means a
misspelled `actionPattern` is a control that silently does not exist.

```bash
# 1. a template (approver groups must exist as groups; scopePattern uses the same scope grammar)
curl -s -X PUT $B/api/access/approval-templates/prod-deletes -H "Authorization: Bearer $A" \
  -H 'Content-Type: application/json' -d '{
  "actionPattern":"pipeline.delete","scopePattern":"prod-*","requiredApprovals":1,
  "approverGroups":["prod-approvers"],"expiresAfterSeconds":3600,
  "escalateAfterSeconds":900,"escalationGroups":[],"enabled":true}' | jq .

# 2. file one (Viewer floor + approval.request at the scope asked for). RequestedBy is the token's.
ID=$(curl -s -X POST $B/api/approvals -H "Authorization: Bearer $E" \
  -H 'Content-Type: application/json' \
  -d '{"action":"pipeline.delete","scope":"prod-orders","reason":"decommissioning"}' | jq -r .id)

# 3. the inbox, and one request
curl -s "$B/api/approvals?state=Pending&limit=100" -H "Authorization: Bearer $A" | jq '.[].id'
curl -s $B/api/approvals/$ID -H "Authorization: Bearer $A" | jq .

# 4. vote (approval.decide at the REQUEST's scope, plus membership of an approver group)
curl -s -X POST $B/api/approvals/$ID/approve -H "Authorization: Bearer $A" \
  -H 'Content-Type: application/json' -d '{"comment":"ok"}' | jq '{state, votes}'
curl -s -X POST $B/api/approvals/$ID/reject  -H "Authorization: Bearer $A" -d '{}'   # one rejection is decisive

# 5. withdraw your own (no entitlement needed; idempotent once Cancelled)
curl -s -X POST $B/api/approvals/$ID/cancel -H "Authorization: Bearer $E" | jq .state
```

- **You cannot vote on your own request** — refused case-insensitively by the state machine, with
  `'admin' filed request … and cannot vote on it` (403), before the eligibility check so the message
  is the true one.
- **Two independent controls on a vote**, both required: the route's `approval.decide` entitlement at
  the request's scope, and the store's "in an approver group and not the requester" rule. The store's
  half survives `Auth:Mode=legacy`.
- **Who sees a request**: admin (`access.read` at `*`) OR its requester OR (member of one of its
  `approverGroups` AND `approval.decide` at its scope). Both halves of the last one.
- **`origin: "chat"`** marks a request the AI proposed on a human's behalf (`requestedBy` is the
  human). `Chat:MayExecutePrivileged=true` reverses this — the model then executes a
  `RequiresApproval` action itself, with a warning log and an audit row saying so.
- **Template values are snapshotted at filing time**; editing a template cannot lower the bar on a
  request already collecting votes.

## Audit log

```bash
curl -s $B/api/audit/days -H "Authorization: Bearer $A"                 # ["20260819"] — cheap, wakes no shard
D=$(date -u +%Y%m%d)
curl -s "$B/api/audit/$D?limit=200&offset=0" -H "Authorization: Bearer $A" | jq \
  '{truncated, total, changesIncluded, changesWithheld}'
curl -s "$B/api/audit/$D?actor=editor&action=pipeline.&limit=50" -H "Authorization: Bearer $A" | jq '.entries[]'
# before/after payloads: BOTH ?includeChanges=true AND access.read at *
curl -s "$B/api/audit/$D?action=user.write&includeChanges=true" -H "Authorization: Bearer $A" \
  | jq '.entries[] | {action, scope, outcome, beforeJson, afterJson}'
```

- Filters are exactly: exact `actor`, action **prefix**, `limit` (≤2000), `offset`. `day` is
  `yyyyMMdd` UTC and is validated as a key shape — anything else is a 400.
- `outcome` ∈ `allowed` · `denied` · `requires-approval` (guard rows) · `executed` · `failed`
  (mutation rows). A single mutation normally writes two rows: the decision and the change.
- **`truncated` is the whole point of the cap.** Days are sharded, capped by `Audit:MaxEntriesPerDay`
  (20 000) drop-oldest, and the count is persisted so silence is never read as absence. The in-process
  queue drops the *newest* instead (the onset of a burst is the forensically useful end) and emits a
  real `audit.dropped` row.
- **`beforeJson`/`afterJson` are always masked.** A credential rotation reads
  `"passwordHash": "***" → "***"` — **the key's presence is the signal, the value never is.** The diff
  is computed on the unmasked pair precisely so a rotation is not rendered as "nothing changed".
- **Reads are not audited** (nor allowed `source.ingest`, nor passing a coarse legacy policy), so the
  row "alice changed the prod pipeline" is not buried. **Every refusal is recorded, always.**

## Config keys

| Key | Default | What |
|---|---|---|
| `Auth:Mode` | `entitlements` | `legacy` (exact, case-insensitive) rolls the whole feature back |
| `Auth:PolicyCacheSeconds` | `10` | resolver TTL — how long a revocation can lag on another replica |
| `Auth:StrictViewer` | `true` | the coarse Viewer policy refuses disabled / role-less principals |
| `Approvals:Enabled` | `false` | off ⇒ every `/api/approvals` route 503s |
| `Approvals:SweepSeconds` | `30` | expiry/escalation sweeper interval (a `BackgroundService`, no timers/reminders) |
| `Audit:Enabled` · `Audit:QueueCapacity` | `true` · `2048` | in-process write queue (drop-on-overflow) |
| `Audit:MaxEntriesPerDay` | `20000` | per-day shard cap, drop-oldest, counted in `truncated` |
| `Audit:RecordAllowedMutations` | `true` | off ⇒ only refusals are recorded |
| `Chat:MayExecutePrivileged` | `false` | `true` ⇒ chat executes `RequiresApproval` actions instead of filing them |

## Gotchas

- **`GET /api/access/effective/{u}` short-circuits on a disabled user** — it reports `roles: []`,
  `grants: []`, which is the *decision*, not the configuration. You cannot see what an account would
  hold without re-enabling it. It also answers for a NAME, so a user with **no entry** in the document
  reads empty even though their live token works fine (the evaluator falls back to the token's role
  claim only while no entry exists).
- **On a first start against an empty data dir the bootstrap can beat the user seeder**, mirroring 0
  user role lists ("seeded 3 built-in role(s) and mirrored 0 user role list(s)" in the log). Roles land
  and logins work by fallback, but `/api/access` shows `users: []` until the next host start or the
  next `PUT` through `/api/users` or `/api/access/users/{u}`. Reproduced live.
- **`RequiresApproval` currently *hides* a control in the console**, so such a grant is strictly worse
  for the user than no grant at all. On REST it is a plain 403 whose reason says "requires approval" —
  **no REST route files the approval for you**; only the chat does.
- **An automated caller sharing a human's login defeats the self-vote rule.** An agent filing as
  `admin` produces a request a human admin can approve — same principal, so the store cannot object.
  Give scripts, the MCP server (`SF_USER`) and integrations their own low-privilege login, and keep it
  out of every approver group. `admin/README.md` states this as a configuration requirement.
- **The legacy OR is still live.** `Editor` is satisfied by `catalog.write` **OR** the legacy role
  claim, so a user with an explicit `Deny` on `catalog.write` still passes the coarse Editor policy
  while their token carries the role. Per-action guards (the ones this skill writes) are unaffected.
  The coarse policies ask at scope `*`, so a `prod-*`-scoped grant does not satisfy them.
- **`tag:` scopes do not reach every call site.** Ingest (`source.ingest`, REST and gRPC) passes no
  resource tags by design — a catalog read per message on the hottest route — so a `tag:`-scoped
  ingest grant admits nothing. The console's `can()` passes no tags either, so it errs toward hiding a
  control the server would allow.
- **SignalR is checked at subscribe time only.** A revoked grant keeps delivering until the connection
  drops. Unsubscribe is deliberately ungated.
- **Every guard-level audit row says `origin: "rest"`**, even for a gRPC or SignalR decision. Only the
  chat's rows carry their own origin (and `onBehalfOf`).
- **`GET /api/approvals?state=Bogus` returns 400 with an empty body**, unlike every other refusal on
  those routes. And the inbox applies `limit` *before* the visibility filter, so it is "your requests
  among the most recent N", not "your N most recent" — do not paginate it.
- **The audit and `/api/access` read routes sit behind the coarse `Admin` floor**, so a bespoke
  read-only auditor role holding only `audit.read` / `access.read` cannot reach them yet.
- **Tokens travel between flavours, grants do not.** The dev signing key is identical on Orleans and
  Dapr, so an Orleans-minted token validates on Dapr — but each flavour keeps its own access document.
- **Config import is all-or-nothing against entitlements**: a caller not entitled to every entity in
  the document is refused the whole import (`validate` included), never a partial apply.
