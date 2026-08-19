# Plan 015 — RBAC → entitlements, groups, approvals, escalation, audit

**Status: PLANNED.**

## Why

Authorization today is three role strings in a total order (`Viewer < Editor < Admin`), one
`ClaimTypes.Role` claim in a 12h JWT, three ASP.NET policies, and a SPA `ROLE_ORDER` array. There is no
per-resource entitlement, no group, no audit log, no second pair of eyes on a privileged action, and no way
to say "Editor everywhere except `prod-*`".

## Decisions, and what they cost

**Permissions resolve server-side per request; they are NOT baked into the token.** At a 12h lifetime, a
revoked entitlement or a completed approval would take up to twelve hours to take effect — which makes an
approval workflow theatre. `IPermissionResolver` holds a version-stamped snapshot, polls a cheap
`GetVersionAsync()` on a TTL (`Auth:PolicyCacheSeconds`, default 10) and refetches only on a version change;
local mutations invalidate eagerly. Revocation lands in ~10s cluster-wide instead of 12h, at the cost of one
tiny grain call per 10s per replica. A full per-request store lookup is rejected: on Dapr that is a sidecar
round trip on **every read**.

Disabled users fall out of the same machinery — the resolver returns an empty grant set for
`Disabled == true`, so disabling a user kills their live token within the TTL **without a revocation list**.
That is the cheap 90% of token revocation; a JTI denylist is deferred.

**The decision is tri-state, not boolean:** `Allowed` / `RequiresApproval` / `Denied`. `RequiresApproval` on
the grant is load-bearing all the way to the SPA button label ("Request approval…"), and retrofitting it
later means touching every call site twice.

**The permission grammar:** `Action` is a flat dotted string (`pipeline.update`, `source.ingest`,
`config.replace`, `approval.bypass`) with `*` wildcards — a single string is what a policy name, a claim, an
audit row and a client `can()` all want. `Scope` is `*` | exact id/name | prefix `prod-*` | `tag:finance` —
all three entity types already carry `Tags`, so tag-scoped entitlements come free. `Effect = Allow|Deny` with
deny-overrides costs one field and one line and answers the first thing a real org asks for.

**Groups carry roles and grants; membership lives on the group.** "Who is in this group" is the common query,
and the user list is already rewritten whole on every mutation — a second whole-list-rewrite path would double
the write-conflict surface on the hottest singleton in the system.

**Storage is a NEW singleton, not the user store.** Credentials are rewritten on password change; policy is
read on every request and changes rarely. The split lets the resolver cache policy aggressively without ever
caching password hashes, and leaves `UserStoreGrain`/`UserStoreActor` behaviour untouched. Orleans:
`AccessPolicyGrain` (key `"access"`). Dapr: `AccessPolicyActor` delegating to a **pure** `AccessPolicyStore`
— the repo's own established Dapr testability pattern (`Catalog/CatalogStore.cs`), and what makes the Dapr
side unit-testable without a sidecar.

**The AI chat must stop inheriting the caller's whole Editor surface.** `POST /api/chat` is gated once and
its mutating tools re-check nothing, so without this the chat is the way around every entitlement this plan
adds. Each tool checks the same permission its REST equivalent checks, and `Chat:MayExecutePrivileged`
(default `false`) makes a `RequiresApproval` decision **file the approval request and return its id** instead
of executing. The model proposes; a human approves.

**Escalation is driven by a shared hosted sweeper, not by grain timers or Dapr reminders.** The Dapr compose
stack runs with no scheduler ("timers only"), so reminders are off the table; both hosts already run
`BackgroundService` supervisors, so one shape works identically on both flavours.

**Approvals ship disabled** (`Approvals:Enabled=false`) with inert seeded templates, so existing deployments
are byte-identical and both suites stay green without touching a pre-existing test.

**Audit is day-sharded, not a singleton** — `AuditLogGrain`/`AuditLogActor` keyed `audit:{yyyyMMdd}`, so a day
activates only when written to or read and is evicted when idle, the same mechanism plan 011-D1 established
for `TableShardGrain`. Within a day, `Audit:MaxEntriesPerDay` (default 20 000) drop-oldest with a persisted
`Truncated` counter, so silence is never mistaken for absence. The write path is a bounded in-process
`Channel` with drop-on-overflow: **audit must never make a request fail or slow.**

**One intentional behaviour change:** the `Viewer` policy stops admitting a principal whose user is disabled
or whose role no longer exists (`Auth:StrictViewer`, default `true`). Required for disablement to mean
anything before token expiry.

**Backward compatibility is structural.** The three policy names stay registered — `Editor` is satisfied by
`perm:catalog.write` **OR** the legacy `RequireRole`, `Admin` likewise — so all 59 `RequireAuthorization`
sites and 30 gRPC attributes keep compiling unchanged and Wave 3 migrates them because it should, not because
it must. `Auth:Mode = legacy|entitlements` is a one-flag rollback of the entire feature.

**Testing takes no HTTP harness.** The decision lives in a pure `PermissionEvaluator` in AppCore, which both
test projects already reference — so its tests run in **both** suites and prove cross-flavour parity for free,
exactly as plan 005 did with `PasswordHasher`. The single highest-leverage test is an **endpoint-metadata**
test: build a `WebApplication` in-process, call `MapStreamForgeApi`, read `EndpointDataSource`, never `Run()`,
never bind a port, never start a silo. That turns all 59 authorize sites into a table-driven assertion — today
they are pinned by nothing. `WebApplicationFactory` is declined: it needs both `Program.cs` files startable
without their runtimes, which is a refactor of the two most dangerous files in the repo.

## Waves

| Wave | What | Model |
|---|---|---|
| **0** | Contracts + spikes, orchestrator alone: `UserRecord` OIDC seams, new `AccessModels.cs`, facades, `StreamConstants` keys, `CatalogRecordMerge` 4-arg overload, `types.ts`; spike the `EndpointDataSource` test and the sweeper on the scheduler-less Dapr stack — **DONE**, see below | Opus 5 high |
| **1** | 4 parallel: pure evaluator (**Opus**) ∥ Orleans store ∥ Dapr store ∥ seeds + `LegacyRoleMigration`. Gate includes booting both flavours against a **pre-upgrade** data dir | mixed |
| **2** | 3 parallel: ASP.NET binding + resolver + guard (**Opus**) ∥ coverage audit + `tools/authz-matrix.sh` ∥ access/users REST | mixed |
| **3** | 3 parallel: REST routes ∥ gRPC + SignalR ∥ chat + config import (**Opus** — where an LLM's action is attributed to a human) | mixed |
| **4** | 4 parallel: approval state machine (**Opus**) ∥ Orleans stores ∥ Dapr stores ∥ audit sink + sweeper | mixed |
| **5** | 2 parallel: approvals + audit REST ∥ before/after detail on mutation sites | Sonnet 5 high |
| **6** | 3 parallel: permission client core + `RoleGate` shim (**Opus**) ∥ access/approvals UI ∥ audit UI | mixed |
| **7** | 2 parallel: docs + `sf-access` skill ∥ `admin/` `access`/`approvals`/`audit` commands. The MCP server gets `request_approval` and **not** `approve` | Sonnet 5 high |

### Wave 0 — what actually landed, and where it differs from the sketch above

**`UserRecord` got two fields, not eight.** The sketch said `[Id(6..13)]`; the shape that survived contact
with its own decision ("the split lets the resolver cache policy aggressively without ever caching password
hashes") puts *everything authorization reads* — `Disabled`, effective roles, direct grants — in the access
document as `UserAccessEntry`, not on the credential record. A resolver that had to read `UserRecord` to
learn a user is disabled would be caching exactly the thing the split exists to avoid. So `UserRecord`
gained only the two OIDC seams, `[Id(6)] ExternalSubject` and `[Id(7)] IdentityProvider`.

That has a consequence the sketch left implicit and Wave 1 now owns: since the effective role list lives in
`UserAccessEntry.Roles`, **the user store must mirror `UserRecord.Role` there on every create/update**, and
`LegacyRoleMigration` does it once for an existing data dir. Without the mirror, a role *change* would keep
taking effect only at the next login (today's behaviour) rather than within the resolver's TTL — which is
half of what "revocation lands in ~10s" is supposed to mean. The evaluator falls back to the token's role
claim only when no entry exists, i.e. against a pre-upgrade catalog.

**`UpdatedBy` landed on all three definitions** (`SourceDefinition` `[Id(13)]`, `PipelineDefinition`
`[Id(13)]`, `TableDefinition` `[Id(26)]`) alongside the 4-arg `CatalogRecordMerge` overload. The 3-arg
overload survives, delegating with `existing.UpdatedBy`, so migrations and tests are not forced to invent a
principal.

**The `EndpointDataSource` spike works, and it found something on its first run.** Two facts the plan did
not know: the routes are not in the composite `EndpointDataSource` until the routing middleware folds them
in at `Run()` time — and `Run()` is exactly what this test refuses to call — so the test reads
`((IEndpointRouteBuilder)app).DataSources` directly; and minimal-API parameter binding decides
"service or request body?" at *map* time by asking the container, so every handler dependency has to be
registered (with a throwing factory: none is ever resolved, and an accidental resolve should be loud).

What it found: **`POST /api/auth/login` carried no authorization metadata at all.** Behaviourally that was
already correct — no fallback policy is registered, so unmarked means anonymous — but "nobody marked it" and
"deliberately open" were indistinguishable from the outside, on the one route whose whole job is to hand out
tokens. It is now `.AllowAnonymous()`. That is the entire argument for this test, produced by the test
before a single enforcement site was migrated.

**The sweeper spike needed no code.** Both hosts already register hosted services
(`orleans/src/StreamForge.Host/Program.cs` runs five, `dapr/src/StreamForge.Dapr.Host/Program.cs` runs
`CatalogInitializationService`), so a `BackgroundService` sweeper is the one shape that works identically on
a stack with no scheduler. Confirmed by reading the wiring, not by writing a throwaway service.

### Wave 2 — the decisions the briefs left open, and one gap they exposed

**The Admin policy is satisfied by `access.write`, not `user.write`.** `/api/users` is the only
Admin-gated surface today, which makes `user.write` the obvious pick and the wrong one: whatever
permission satisfies the Admin *policy* becomes the key to every Admin-gated route, including the
`/api/access` routes. Under `user.write`, a narrowly intended "user administrator" role would silently
gain the power to rewrite the entitlement document; under `access.write` that role is merely refused
`/api/users` until wave 3 migrates the group to per-action guards. Over-granting and under-granting are
not symmetric mistakes. `access.write` is also the honest reading of what Admin already meant: the
entitlement from which every other one can be self-granted.

**`Auth:Mode` defaults to `entitlements`.** The plan calls the flag a rollback, not an opt-in — `legacy`
is what you set when something is wrong, which only makes sense if the feature is on. `Auth:StrictViewer`
would also be dead configuration under a `legacy` default. Anything but the exact string `legacy`
(case-insensitive) means entitlements, so a typo leaves enforcement **on**.

**`Auth:StrictViewer` fails open in three cases**, deliberately: no snapshot (the store is unreachable),
a document with zero roles (a pre-upgrade catalog whose migration has not run), and a user carrying
direct grants or group membership but no role. It denies only a `Disabled` user or one whose every role
name is absent from the document. Nothing pre-existing broke, because no test exercises the ASP.NET
policies at all — `AuthorizationCoverageTests` reads metadata and never authorizes.

**What the OR costs, stated plainly.** Because `Editor` is satisfied by `catalog.write` **OR** the legacy
role claim, a user with an explicit `Deny` on `catalog.write` still passes the Editor policy while their
token carries the legacy `Editor` role. A test pins exactly that. It is the price of no flag day and it
disappears route by route as wave 3 migrates call sites to scoped guards; the same is true of the
coarse policies asking at scope `*`, which means an entitlement scoped to `prod-*` does not satisfy them.
Widening either would have defeated a Deny written at `*` — the one direction not worth going.

**The gap wave 2 exposed: nothing mirrors a runtime role change.** Wave 0's note assigned the user
store's mirror of `UserRecord.Role` into `UserAccessEntry.Roles` to wave 1, and it did not land there.
`AccessBootstrapService` mirrors existing users once per start, so a user created or role-changed at
runtime has no entry until the next restart and falls back to the token's role claim — safe, but it
means "revocation lands in ~10s" is only half true. Wave 2-C owns closing it at the REST layer.

### Wave 3 — entitlements actually enforced, and the divergence three agents produced

Every coarse `RequireAuthorization` stayed exactly where it was, on every route, method and hub, and each
handler additionally asks `AccessGuard` for its own action at its own scope. So the 99-row coverage table
and the live 51-cell matrix both stayed green, which is the point: this wave adds enforcement without
moving a single piece of route metadata.

**The scope has to be the entity's NAME, and three agents independently disagreed about it.** The REST
agent scoped pipelines and tables on the stored `Name`; the gRPC agent and the chat agent scoped them on
the `{id}` their surface addresses by. Both readings are defensible in isolation and together they are a
bug: `RegistryGrain.Create*Async` mints `Guid.NewGuid().ToString("n")`, so a `prod-*` scope written by an
operator matches **nothing at all** on an id — the same grant would work in the console and silently fail
over gRPC and through the chat. Reconciled to the name on all three surfaces (falling back to the raw
identifier when the definition cannot be loaded, which only ever narrows). Sources were never affected:
their route segment IS the name. If a GUID-scoped grant is ever wanted, OR in a second check on the id —
strictly additive.

**The guard runs before the 404.** Otherwise an authenticated but unentitled caller enumerates which
names exist by reading 404 against 403. This cost one extra catalog read on delete, start/stop, and a few
data routes: without the definition there is no name and no tags, so a scoped entitlement could not match
and those routes would be the holes in the fence.

**Lists filter; they do not refuse.** A caller entitled to three of ten pipelines gets three, and a
caller entitled to none gets `200 []`, not a 403 — a list is not an entity.

**Two places deliberately keep a weaker rule, both marked `ponytail:`.** The ingest paths (REST and gRPC)
pass no resource tags, because reading the source definition per push would put a catalog round trip on
the platform's hottest route; a `tag:`-scoped `source.ingest` grant therefore does not admit a push. And
on the ingest JWT branch an entitlement refusal **falls through** to the ingest-key branch rather than
short-circuiting, so every message the old code admitted is still admitted — the key path is consulted in
strictly more cases than before and never fewer.

**SignalR was the quiet hole:** the hub was gated once at `Viewer` and its per-subscription methods
checked nothing, so any authenticated user could subscribe to any stream. Each subscribe now checks the
entity's read action; unsubscribe is deliberately ungated, because a caller whose grant was just revoked
must still be able to detach. Refusal is a `HubException` — the only type SignalR relays verbatim without
`EnableDetailedErrors`, so the SPA gets the same sentence the REST 403 carries. Checked at subscribe time
only: a revoked grant keeps delivering until the connection drops, and the upgrade path (re-run each
connection's remembered subscriptions when the policy version moves) is written down rather than built.

**The chat stops being the way around everything.** All sixteen tools check the same action their REST
equivalent checks, at the same scope, from one table that is the source of truth — and a tool missing
from that table is denied, not defaulted, so a seventeenth tool added without a permission is a dead tool
with a legible reason. `Chat:MayExecutePrivileged=false` turns a `RequiresApproval` into a filed request;
until wave 4 wires the store, the model is handed the **correlation id the refusal was logged under,
labelled as a correlation id** — there is no approval yet, and telling a model otherwise sends a user
hunting for a request that does not exist. Attribution is built once and carried everywhere: actor is the
model, `OnBehalfOf` is the human, origin is `chat`, and they never collapse into one field.

**Config import refuses whole rather than applying in part.** A config document's parts reference each
other, so applying the entitled half applies a document nobody wrote; in `replace` mode a partial apply
would delete the entities the caller IS entitled to and leave the ones they are not, converging on
neither the old state nor the document. Refusing is recoverable, partial application is not. `validate`
gets the same check — a dry run that says yes where the real run 403s is worse than no dry run.

### Wave 4 — approvals and audit, and what four agents had to be stopped from each deciding twice

Wave 3 produced a three-way divergence because three agents each answered the same question for
themselves. So wave 4 was built the other way round: one pure `ApprovalStateMachine` decides **every**
transition, and both stores are storage. Neither flavour contains a rule about who may vote, when a
request expires, or what counts.

**The rules worth writing down**, all pinned by their own tests:

- **The requester's own vote never counts.** A second pair of eyes that can be the first pair is not a
  control. The self-vote check runs *before* the eligibility check, so an administrator who is also a
  legitimate approver is told "you filed this" rather than "you are not an approver" — the second reads
  as a misconfiguration and gets "fixed".
- The self-vote comparison is the one **case-insensitive** username comparison in the repo. Refusing
  "ALICE" a vote on "alice"'s request is an inconvenience somebody notices; letting a requester
  self-approve through capitalisation is a control silently not existing.
- **One rejection is decisive.** Requiring N rejections would let a requester shop for approvers.
- **A past-deadline request expires at vote time, not only when the sweeper runs.** The sweeper is a
  timer; "still Pending" and "not yet expired" are different statements, and trusting the former makes a
  late approval land or not depending on how recently a tick happened. This is why the state machine
  reports `Accepted` and `StateChanged` separately — a store must persist a request whose vote it just
  refused.
- **Eligibility cannot be forgotten**: a required positional enum whose `default` is `NotAnApprover`, so
  a zeroed field or a literal `default` fails closed. And it is resolved **inside** the store from the
  policy document, not taken from the caller — a transport asserting "this voter is an approver" would
  make the store trust its input for the one rule the feature exists to enforce.
- **Template values are snapshotted at filing time**, so editing a template cannot lower the bar under a
  request already collecting votes. Identity comes from the authenticated caller and not from the draft,
  or the self-vote rule is defeated by filing under someone else's name.
- The fail-open direction is inverted from the evaluator's and is called out in the code: **no matching
  template means no approval required**, which is what keeps `Approvals:Enabled=false` byte-identical —
  and it means a misspelled `ActionPattern` is a control that silently does not exist. Anything that must
  not fail open belongs behind a `Deny` grant, not a template.

**Audit drops the newest, not the oldest — and the store does the opposite, deliberately.** In the
in-process sink the competing rows are milliseconds apart during a burst, so the onset is the
forensically valuable end and the hole lands in the middle with recording resuming after the burst; in
the day shard they are a whole day apart, so recent wins. Both count what they dropped, and the sink
additionally emits a real `audit.dropped` row so the gap reaches the log itself and not only a metric.

**What is deliberately not audited**: allowed reads (the row "alice changed the prod pipeline" must not
be buried under "alice listed the pipelines"), allowed `source.ingest` (one check per message on the
platform's hottest path), and passing the coarse legacy `catalog.*` doors — passing a door is not doing a
thing, and the route's own scoped check is the row that says what happened. **Every refusal is recorded,
always.**

**Three divergences the wave produced anyway, and how they were closed.** The Orleans store first walked
`policy.Groups` by hand while Dapr resolved through `EffectivePermissionsBuilder` — not the same rule,
because the builder returns no groups for a **disabled** user; that agent found it itself and converged,
with a test that a hand-rolled walk passes everywhere else and fails only there. I closed the other two:
`Audit:MaxEntriesPerDay=0` kept 20 000 rows on one flavour and 1 on the other, and the scope grammar had
been copied into the state machine because `ScopeMatches` was private — an operator writing `tag:prod` in
a grant and in an approval template would have got two behaviours.

### Wave 5 — the audit row says what changed, and the credential hazard that made it non-trivial

**The hazard first, because it is the reason this wave is not a serialisation exercise.** A source
definition contains credentials. Serialising one into `BeforeJson`/`AfterJson` without the masking pass
would turn the audit log — append-only, readable by anyone with `audit.read`, holding 20 000 rows a day,
and thought of by nobody as a secret store — into a plaintext credential dump with a history feature.
That is strictly worse than the bug `[Secret]` was introduced to prevent, because `GET /api/config/export`
at least has an `includeSecrets` flag and an Admin check. So every value written goes through the same
descriptor-driven `SecretsMasker` pass the read paths use, and `CatalogChangeAudit.Record<T>` takes the
mask function as a **required** parameter — there is no entry point that accepts a definition and does
not mask it.

**Masking breaks the diff on the one edit that matters most, and that had to be handled separately.**
The mask collapses both the old and the new password to `***`, so a diff taken after masking reports a
credential rotation as "nothing changed" — the single most audit-relevant edit anyone makes to a source,
silently missing. The unmasked pair is compared to decide *which* properties moved; every byte written
comes from the masked serialisations.

**Size**: an update records only the changed top-level properties on both sides, a create and a delete
the whole masked document (there is no other side to diff against, and on a delete the row is the last
surviving copy). Over the cap it degrades to the changed field *names*, not a truncated blob — JSON cut
mid-object parses as nothing and answers nothing, while the names still answer "what changed".

**Approvals sit on the Viewer floor and audit on the Admin one**, and the asymmetry is deliberate: the
coarse policy is the only control that survives `Auth:Mode=legacy`, where the guard allows everything.
Approvals can afford Viewer because the *store's* eligibility and self-vote rules are mode-independent —
and because an approver is by design an ordinary user in a group, so an Admin floor would make the
feature unusable by the people it is for. Audit has no store-side control at all, so it fails closed.

**Listing an approval inbox is not a side channel.** A request is visible to the administrator
(`access.read` at `*`), to its requester, or to someone who is both in one of its approver groups **and**
allowed `approval.decide` at its scope. Both halves of the last are required: membership alone shows a
team's traffic to someone who cannot act on it, the entitlement alone turns `approval.decide` at `*` into
a feed of what every other team is doing. The filter runs through `PermissionEvaluator` directly and
never through `AccessGuard` — one guard call per candidate row would write a denied audit row for every
approval merely not in your inbox, on every poll, which is exactly the flood that would stop "a refusal
is rare by construction" from being true.

**`BeforeJson`/`AfterJson` are withheld from audit readers by default**, released only on an explicit
`?includeChanges=true` **and** `access.read` at `*` — the same opt-in shape `includeSecrets` already
established — and the withholding is never silent: the response says whether it carries them and counts
the rows that had something to carry. Redaction is a whitelist copy, so a field added to the frozen
`AuditEntry` later is dropped rather than leaked.

**Two more cross-flavour divergences, both closed.** `VoteAsync` returned the stored request on refusal
on Orleans and `null` on Dapr — reconciled to the Orleans reading, because **null has to mean one
thing**: conflating "no such request" with "you may not vote on this" forces every caller into a second
read and hands the user "not found" for a request sitting right in front of them. And `ListAsync` with a
non-positive limit returned everything on Orleans and a page of 100 on Dapr; reconciled to the page,
since nothing prunes terminal requests yet.

**One gap the wave found and closed on the way out.** The chat's mutating tools call `ICatalogFacade`
directly and never reach the REST handlers, so a change the *model* made was the one change with no
record of what it changed — on the surface where that question matters most. Wired through the explicit
seam `CatalogChangeAudit` already documented, so the row keeps the model as `Actor`, the human as
`OnBehalfOf` and `chat` as `Origin`, and goes through the same masking.

`hasRole` survives, implemented on top of `can()` — **zero of the 57 `RoleGate` references changes at
cut-over.** That is the whole no-flag-day answer. The SPA treats a missing `permissions[]` as an old server
and falls back to today's ordinal semantics, so a rolling deploy is safe.

### Wave 6 — the console, and the two server bugs only a console could find

Three agents inside one `web/` tree, so the router, the sidebar and the `can(action, scope?)` signature
were pre-committed as a seam (`8f47e27`). The seam's `can` body was today's ordinal role answer — not
scaffolding, but the **else-branch written first**: 6-A had to keep exactly that as the fallback for a
server sending no `permissions[]`, so nothing was thrown away.

**`hasRole` survived, and zero of the 57 `RoleGate` call sites changed.** `RoleGate` gained optional
`action`/`scope`; supplying both `min` and `action` requires BOTH to pass, because adding a condition to
a gate must never widen it.

**The client evaluator is a third implementation of the scope grammar, and that is the risk it carries.**
`permissions.ts` mirrors `PermissionEvaluator.cs` — same ordering, same ordinal comparisons, same
iterative glob. 20 `bun test` assertions pin it, including a parity block asserting the snapshot and the
role fallback answer identically for all 26 actions. It lives in `web/test/` because `tsconfig.json`
includes only `src`, so the file is neither typechecked nor bundled and needs no new dependency.

**Two server bugs, both found by writing the UI, neither visible to any suite:**

1. **`PUT /api/access/users/{u}/disabled` silently demoted anyone with no entry yet.** The route created
   an entry carrying only `Disabled`, with empty `Roles` — and `EffectivePermissionsBuilder` consults the
   token's role claim ONLY while no entry exists (`entry is not null` suppresses the fallback whatever
   `Roles` holds). So disable+enable returned a login that worked and could do nothing. Reproduced end to
   end on the seeded `editor`: 201 → disable → enable → **403 on `POST /api/pipelines`**. The old comment
   reasoned `LegacyRoleMigration` fills the gap in later, and it does — at the *next host start*, which is
   not the window an administrator disabling an account during an incident is operating in. Fixed by
   seeding `Roles` from the credential record, the same way `MirrorUserRoleAsync` seeds it. 6-B had
   worked around it client-side; that workaround was deleted once the server was right, because every
   other caller (curl, the admin CLI, wave 7's `admin/ access`) was still exposed.
2. **`/api/users` wrote no audit row at all** — not the guard's decision, not the mutation. Creating an
   account and changing a role were the only privileged mutations invisible in the log. It was also the
   per-action migration wave 2 promised and wave 3 missed: `user.write` was declared, matrix-tested in
   AppCore, and enforced nowhere. Fixed in the same commit (`687cb84`).

The credential hazard on that second fix is sharper than a source's: the secret is not a config field the
masker walks, it **is** the record. So the redactor is a projection and `PasswordHash`/`PasswordSalt` are
MASKED rather than dropped — load-bearing, because the diff is computed on the unmasked pair, so a reset
moves those keys and the row reads `passwordHash: "***" → "***"`. Dropping them would render a password
reset as an empty diff, i.e. as nothing having happened. Verified live: the plaintext appears in no row.

**A 403 from `/api/auth/me` now ends the session.** Everywhere else a 403 means "you may not do that" and
the screen says so; on that route it can only mean the account was disabled or its every role deleted, and
treating it as an ordinary refusal left a disabled user looking at a working console until their next
click. Nothing was over-granted — the server refused everything — but the session has to end where it
actually ended.

**Re-poll cadence:** on mount, on login, on `visibilitychange`, and a 60s interval **only while the tab is
visible**. The server caches the same snapshot for `Auth:PolicyCacheSeconds` (default 10), so it cannot
answer fresher than that; a background tab polls not at all, so a reopened laptop resolves on the first
glance rather than the first click.

**Known gaps, recorded rather than fixed:**

- `RequiresApproval` currently *hides* a control, so such a grant is strictly worse for the user than no
  grant. `decide` is on the context specifically so the wave that renders "Request approval…" needs no
  change here.
- `tag:`-scoped grants cannot match at most client call sites, because `can(action, scope?)` passes no
  tags. Errs toward hiding a control the server would allow.
- The client is **stricter than the server on the coarse policy ask**: server-side `Editor` is satisfied
  by `catalog.write` OR the legacy role claim, and the client has no token-claim axis. Identical on wave
  3's per-action guards, and it self-corrects as the OR retires.
- `GET /api/access/effective/{u}` short-circuits on a disabled user, so an admin cannot see what that
  account *would* hold without re-enabling it first. Observed again while verifying bug 1 above — the
  first probe read `roles: []` and proved nothing.
- `GET /api/approvals?state=Bogus` → 400 with an **empty body**, unlike every other refusal on those
  routes.
- The inbox applies `limit` *before* visibility filtering, so it is "your requests among the most recent
  N", not "your N most recent". The approvals page therefore does not paginate at all.
- No route lists the action vocabulary; the picker's list is hand-copied from `Actions`.
- `origin` is `"rest"` on every row a REST caller can produce, and `chat`/`onBehalfOf` rendering is
  verified against the type only — producing a real chat-origin row needs `GEMINI_API_KEY`.

### Wave 7 — docs, the `sf-access` skill, the admin surfaces, and two findings that outlive the plan

Landed: `.claude/skills/sf-access/SKILL.md`; a rewritten authorization section in `SECURITY.md`; four new
sub-sections under `#rbac` in `orleans/docs/index.html` plus six REST-table rows; `README.md`,
`orleans/README.md`, `AGENTS.md`. On the admin side: 18 client methods, `sf access|approvals|audit`, seven
new MCP tools, `bun test admin/` 21 → 30.

**The MCP server gets `request_approval` and NOT `approve`**, and no access-write tool at all. An agent
that can both propose and approve is the same pair of eyes twice, and shipping the approve tool would
convert the mechanism into a formality that logs itself.

**That boundary holds at the tool list only, and verification found where it stops holding.** The MCP
server authenticates as whatever `SF_USER` is. In the live run that was `admin`, who was also in the
template's approver group — so the agent filed *as the administrator*, and a human could then have
approved the agent's own proposal with the store unable to tell, because the self-vote rule compares
identities and the agent's identity WAS the administrator's. No tool list fixes that. The MCP server needs
its own low-privilege login that is in no approver group; written into `admin/README.md` as a requirement
and into `SECURITY.md` in its general form. `AuditEntry.onBehalfOf` and `origin` make the collapse visible
after the fact; they do not prevent it.

#### Two findings that are not documentation bugs

**1. `requiresApproval` is not an override, so the natural way to express "prod deletes need a second pair
of eyes" silently does nothing.** Observed live: an editor holding the Editor role (unconditional
`pipeline.delete` on `*`) plus a narrower `{pipeline.delete, dev-*, requiresApproval: true}` grant
**deleted the pipeline outright**, audit row `allowed by grant pipeline.delete on *`. Strip the role's
plain Allow and the same grant correctly produces `403 … requires approval`.

`PermissionEvaluator` prefers any unconditional Allow over any approval Allow, with no specificity
ordering, and wave 1 had a reason: *"alice may deploy to prod-\*, and separately alice may deploy anywhere
with an approval" must not force alice through an approval for prod.* That scenario is real, so simply
inverting the rule swaps one footgun for its mirror image. **Both are resolved by the upgrade path the
evaluator's own ponytail note already names** — score each matching grant by pattern specificity (tag <
prefix < exact) and let the most specific win, Deny breaking ties. Alice's exact `prod-*` grant then beats
her `*` approval grant; the operator's `dev-*` approval grant beats the role's `*` allow. It is a
deliberate semantic change to the security core, in three places (the pure evaluator, both flavours by
construction, and the TypeScript twin) — deferred to a decision, not done silently.

**2. An approval executes nothing.** `IApprovalFacade.RecordOutcomeAsync` is implemented on both flavours
and called from **no** REST route, gRPC service or chat path — verified by grepping every caller. So
`Executed` and `Failed` are unreachable outside tests, and approving a request grants the requester no
capability they did not already have: they must retry the original action, which will be refused again
unless their grants changed. `ApprovalRequest.PayloadJson` was designed to carry "the request that would
have executed", and nothing replays it. Wave table 015-E promised *request → N-of-M approve → execute*;
the execute half is missing. Documented honestly as "an approval is a record, not an execution" rather
than papered over.

Also observed: **on a first start against an empty data dir the access bootstrap can beat the user
seeder** — the log reads "seeded 3 built-in role(s) and mirrored **0** user role list(s)", `GET /api/access`
shows `users: []`, and `effective/{u}` reads empty for a login that works fine by token-claim fallback,
until the next host start or the next user PUT.

### Wave 8 — the two wave-7 findings, closed

Both were decisions rather than bugs, so both were put to the user before being built.

#### Grant specificity, on the approval axis only

`PermissionEvaluator` now scores every matching Allow and lets **the most specific one** decide whether
approval is required. Per axis: `*` (nothing named) < `tag:` < prefix < exact, tier dominating, literal
count breaking ties inside a tier so `prod-eu-*` beats `prod-*` — nested prefixes are the commonest way an
operator carves a narrower area out of a broader one, and without the tiebreak they would tie and gate the
narrower one too. The two axes are **summed, not ranked**: neither "this action anywhere" nor "any action
on this resource" is obviously the senior kind of specific, and inventing a priority would decide cases
nobody asked about. A tie resolves to `RequiresApproval` — the safer answer, and it removes document order
from the decision entirely.

**Deny stays absolute — a deliberate narrowing of the upgrade path the evaluator's own note proposed.**
That note said "…with Deny breaking ties", i.e. most-specific-wins across all grants, which would let a
guardrail `Deny pipeline.* on prod-*` be defeated by any forgotten `Allow pipeline.delete on prod-orders`.
The reported bug does not need it, so it does not get it. The cost is unchanged and still documented: you
cannot carve an Allow out of a broad Deny; narrow the Deny's scope instead.

`tag:` ranks below both name forms, and that is the residual footgun, pinned by a test so it is a decision
rather than a surprise: an approval-gated `tag:finance` grant loses to a plain `prod-*` Allow on a resource
that is both. A `prod-*` grant at least bounds the names it will ever cover; `tag:finance` covers whatever
anyone holding catalog write has tagged since — a set the grant's author neither enumerated nor can see the
edge of. Gate by name, or use a Deny.

**No pre-existing expectation changed.** Two tests were renamed with an in-place comment because their
NAMES asserted the old rule while their assertions still hold under the new one. `web/src/api/permissions.ts`
was changed in the same wave; the parity block covers all 26 actions.

Verified live: an editor holding the Editor role plus a narrow `{pipeline.delete, w8-*, requiresApproval}`
grant now gets **403** on a direct delete. Before this wave the same configuration returned 204.

#### An approval now executes

`ApprovalExecutor` runs when the post-vote re-read shows `Approved`. What was approved is the `(Action,
Scope)` pair the approver saw, and three rules keep it that way: the operation comes from `Action` alone
(closed switch, no default); the target comes from `Scope` alone and the scope must name **exactly one**
entity (`*`, `prod-*`, `tag:` are refused — an approval given for a set is never cashed against a member the
approver never looked at); and `PayloadJson`, which the *requester* wrote, may only supply a body that
cannot move either, with any name or id in it checked against the entity already resolved from the scope.
`source.write` payloads additionally run the PUT handler's own validation and secret merge, so an approval
cannot become the way to store a definition REST would have rejected.

Executors exist for `source.write`, `source.delete`, `pipeline.delete`, `table.delete`, `pipeline.control`,
`table.control`. `pipeline.write`/`table.write` deliberately have none: their REST handlers are ~70 lines
each of DTO→definition translation, and a second implementation of that is exactly the divergence this plan
produced three times already. An action with no executor records `Failed` with a sentence — never
`Executed`, never a silent success.

**Claim before plan, and that order IS the correctness argument.** The executor claims the request with an
atomic transition out of `Approved` carrying a unique token, and only the caller that reads its own token
back may do anything. It was written plan-first, which reads better — the knowable failures would reach
`Failed` without passing through the claim's optimistic `Executed`. Testing it against two concurrent
callers showed why that is wrong: **a plan is computed against live catalog state, so the caller that LOST
the race plans against a world the winner has already changed, concludes "the entity is gone", and records
that failure over the winner's success.** The loser must write nothing at all, and the only thing that can
tell it it lost is the claim. Pinned by an assertion on the STORED request, not just on what each caller
returned.

`ApprovalStateMachine.RecordOutcome` gained exactly one transition — `Executed → Failed`, and only when
`executed` is false. Without it a run that threw left the request reading `Executed` forever while its audit
row said otherwise, and a wave about the record being true cannot ship a record that says an action
succeeded because it was attempted. Narrow on purpose: a general re-statement would let any terminal state
be rewritten, and correcting a `Failed` to `Executed` would let a retry launder a failure. It is safe only
because the executor claims before it plans, so the caller making the correction is always the claim holder
describing its own attempt.

**`approval.bypass` was left untouched and still referenced nowhere.** It names a grant a *human* holds to
skip the second pair of eyes; the executor holds no grant and skips nothing — it cashes in an approval that
was actually given. Wiring it here would mean anyone later granted break-glass silently inherits the
executor's authority. The executor uses no action constant at all: its authority is the approval, bounded
by `(Action, Scope)`, and `AuditEntry.ApprovalId` is what makes it accountable.

Live round trip: file → approve → `state: Executed` → the pipeline is actually gone (404) → a second
approve is 409. The audit log carries both halves of the story — a `requires-approval` row from the refusal
that sent the requester to file, and an `executed` row with `origin: approval`, the approval id, and the
**requester** as actor, because the change is theirs.

## OIDC — deferred to its own plan; the seams land here

The `.AddJwtBearer("Oidc", …)` + issuer-selecting `PolicyScheme` is ~80 lines. These five are the real work:

1. **The federated `grpc` source logs in with a username and password** on every reconnect. An OIDC-only
   deployment has no password grant for it, so solving it properly means **service accounts / client
   credentials** — a third credential type with its own issuance, scoping, rotation and audit story.
2. The SPA login page becomes authorization-code + PKCE with a callback route and changed token custody —
   colliding head-on with Wave 6, which is exactly how you get the flag day Wave 6 exists to avoid.
3. `sf_docs` currently carries the local JWT. An external access token may be opaque, huge, or 5-minutes
   short, so the cookie would have to become a server-issued session — a different security review of a
   component that is currently correct.
4. JIT provisioning policy (create? match by email? refuse?) has an audit and admin-UI consequence either way.
5. Verifying it needs an IdP or a fake metadata endpoint — new test infrastructure in a repo with no
   HTTP-level harness.

Landed here instead, at about half a wave: `ExternalSubject`/`IdentityProvider` reserved on `UserRecord`,
`ExternalClaimValues[]` on `GroupDefinition`, the resolver taking group membership from **both** the store and
a `groups` claim (unit-tested from day one with a synthetic `ClaimsPrincipal`), and scheme setup factored into
one method. When OIDC lands, IdP group mapping is already implemented and tested.

## Cut, explicitly

- OIDC (above) · outbound notification channels for escalation (in-plan = audit + SignalR + badge) · tenant
  isolation (user-excluded) · service accounts beyond ingest keys · JTI revocation / refresh tokens ·
  per-field permissions · a general frontend testing initiative (exactly two `bun test` files, because the
  client matcher is a security-visible mirror of server logic).
- **Known quirk to document, not fix:** the dev signing key is identical across flavours, so a token minted by
  Orleans validates on Dapr — but each flavour has its own access store, so the *grants* do not travel.
