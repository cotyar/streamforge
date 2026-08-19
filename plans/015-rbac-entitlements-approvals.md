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

`hasRole` survives, implemented on top of `can()` — **zero of the 57 `RoleGate` references changes at
cut-over.** That is the whole no-flag-day answer. The SPA treats a missing `permissions[]` as an old server
and falls back to today's ordinal semantics, so a rolling deploy is safe.

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
