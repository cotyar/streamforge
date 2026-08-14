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
| **0** | Contracts + spikes, orchestrator alone: `UserRecord` `[Id(6..13)]`, new `AccessModels.cs`, facades, `StreamConstants` keys, `CatalogRecordMerge` 4-arg overload, `types.ts`; spike the `EndpointDataSource` test and the sweeper on the scheduler-less Dapr stack | Opus 5 high |
| **1** | 4 parallel: pure evaluator (**Opus**) ∥ Orleans store ∥ Dapr store ∥ seeds + `LegacyRoleMigration`. Gate includes booting both flavours against a **pre-upgrade** data dir | mixed |
| **2** | 3 parallel: ASP.NET binding + resolver + guard (**Opus**) ∥ coverage audit + `tools/authz-matrix.sh` ∥ access/users REST | mixed |
| **3** | 3 parallel: REST routes ∥ gRPC + SignalR ∥ chat + config import (**Opus** — where an LLM's action is attributed to a human) | mixed |
| **4** | 4 parallel: approval state machine (**Opus**) ∥ Orleans stores ∥ Dapr stores ∥ audit sink + sweeper | mixed |
| **5** | 2 parallel: approvals + audit REST ∥ before/after detail on mutation sites | Sonnet 5 high |
| **6** | 3 parallel: permission client core + `RoleGate` shim (**Opus**) ∥ access/approvals UI ∥ audit UI | mixed |
| **7** | 2 parallel: docs + `sf-access` skill ∥ `admin/` `access`/`approvals`/`audit` commands. The MCP server gets `request_approval` and **not** `approve` | Sonnet 5 high |

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
