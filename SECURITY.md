# Security policy

## Reporting a vulnerability

Please report security issues privately through
[GitHub's private vulnerability reporting](https://github.com/cotyar/streamsforge/security/advisories/new)
rather than a public issue. Expect a first response within a few days; this is a side project, not
a vendor with an on-call rotation.

## What this project is — and is not

StreamsForge is a **reference implementation and demo**, not a hardened production system. Before
pointing it at anything real, know that:

- **The seeded demo users are public knowledge** (`admin/admin123!`, `editor/editor123!`,
  `viewer/viewer123!`) and are created automatically into an empty data directory. Any deployment
  reachable by other people needs them removed or changed.
- **JWT signing key**: HS256 with a development key unless you set your own via configuration.
  Set one before exposing the API.
- **TLS is off by default and native when you turn it on.** `Tls:Enabled=true` puts TLS on *both*
  listeners — REST/SignalR/SPA and gRPC (which then serves ALPN-negotiated h2 rather than cleartext
  h2c) — using Kestrel's own `Kestrel:Certificates:Default` section (`Path`+`KeyPath` for a PEM pair,
  `Path`+`Password` for a PFX, or `Subject`+`Store`); the host **refuses to start** if the flag is set
  with no certificate configured, and `tools/tls/dev-cert.sh <dir>` mints a development pair. A
  TLS-terminating proxy in front still works and is unchanged — set
  `ASPNETCORE_FORWARDEDHEADERS_ENABLED=true` so the host believes the proxy's `X-Forwarded-Proto`
  and reports its own endpoints as `https://` (without that flag the header is ignored, deliberately:
  trusting it while directly exposed lets any caller dictate the scheme the instance reports).
- **Outbound TLS trust is a separate axis.** What this instance accepts when *it* dials out — a
  federated peer, an HTTP sink, a `url` source — is the system trust store by default.
  `Tls:TrustedCaPath` adds a PEM bundle of private certificate authorities on top of it (a name
  mismatch is still refused; a custom CA answers "who signed this", never "is this the host I asked
  for"). `Tls:AcceptAnyCertificate=true` disables outbound validation entirely and logs a warning at
  startup — development only.
- **The AI control chat can mutate the catalog.** `POST /api/chat` gives the model function-calling
  access to create, edit, start and stop sources, pipelines and tables. Every tool re-checks the same
  entitlement its REST equivalent checks, at the same scope, attributed to the model with the human as
  `onBehalfOf` — but a caller entitled to edit prod can still ask the model to edit prod. A publicly
  reachable instance with `GEMINI_API_KEY` set hands those capabilities to whoever can log in. Each
  login session is capped at `Chat:MaxRequestsPerSession` calls (default 10) so one visitor can't
  drain the API quota, but that is a spend guard, not an authorization boundary. Leave
  `Chat:MayExecutePrivileged` at its default `false`: `true` lets the model execute actions whose
  grant says they need approval, instead of filing the approval request.
- **State is process-local.** Both flavors keep the working set in memory (Orleans grains / Dapr
  actors); there is no durability, replication or recovery story worth relying on.

## Authorization — what it does and does not do

Authorization is per-resource **entitlements**, not the three role strings it started as. A decision
is `Allowed` / `RequiresApproval` / `Denied`, taken from a grant list flattened out of the user's own
entry, their groups and their roles: `action` (`pipeline.update`, `source.ingest`, `*`) × `scope`
(`*`, an exact entity **name**, a prefix glob `prod-*`, or `tag:finance`) × `Allow`/`Deny`, with
**deny-overrides**. The three legacy roles survive as built-in role definitions with the grants they
always implied. Details and recipes: the docs site's "Roles, entitlements & approvals" section.

What matters if you are exposing this to anyone:

- **Permissions resolve server-side on every request; they are not baked into the token.** The JWT
  still lives 12 hours and still carries one role claim, but it is not what decides. A revoked grant
  or a disabled login takes effect within `Auth:PolicyCacheSeconds` (default 10) — not at the next
  login.
- **Disabling a login is not token revocation.** `PUT /api/access/users/{u}/disabled` makes the
  resolver return an empty grant set, so the token stops being useful within that same TTL, but it
  remains valid and signed. There is no JTI denylist and no refresh-token story; a stolen key or a
  compromised signing secret is not addressed by disabling a user.
- **`Auth:Mode=legacy` turns the whole thing off** and restores the pre-entitlement behaviour. It
  defaults to `entitlements`, and only the exact string `legacy` disables it — a typo leaves
  enforcement on, deliberately. `Auth:StrictViewer` (default `true`) is what makes even the coarse
  read policy refuse a disabled principal; it fails *open* when the policy store is unreachable.
- **Approvals ship disabled** (`Approvals:Enabled=false`) and are a record, not an enforcement
  mechanism: approving a request does not perform the action, and an action with **no matching
  template requires no approval**. A misspelled `actionPattern` is a control that silently does not
  exist — anything that must not fail open belongs behind a `Deny` grant instead.
- **An automated caller must not share a human's login.** Every rule that reasons about *who* somebody
  is collapses when a script, an agent or an integration authenticates as a person. Approvals are
  where it shows most clearly: the "you cannot approve your own request" rule compares identities, so
  an agent filing as `admin` produces a request a human administrator can then approve — the store
  cannot tell the two apart, because they are the same principal. The MCP server (`bun admin/mcp.ts`)
  deliberately exposes `request_approval` and **not** `approve` for exactly this reason, but that
  boundary lives in the tool list and nowhere else. Give any automated caller **its own login**, with
  the narrowest grants that let it work, and **keep it out of every approver group** — see
  [`admin/README.md`](admin/README.md), where this is a configuration requirement rather than advice.
  The audit log makes a shared identity visible after the fact but cannot prevent it: `onBehalfOf`
  exists so an AI-originated mutation is attributed to both parties (`actor` is the model,
  `onBehalfOf` the human whose token it carried), and `origin` marks a row `chat` rather than `rest`.
  Guard-level rows are still stamped `rest` whatever transport the decision arrived on, so `origin` is
  a hint about *how* an action was proposed, never proof of who proposed it.
- **The audit log is not a secret store.** It records every refusal and every privileged mutation,
  and its optional before/after payloads are readable by anyone holding `access.read` at `*`. Those
  payloads are always run through the same secret-masking pass the export routes use — a credential
  rotation reads as `"***" → "***"`, which is deliberate: the presence of the key is the signal, the
  value never is. Rows are capped per day (`Audit:MaxEntriesPerDay`, drop-oldest) with the dropped
  count persisted, so a gap is visible rather than silent.

Known-by-design limitations are documented in
[`orleans/DESIGN.md`](orleans/DESIGN.md) and the docs site's "Limits" section.
