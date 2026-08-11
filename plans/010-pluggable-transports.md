# Plan 010 — Pluggable ingress/egress transports

**Status: DONE.** Recipe for adding one: [`TRANSPORTS.md`](../TRANSPORTS.md).

## Why

After plan 009 shipped NATS in both directions, adding a second transport meant editing ~14 places across
both runtime flavors:

| where | what |
|---|---|
| `SourceValidation` | a `case SourceKinds.Nats:` arm, a known-kinds set, a mapping-applicability list |
| `SecretsMasker` | three hand-written blocks per direction (mask / merge / has-masked) — **eight** per transport |
| `ConnectorGrain` (Orleans) | `if (kind == Nats)`, `_natsCts`/`_natsTask`, `CancelNats` |
| `ConnectorActor` (Dapr) | the same three again |
| `SinkSelection` | an eligibility predicate written in terms of NATS' own fields |
| `NatsPublisherService` / `NatsSinkPublisherService` | both typed on the concrete `NatsSinkClient` |
| `web/` | `types.ts`, a per-kind `XConfigEditor.tsx`, `SourcesPage`, `SinksEditor` |

None of that was essential. The subscribe loop was NATS-specific in exactly two expressions; payload→row
already went through the same shared path a polled HTTP body uses. So this plan is mostly generalization and
deletion, not new machinery.

**The cost now:** one `IInboundTransport` and/or one `ISinkTransport`, plus one line in a registry.

## What landed

- **`SubscriberCore`** — reconnect/backoff/parse/coerce/dedup/ack, once for every transport.
  `NatsSubscriberCore` became a thin wrapper so its 390-line test suite drives the generalized loop
  **unmodified** — that is the proof of behavior preservation, and the reason the wrapper exists.
- **`[Secret]` + `SecretWalk`** — which values are secret is declared next to the field. `SecretsMasker`
  gains zero lines per transport; the old shape's failure mode was a slot missing from one of the lists,
  leaking a credential through an export, silently.
- **`InboundTransports` / `SinkTransports`** — plain static lists (not DI discovery: injecting into a grain
  has broken this repo's test cluster before) with a `Register()` for transports whose client library cannot
  ship here. That case is real: `TIBCO.Rendezvous` is not on public NuGet.
- **`ISinkClient` / `ISinkTransport`** — both publisher services hold `List<ISinkClient>`;
  `SinkSelection.Active` asks the registry what is eligible.
- **`GET /api/transports` + `TransportConfigEditor.tsx`** — the console renders every transport's form from
  a descriptor. `NatsConfigEditor.tsx` (261 lines) deleted; `SinksEditor` gained a kind picker it never had.

Deliberately **not** folded in: the `grpc` source kind (typed frames against a remote schema — it never asks
the payload-format question this seam is built around) and DI-based transport discovery. Both are argued in
`TRANSPORTS.md`.

## Acceptance criteria — all met

- Orleans **798 + 710 = 1508**, Dapr **280**, all green; **no pre-existing test file modified**.
- `TransportRegistryTests` registers a transport the repo has never heard of and asserts the platform
  validates it, masks its credentials, drives its messages into rows, and lists it in the catalog — the
  extensibility claim as a test rather than a doc comment.
- Sabotage-checked: masking only one config container, a hardcoded `nats` validation arm, and a dropped
  `[Secret]` each fail exactly the test that exists for them.
- `cd web && bun run build` clean (`tsc -b` included).
- Live on isolated ports: unknown kind rejected with a registry-built message; `nats` validated by its own
  transport; credentials masked on read and in config export (0 leaks); a masked PUT round-trip preserving
  the stored secrets; the driver arming a subscriber and degrading on an absent broker without crashing;
  `GET /api/transports` served to Viewer; and a source created **through the console's generic form**
  (JetStream group toggled on, declared defaults applied, secret masked on read-back).

## Known limits

- One console-form regression, taken deliberately: NATS' four credential fields are shown as themselves
  instead of behind an "authentication mode" picker. The config genuinely has four independent slots, and the
  group's help now states the precedence the server actually applies (creds file > token > user+password) —
  which the picker hid.
- The descriptor has no conditional-visibility or cross-field rules, on purpose: validation lives on the
  server and the modal renders its messages. A rules language here would be a second validator that drifts.
- `Register()` has no ordering guard beyond "before any source starts"; a late registration is a programming
  error, not a supported hot-plug.
