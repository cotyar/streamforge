# Plan 019 — FIX order entry: one bidirectional session across two registries

**Status: PLANNED — not started, and not startable without the first decision below being made
deliberately rather than discovered halfway through.**
**Depends on**: 018 (the `fix` format, the session project, `QuickFIXn.Core` already carried)

## Why this is a separate plan and not a wave of 018

Receiving market data is one direction of data through a session whose other direction carries only
session-level housekeeping. Order entry is the platform sending application messages that a counterparty
acts on, and it breaks three things that plan 018 leaves comfortably intact.

### 1. FIX has one session; StreamForge has two registries

`NewOrderSingle` out and `ExecutionReport` back travel the **same** FIX session, sharing one TCP
connection, one `SenderCompID`/`TargetCompID` pair and one pair of sequence-number streams. The platform's
ingress (`InboundTransports`) and egress (`SinkTransports`) are independent registries with independent
config objects, opened and closed independently — `SinkSelection.Signature` tears a sink client down and
rebuilds it on any edit to its `SinkSpec`, on a refresh cycle the source knows nothing about.

Configure a `fix` source and a `fix` sink against the same venue today and you get **two** logons with two
sequence streams, which a real counterparty will reject outright, and should.

Three ways out, in increasing order of honesty and cost:

- **(a) A shared session manager.** A process-global registry keyed by
  `(host, port, senderCompId, targetCompId, beginString)`; the source and the sink each acquire a
  reference to the same live session and release it on dispose. Smallest diff. Cost: the session's
  lifetime is now owned by neither of the two things that appear to own it, teardown is refcounted, and a
  sink edit that changes the key silently re-logs-on and disturbs the source. Needs a real test for the
  edit-while-connected case, which is exactly the concurrency shape `IBatchSinkClient`'s doc comment says
  the batch design exists to avoid.
- **(b) A first-class bidirectional connector concept.** One entity that declares both an inbound and an
  outbound half, registered once. Correct, and a genuine addition to the platform's model — the third
  seam after `IInboundTransport` and `IPolledTransport`. Cost: contracts, console, validation, config
  import/export, lineage, and both drivers.
- **(c) A sidecar.** The FIX session lives in its own process speaking NATS on the inside; the platform
  keeps a plain `nats` source and a plain `nats` sink and learns nothing new. Cheapest by far and exactly
  how plan 014 decided to consume Debezium. Cost: another process to deploy, and the order path acquires a
  broker hop it did not have.

**Recommendation, stated up front so the plan is not written twice: (c) unless something specifically
requires in-process order flow, then (b). Not (a)** — (a) is the option that looks cheapest on the diff
and is worst to operate, because the failure it produces is an intermittent one in the session layer of a
production order path.

### 2. Fire-and-forget is the wrong contract for an order

`ISinkClient.PublishAsync` must **never throw** and must not block past ~3s; delivery is at-most-once with
no backpressure, and `IBatchSinkClient`'s own doc states plainly that a batch "does not buy reliability,
acknowledgement or retry". For republished quotes that ceiling is fine. For a `NewOrderSingle` it is not:
silently dropping an order and counting it in a failure counter is not a behaviour anyone will accept, and
neither is "the order may or may not have reached the venue".

So order entry needs something the sink seam does not have today: a publish that can **fail loudly**, with
the sequence number and `ClOrdID` of what did not go out, surfaced where an operator sees it. That is a
change to the egress contract, or a decision that order entry does not use the egress contract at all —
which is another argument for (b) or (c) above.

### 3. Sequence numbers become correctness, not convenience

Plan 018 defaults a market-data session to `ResetOnLogon=Y` and an in-memory store because losing the
count costs at worst some re-sent quotes. On an order session the store is the record of what was sent;
losing it means a resend request the platform cannot answer, and a gap the venue resolves by its own
rules. File-backed persistence stops being an option and becomes mandatory, which drags in: the store's
durability under container restart and rescheduling, whether two instances can ever hold the same session
(they must not — this is a singleton across the cluster, which neither Orleans grain placement nor Dapr
actor placement is currently asked to guarantee for a transport), and a documented recovery procedure.

## What the plan would contain, once (a)/(b)/(c) is chosen

Sketched, not sequenced — the wave breakdown depends entirely on that choice.

- The session-ownership mechanism itself, whichever of the three.
- An outbound message builder: rows → FIX. The `INSERT INTO` sugar from 014 and `SinkSpec` mapping give a
  shape for "which columns become which tags", but required-field validation per `MsgType` needs the FIX
  dictionary that plan 018 deliberately does not build.
- `ClOrdID` generation and uniqueness, and the `OrigClOrdID` chain for cancel/replace — the platform has
  no notion of an entity that is amended rather than appended.
- Execution-report correlation: matching `ExecutionReport` back to the order that caused it, which is a
  stateful join the Engine can express but nothing currently sets up.
- Failure surfacing per §2, including `35=3` (Reject) and `35=9` (OrderCancelReject) as first-class
  operator-visible outcomes rather than rows.
- The cluster-singleton guarantee per §3, in both flavours.
- A FIX dictionary, at last — required-field validation on the way out is not optional for orders.
- Drop-copy reconciliation as the acceptance test: send N orders, receive N execution reports, prove the
  set matches.

## Not in this plan either

Pre-trade risk checks, order state machines beyond what the counterparty reports, FIX 5.0 application
version negotiation per message, and anything resembling an OMS. This plan is a transport for order flow,
not a place to keep orders.
