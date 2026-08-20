---
name: sf-federate
description: StreamForge instance discovery and gRPC federation — curl recipes for /api/meta/instance, the Discovery:Peers directory, probing a peer, and creating a federated grpc source addressed by peer name + entity name (no address, no GUID), plus @name named external endpoints for making the same catalog document portable across environments. Use when asked to federate two StreamForge instances, check what a running instance is/can do, or make a source's endpoint config environment-independent.
---

# sf-federate — instance discovery, peer federation, named endpoints

Full user docs: `orleans/docs/index.html` (§ Instance discovery & federation, § Ingestion connectors →
Named external endpoints). Contributor-facing wiring: `TRANSPORTS.md` (Named external endpoints section,
and the `grpc` note under "What is deliberately not pluggable"). Plan: `plans/016-identity-versioning-discovery.md`.

## The rules that bite

- **The peer directory is not an HA service registry.** No heartbeat, no expiry, no leader election. A
  probe result is kept until the next probe — a peer that went away is not noticed until someone probes
  it again.
- **`GET /api/meta/instance` is anonymous, on purpose** — a peer has to be able to learn what a deployment
  is before it holds any credential for it. It reports entity **counts** and connector **kind names**,
  never entity names, and there is no switch to gate it.
- **A peer probe dials out.** `POST /api/meta/peers/{name}/probe` is Viewer-gated but still makes the
  server issue an outbound HTTP request — only to a configured peer's configured address, never
  caller-supplied, but worth knowing before handing out Viewer broadly.
- **`GrpcSubConfig.Peer` wins over `Address`/`RestAddress` when set**, not merely fills them in when
  blank — a source naming a peer and also carrying a stale literal address must never silently connect to
  the stale one.
- **`@name` resolves at connect time only, never at validate/save time**, and the resolved value is
  **never written back** — an export still reads `@primary-oltp`. An unresolvable `@name` is a *warning*
  at import (promotion must still work) but an *error* at connect (there's nothing to dial).
- **`Endpoints:` values are not masked.** `GET /api/meta/endpoints` (Viewer) returns every configured
  name's value in the clear — a connection string behind a name carries its credential exactly as
  configured.
- **A sink whose `@name` fails to resolve just drops out of its entity with a log line** — the entity's
  own status has no per-sink field to say so.

## Setup for every recipe

```bash
P=http://localhost:5199   # the "producer" instance — the one being federated FROM
C=http://localhost:5399   # the "consumer" instance — the one federating IN (e.g. the Dapr flavour)
login(){ curl -s -X POST $1/api/auth/login -H 'Content-Type: application/json' \
  -d "{\"username\":\"$2\",\"password\":\"$3\"}" | sed -n 's/.*"token":"\([^"]*\)".*/\1/p'; }
TP=$(login $P admin 'admin123!'); TC=$(login $C admin 'admin123!')
```

## Instance identity (anonymous — no token needed)

```bash
curl -s $P/api/meta/instance | jq '{instanceId, name, flavor, version, endpoints, capabilities, catalogCounts, catalogWarnings}'
```

`instanceId` is persisted at `{DataDir}/instance.json`, so it survives a restart (deleting `DataDir`
mints a new one — the documented Orleans reseed; merely arbitrary on Dapr, whose real state lives in
Redis). `catalogWarnings` are counts/kind-names only, e.g. `"1 pipeline(s) have a stale pin"`.

## Configuring and probing a peer

The consumer needs the producer configured as a peer — `Discovery:Peers` is a **section**, so it binds
as an array from any provider:

```bash
# on the CONSUMER's command line, at startup:
--Discovery:Peers:0:Name producer \
--Discovery:Peers:0:RestEndpoint http://localhost:5199 \
--Discovery:Peers:0:GrpcEndpoint http://localhost:5299
# or DISCOVERY__PEERS__0__NAME / DISCOVERY__PEERS__0__RESTENDPOINT env vars, or an
# appsettings.json { "Discovery": { "Peers": [ { "Name": "...", "RestEndpoint": "...", ... } ] } }
```

```bash
curl -s $C/api/meta/peers -H "Authorization: Bearer $TC" | jq .
# [{"name":"producer","instanceId":"","restEndpoint":"...","lastSeenAtMs":0,"lastError":null,"info":null}]

curl -s -X POST $C/api/meta/peers/producer/probe -H "Authorization: Bearer $TC" | jq .
# instanceId/lastSeenAtMs/info now filled in from the producer's own /instance
```

## Federating a table (or source, or pipeline) by NAME

No address, no GUID, anywhere in the resulting source's own definition:

```bash
# try it first — dials out via the peer, resolves entityKey's id->name round trip on table/pipeline keys
curl -s -X POST $C/api/sources/schema/from-remote -H "Authorization: Bearer $TC" -d '{
  "grpc": { "peer": "producer", "entityKey": "table:positions",
            "username": "admin", "password": "admin123!", "schemaSource": "reflection" }
}' | jq '{fields, diagnostics}'

# then create the federated source for real, fields from the probe above
curl -s -X POST $C/api/sources -H "Authorization: Bearer $TC" -d '{
  "name": "fed_positions", "kind": "grpc", "enabled": true,
  "fields": [ ... ],
  "connector": { "grpc": { "peer": "producer", "entityKey": "table:positions",
                            "username": "admin", "password": "admin123!" } }
}' | jq '{name, enabled}'
```

`entityKey` is `source:{name}` | `pipeline:{name-or-id}` | `table:{name-or-id}` on the REMOTE — id-or-name
resolution (see the docs' REST API section) is what lets the name-only form work. An unknown peer name is
an actionable diagnostic (`"GrpcSubConfig.Peer 'x' is not a configured peer."`), not a crash; an
unreachable one takes the same status-error path and backoff every other connector uses.

## Named external endpoints — the same catalog, different backends per environment

Author the endpoint-shaped field as `@name` instead of a literal, once, and never touch the document
again:

```bash
# authoring time — the document itself:
{ "connector": { "url": { "url": "@feed" } } }

# environment 1:
--Endpoints:feed http://prod-host/feed.json
# environment 2, same DataDir, same catalog, only this flag differs:
--Endpoints:feed http://dev-host/feed.json
```

```bash
curl -s $P/api/meta/endpoints -H "Authorization: Bearer $TP" | jq .   # every configured name AND value
curl -s $P/api/sources/env_feed -H "Authorization: Bearer $TP" | jq -r '.connector.url.url'   # still "@feed"
curl -s $P/api/config/export -H "Authorization: Bearer $TP" | jq -r '.sources[].connector.url.url'  # still "@feed"
```

Works identically for a `grpc` connector's `address`/`restAddress` (when no `peer` is set), NATS/Postgres/
MSSQL/FIX hosts, and an HTTP sink's `url` — any endpoint-shaped field, on any kind, everywhere in the
catalog.

## Gotchas

- **Only `GET` routes are id-or-name; `PUT`/`DELETE`/`start`/`stop` are id-only.** Resolve a federated
  entity's name to an id first if you need to mutate it directly on the producer.
- **Import validate never evaluates a `dependsOn` pin.** `staleReason` is computed by the registry only
  when the pinned entity, or the thing it depends on, is actually written — `mode=validate` reports an
  empty diagnostics list even for an already-broken pin. Read the entity back after a real
  `merge`/`replace` to see it.
- **A `requires: [{kind, version}]` mismatch refuses the WHOLE import**, not just the entities using that
  kind, and — like every refusal on this route — comes back as HTTP `200` with `"ok": false`. Check `ok`,
  not the status code (`curl -f` will not catch it).
- **A source can never be renamed and has no id** — its name is simultaneously its REST route, stream
  key, SQL namespace entry and federated `EntityKey`. A table can be renamed, but only while `Stopped`,
  unsharded, and referenced by no other table's inputs.
