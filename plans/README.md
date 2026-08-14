# Advisor Plans

Written against commit `49d6979` by the improve skill (focused `plan` invocation — full audit skipped at user request; scope was predetermined: shadcn/ui migration + corporate branding).

| # | Plan | Status | Depends on |
|---|------|--------|------------|
| 001 | [shadcn/ui migration + corporate branding](001-shadcn-branding.md) | DONE | — |
| 002 | [Multileg & multistage instruments](002-multileg-multistage-instruments.md) | DONE (L1+L2+L3, incl. LATEST BY) | — |
| 003 | [Materialize territory: partitioned differential dataflow](003-materialize-territory.md) | DONE (M0–M5; soak p99 0.70× vs single-grain) | superseded interim sharding idea |
| 004 | [Nested queries (non-recursive)](004-nested-queries.md) | DONE (N1–N4 + scope-aware editor autocomplete) | — |
| 005 | [Dapr sibling runtime (shared core + polyglot)](005-dapr-port.md) | DONE (W0–W9; original scoreboard Dapr p50 7ms / Orleans p50 122ms was later root-caused to Orleans' pull-based memory streams, not the actor model — with the post-plan `--Streams:Transport push` bus Orleans measures p50 1ms; see comparison.html and plan status addenda) | 001–004 (ports the platform they built) |
| 006 | [Ingestion connectors + config import/export](006-connectors-and-config.md) | DONE (W0–W7; four real connector kinds + config import/export live on both flavors; headline cross-flavor federation — Dapr subscribed an Orleans table by id over gRPC, real rows flowing) | 005 (shared core + both flavors) |
| 007 | [Containers + Cloud Run prep, admin app, AI control chat](007-cloudrun-admin-aichat.md) | DONE (both flavors containerized + Cloud Run-ready; admin app drove both stacks live; Gemini AI control chat on both flavors with audit-trace UI) | 005, 006 |

| 008 | [REST discoverability, table joins, set operations, ingress, lineage](008-rest-joins-setops-ingress-lineage.md) | DONE (W1–W5 landed) | 005, 006, 007 |
| 009 | [Ingress hardening, NATS transport, table delta journal](009-ingress-hardening-nats-journal.md) | DONE (A1–A2, B1–B3, C1–C2, D landed; NATS verified end to end against a real broker) | 008 |
| 010 | [Pluggable ingress/egress transports](010-pluggable-transports.md) | DONE (a transport is now one class + one registry line; recipe in [TRANSPORTS.md](../TRANSPORTS.md)) | 009 |
| 011 | [Lineage edges, SQL editor UX, memory stability, sharded tables](011-lineage-sql-memory-sharding.md) | DONE (lineage edges restored; SQL revert/format; the flush amplifier fixed 287→109 MB/min and row retention plateaus a table; per-key shards shipped but **measured NOT to be a memory optimisation** — query locality and swap-out only; per-entity Scalar docs) | 010 |
| 012 | [CSV in, CSV out](012-csv-io.md) | DONE (url sources read CSV/NDJSON; TSV/semicolon/pipe detected; a `file` sink kind writes CSV or NDJSON; CSV download for table rows and pipeline results) | 010 |
| 013 | [`sf` admin CLI + MCP server](013-admin-cli-mcp.md) | DONE (`bun admin/sf.ts` for a terminal, `bun admin/mcp.ts` for an agent — one shared REST client, zero npm deps, MCP stdio hand-written to the spec and pinned by subprocess conformance tests) | 012 |
| 014 | [Pluggable connectors + database ingress/egress](014-pluggable-connectors-databases.md) | IN PROGRESS | 013 |
| 015 | [RBAC → entitlements, groups, approvals, escalation, audit](015-rbac-entitlements-approvals.md) | PLANNED | independent of 014 |
| 016 | [Name resolution, versioning & dependencies, service discovery](016-identity-versioning-discovery.md) | PLANNED | 014 (plugin versions only) |
| 017 | [Native .NET CDC](017-native-cdc.md) | IN PROGRESS (postgres-cdc + mssql-cdc registered and documented; live DB integration running concurrently) | 014 (reuses `IPolledTransport`, `DbSourceConfig`, `CdcEnvelope`'s vocabulary) |

Design tokens source of truth: `orleans/design-system/streamforge/MASTER.md`.
