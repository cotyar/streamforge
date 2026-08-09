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

Design tokens source of truth: `orleans/design-system/streamforge/MASTER.md`.
