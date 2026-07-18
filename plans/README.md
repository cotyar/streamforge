# Advisor Plans

Written against commit `49d6979` by the improve skill (focused `plan` invocation — full audit skipped at user request; scope was predetermined: shadcn/ui migration + the client branding).

| # | Plan | Status | Depends on |
|---|------|--------|------------|
| 001 | [shadcn/ui migration + the client branding](001-shadcn-corporate-branding.md) | DONE | — |
| 002 | [Multileg & multistage instruments](002-multileg-multistage-instruments.md) | APPROVED — L1 IN PROGRESS | L2 queued in engine chain (after 004) |
| 003 | [Materialize territory: partitioned differential dataflow](003-materialize-territory.md) | APPROVED — M0 DONE, M1 IN PROGRESS | supersedes interim sharding idea |
| 004 | [Nested queries (non-recursive)](004-nested-queries.md) | APPROVED | 003-M1 |

Engine-exclusive chain (serialized): 003-M1 → 004 (N1–N4) → 002-L2 UNNEST + LATEST BY → 003-M2.
Parallel host/web tracks: 002-L1 (with M1), 002-L3 (with 004), 003-M5 (with M3).

Design tokens source of truth: `orleans/design-system/streamforge/MASTER.md`.
