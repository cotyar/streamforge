# 004 — Nested Queries (non-recursive)

Status: **APPROVED — queued behind 003-M1** (rides its operator-tree refactor).
Recursive CTEs are permanently out of scope (would require multitemporal timestamps,
overturning 003's DBSP decision). Every tier below applies to BOTH pipeline and table modes
unless noted.

## N1 — Derived tables + WITH (CTEs)

- Grammar: `FROM ( SELECT … ) alias` (alias mandatory) and `WITH name AS ( SELECT … ), … SELECT …`
  (CTEs are namespaced per-statement, may reference earlier CTEs in the same WITH list, no
  self/forward references — that's recursion, rejected with a positioned diagnostic).
- AST: FROM item becomes `NamedSource | DerivedTable(SelectQuery)`; CTEs desugar to derived
  tables at parse time (single mechanism downstream).
- Validation: scope stack; a derived table's output schema (existing BuildOutputSchema) is the
  synthetic source schema one level up. Inner diagnostics keep inner line/col positions.
- Planning: derived node wraps a child operator chain (M1's op-tree). Table mode: an inline
  intermediate Z-set operator (same machinery as named table-over-table chaining).
- Windows-in-windows (pipelines): an inner windowed query's emissions enter the outer level as
  events timestamped at **window end**; outer WINDOW then buckets those. Document in SQL ref.

## N2 — IN / EXISTS → semi-join; NOT IN / NOT EXISTS → anti-join

- `expr IN (SELECT col FROM …)`, `[NOT] EXISTS (SELECT … )` in WHERE only (not SELECT list).
- Rewrite at plan time: semi = join + distinct-by-key; anti = A − (A ⋉ B) via Z-set weights.
- NOT IN NULL trap: if the subquery's column kind is nullable-in-practice (any NULL observed is
  undetectable statically) we adopt the pragmatic rule: NULLs in the subquery result are
  ignored, documented loudly in the SQL reference as a deviation from three-valued SQL.

## N3 — Uncorrelated scalar subqueries

- `(SELECT agg(…) FROM …)` usable as an expression. Must be provably single-row: an aggregate
  query with no GROUP BY — anything else is a positioned diagnostic.
- Plan: singleton Z-set cross-join (bilinear; retractions flow when the scalar changes).
- Pipelines: the scalar's source must be a table or share the outer query's WINDOW spec
  (validator-enforced) so alignment is well-defined.

## N4 — Equality-correlated aggregate subqueries

- Pattern: `(SELECT agg(x) FROM s2 WHERE s2.k = outer.k [AND …])` — decorrelate to
  GROUP-BY-k aggregate joined on k. Only equality correlation to a single outer column set;
  everything else → diagnostic "correlated subqueries beyond equality are not supported —
  rewrite as a JOIN".

## Editor/UX (separate web task, after N1)

Scope-aware autocomplete in SqlEditor: CTE names as sources, derived-table alias columns from
the inner projection, inner-query completion context.

## Acceptance (end-to-end examples that must run)

```sql
WITH hot AS (SELECT symbol FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS))
SELECT t.symbol, AVG(t.price) AS px FROM trades t
WHERE t.symbol IN (SELECT symbol FROM hot)
GROUP BY t.symbol WINDOW TUMBLING(SIZE 5 SECONDS)
```
```sql
SELECT symbol, price / (SELECT AVG(price) FROM trades GROUP BY () …) -- table mode: rel-value
FROM trades …
```
(exact seed demos chosen in-implementation; each tier ships parser+validator+executor tests,
table-mode retraction tests, and SQL-reference docs.)
