using StreamForge.Engine.Dataflow;
using StreamForge.Engine.Planning;
using StreamForge.Engine.Sql;

namespace StreamForge.Engine.Runtime.Ops;

/// <summary>
/// Plan 002 L3 (deferred sugar, landed alongside L2's UNNEST): `LATEST BY (col[, col...])` — table-mode
/// "keep the most recent row per key" running argmax, ordered by event timestamp (<see cref="WorkingRow.Ts"/>,
/// i.e. `_ts`). This is the terminal stage for a LATEST-BY-shaped table plan, playing the exact role
/// <see cref="TableReduceOp"/> plays for a GROUP BY-shaped one (TableExecutorImpl picks whichever of the two
/// applies — the two are mutually exclusive by construction; see Validator's exclusivity diagnostics) —
/// same "hand the WHERE-filtered-but-still-WorkingRow delta to this op, it produces the table's final
/// output TableDeltas directly" shape, just a different per-key policy than aggregation.
///
/// SEMANTICS (see class-level acceptance in plan 002 L3 / the L2+L3 implementation report for the exact
/// wording pinned by tests):
///  - Assertion (weight &gt; 0 — including weight &gt; 1, which this op treats identically to a plain
///    assertion; see the "Weight &gt;1 rows: treat as assert" ceiling note below) for a key with NO
///    currently-retained row: retain it, emit assert(row, +1). No retraction (nothing to retract yet).
///  - Assertion for a key that already has a retained row: replace it ONLY IF the arriving row's Ts is
///    &gt;= the retained row's Ts (ties replace — "last write wins" among same-timestamp arrivals); emit
///    retract(oldProjectedRow, -1) then assert(newProjectedRow, +1). A STRICTLY OLDER arriving row (Ts &lt;
///    retained Ts) is a late/out-of-order arrival: it is IGNORED — no output, no state change, and (this is
///    the honest limitation to document) it is counted NOWHERE — there is no "late row" metric this op
///    exposes.
///  - Retraction (weight &lt;= 0) that matches the row THIS op currently has retained for that key
///    (upstream retraction — e.g. an earlier WHERE-passing row now filtered out, or a genuine upstream
///    delete): drop the key entirely and emit retract(row, -1) — "no surviving row" is the honest answer
///    here; this op has no multiset/history of PAST contributing rows for a key to fall back to (a true
///    "argmax with fallback to the next-most-recent row" would need exactly that — a full per-key row
///    history — which is out of scope for this tier; see plan 002 L3's own scoping note). A retraction that
///    does NOT match the currently-retained row (e.g. retracting some row that was never the latest, or
///    retracting after the key was already dropped) is a no-op.
///  - PROJECTION applies to the retained WorkingRow directly (not a synthetic aggregate-state row the way
///    TableReduceOp's BuildRow needs — LATEST BY keeps the real row, so every output expression, key or
///    non-key alike, evaluates normally against it; there is no GroupByIndex substitution mechanism here).
///
/// STATE: <see cref="Current"/> — encoded-key string -> the single retained (WorkingRow, Ts) pair. No
/// per-key history beyond that one row (see the retraction-ceiling note above).
/// </summary>
internal sealed class TableLatestByOp : ITableOp
{
    internal sealed class KeyState
    {
        public required WorkingRow Row;
        public required long Ts;
    }

    private readonly CompiledTablePlan _plan;
    private readonly List<Expr> _keys;

    /// <summary>This op's state: encoded LATEST BY key -> the single row currently retained for it.</summary>
    public Dictionary<string, KeyState> Current { get; } = [];

    public TableLatestByOp(CompiledTablePlan plan, List<Expr> keys)
    {
        _plan = plan;
        _keys = keys;
    }

    public IReadOnlyList<TableDelta> OnBatch(Epoch epoch, IReadOnlyList<TableRowDelta> input)
    {
        var results = new List<TableDelta>();
        foreach (var d in input) results.AddRange(OnDelta(d.Row, d.Weight));
        return results;
    }

    private List<TableDelta> OnDelta(WorkingRow row, long weight)
    {
        var results = new List<TableDelta>();
        var key = EncodeKey(row);

        if (weight > 0)
        {
            // Assertion (weight >1 is deliberately NOT tracked as a multiplicity — see class doc's
            // documented ceiling: this op treats any positive weight as a single assert).
            if (Current.TryGetValue(key, out var existing))
            {
                if (row.Ts < existing.Ts) return results; // strictly older late arrival: ignored, counted nowhere
                results.Add(new TableDelta(ProjectRow(existing.Row), -1));
            }
            Current[key] = new KeyState { Row = row, Ts = row.Ts };
            results.Add(new TableDelta(ProjectRow(row), 1));
            return results;
        }

        // Retraction: only meaningful when it retracts the row THIS op currently holds for the key — see
        // class doc on why any other retraction (of a non-current row) is a no-op here.
        if (Current.TryGetValue(key, out var current) && SameRow(current.Row, row))
        {
            Current.Remove(key);
            results.Add(new TableDelta(ProjectRow(current.Row), -1));
        }
        return results;
    }

    private static bool SameRow(WorkingRow a, WorkingRow b) =>
        JsonText.SerializeCanonicalRow(a.Fields) == JsonText.SerializeCanonicalRow(b.Fields);

    private string EncodeKey(WorkingRow row)
    {
        var ctx = new EvalContext(row, _plan.Bindings);
        var values = _keys.Select(k => ExpressionEvaluator.Eval(k, ctx)).ToArray();
        return TableKeyEncoding.EncodeGroupKey(values);
    }

    private EventRecord ProjectRow(WorkingRow row)
    {
        var evt = new EventRecord();
        var ctx = new EvalContext(row, _plan.Bindings);
        foreach (var item in _plan.Output) evt[item.Name] = ExpressionEvaluator.Eval(item.Expression, ctx);
        evt[EventRecord.TimestampField] = row.Ts;
        evt[EventRecord.SourceField] = _plan.SourceLabel;
        return evt;
    }

    /// <summary>Pass-through — see TableReduceOp's identical OnFrontier doc: no epoch-driven state to flush
    /// today.</summary>
    public IReadOnlyList<TableDelta> OnFrontier(Epoch epoch) => [];
}
