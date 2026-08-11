using StreamForge.Engine.Dataflow;

namespace StreamForge.Engine.Runtime.Ops;

/// <summary>
/// Plan 008 W3: UNION (distinct)'s row-level dedup operator — table mode only (pipeline mode has no Z-set
/// weights to dedup with; see Sql/Validator.cs's PipelineUnionDistinctMessage and DESIGN.md §D11). Sits
/// downstream of a union's own UNION ALL concatenation (see TableExecutorImpl's union-root HandleIncoming
/// path, which feeds every branch's raw emitted <see cref="TableDelta"/>s through this op before folding
/// into the table's consolidated output) and turns "N branches' raw weight-summed deltas for this row" into
/// the presence-flip pair SQL's DISTINCT semantics require: a row's TOTAL running weight across every
/// branch going 0 → positive emits ONE +1 (regardless of how many deltas/branches contributed to that
/// crossing); positive → 0 emits ONE -1. Any delta that changes the row's running weight WITHOUT crossing
/// that boundary (e.g. a second branch's assertion of a row a first branch already asserted — the running
/// weight goes 1 → 2, still positive) emits NOTHING — that is the entire "unique" half of DISTINCT: two
/// branches producing the identical row is still exactly one row in the union's output, not two.
///
/// Modeled directly on <see cref="TableReduceOp"/>'s own zero-crossing TotalWeight transition (see its
/// class doc: "emits a retraction ... followed by an assertion ... when the group's total contributing
/// weight reaches (or starts at) zero, only the retraction is emitted") — this op is exactly that
/// transition with group key = the WHOLE ROW (its canonical serialization — see
/// <see cref="TableExecutor.CanonicalRowKey"/>) and no aggregators to maintain, i.e. TableReduceOp
/// specialized to `GROUP BY *, no SELECT aggregates`.
///
/// PRESENCE RULE: `weight &gt; 0`, matching TableExecutorImpl's own consolidation ledger (a row is
/// user-visible iff its net weight is positive — see ApplyConsolidation's `newWeight &gt; 0` branch), NOT
/// the `weight != 0` rule TableOuterJoinOp/TableSemiAntiOp use for JOIN foreign-key presence — that is a
/// different concept (whether ANY matching right-side row currently exists) from this one (how many
/// branch-rows currently assert this exact output row). Using `&gt; 0` here is what makes an out-of-order
/// retraction (arriving before its matching assertion) self-heal SILENTLY instead of incorrectly flashing a
/// premature assertion: a lone early retraction takes the running weight to -1 (still &lt;= 0, no emission),
/// and the matching assertion that follows nets it back to 0 (still &lt;= 0, no emission) — the row was
/// never actually visible, and this op never claimed otherwise.
///
/// ORDER INDEPENDENCE: <see cref="_weights"/> holds the running weight for EVERY row this op has ever seen a
/// delta for — including rows whose running weight is currently zero or negative (a single dictionary is
/// enough here, unlike TableExecutorImpl's own `_consolidated`/`_debtWeights` split, because nothing outside
/// this op ever reads its state back out the way TableExecutor.Snapshot() must — see that type's doc
/// comment for why THAT split exists). Keeping every seen key means a negative delta arriving before its
/// matching positive one is recorded as debt rather than dropped, so a LATER positive delta nets against it
/// instead of starting over — the same order-independence guarantee TableExecutorImpl's ledger split
/// documents (commit 9de443e), reimplemented here with one dictionary because DISTINCT's own state has no
/// external reader to protect.
/// </summary>
internal sealed class TableDistinctOp : ITableOp
{
    /// <summary>Canonical row key (see <see cref="TableExecutor.CanonicalRowKey"/>) → running total weight
    /// across every delta this op has folded in so far, including keys whose running weight is currently
    /// zero (removed once it nets to exactly zero — no residue for fully-cancelled-out rows) or negative
    /// (an out-of-order retraction's debt — see class doc).</summary>
    private readonly Dictionary<string, long> _weights = [];

    public IReadOnlyList<TableDelta> OnBatch(Epoch epoch, IReadOnlyList<TableDelta> input)
    {
        var results = new List<TableDelta>();
        foreach (var d in input)
        {
            var key = JsonText.SerializeCanonicalRow(d.Row);
            long before = _weights.GetValueOrDefault(key);
            long after = before + d.Weight;

            if (after == 0) _weights.Remove(key);
            else _weights[key] = after;

            bool wasPresent = before > 0;
            bool isPresent = after > 0;
            if (!wasPresent && isPresent) results.Add(new TableDelta(d.Row, 1));
            else if (wasPresent && !isPresent) results.Add(new TableDelta(d.Row, -1));
        }
        return results;
    }

    /// <summary>Pass-through — see TableReduceOp's identical OnFrontier doc: table mode has no epoch-driven
    /// eviction; this op's state (like TableReduceOp's groups) is unbounded and consolidated by design.</summary>
    public IReadOnlyList<TableDelta> OnFrontier(Epoch epoch) => [];
}
