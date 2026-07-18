using StreamForge.Engine.Dataflow;

namespace StreamForge.Engine.Runtime.Ops;

/// <summary>
/// The internal wire type between table-mode ops once a delta has been admitted (see
/// <see cref="TableIngestOp"/>): a <see cref="WorkingRow"/> (alias-qualified, join/WHERE/GROUP-BY-ready)
/// paired with its Z-set weight.
///
/// This is DELIBERATELY not the public <see cref="TableDelta"/> (EventRecord-based) type for interior
/// edges — see plans/003-materialize-territory.md M1 acceptance and the M1 task's suggested op signature
/// `OnBatch(Epoch, IReadOnlyList&lt;TableDelta&gt;)`. TableIngestOp's input and TableReduceOp's/
/// TableFilterProjectOp's terminal output ARE exactly that public shape (see their OnBatch signatures) —
/// those are the true plan-facing boundaries (ingest admission, final output emission). But a join's ON
/// clause and a GROUP BY's key expression are bound against alias-qualified columns (e.g. "t_symbol" vs
/// "r_symbol" — see WorkingRow's doc comment), which only WorkingRow carries; collapsing interior edges
/// to plain EventRecord would mean re-deriving alias-qualification from field-name string conventions at
/// every stage, a correctness risk this refactor's hard constraint (byte-for-byte behavior, the whole
/// existing suite as regression net) makes not worth taking. Documented here as the one deliberate,
/// disclosed deviation from the letter of the M1 task's op signature — see the M1 completion report's
/// residue section for the full reasoning.
/// </summary>
internal readonly record struct TableRowDelta(WorkingRow Row, long Weight);

/// <summary>
/// Frontier hook every table-mode op implements. Table mode has no epoch/watermark-driven eviction today
/// (plan 003: "no WITHIN eviction — state is unbounded and consolidated" for joins; GROUP BY groups "live
/// forever, updated by every contributing delta" — see TableGroupOperator's original doc comment, now
/// TableReduceOp). Every op below therefore returns an empty list — a real, tested, documented pass-
/// through — from OnFrontier. The hook exists per plan 003 M1 ("most table ops are epoch-agnostic today —
/// that's fine, the hook exists for M2/M4") so M2 (partition frontier propagation) and M4 (frontier-
/// consistent reads, EMIT FINAL close-on-frontier for table variants) have somewhere to hang real
/// behavior without another signature-breaking refactor.
/// </summary>
internal interface ITableOp
{
    IReadOnlyList<TableDelta> OnFrontier(Epoch epoch);
}
