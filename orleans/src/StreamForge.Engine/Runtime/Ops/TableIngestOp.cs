using StreamForge.Engine.Dataflow;

namespace StreamForge.Engine.Runtime.Ops;

/// <summary>
/// Per-input-role delta admission (plan 003 M1's suggested op set: "TableIngestOp (per input: delta
/// admission + weight normalization)"). One instance per (source-or-table, alias) role a table plan
/// references — the same role granularity TableExecutor's `_roles` dispatch table already used pre-M1.
///
/// "Delta admission": tags the incoming row with this role's alias via <see cref="WorkingRow.FromEvent"/>
/// so downstream joins/WHERE/GROUP BY can bind alias-qualified columns.
/// "Weight normalization": a raw stream event has no inherent Z-set weight — TableExecutor's façade
/// wraps it as a <see cref="TableDelta"/> with Weight=1 before calling OnBatch (a stream event always
/// asserts, never retracts); an upstream table's delta already carries its own signed weight and passes
/// through unchanged. Either way, by the time OnBatch sees it, normalization already happened at the
/// call boundary — this op's own job is purely the alias tagging.
///
/// STATE: none. Ingest is a pure per-delta transform with no memory between calls — nothing for M2 to
/// checkpoint beyond "which epoch did this partition last see", which belongs to FrontierTracker, not
/// duplicated here.
/// </summary>
internal sealed class TableIngestOp(string alias) : ITableOp
{
    public string Alias { get; } = alias;

    public IReadOnlyList<TableRowDelta> OnBatch(Epoch epoch, IReadOnlyList<TableDelta> input)
    {
        var results = new List<TableRowDelta>(input.Count);
        foreach (var d in input)
        {
            results.Add(new TableRowDelta(WorkingRow.FromEvent(Alias, d.Row), d.Weight));
        }
        return results;
    }

    /// <summary>Pass-through — see class doc: no state to flush on frontier advance.</summary>
    public IReadOnlyList<TableDelta> OnFrontier(Epoch epoch) => [];
}
