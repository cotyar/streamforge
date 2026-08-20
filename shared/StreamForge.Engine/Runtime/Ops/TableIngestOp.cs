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
/// THE ONE EXCEPTION (wishlist "explicit key retraction through ingest"): a row stamped
/// <see cref="RetractField"/> = true — set by AppCore's <c>IngressRowAcceptance.Accept</c> on a genuine
/// client-pushed ingest row, and (plan 020) by <c>StreamForge.Connectors.Crdt.CrdtProjector</c> on the
/// tombstone it emits when a key leaves a CRDT document, which is the same request made from a source
/// grain rather than a REST push; no table projection ever copies it into its own output, so it still
/// cannot reappear on a table-over-table delta — asks for weight -1 instead of the assert
/// TableExecutorImpl's OnStreamEventCore otherwise hardcodes every stream event to (see PublicApi.cs —
/// that call site is frozen, so this is the earliest point downstream of it that still sees the raw
/// ingest row and can override the weight it arrives with). This is still "weight normalization", just
/// with a second input beside the call boundary: the row's own content. Whether -1 is actually
/// MEANINGFUL for the table this op happens to feed (only a LATEST BY terminal op — TableLatestByOp —
/// knows what "the current row for a key" is) is NOT decided here; this op has no visibility into which
/// op sits downstream of it (TableExecutorImpl builds the chain, and its call site is frozen — see that
/// class's EnsureInit/AddRole). That check runs before admission, at the REST ingest boundary
/// (StreamForge.Api's SourcesEndpoints, backed by AppCore's RetractConsumerValidation) — note that the
/// CRDT path named above does NOT cross that boundary and so is NOT pre-validated for a LATEST BY
/// consumer; it relies entirely on the safety described next, which is why that safety is a contract
/// here and not an implementation detail. This op unconditionally honors the flag for every shape, relying on TableLatestByOp to interpret it
/// correctly and on TableReduceOp's own unmatched-retraction handling to stay safe (never corrupt, at
/// worst under-report) for any other shape a flagged row still reaches.
///
/// STATE: none. Ingest is a pure per-delta transform with no memory between calls — nothing for M2 to
/// checkpoint beyond "which epoch did this partition last see", which belongs to FrontierTracker, not
/// duplicated here.
/// </summary>
internal sealed class TableIngestOp(string alias) : ITableOp
{
    /// <summary>The reserved ingest-row key that flips a row's weight below. Duplicated as a literal
    /// against AppCore's <c>IngressRowAcceptance.RetractField</c> rather than shared as one symbol —
    /// the Engine deliberately does not reference AppCore (AppCore depends on Engine, not the other
    /// way; "the Engine stays pure" — AGENTS.md), and this const isn't part of PublicApi.cs's frozen
    /// contract, so there is no legal cross-assembly handle to share. Same reasoning, same shape, as
    /// FieldValueCoercion.ToFieldKind's "parallel by construction, separate types only because the
    /// Engine deliberately does not depend on Contracts" — if this literal ever needs to change, both
    /// copies change together.</summary>
    internal const string RetractField = "_retract";

    public string Alias { get; } = alias;

    public IReadOnlyList<TableRowDelta> OnBatch(Epoch epoch, IReadOnlyList<TableDelta> input)
    {
        var results = new List<TableRowDelta>(input.Count);
        foreach (var d in input)
        {
            var weight = IsKeyRetraction(d.Row) ? -1L : d.Weight;
            results.Add(new TableRowDelta(WorkingRow.FromEvent(Alias, d.Row), weight));
        }
        return results;
    }

    /// <summary>See class doc's "the one exception". Checked on the raw <see cref="EventRecord"/>
    /// (before <see cref="WorkingRow.FromEvent"/> alias-qualifies it) because the flag's presence, not
    /// its alias, is what decides the weight — WorkingRow.FromEvent copies it through regardless
    /// (it copies every key unconditionally), which is what lets TableLatestByOp find it again under
    /// its alias-qualified name ("{alias}__retract") to distinguish this from an ordinary content-
    /// matched retraction.</summary>
    private static bool IsKeyRetraction(EventRecord row) =>
        row.TryGetValue(RetractField, out var v) && v is true;

    /// <summary>Pass-through — see class doc: no state to flush on frontier advance.</summary>
    public IReadOnlyList<TableDelta> OnFrontier(Epoch epoch) => [];
}
