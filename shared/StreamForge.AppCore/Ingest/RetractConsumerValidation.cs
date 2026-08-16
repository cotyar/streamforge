using StreamForge.Abstractions;
using StreamForge.Engine;

namespace StreamForge.AppCore.Ingest;

/// <summary>
/// Wishlist "explicit key retraction through ingest": the validate-time rule that makes a
/// <c>"_retract": true</c> row (<see cref="IngressRowAcceptance.RetractField"/>) something other than
/// a foot-gun. A retraction only means anything to a <c>LATEST BY</c> table — it is that op, and only
/// that op, that tracks "the last asserted row for a key" (see
/// <see cref="StreamForge.Engine.Runtime.Ops"/>'s TableLatestByOp doc). Every OTHER shape reading the
/// same source directly receives the identical weight -1 delta TableIngestOp produces (it has no way
/// to know which table it feeds — see that op's own doc on why), so this has to be caught before
/// admission, not discovered afterward as a wrong or silently-missing answer.
///
/// Infrastructure-free by design (no grain/actor call, no I/O beyond the two lists the caller already
/// has) — mirrors this folder's own <see cref="IngressAdmission"/> convention: pure rule evaluation
/// over already-fetched data, exhaustively testable with plain POCOs and no cluster/host setup. The
/// one exception to "no I/O" is <see cref="SqlCompiler.CompileTable"/> itself, which is CPU-only (no
/// disk/network) and is the SAME frozen entry point table creation already runs — recompiling here
/// costs one extra parse per retract-flagged push, against however many tables directly read the
/// source, which is the honest price of not having a persisted "is this table LATEST BY" flag
/// (<see cref="TableDefinition"/> keeps OutputFields/StreamInputs/TableInputs from the last successful
/// compile, but not its shape).
/// </summary>
public static class RetractConsumerValidation
{
    /// <summary>Null when a "_retract" row targeting <paramref name="sourceName"/> is safe to admit —
    /// i.e. every RUNNING table whose SQL reads it DIRECTLY is LATEST BY-shaped. Otherwise the name of
    /// the first offending table, for the caller's diagnostic.
    ///
    /// DIRECT consumers only, deliberately: a table two hops away (through an intermediate table) is
    /// reached only if that intermediate table is itself a direct, running consumer — which this
    /// already checks and would already reject if it isn't LATEST BY. A table that is Stopped is
    /// excluded because it is not currently wired to receive this source's live deltas at all (see
    /// TablesEndpoints' Start/Stop semantics) — its shape carries no risk until it starts, at which
    /// point it never sees ingest history anyway (ingest has no replay — see CdcEnvelope's own "honest
    /// limit" doc for the general version of that limitation).
    ///
    /// A table whose SQL no longer compiles against the CURRENT catalog (a since-renamed upstream, a
    /// dropped column) is treated as non-LATEST-BY — rejected, not skipped. "Can't prove this table is
    /// safe" is not the same claim as "this table is safe."</summary>
    public static string? FindNonLatestByConsumer(
        string sourceName,
        IReadOnlyList<SourceDefinition> sources,
        IReadOnlyList<TableDefinition> tables)
    {
        var candidates = tables
            .Where(t => t.Status == PipelineStatus.Running && t.StreamInputs.Contains(sourceName))
            .ToList();
        if (candidates.Count == 0)
        {
            return null; // nothing live is reading this source directly yet — nothing to corrupt
        }

        var streamSchemas = sources.ToDictionary(
            s => s.Name,
            s => new SourceSchema(s.Name, s.Fields.ToDictionary(f => f.Name, f => MapFieldKind(f.Type))));
        var tableSchemas = tables
            .Where(t => t.OutputFields.Count > 0)
            .ToDictionary(
                t => t.Name,
                t => new SourceSchema(t.Name, t.OutputFields.ToDictionary(f => f.Name, f => MapFieldKind(f.Type))));

        foreach (var table in candidates)
        {
            var compiled = SqlCompiler.CompileTable(table.Sql, streamSchemas, tableSchemas);
            if (!IsLatestByShaped(compiled))
            {
                return table.Name;
            }
        }

        return null;
    }

    /// <summary>A LATEST BY plan's <see cref="TableCompileResult.PlanSummary"/> always contains
    /// "LATEST BY " (TablePlanner.BuildPlanSummary appends it iff the parsed query has a LATEST BY
    /// clause); a GROUP BY/aggregate or a plain projection never does — the Validator enforces LATEST
    /// BY and GROUP BY as mutually exclusive, and a plain projection has neither. This is the cheapest
    /// shape signal available from outside StreamForge.Engine's assembly boundary: TablePlan exposes
    /// only <see cref="TablePlan.SupportsRetention"/> (true for LATEST BY AND a plain projection alike
    /// — too coarse) as a public shape predicate; nothing narrower is frozen-contract surface, and
    /// adding one is out of scope for this feature (see PublicApi.cs's own "frozen, do not change
    /// signatures" header).</summary>
    private static bool IsLatestByShaped(TableCompileResult compiled) =>
        compiled.Ok && compiled.PlanSummary is not null && compiled.PlanSummary.Contains("LATEST BY ", StringComparison.Ordinal);

    private static FieldKind MapFieldKind(FieldType type) => type switch
    {
        FieldType.String => FieldKind.String,
        FieldType.Double => FieldKind.Double,
        FieldType.Long => FieldKind.Long,
        FieldType.Bool => FieldKind.Bool,
        FieldType.Timestamp => FieldKind.Timestamp,
        FieldType.Json => FieldKind.Json,
        _ => throw new ArgumentOutOfRangeException(nameof(type), type, "Unknown field type"),
    };
}
