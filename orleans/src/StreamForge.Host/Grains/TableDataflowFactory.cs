using Orleans;
using StreamForge.Abstractions;
using StreamForge.Engine;
using StreamForge.Engine.Dataflow;
using StreamForge.Host.Facades;

namespace StreamForge.Host.Grains;

/// <summary>
/// Plan 003 M2: shared compile-and-build helper for the partitioned grain topology (TableIngestGrain,
/// TableStageGrain, and TableGrain's Parallelism&gt;=2 coordinator mode). Every one of those grains
/// independently calls <see cref="BuildAsync"/> from its own StartAsync rather than receiving a
/// pre-built <see cref="TableDataflowPlan"/> over the wire — compilation is a pure function of
/// (Sql, streamSchemas, tableSchemas), so every grain activation deterministically arrives at the
/// identical stage/edge graph (same stage ids, same EdgeId values, same routing) without needing to
/// serialize Expr-bearing internals across grain boundaries. Mirrors TableGrain.StartAsync's existing
/// schema-building + SqlCompiler.CompileTable call exactly (kept as a near-duplicate there rather than
/// refactored in, to keep the Parallelism==1 path's existing code — and its existing test coverage —
/// completely untouched).
/// </summary>
internal static class TableDataflowFactory
{
    public static async Task<(TableCompileResult Compile, TableDataflowPlan Dataflow)> BuildAsync(IGrainFactory grainFactory, TableDefinition def)
    {
        // Plan 021 D5 — def.Environment, not any ambient (see PipelineGrain.StartAsync's identical
        // comment); every caller here already has `def` in hand.
        var registry = grainFactory.RegistryFor(def.Environment);
        var sources = await registry.GetSourcesAsync();
        var streamSchemas = sources.ToDictionary(
            s => s.Name,
            s => new SourceSchema(s.Name, s.Fields.ToDictionary(f => f.Name, f => MapFieldKind(f.Type))));

        var tables = await registry.GetTablesAsync();
        var tableSchemas = tables
            .Where(t => t.OutputFields.Count > 0)
            .ToDictionary(
                t => t.Name,
                t => new SourceSchema(t.Name, t.OutputFields.ToDictionary(f => f.Name, f => MapFieldKind(f.Type))));

        var compileResult = SqlCompiler.CompileTable(def.Sql, streamSchemas, tableSchemas);
        if (!compileResult.Ok || compileResult.Plan is null)
        {
            var message = string.Join("; ", compileResult.Diagnostics.Select(d => $"{d.Line}:{d.Column} {d.Message}"));
            throw new InvalidOperationException(message);
        }

        var dataflow = compileResult.Plan.CreateDataflow(Math.Max(1, def.Parallelism));
        return (compileResult, dataflow);
    }

    public static FieldKind MapFieldKind(FieldType type) => type switch
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
