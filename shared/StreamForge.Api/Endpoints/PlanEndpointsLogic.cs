using StreamForge.Abstractions;
using StreamForge.Engine;
using StreamForge.Engine.Dataflow;

namespace StreamForge.Api;

/// <summary>
/// Plan 008 W5: pure projection/degradation logic behind GET /api/pipelines/{id}/plan and
/// GET /api/tables/{id}/plan — kept out of the endpoint handlers (which only do the I/O: resolve the
/// definition, build the schema dictionaries via ICatalogFacade) so it's directly unit-testable without
/// an HTTP harness, mirroring SourceSchemaService/SourceValidation's split from their endpoints.
///
/// Recompiles the SQL fresh on every call (never persists a plan) — same "recompile per request,
/// best-effort" precedent as OrleansArrangementMetaFacade.GetArrangementsAsync
/// (orleans/src/StreamForge.Host/Facades/OrleansFacades.cs), except this always answers (200 +
/// UnavailableReason) rather than silently skipping, since a single entity's plan has nowhere to hide a
/// skip behind.
///
/// A pipeline's plan is ALWAYS the logical view (Physical: false) — <see cref="PipelinePlan"/> has no
/// partitioned stage/edge dataflow concept at all (that's a table-mode-only thing, TableDataflowPlan).
/// A table's plan is physical only when: it compiles, Parallelism &gt;= 2, the caller is the Orleans
/// flavor (partitioned execution is Orleans-only, decision D-F), and TablePlan.CreateDataflow doesn't
/// throw NotSupportedException (a derived table in FROM/JOIN position, or a CROSS join, at
/// Parallelism &gt; 1 — see TableDataflowPlan's class doc).
/// </summary>
public static class PlanEndpointsLogic
{
    /// <summary>Pipelines report Parallelism as 1 in the DTO — there is no partitioned-execution knob
    /// for pipelines at all (unlike tables' real Parallelism field), so this is just the DTO's
    /// "ran as one thing" convention, not a claim about a real setting.</summary>
    private const int PipelineNominalParallelism = 1;

    public static ExecutionPlanResponse BuildPipelinePlan(PipelineDefinition def, IReadOnlyDictionary<string, SourceSchema> streamSchemas)
    {
        var result = SqlCompiler.Compile(def.Sql, streamSchemas);
        if (!result.Ok)
        {
            return new ExecutionPlanResponse(
                PlanSummary: null,
                Inputs: [],
                Stages: [],
                Edges: [],
                Parallelism: PipelineNominalParallelism,
                Physical: false,
                UnavailableReason: DiagnosticsMessageOr(result.Diagnostics, "Pipeline SQL does not currently compile."));
        }

        return new ExecutionPlanResponse(
            result.PlanSummary,
            result.SourceNames.ToList(),
            Stages: [],
            Edges: [],
            Parallelism: PipelineNominalParallelism,
            Physical: false,
            UnavailableReason: "Pipelines execute as a single dataflow chain — there is no partitioned stage/edge graph to report.");
    }

    /// <param name="flavor"><see cref="StreamForgeApiOptions.Flavor"/> ("orleans" | "dapr") — partitioned
    /// execution is Orleans-only (decision D-F); every Dapr table also always has Parallelism == 1 in
    /// practice (CatalogStore.ValidateParallelism rejects anything else), so this parameter is really a
    /// belt-and-braces explicit check rather than a case that can currently arise standalone.</param>
    public static ExecutionPlanResponse BuildTablePlan(
        TableDefinition def,
        IReadOnlyDictionary<string, SourceSchema> streamSchemas,
        IReadOnlyDictionary<string, SourceSchema> tableSchemas,
        string flavor)
    {
        var result = SqlCompiler.CompileTable(def.Sql, streamSchemas, tableSchemas);
        if (!result.Ok || result.Plan is null)
        {
            return new ExecutionPlanResponse(
                PlanSummary: null,
                Inputs: [],
                Stages: [],
                Edges: [],
                Parallelism: def.Parallelism,
                Physical: false,
                UnavailableReason: DiagnosticsMessageOr(result.Diagnostics, "Table SQL does not currently compile."));
        }

        IReadOnlyList<string> inputs = [.. result.StreamInputs, .. result.TableInputs];

        if (def.Parallelism <= 1)
        {
            return new ExecutionPlanResponse(
                result.PlanSummary,
                inputs,
                Stages: [],
                Edges: [],
                Parallelism: def.Parallelism,
                Physical: false,
                UnavailableReason: "Parallelism is 1 — this table runs the classic single-grain path, which has no partitioned stage/edge graph.");
        }

        if (!string.Equals(flavor, "orleans", StringComparison.OrdinalIgnoreCase))
        {
            return new ExecutionPlanResponse(
                result.PlanSummary,
                inputs,
                Stages: [],
                Edges: [],
                Parallelism: def.Parallelism,
                Physical: false,
                UnavailableReason: "Partitioned execution is Orleans-only (decision D-F) — this flavor reports the logical plan only.");
        }

        TableDataflowPlan dataflow;
        try
        {
            dataflow = result.Plan.CreateDataflow(def.Parallelism);
        }
        catch (NotSupportedException ex)
        {
            return new ExecutionPlanResponse(
                result.PlanSummary,
                inputs,
                Stages: [],
                Edges: [],
                Parallelism: def.Parallelism,
                Physical: false,
                UnavailableReason: $"This table's plan shape does not support a partitioned dataflow graph: {ex.Message}");
        }

        return ProjectDataflow(dataflow, result.PlanSummary, inputs);
    }

    /// <summary>Projects a real <see cref="TableDataflowPlan"/> (already built for Parallelism &gt;= 2 on
    /// the Orleans flavor) into the wire DTO — the Physical: true path. Internal (not private) so it's
    /// separately unit-testable against a hand-built TableDataflowPlan without going through a full
    /// TableDefinition/SqlCompiler round trip.</summary>
    internal static ExecutionPlanResponse ProjectDataflow(TableDataflowPlan dataflow, string? planSummary, IReadOnlyList<string> inputs)
    {
        var stages = dataflow.Stages
            .Select(s => new PlanStageDto(
                s.StageId,
                s.Kind.ToString(),
                s.Alias,
                s.InEdges.Select(e => new PlanStageInEdgeDto(e.EdgeId.Value, e.Role)).ToList()))
            .ToList();

        var edges = dataflow.Edges
            .Select(e => new PlanEdgeDto(
                e.EdgeId.Value,
                e.FromStageId,
                e.ToStageId,
                e.Role,
                e.Mode.ToString(),
                e.ExternalInputNames.ToList(),
                e.ArrangeKeyFields?.ToList()))
            .ToList();

        return new ExecutionPlanResponse(planSummary, inputs, stages, edges, dataflow.PartitionCount, Physical: true, UnavailableReason: null);
    }

    private static string DiagnosticsMessageOr(IReadOnlyList<SqlDiagnostic> diagnostics, string fallback)
    {
        var message = string.Join("; ", diagnostics.Select(d => $"{d.Line}:{d.Column} {d.Message}"));
        return string.IsNullOrEmpty(message) ? fallback : message;
    }
}
