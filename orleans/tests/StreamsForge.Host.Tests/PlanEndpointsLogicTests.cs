using StreamsForge.Abstractions;
using StreamsForge.Api;
using StreamsForge.Engine;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>
/// Plan 008 W5: unit tests for <see cref="PlanEndpointsLogic"/> — the pure projection/degradation logic
/// behind GET /api/pipelines/{id}/plan and GET /api/tables/{id}/plan. No HTTP harness, no grain/actor
/// runtime — mirrors SourcesEndpointsLogicTests' "logic lives in a pure static class, test it directly"
/// convention. Covers: projecting a real TableDataflowPlan into the wire DTO (Physical: true), and every
/// degradation path (Parallelism == 1, Dapr flavor, non-compiling SQL, NotSupportedException from
/// TablePlan.CreateDataflow, and a pipeline — which is always the logical view).
/// </summary>
public class PlanEndpointsLogicTests
{
    private static readonly Dictionary<string, SourceSchema> TradesOnly = new()
    {
        ["trades"] = new SourceSchema("trades", new Dictionary<string, FieldKind>
        {
            ["symbol"] = FieldKind.String,
            ["price"] = FieldKind.Double,
            ["qty"] = FieldKind.Long,
        }),
    };

    private static readonly Dictionary<string, SourceSchema> RefOnly = new()
    {
        ["ref"] = new SourceSchema("ref", new Dictionary<string, FieldKind>
        {
            ["symbol"] = FieldKind.String,
            ["tag"] = FieldKind.String,
        }),
    };

    private const string JoinAggSql = "SELECT t.symbol, SUM(t.qty) AS total FROM trades t JOIN ref r ON t.symbol = r.symbol GROUP BY t.symbol";

    private static TableDefinition TableDef(string sql, int parallelism) => new()
    {
        Id = "tbl1",
        Name = "tbl1",
        Sql = sql,
        Parallelism = parallelism,
    };

    private static PipelineDefinition PipelineDef(string sql) => new()
    {
        Id = "p1",
        Name = "p1",
        Sql = sql,
    };

    // ------------------------------------------------------------------
    // Physical: true — a real TableDataflowPlan gets projected into stages/edges.
    // ------------------------------------------------------------------

    [Fact]
    public void BuildTablePlan_ParallelismFourOnOrleans_ProducesPhysicalPlanWithStagesAndEdges()
    {
        var response = PlanEndpointsLogic.BuildTablePlan(TableDef(JoinAggSql, parallelism: 4), TradesOnly, RefOnly, flavor: "orleans");

        Assert.True(response.Physical);
        Assert.Null(response.UnavailableReason);
        Assert.Equal(4, response.Parallelism);
        Assert.NotNull(response.PlanSummary);
        Assert.Contains("trades", response.Inputs);
        Assert.Contains("ref", response.Inputs);

        Assert.NotEmpty(response.Stages);
        Assert.NotEmpty(response.Edges);

        // Every stage's InEdges reference edge ids that actually exist in the flattened Edges list —
        // proves the projection didn't silently drop/renumber anything relative to TableDataflowPlan.
        var edgeIds = response.Edges.Select(e => e.EdgeId).ToHashSet();
        foreach (var stage in response.Stages)
        {
            foreach (var inEdge in stage.InEdges)
            {
                Assert.Contains(inEdge.EdgeId, edgeIds);
            }
        }

        // Exactly one terminal edge (ToStageId == -1), exactly one external-input edge referencing each
        // real input — mirrors TableDataflowPlan's own documented shape.
        Assert.Single(response.Edges, e => e.ToStageId == -1);
        Assert.Contains(response.Edges, e => e.FromStageId == -1 && e.ExternalInputNames.Contains("trades"));
        Assert.Contains(response.Edges, e => e.FromStageId == -1 && e.ExternalInputNames.Contains("ref"));
    }

    // ------------------------------------------------------------------
    // Degradation: Parallelism == 1.
    // ------------------------------------------------------------------

    [Fact]
    public void BuildTablePlan_ParallelismOne_ReturnsLogicalViewOnly()
    {
        var response = PlanEndpointsLogic.BuildTablePlan(TableDef(JoinAggSql, parallelism: 1), TradesOnly, RefOnly, flavor: "orleans");

        Assert.False(response.Physical);
        Assert.Empty(response.Stages);
        Assert.Empty(response.Edges);
        Assert.Equal(1, response.Parallelism);
        Assert.NotNull(response.PlanSummary);
        Assert.Contains("trades", response.Inputs);
        Assert.Contains("ref", response.Inputs);
        Assert.NotNull(response.UnavailableReason);
        Assert.Contains("Parallelism is 1", response.UnavailableReason);
    }

    // ------------------------------------------------------------------
    // Degradation: Dapr flavor (partitioned execution is Orleans-only, decision D-F).
    // ------------------------------------------------------------------

    [Fact]
    public void BuildTablePlan_DaprFlavor_ReturnsLogicalViewEvenAtParallelismFour()
    {
        var response = PlanEndpointsLogic.BuildTablePlan(TableDef(JoinAggSql, parallelism: 4), TradesOnly, RefOnly, flavor: "dapr");

        Assert.False(response.Physical);
        Assert.Empty(response.Stages);
        Assert.Empty(response.Edges);
        Assert.Equal(4, response.Parallelism);
        Assert.NotNull(response.PlanSummary);
        Assert.NotNull(response.UnavailableReason);
        Assert.Contains("Orleans-only", response.UnavailableReason);
    }

    // ------------------------------------------------------------------
    // Degradation: SQL that doesn't currently compile — 200 with a diagnostics-derived reason, not an
    // error.
    // ------------------------------------------------------------------

    [Fact]
    public void BuildTablePlan_NonCompilingSql_ReturnsDiagnosticsDerivedReason()
    {
        var response = PlanEndpointsLogic.BuildTablePlan(TableDef("SELECT * FROM nonexistent_stream", parallelism: 4), TradesOnly, RefOnly, flavor: "orleans");

        Assert.False(response.Physical);
        Assert.Null(response.PlanSummary);
        Assert.Empty(response.Inputs);
        Assert.Empty(response.Stages);
        Assert.Empty(response.Edges);
        Assert.Equal(4, response.Parallelism);
        Assert.NotNull(response.UnavailableReason);
        Assert.NotEmpty(response.UnavailableReason!);
    }

    // ------------------------------------------------------------------
    // Degradation: TablePlan.CreateDataflow throws NotSupportedException (derived table in FROM
    // position at Parallelism > 1) — must degrade, never propagate/500.
    // ------------------------------------------------------------------

    [Fact]
    public void BuildTablePlan_DerivedTableInFromPositionAtParallelismFour_DegradesInsteadOfThrowing()
    {
        var response = PlanEndpointsLogic.BuildTablePlan(
            TableDef("SELECT x.symbol FROM (SELECT symbol FROM trades) x", parallelism: 4),
            TradesOnly,
            new Dictionary<string, SourceSchema>(),
            flavor: "orleans");

        Assert.False(response.Physical);
        Assert.Empty(response.Stages);
        Assert.Empty(response.Edges);
        // The SQL DOES compile (it's just Parallelism > 1 that's unsupported for this shape) — the
        // logical view (PlanSummary/Inputs) should still be populated, unlike the non-compiling case.
        Assert.NotNull(response.PlanSummary);
        Assert.Contains("trades", response.Inputs);
        Assert.NotNull(response.UnavailableReason);
        Assert.Contains("does not support a partitioned dataflow graph", response.UnavailableReason);
    }

    // ------------------------------------------------------------------
    // Pipelines: always the logical view — no partitioned stage/edge dataflow concept at all.
    // ------------------------------------------------------------------

    [Fact]
    public void BuildPipelinePlan_CompilingSql_ReturnsLogicalViewWithSourceNames()
    {
        var response = PlanEndpointsLogic.BuildPipelinePlan(PipelineDef("SELECT symbol FROM trades"), TradesOnly);

        Assert.False(response.Physical);
        Assert.Empty(response.Stages);
        Assert.Empty(response.Edges);
        Assert.Equal(1, response.Parallelism);
        Assert.NotNull(response.PlanSummary);
        Assert.Equal(["trades"], response.Inputs);
        Assert.NotNull(response.UnavailableReason);
    }

    [Fact]
    public void BuildPipelinePlan_NonCompilingSql_ReturnsDiagnosticsDerivedReason()
    {
        var response = PlanEndpointsLogic.BuildPipelinePlan(PipelineDef("SELECT * FROM nonexistent_stream"), TradesOnly);

        Assert.False(response.Physical);
        Assert.Null(response.PlanSummary);
        Assert.Empty(response.Inputs);
        Assert.NotNull(response.UnavailableReason);
        Assert.NotEmpty(response.UnavailableReason!);
    }
}
