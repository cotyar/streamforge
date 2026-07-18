namespace StreamForge.Engine.Runtime.Ops;

/// <summary>
/// The composability contract for a pipeline-mode op-chain (plan 003 M1 Part B: "decompose into ops with
/// `OnEvent(sourceName, EventRecord) -&gt; emitted rows` and `AdvanceWatermark(long) -&gt; emitted rows`
/// composability, such that an entire op-chain can itself be wrapped as a node inside another chain").
///
/// <see cref="PipelineExecutor"/> implements this (see ExecutorImpl.cs's partial class declaration —
/// PublicApi.cs, which owns PipelineExecutor's frozen public signatures, is untouched; a partial class's
/// interface list can be declared on any one of its parts). That is deliberate: the composability plan
/// 004's N1 (derived tables / windows-in-windows) needs — "child chain's emissions become parent's input
/// events" — is ALREADY exactly PipelineExecutor's existing public shape (OnEvent/AdvanceWatermark both
/// return the emitted rows). No new wrapper type is needed to embed one compiled pipeline's executor
/// inside another: feed the inner executor's OnEvent/AdvanceWatermark return values into the outer
/// executor's OnEvent one row at a time, using the inner query's output schema as the outer query's
/// synthetic FROM source. See PipelineComposabilityTests for a hand-built two-level chain proving exactly
/// this (N1's smoke test, built without parser/planner support for a real DerivedTable AST node — see
/// plan 004 N1, "planning: derived node wraps a child operator chain").
///
/// This interface exists as a documentation/design anchor for that seam (so N1's planner code has a
/// named contract to depend on instead of a concrete PipelineExecutor type) — it adds no behavior.
/// </summary>
internal interface IPipelineOpChain
{
    IReadOnlyList<EventRecord> OnEvent(string sourceName, EventRecord evt);

    IReadOnlyList<EventRecord> AdvanceWatermark(long nowMs);
}

/// <summary>The two-input-edge contract every pipeline-mode join-chain stage implements — ordinary
/// WITHIN-buffered interval joins (<see cref="PipelineJoinOp"/>) and plan 004 N2/N3/N4's rolling-snapshot
/// subquery stages (<see cref="PipelineSubqueryOp"/>) alike. Extracted so
/// <see cref="StreamForge.Engine.PipelineExecutor"/>'s `_joins` list can hold either kind uniformly.
/// OnRight/Evict are still called for a snapshot stage (they're part of the shared per-row RIGHT-arrival
/// path an ORDINARY join-position role uses — see ExecutorImpl.ProcessIncomingRow) but always return an
/// empty list there: a snapshot stage's B-side state only ever changes via
/// <see cref="IPipelineSnapshotJoinStage.OnRightBatch"/>, which ExecutorImpl routes a derived subquery
/// role's WHOLE emission batch through instead (see RoleEntry.IsSnapshotJoin) — see
/// PipelineSubqueryOp's class doc for why a snapshot must be replaced from a whole batch, never built up
/// row-by-row through the ordinary per-row OnRight path.</summary>
internal interface IPipelineJoinStage
{
    List<WorkingRow> OnLeft(WorkingRow row);
    List<WorkingRow> OnRight(WorkingRow row);
    List<WorkingRow> Evict(long watermark);
}

/// <summary>Additional contract for plan 004 N2/N3/N4's rolling-snapshot join stages
/// (<see cref="PipelineSubqueryOp"/>) — batch-shaped B-side delivery instead of the ordinary per-row
/// <see cref="IPipelineJoinStage.OnRight"/>. See PipelineSubqueryOp's class doc.</summary>
internal interface IPipelineSnapshotJoinStage : IPipelineJoinStage
{
    void OnRightBatch(IReadOnlyList<WorkingRow> rows);
}
