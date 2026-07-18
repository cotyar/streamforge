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
