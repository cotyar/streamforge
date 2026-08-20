using StreamForge.Abstractions;

namespace StreamForge.Dapr.Host.Lifecycle;

/// <summary>
/// Plan 005 (Dapr sibling runtime) W4: every side effect <c>RegistryGrain</c> drives by calling straight
/// into a worker grain (GeneratorGrain.StartAsync/StopAsync, PipelineGrain.StartAsync/StopAsync,
/// TableGrain.StartAsync/StopAsync, ITableHistoryGrain.ResetAsync/DisableAsync, and the lifecycle stream
/// publish) is routed through this seam instead, on the Dapr flavor.
///
/// <para><b>Why a seam and not direct actor-to-actor calls:</b> none of GeneratorActor (W5),
/// PipelineActor (W6), TableActor/TableHistoryActor (W7) exist yet — but more importantly, the REENTRANCY
/// DECISION for this plan (see dapr/ARCHITECTURE.md) is that <c>RegistryActor</c> must never call
/// directly into a worker actor from inside one of its own turns. Orleans' RegistryGrain needed a
/// <c>[MayInterleave]</c> allowlist to avoid exactly this shape of cycle (a worker grain's own StartAsync
/// reads back from RegistryGrain while RegistryGrain's own turn that triggered the start is still
/// in-flight) — Dapr actor turns are similarly non-reentrant by default, and enabling reentrancy is extra
/// configuration surface this plan chooses to avoid entirely (see the ARCHITECTURE.md note) by keeping
/// orchestration ACYCLIC instead: RegistryActor talks to this interface, never to another actor's proxy,
/// so there is no call path back into RegistryActor to deadlock on.</para>
///
/// <para><b>W4 behavior:</b> <see cref="NoopLifecycleOrchestrator"/> logs a warning ("no runtime yet") and
/// reports success for every Start call — catalog status bookkeeping (Running/Stopped) therefore works
/// end-to-end today, it just doesn't yet drive any real generator/pipeline/table process. W5 (generators),
/// W6 (pipelines), W7 (tables/history) replace this with an implementation that publishes to Dapr pub/sub
/// topics and/or invokes the relevant actor via a NON-reentrant path (e.g. fire-and-forget pub/sub message
/// rather than an inline actor-to-actor method call) — RegistryActor and CatalogStore need no further
/// changes when that lands.</para>
/// </summary>
public interface ILifecycleOrchestrator
{
    /// <summary>A source was created/updated — <paramref name="def"/><c>.Enabled</c> mirrors
    /// SourceDefinition.Enabled (true means "a generator should be publishing for this source").
    ///
    /// <para><b>Plan 005 W5-A signature change from W4</b> (was <c>NotifySourceChangedAsync(string name,
    /// bool enabled)</c>): the real Dapr implementation (<c>DaprLifecycleOrchestrator</c>) needs the
    /// source's full generator profile/EventsPerSecond/field schema to start its <c>GeneratorActor</c>,
    /// not just its name and enabled flag. Fetching those INSIDE this call — e.g. via
    /// <c>ICatalogFacade.GetSourceAsync</c>, which on the Dapr flavor resolves to an actor-proxy call back
    /// into <c>RegistryActor</c> — would be a self-call while <c>RegistryActor</c>'s own turn (the one that
    /// invoked this orchestrator method in the first place, from inside <c>CatalogStore.UpsertSourceAsync</c>)
    /// is still in-flight: exactly the reentrancy deadlock dapr/ARCHITECTURE.md's reentrancy decision exists
    /// to prevent (a non-reentrant actor turn can't be re-entered by a call that's waiting on that same turn
    /// to finish). <c>CatalogStore</c> already holds the full definition it was just given, so passing it
    /// through here needs no such call at all.</para></summary>
    Task NotifySourceChangedAsync(SourceDefinition def);

    /// <summary>A source was deleted — stop/tear down its generator, if any.
    ///
    /// <para><b>Plan 021 signature change</b> (added <paramref name="environment"/>): unlike
    /// <see cref="NotifySourceChangedAsync"/>, <c>CatalogStore.DeleteSourceAsync</c> has no definition left
    /// to read an environment off by the time it calls this — the source is already removed from
    /// <c>state.Sources</c>. <c>CatalogStore</c> knows its OWN environment regardless (it is handed one at
    /// construction — see that class's own doc comment), so it passes it through explicitly here instead.
    /// </para></summary>
    Task NotifySourceRemovedAsync(string name, string environment);

    /// <summary>Start (or restart) a pipeline. Mirrors PipelineGrain.StartAsync's outcome contract: success
    /// or a human-readable error CatalogStore turns into Status=Failed/Error (never throws).
    ///
    /// <para><b>Plan 005 W6 signature change</b> (was <c>StartPipelineAsync(PipelineDefinition def)</c>):
    /// the real Dapr implementation (<c>DaprLifecycleOrchestrator</c>) needs every known source's schema
    /// to compile the pipeline's SQL (same schema-building <c>PipelineGrain.StartAsync</c> does via
    /// <c>IRegistryGrain.GetSourcesAsync()</c>) — fetching that INSIDE this call via
    /// <c>ICatalogFacade.GetSourcesAsync</c> would be a self-call while <c>RegistryActor</c>'s own turn
    /// (the one that invoked this orchestrator method from inside <c>CatalogStore</c>) is still in-flight:
    /// the exact reentrancy deadlock this plan's reentrancy decision exists to prevent (same rationale as
    /// <see cref="NotifySourceChangedAsync"/>'s earlier W5-A signature change). <c>CatalogStore</c> already
    /// holds <c>state.Sources</c> in full at every call site, so passing it through needs no such
    /// call.</para></summary>
    Task<LifecycleOutcome> StartPipelineAsync(PipelineDefinition def, IReadOnlyList<SourceDefinition> sources);

    Task StopPipelineAsync(string pipelineId);

    /// <summary>Start (or restart) a table. Mirrors TableGrain.StartAsync's outcome contract.
    ///
    /// <para><b>Plan 005 W7-A signature change from W4</b> (was <c>StartTableAsync(TableDefinition def)</c>):
    /// the real Dapr implementation (<c>DaprLifecycleOrchestrator</c>) needs every known source's AND
    /// table's schema to compile the table's SQL (same schema-building <c>TableGrain.StartClassicAsync</c>
    /// does via <c>IRegistryGrain.GetSourcesAsync()</c>/<c>GetTablesAsync()</c>) — fetching that INSIDE this
    /// call via <c>ICatalogFacade.GetSourcesAsync</c>/<c>GetTablesAsync</c> would be a self-call while
    /// <c>RegistryActor</c>'s own turn (the one that invoked this orchestrator method from inside
    /// <c>CatalogStore</c>) is still in-flight: the exact reentrancy deadlock this plan's reentrancy
    /// decision exists to prevent (same rationale as <see cref="StartPipelineAsync"/>'s earlier W6
    /// signature change). <c>CatalogStore</c> already holds <c>state.Sources</c>/<c>state.Tables</c> in
    /// full at every call site, so passing them through needs no such call.</para></summary>
    Task<LifecycleOutcome> StartTableAsync(TableDefinition def, IReadOnlyList<SourceDefinition> sources, IReadOnlyList<TableDefinition> tables);

    /// <summary>Plan 021 signature change (added <paramref name="environment"/>) — same reason as
    /// <see cref="NotifySourceRemovedAsync"/>: every call site here passes a bare table NAME with no
    /// definition attached, and <c>CatalogStore</c> knows its own environment regardless.</summary>
    Task StopTableAsync(string tableName, string environment);

    /// <summary>Mirrors ITableHistoryGrain.ResetAsync — (re)configure row-history collection for a table
    /// that was just created, or whose SQL/history config just changed.</summary>
    Task ResetTableHistoryAsync(TableDefinition def);

    /// <summary>Mirrors ITableHistoryGrain.DisableAsync — called on table delete. Plan 021 signature
    /// change (added <paramref name="environment"/>) — same reason as <see cref="StopTableAsync"/>.</summary>
    Task DisableTableHistoryAsync(string tableName, string environment);

    /// <summary>Mirrors RegistryGrain.PublishLifecycleAsync (the Orleans "lifecycle" stream). W5 replaces
    /// this with a real publish to the sf-lifecycle pub/sub topic (decision D-D).</summary>
    Task PublishLifecycleAsync(string entityId, string kind, PipelineStatus status);
}

/// <summary>Outcome of a start attempt — mirrors the try/catch-and-record-Failed pattern
/// RegistryGrain.SetPipelineStatusAsync/SetTableStatusAsync use around a worker grain's StartAsync, without
/// requiring the orchestrator to throw across the actor boundary (see CatalogStore's doc comment on why
/// actor-boundary calls prefer result types over thrown exceptions).</summary>
public readonly record struct LifecycleOutcome(bool Ok, string? Error)
{
    public static LifecycleOutcome Success { get; } = new(true, null);
    public static LifecycleOutcome Failure(string error) => new(false, error);
}
