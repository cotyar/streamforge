using Dapr.Actors;
using StreamsForge.Abstractions;
using StreamsForge.Abstractions.Streaming;

namespace StreamsForge.Dapr.Host.Actors;

/// <summary>Request payload for <see cref="IPipelineActor.StartAsync"/> — a Dapr actor method takes at
/// most one parameter (see <see cref="SetStatusRequest"/>'s doc comment for the same constraint on
/// <see cref="IRegistryActor"/>). <see cref="Sources"/> is every source the catalog currently knows
/// about (not just the ones this pipeline's SQL references) — exactly what
/// <c>PipelineGrain.StartAsync</c> builds schemas from on the Orleans side
/// (<c>IRegistryGrain.GetSourcesAsync()</c>, called from inside the grain itself); here
/// <see cref="Catalog.CatalogStore"/> already has the full list in hand (<c>state.Sources</c>) at every
/// call site, so no lookup back through <see cref="Abstractions.ICatalogFacade"/> is needed (that would
/// be an actor-proxy call back into <c>RegistryActor</c> from inside its own turn — exactly the
/// reentrancy hazard <c>dapr/ARCHITECTURE.md</c>'s reentrancy decision exists to avoid; same rationale as
/// <see cref="Lifecycle.ILifecycleOrchestrator.NotifySourceChangedAsync"/>'s W5-A signature change).</summary>
public sealed record PipelineStartRequest(PipelineDefinition Def, List<SourceDefinition> Sources);

/// <summary>
/// Actor-invocation surface for one running pipeline — actor type "PipelineActor", key = the pipeline's
/// <see cref="PipelineDefinition.Id"/>. Dapr counterpart of Orleans' <c>IPipelineGrain</c>
/// (orleans/src/StreamsForge.Abstractions/GrainInterfaces.cs).
///
/// <para><b>Acyclic by construction (plan 005 W6, mirroring <see cref="IGeneratorActor"/>'s own doc
/// comment):</b> this actor never resolves <see cref="Abstractions.ICatalogFacade"/>, a
/// <c>IRegistryActor</c> proxy, or any other actor — everything it needs arrives via
/// <see cref="StartAsync"/>'s <see cref="PipelineStartRequest"/> or a subsequent
/// <see cref="ProcessEventsAsync"/> call. It only ever talks OUTWARD, to Dapr pub/sub (results +
/// metrics). This is what keeps <c>RegistryActor</c> → <c>Lifecycle.DaprLifecycleOrchestrator</c> →
/// PipelineActor acyclic, exactly like the generator call chain.</para>
///
/// <para><b>Where events come from:</b> unlike Orleans' <c>PipelineGrain</c> (which subscribes its own
/// Orleans stream handles per source name inside <c>StartAsync</c>), Dapr's fixed-topic transport
/// (decision D-D) means this actor never subscribes to anything itself — <c>Streaming/
/// PipelineEventRouter.cs</c> (registered as one more <c>ISourceEventsSink</c> alongside the SignalR
/// bridge) fans <c>sf-sources</c> envelopes out to every pipeline actor whose SQL depends on that source,
/// via <see cref="ProcessEventsAsync"/>.</para>
/// </summary>
public interface IPipelineActor : IActor
{
    /// <summary>(Re)starts this pipeline: compiles <see cref="PipelineStartRequest.Def"/>'s SQL against
    /// <see cref="PipelineStartRequest.Sources"/>' schemas via the shared Engine (same compile path
    /// <c>PipelineGrain.StartAsync</c> uses), replacing any previous executor/timer. On success, arms the
    /// 500ms watermark timer and returns the distinct source names the compiled plan depends on
    /// (<c>CompileResult.SourceNames</c>) — <c>Lifecycle.DaprLifecycleOrchestrator</c> uses this to
    /// register <c>PipelineEventRouter</c>'s routing table without a second, redundant compile. On compile
    /// failure, returns a Failure result (mirroring the actor-boundary "result types, not thrown
    /// exceptions" convention — see <see cref="ActorResult{T}"/>'s class doc) instead of letting an
    /// exception cross the Dapr actor-invocation wire; the pipeline is left/kept stopped.
    ///
    /// <para><b>Plan 025 (PARITY.md D6): this method now also registers
    /// <c>Streaming.PipelineEventRouter</c> and replays its connector-kind sources' recent rows, from
    /// inside its own turn</b> — see <c>PipelineActor.RegisterRouterAndAttachToSourcesAsync</c> for the
    /// protocol. The ordering argument is the same one <see cref="ITableActor.StartAsync"/>'s doc comment
    /// makes: Dapr actors process at most one invocation at a time per actor id (dapr/ARCHITECTURE.md's
    /// reentrancy decision), so anything the freshly-registered router routes to THIS actor id while this
    /// call is still executing is a NEW invocation that Dapr queues behind it — never dropped, never
    /// interleaved with it. That is what lets the replayed rows be guaranteed to reach the executor ahead
    /// of any live row that raced them. <c>Lifecycle.DaprLifecycleOrchestrator.StartPipelineAsync</c> still
    /// registers the router after this returns; that call is now an idempotent duplicate, kept because it
    /// costs nothing and still covers a caller that somehow reaches the router without reaching this
    /// method.</para>
    /// <para>The returned source names are unchanged and still BARE (this pipeline's own compile).</para></summary>
    Task<ActorResult<List<string>>> StartAsync(PipelineStartRequest request);

    /// <summary>Stops the pipeline (unregisters the watermark timer, drops the compiled executor).
    /// Idempotent.</summary>
    Task StopAsync();

    /// <summary>True if this actor currently has a live executor + timer armed (backed by persisted
    /// state — see <see cref="PipelineActor"/>'s class doc). <c>Services.PipelineSupervisorService</c>'s
    /// sweep checks this before deciding whether a catalog-Running pipeline needs a full restart.</summary>
    Task<bool> IsRunningAsync();

    /// <summary>The source names this pipeline is currently compiled against (empty if not running) —
    /// cheap in-memory read, no recompile. Lets <c>Services.PipelineSupervisorService</c> repair
    /// <c>PipelineEventRouter</c>'s routing table for an actor that self-healed on reactivation (see
    /// <see cref="PipelineActor.OnActivateAsync"/>) without forcing a disruptive full restart, which would
    /// discard the actor's in-flight window/join state.</summary>
    Task<List<string>> GetSourceNamesAsync();

    /// <summary>Feeds one batch of raw source events through this pipeline's compiled executor exactly
    /// like <c>PipelineGrain</c>'s per-event stream handler — routed here by
    /// <c>Streaming/PipelineEventRouter.cs</c>, not a direct subscription. A no-op if the pipeline isn't
    /// currently running (e.g. a routing-table entry raced a concurrent <see cref="StopAsync"/>).
    ///
    /// <para><b>JsonElement re-normalization (important — see dapr/ARCHITECTURE.md's serialization
    /// note):</b> <paramref name="envelope"/> crosses the Dapr actor-invocation wire (client proxy → this
    /// actor), which round-trips through System.Text.Json with no static type for
    /// <c>Dictionary&lt;string, object?&gt;</c> values — so even though the router's caller already
    /// normalized this exact envelope once at the <c>sf-sources</c> pub/sub ingress endpoint (decision
    /// D-D), every event dictionary's values come back out as <see cref="System.Text.Json.JsonElement"/>
    /// AGAIN on this side of the actor call. <see cref="PipelineActor"/>'s implementation re-normalizes
    /// before handing rows to the Engine — see <c>dapr/tests/StreamsForge.Dapr.Tests/
    /// PipelineActorProcessEventsWireTests.cs</c> for a round-trip test proving this is not a no-op.</para>
    /// </summary>
    Task ProcessEventsAsync(SourceEventsEnvelope envelope);

    /// <summary>Mirrors <c>PipelineGrain.GetRecentResultsAsync</c> — the last (up to) <c>limit</c> emitted
    /// result rows from an in-memory, bounded ring (capacity 100, same as Orleans). Backs
    /// <c>GET /api/pipelines/{id}/results</c> via <c>Facades.DaprPipelineReadFacade</c>.</summary>
    Task<List<ResultEnvelope>> GetRecentResultsAsync(int limit);

    /// <summary>Mirrors <c>PipelineGrain.GetMetricsAsync</c>. Backs <c>GET /api/pipelines/{id}/metrics</c>
    /// via <c>Facades.DaprPipelineReadFacade</c>.</summary>
    Task<PipelineMetrics> GetMetricsAsync();
}
