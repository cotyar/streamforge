using Dapr.Actors;
using Dapr.Actors.Client;
using Dapr.Client;
using StreamForge.Abstractions;
using StreamForge.Dapr.Host.Actors;
using StreamForge.Dapr.Host.Streaming;

namespace StreamForge.Dapr.Host.Lifecycle;

/// <summary>
/// Plan 005 (Dapr sibling runtime) W5-A: replaces <see cref="NoopLifecycleOrchestrator"/> as the real
/// <see cref="ILifecycleOrchestrator"/> once <see cref="GeneratorActor"/> exists — registered in
/// <c>Actors/GeneratorRuntimeSetup.cs</c>'s <c>AddServices</c>, which runs AFTER Program.cs's Noop
/// registration, so this implementation wins.
///
/// <para><b>Sources are real; pipelines/tables/history are still W4's no-op behavior.</b> This wave only
/// builds the generator runtime, so <see cref="StartPipelineAsync"/>/<see cref="StartTableAsync"/>/
/// <see cref="ResetTableHistoryAsync"/>/etc. are copied verbatim from <see cref="NoopLifecycleOrchestrator"/>
/// (log a warning, report success) — W6/W7 replace them; the <see cref="LifecycleOutcome"/> contract is
/// unchanged.</para>
///
/// <para><b>Acyclic by construction (see dapr/ARCHITECTURE.md's reentrancy decision).</b> This class is
/// invoked synchronously, inline, from <see cref="Catalog.CatalogStore"/>'s methods — which themselves run
/// inside <see cref="Actors.RegistryActor"/>'s own (non-reentrant) actor turn. The hazard that decision
/// guards against is a CYCLE: RegistryActor's turn blocked waiting on a call chain that loops back into
/// that same still-in-flight turn. Calling into <see cref="IGeneratorActor"/> from here is NOT such a
/// cycle — <see cref="GeneratorActor"/> never calls RegistryActor, <c>ICatalogFacade</c>, or any other
/// actor (see that class's own doc comment: everything it needs arrives as the <c>StartAsync</c>
/// parameter). It is a pure leaf in the call graph, so a synchronous actor-to-actor call to it can never
/// deadlock — there is nothing to make fire-and-forget here. Awaiting it inline (exactly like Orleans'
/// <c>RegistryGrain</c> awaiting <c>GeneratorGrain.StartAsync</c>/<c>StopAsync</c> directly) is both
/// simpler and sufficient. <b>Rule for whoever builds W6/W7's pipeline/table orchestration on this same
/// seam:</b> that only holds because GeneratorActor is a leaf — a worker actor that reads back from the
/// registry (directly or via a facade) would reintroduce the exact cycle this design avoids, and MUST
/// go through fire-and-forget pub/sub or an equivalent non-blocking path instead.</para>
/// </summary>
public sealed class DaprLifecycleOrchestrator(DaprClient daprClient, ILogger<DaprLifecycleOrchestrator> logger) : ILifecycleOrchestrator
{
    public async Task NotifySourceChangedAsync(SourceDefinition def)
    {
        var actor = GeneratorActorProxy(def.Name);
        if (def.Enabled)
        {
            await actor.StartAsync(def);
        }
        else
        {
            await actor.StopAsync();
        }
    }

    public async Task NotifySourceRemovedAsync(string name)
    {
        await GeneratorActorProxy(name).StopAsync();
    }

    private static IGeneratorActor GeneratorActorProxy(string sourceName) =>
        ActorProxy.Create<IGeneratorActor>(new ActorId(sourceName), nameof(GeneratorActor), ActorProxyDefaults.Options);

    // ------------------------------------------------------------------
    // Pipelines/tables/history: W6/W7 replace these. Logic copied verbatim from
    // NoopLifecycleOrchestrator — see that class's doc comment.
    // ------------------------------------------------------------------

    public Task<LifecycleOutcome> StartPipelineAsync(PipelineDefinition def)
    {
        WarnNoRuntime("StartPipeline", def.Id);
        return Task.FromResult(LifecycleOutcome.Success);
    }

    public Task StopPipelineAsync(string pipelineId)
    {
        WarnNoRuntime("StopPipeline", pipelineId);
        return Task.CompletedTask;
    }

    public Task<LifecycleOutcome> StartTableAsync(TableDefinition def)
    {
        WarnNoRuntime("StartTable", def.Name);
        return Task.FromResult(LifecycleOutcome.Success);
    }

    public Task StopTableAsync(string tableName)
    {
        WarnNoRuntime("StopTable", tableName);
        return Task.CompletedTask;
    }

    public Task ResetTableHistoryAsync(TableDefinition def)
    {
        WarnNoRuntime("ResetTableHistory", def.Name);
        return Task.CompletedTask;
    }

    public Task DisableTableHistoryAsync(string tableName)
    {
        WarnNoRuntime("DisableTableHistory", tableName);
        return Task.CompletedTask;
    }

    private void WarnNoRuntime(string action, string id) =>
        logger.LogWarning("{Action}({Id}): no runtime yet (W6/W7) — catalog status updated, no process started.", action, id);

    /// <summary>Real publish to the <c>sf-lifecycle</c> topic (decision D-D) — the one W4 left as a log
    /// line only (see <see cref="NoopLifecycleOrchestrator.PublishLifecycleAsync"/>). Publishing here is
    /// an outward call to Dapr pub/sub, not an actor call, so it carries none of the reentrancy
    /// considerations discussed in this class's own doc comment.</summary>
    public async Task PublishLifecycleAsync(string entityId, string kind, PipelineStatus status)
    {
        await daprClient.PublishEventAsync(StreamingRuntimeSetup.PubsubName, StreamingRuntimeSetup.LifecycleTopic, new LifecycleEvent
        {
            PipelineId = entityId,
            Kind = kind,
            Status = status,
            TimestampMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
        });
    }
}
