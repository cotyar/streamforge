using Dapr.Actors;
using Dapr.Actors.Client;
using StreamsForge.Abstractions.Streaming;
using StreamsForge.Dapr.Host.Actors;

namespace StreamsForge.Dapr.Host.Streaming;

/// <summary>
/// Plan 005 (Dapr sibling runtime) W6: the in-process routing table <c>Sinks.cs</c>'s class doc
/// anticipates ("W6's PipelineActor routes matching sources into SQL execution") — registered as one more
/// <see cref="ISourceEventsSink"/> alongside <see cref="DaprStreamBridge"/>
/// (<see cref="StreamingRuntimeSetup.AddServices"/>), so every <c>sf-sources</c> envelope this process
/// receives is fanned out to it exactly like the bridge.
///
/// <para><b>What it tracks:</b> which pipeline ids are currently subscribed to which source names — a
/// pure in-memory (never persisted) many-to-many index, maintained by
/// <see cref="Lifecycle.DaprLifecycleOrchestrator.StartPipelineAsync"/>/<see cref="Lifecycle.DaprLifecycleOrchestrator.StopPipelineAsync"/>
/// on every explicit start/stop, and repaired by <see cref="Services.PipelineSupervisorService"/>'s sweep
/// for a <see cref="PipelineActor"/> that self-healed on reactivation (see that class's own doc comment)
/// without going through either of those two calls. Being in-memory-only (not Dapr actor state) is
/// deliberate: this table is a pure routing cache — the actor's own persisted state (which pipeline is
/// Running, which sources it depends on) is the single source of truth it gets rebuilt from, exactly the
/// same relationship <c>CatalogStore</c>'s in-memory <c>PipelineDefinition.Status</c> has to whatever a
/// <see cref="PipelineActor"/> is actually doing.</para>
///
/// <para><b>Fan-out, not a direct subscription:</b> unlike <c>PipelineGrain</c> (which subscribes its own
/// Orleans stream handles per source name), this router is what makes the fixed <c>sf-sources</c> topic
/// (decision D-D) behave like a per-source subscription from a <see cref="PipelineActor"/>'s point of
/// view — <see cref="OnSourceEventsAsync"/> forwards the envelope, unmodified, to
/// <see cref="IPipelineActor.ProcessEventsAsync"/> on every pipeline actor registered against that
/// envelope's <see cref="SourceEventsEnvelope.Source"/>.</para>
/// </summary>
public sealed class PipelineEventRouter(ILogger<PipelineEventRouter> logger) : ISourceEventsSink
{
    private readonly object _gate = new();

    /// <summary>sourceName → set of pipeline ids currently subscribed to it.</summary>
    private readonly Dictionary<string, HashSet<string>> _bySource = new(StringComparer.Ordinal);

    /// <summary>pipelineId → set of source names it's currently subscribed to — reverse index so
    /// <see cref="Unregister"/> is O(subscriptions for this pipeline) instead of a full table scan.</summary>
    private readonly Dictionary<string, HashSet<string>> _byPipeline = new(StringComparer.Ordinal);

    /// <summary>Replaces (or creates) <paramref name="pipelineId"/>'s subscription set with exactly
    /// <paramref name="sourceNames"/> — idempotent; safe to call repeatedly with the same set (e.g. a
    /// supervisor sweep re-registering an already-routed pipeline).</summary>
    public void Register(string pipelineId, IReadOnlyList<string> sourceNames)
    {
        lock (_gate)
        {
            UnregisterLocked(pipelineId);

            if (sourceNames.Count == 0)
            {
                return;
            }

            var set = new HashSet<string>(sourceNames, StringComparer.Ordinal);
            _byPipeline[pipelineId] = set;
            foreach (var source in set)
            {
                if (!_bySource.TryGetValue(source, out var pipelines))
                {
                    pipelines = new HashSet<string>(StringComparer.Ordinal);
                    _bySource[source] = pipelines;
                }

                pipelines.Add(pipelineId);
            }
        }
    }

    /// <summary>Removes every subscription for <paramref name="pipelineId"/>. Idempotent — a no-op if it
    /// wasn't registered.</summary>
    public void Unregister(string pipelineId)
    {
        lock (_gate)
        {
            UnregisterLocked(pipelineId);
        }
    }

    private void UnregisterLocked(string pipelineId)
    {
        if (!_byPipeline.Remove(pipelineId, out var sources))
        {
            return;
        }

        foreach (var source in sources)
        {
            if (_bySource.TryGetValue(source, out var pipelines))
            {
                pipelines.Remove(pipelineId);
                if (pipelines.Count == 0)
                {
                    _bySource.Remove(source);
                }
            }
        }
    }

    /// <summary>Point-in-time snapshot of the pipeline ids subscribed to <paramref name="sourceName"/> —
    /// exposed for tests (see dapr/tests/StreamsForge.Dapr.Tests/PipelineEventRouterTests.cs); the
    /// dispatch path below takes its own lock-protected snapshot inline rather than calling this.</summary>
    public IReadOnlyCollection<string> SubscribersOf(string sourceName)
    {
        lock (_gate)
        {
            return _bySource.TryGetValue(sourceName, out var pipelines) ? pipelines.ToList() : [];
        }
    }

    public async Task OnSourceEventsAsync(SourceEventsEnvelope envelope)
    {
        List<string> pipelineIds;
        lock (_gate)
        {
            if (!_bySource.TryGetValue(envelope.Source, out var pipelines) || pipelines.Count == 0)
            {
                return;
            }

            pipelineIds = pipelines.ToList();
        }

        foreach (var pipelineId in pipelineIds)
        {
            try
            {
                var actor = ActorProxy.Create<IPipelineActor>(new ActorId(pipelineId), nameof(PipelineActor), ActorProxyDefaults.Options);
                await actor.ProcessEventsAsync(envelope);
            }
            catch (Exception ex)
            {
                // Best-effort per pipeline, mirroring GeneratorActor's own per-tick try/catch — one
                // misbehaving/unreachable pipeline actor must never stop this batch from reaching the
                // rest, nor tear down the router.
                logger.LogWarning(
                    ex,
                    "PipelineEventRouter: failed to forward {Count} event(s) from source '{Source}' to pipeline '{PipelineId}'.",
                    envelope.Events.Count, envelope.Source, pipelineId);
            }
        }
    }
}
