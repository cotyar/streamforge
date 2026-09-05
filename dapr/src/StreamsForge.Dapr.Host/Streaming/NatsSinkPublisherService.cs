using StreamsForge.Abstractions;
using StreamsForge.Abstractions.Streaming;
using StreamsForge.AppCore.Environments;
using StreamsForge.AppCore.Sinks;
using StreamsForge.Dapr.Host.Facades;

namespace StreamsForge.Dapr.Host.Streaming;

/// <summary>
/// Plan 009 B2: Dapr counterpart of orleans/src/StreamsForge.Host/Services/NatsPublisherService.cs — the
/// platform's first outbound sink, fire-and-forget republishing pipeline results / table deltas to NATS
/// for entities with <c>Sinks</c> configured. Read <see cref="DaprStreamBridge"/> first; this class is
/// deliberately structured the same way (BackgroundService that ALSO participates in the fixed-topic
/// fan-out — see Streaming/Sinks.cs's class doc) rather than porting Orleans stream types across, per
/// this wave's brief.
///
/// <para><b>Registration is two different shapes for the two topics it needs</b> (mirrors
/// <see cref="DaprStreamBridge"/>'s own split): registered as an <see cref="ITableDeltaSink"/> for
/// <c>sf-table-delta</c> (fans out through the existing <c>IEnumerable&lt;ITableDeltaSink&gt;</c>
/// mechanism in <see cref="StreamingRuntimeSetup"/> — no endpoint change needed there). There is no
/// equivalent sink interface for <c>sf-pipeline-out</c> yet (nothing needed one before this wave — see
/// Sinks.cs's class doc, which explains why that topic calls <see cref="DaprStreamBridge"/> directly by
/// concrete type instead), so <see cref="StreamingRuntimeSetup.MapTopicEndpoints"/> calls
/// <see cref="OnPipelineResultsAsync"/> on this class directly, one extra line next to its existing
/// <c>bridge.OnPipelineResultsAsync</c> call, rather than introducing a whole new sink interface for a
/// single additional consumer.</para>
///
/// <para><b>Why this is a BackgroundService too (unlike DaprStreamBridge, which is a plain singleton):
/// </b> the fixed-topic handlers above only tell it WHAT arrived, never WHICH sinks (if any) that
/// pipeline/table currently has configured — Dapr's envelopes carry no Sinks field (see
/// <c>PipelineResultsEnvelope</c>/<c>TableDeltaEnvelope</c>), and there is no per-entity subscribe/
/// unsubscribe to piggyback a lookup onto (see <see cref="DaprStreamBridge"/>'s own doc on why this
/// flavor's topics are fixed, not per-entity). So this class independently polls
/// <see cref="ICatalogFacade"/> — same 15s cadence
/// <see cref="StreamsForge.Dapr.Host.Services.PipelineSupervisorService"/>/<c>TableSupervisorService</c>
/// already use for their own catalog sweeps — to keep an in-memory map of entity id/name -&gt; active
/// <see cref="NatsSinkClient"/> list, which the topic handlers then just look up.</para>
///
/// <para><b>Fire-and-forget, honestly.</b> See <see cref="NatsSinkClient"/>'s class doc for the
/// publish-level contract this class relies on (never blocks past its own timeout, never throws) — this
/// class never wraps a <see cref="NatsSinkClient.PublishAsync{T}"/> call in anything extra.</para>
/// </summary>
public sealed class NatsSinkPublisherService(
    ICatalogFacadeFactory catalogFactory,
    IEnvironmentFacade environments,
    IHostApplicationLifetime lifetime,
    ILogger<NatsSinkPublisherService> logger) : BackgroundService, ITableDeltaSink, IPipelineResultsSink
{
    private static readonly TimeSpan RefreshInterval = TimeSpan.FromSeconds(15);

    private sealed record EntrySinks(string Signature, List<ISinkClient> Clients);

    private readonly Dictionary<string, EntrySinks> _pipelineSinks = new();
    private readonly Dictionary<string, EntrySinks> _tableSinks = new();
    private readonly object _gate = new();

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await WaitForApplicationStartedAsync(stoppingToken);

        using var timer = new PeriodicTimer(RefreshInterval);
        do
        {
            try
            {
                await RefreshAsync();
            }
            catch (Exception ex)
            {
                logger.LogDebug(ex, "NatsSinkPublisherService: sink refresh sweep failed (sidecar likely not ready yet) — will retry next tick.");
            }
        } while (await timer.WaitForNextTickAsync(stoppingToken));

        List<ISinkClient> toDispose;
        lock (_gate)
        {
            toDispose = [.. _pipelineSinks.Values.SelectMany(e => e.Clients), .. _tableSinks.Values.SelectMany(e => e.Clients)];
        }

        foreach (var c in toDispose)
        {
            await c.DisposeAsync();
        }
    }

    // Plan 021 D5: iterates every environment — same reasoning as the supervisors (this is a background
    // sweep, EnvironmentAmbient.Current is empty here). Pipeline ids are GUIDs (globally unique already,
    // D3's exception), so _pipelineSinks needs no qualification of its OWN key — but the sweep still has
    // to visit every environment's catalog to find every running pipeline in the first place. Table names
    // DO need qualifying (D6): envelope.Table (OnTableDeltaAsync's lookup key, below) is the qualified
    // name every TableActor stamps on its own published deltas.
    //
    // <b>All environments are flattened into ONE (id/name, sinks) sequence BEFORE calling
    // RefreshGroupAsync</b> — that method's own "stale key" sweep (anything in the map but not in THIS
    // call's `seen` set gets disposed) would otherwise treat environment A's still-running tables as stale
    // the moment environment B's turn through a per-environment loop ran, and tear their sink clients down.
    private async Task RefreshAsync()
    {
        var runningPipelines = new List<(string Id, List<SinkSpec> Sinks)>();
        var runningTables = new List<(string QualifiedName, List<SinkSpec> Sinks)>();

        foreach (var env in await environments.ListAsync())
        {
            var environment = EnvKeys.Normalize(env.Name);
            var catalog = catalogFactory.For(environment);
            var pipelines = await catalog.GetPipelinesAsync();
            var tables = await catalog.GetTablesAsync();

            runningPipelines.AddRange(pipelines.Where(p => p.Status == PipelineStatus.Running).Select(p => (p.Id, p.Sinks)));
            runningTables.AddRange(tables.Where(t => t.Status == PipelineStatus.Running).Select(t => (EnvKeys.Qualify(environment, t.Name), t.Sinks)));
        }

        await RefreshGroupAsync(_pipelineSinks, runningPipelines, "pipeline");
        await RefreshGroupAsync(_tableSinks, runningTables, "table");
    }

    private async Task RefreshGroupAsync(
        Dictionary<string, EntrySinks> map, IEnumerable<(string Key, List<SinkSpec> Sinks)> running, string entityKind)
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var toDisposeOld = new List<ISinkClient>();

        foreach (var (key, sinks) in running)
        {
            var active = SinkSelection.Active(sinks);
            if (active.Count == 0)
            {
                continue;
            }

            seen.Add(key);
            var signature = SinkSelection.Signature(active);

            lock (_gate)
            {
                if (map.TryGetValue(key, out var existing) && existing.Signature == signature)
                {
                    continue;
                }
            }

            // Plan 021 wave 2 — loopback/duplex sinks name a CATALOG ENTITY, so they are read in the
            // environment that authored them; see SinkEnvironmentScoping's class doc for the leak this
            // closes. `key` is already the qualified entity key, so it IS the environment.
            var clients = active.Select(s => NewClient(SinkEnvironmentScoping.Scope(s, EnvKeys.EnvOf(key)), entityKind, key))
                .OfType<ISinkClient>().ToList();
            lock (_gate)
            {
                if (map.TryGetValue(key, out var stale))
                {
                    toDisposeOld.AddRange(stale.Clients);
                }

                map[key] = new EntrySinks(signature, clients);
            }
        }

        lock (_gate)
        {
            foreach (var staleKey in map.Keys.Where(k => !seen.Contains(k)).ToList())
            {
                toDisposeOld.AddRange(map[staleKey].Clients);
                map.Remove(staleKey);
            }
        }

        foreach (var c in toDisposeOld)
        {
            await c.DisposeAsync();
        }
    }

    /// <summary>Plan 010: see the Orleans twin — the sink's KIND selects the client implementation, and
    /// SinkSelection.Active guarantees a registered transport exists for it.
    ///
    /// <para>Plan 016 wave 6: see the Orleans twin's identical doc paragraph — <c>Create</c> can now throw
    /// synchronously for an unresolvable <c>@name</c> endpoint reference, and letting that propagate out of
    /// the bare <c>.Select(...).ToList()</c> below would abort this whole entity-group's refresh instead of
    /// just dropping the one misconfigured sink. Caught here, logged the same way every other sink failure
    /// is, and null filtered out by the caller.</para></summary>
    private ISinkClient? NewClient(SinkSpec spec, string entityKind, string entityName)
    {
        try
        {
            return SinkTransports.Find(spec.Kind)!.Create(spec, entityKind, entityName, (destination, ex) => logger.LogWarning(
                ex,
                "{Kind} sink publish failed for {EntityKind} '{EntityName}' destination '{Destination}' — the {EntityKindRepeat} itself keeps running; this sink is dropping messages until the broker/credentials/destination are fixed.",
                spec.Kind, entityKind, entityName, destination, entityKind));
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "{Kind} sink could not be constructed for {EntityKind} '{EntityName}' — the {EntityKindRepeat} itself keeps running without this sink; it is retried on the next refresh sweep.",
                spec.Kind, entityKind, entityName, entityKind);
            return null;
        }
    }

    // ------------------------------------------------------------------
    // Topic dispatch (called from StreamingRuntimeSetup — see this class's own doc for why the two
    // topics get wired up two different ways).
    // ------------------------------------------------------------------

    /// <summary>Called directly (by concrete type) from the <c>sf-pipeline-out</c> endpoint, alongside
    /// <see cref="DaprStreamBridge.OnPipelineResultsAsync"/>. One NATS message per result row — see
    /// <see cref="NatsPipelineRowMessage"/>'s doc for why per-row rather than per-batch.</summary>
    public async Task OnPipelineResultsAsync(PipelineResultsEnvelope envelope)
    {
        List<ISinkClient>? clients;
        lock (_gate)
        {
            _pipelineSinks.TryGetValue(envelope.PipelineId, out var entry);
            clients = entry?.Clients;
        }

        if (clients is null or { Count: 0 })
        {
            return;
        }

        // Plan 014: the batch survives to the fan-out instead of being taken apart here. For NATS and file
        // clients this is the same calls in a different nesting (see SinkFanout's doc for why that is
        // unobservable); for a client whose delivery unit is a transaction it is the difference between one
        // commit and one per row — and that decision now lives in one place rather than in this method and
        // the three others shaped like it.
        var messages = envelope.Results.Select(result => new NatsPipelineRowMessage
        {
            PipelineId = result.PipelineId,
            Seq = result.Seq,
            TimestampMs = result.TimestampMs,
            Row = result.Row,
        }).ToList();

        await SinkFanout.PublishAllAsync(clients, messages, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Registered as an <see cref="ITableDeltaSink"/> — fanned out to automatically by the
    /// <c>sf-table-delta</c> endpoint alongside <see cref="DaprStreamBridge"/>. One NATS message per
    /// delta entry, all sharing <see cref="TableDeltaEnvelope.Seq"/> (the envelope already carries a
    /// batch seq on this flavor, unlike Orleans — see <see cref="NatsTableDeltaMessage"/>'s doc).</summary>
    public async Task OnTableDeltaAsync(TableDeltaEnvelope envelope)
    {
        List<ISinkClient>? clients;
        lock (_gate)
        {
            _tableSinks.TryGetValue(envelope.Table, out var entry);
            clients = entry?.Clients;
        }

        if (clients is null or { Count: 0 })
        {
            return;
        }

        // Same swap as OnPipelineResultsAsync above, and it matters more here: a delta batch is exactly the
        // set of changes a transactional sink would want to apply atomically.
        var messages = envelope.Deltas.Select(delta => new NatsTableDeltaMessage
        {
            Table = envelope.Table,
            Seq = envelope.Seq,
            Row = delta.Row,
            Weight = delta.Weight,
        }).ToList();

        await SinkFanout.PublishAllAsync(clients, messages, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Small inline equivalent of the Orleans host's internal <c>StartupSignal</c> helper — same
    /// rationale as <see cref="StreamsForge.Dapr.Host.Services.PipelineSupervisorService"/>'s identical
    /// private method (that type is internal to a different assembly).</summary>
    private async Task WaitForApplicationStartedAsync(CancellationToken ct)
    {
        if (lifetime.ApplicationStarted.IsCancellationRequested)
        {
            return;
        }

        var tcs = new TaskCompletionSource();
        await using var registration = lifetime.ApplicationStarted.Register(() => tcs.TrySetResult());
        await tcs.Task.WaitAsync(ct);
    }
}
