using StreamForge.Abstractions;
using StreamForge.Abstractions.Streaming;
using StreamForge.AppCore.Sinks;

namespace StreamForge.Dapr.Host.Streaming;

/// <summary>
/// Plan 009 B2: Dapr counterpart of orleans/src/StreamForge.Host/Services/NatsPublisherService.cs — the
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
/// <see cref="StreamForge.Dapr.Host.Services.PipelineSupervisorService"/>/<c>TableSupervisorService</c>
/// already use for their own catalog sweeps — to keep an in-memory map of entity id/name -&gt; active
/// <see cref="NatsSinkClient"/> list, which the topic handlers then just look up.</para>
///
/// <para><b>Fire-and-forget, honestly.</b> See <see cref="NatsSinkClient"/>'s class doc for the
/// publish-level contract this class relies on (never blocks past its own timeout, never throws) — this
/// class never wraps a <see cref="NatsSinkClient.PublishAsync{T}"/> call in anything extra.</para>
/// </summary>
public sealed class NatsSinkPublisherService(
    ICatalogFacade catalog,
    IHostApplicationLifetime lifetime,
    ILogger<NatsSinkPublisherService> logger) : BackgroundService, ITableDeltaSink
{
    private static readonly TimeSpan RefreshInterval = TimeSpan.FromSeconds(15);

    private sealed record EntrySinks(string Signature, List<NatsSinkClient> Clients);

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

        List<NatsSinkClient> toDispose;
        lock (_gate)
        {
            toDispose = [.. _pipelineSinks.Values.SelectMany(e => e.Clients), .. _tableSinks.Values.SelectMany(e => e.Clients)];
        }

        foreach (var c in toDispose)
        {
            await c.DisposeAsync();
        }
    }

    private async Task RefreshAsync()
    {
        var pipelines = await catalog.GetPipelinesAsync();
        var tables = await catalog.GetTablesAsync();

        await RefreshGroupAsync(
            _pipelineSinks,
            pipelines.Where(p => p.Status == PipelineStatus.Running).Select(p => (p.Id, p.Sinks)),
            "pipeline");

        await RefreshGroupAsync(
            _tableSinks,
            tables.Where(t => t.Status == PipelineStatus.Running).Select(t => (t.Name, t.Sinks)),
            "table");
    }

    private async Task RefreshGroupAsync(
        Dictionary<string, EntrySinks> map, IEnumerable<(string Key, List<SinkSpec> Sinks)> running, string entityKind)
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var toDisposeOld = new List<NatsSinkClient>();

        foreach (var (key, sinks) in running)
        {
            var active = SinkSelection.ActiveNats(sinks);
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

            var clients = active.Select(s => NewClient(s, entityKind, key)).ToList();
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

    private NatsSinkClient NewClient(SinkSpec spec, string entityKind, string entityName) =>
        new(spec.Nats!, entityKind, entityName, (subject, ex) => logger.LogWarning(
            ex,
            "NATS sink publish failed for {EntityKind} '{EntityName}' subject '{Subject}' — the {EntityKindRepeat} itself keeps running; this sink is dropping messages until the broker/credentials/subject are fixed.",
            entityKind, entityName, subject, entityKind));

    // ------------------------------------------------------------------
    // Topic dispatch (called from StreamingRuntimeSetup — see this class's own doc for why the two
    // topics get wired up two different ways).
    // ------------------------------------------------------------------

    /// <summary>Called directly (by concrete type) from the <c>sf-pipeline-out</c> endpoint, alongside
    /// <see cref="DaprStreamBridge.OnPipelineResultsAsync"/>. One NATS message per result row — see
    /// <see cref="NatsPipelineRowMessage"/>'s doc for why per-row rather than per-batch.</summary>
    public async Task OnPipelineResultsAsync(PipelineResultsEnvelope envelope)
    {
        List<NatsSinkClient>? clients;
        lock (_gate)
        {
            _pipelineSinks.TryGetValue(envelope.PipelineId, out var entry);
            clients = entry?.Clients;
        }

        if (clients is null or { Count: 0 })
        {
            return;
        }

        foreach (var result in envelope.Results)
        {
            var message = new NatsPipelineRowMessage
            {
                PipelineId = result.PipelineId,
                Seq = result.Seq,
                TimestampMs = result.TimestampMs,
                Row = result.Row,
            };

            foreach (var client in clients)
            {
                await client.PublishAsync(message, CancellationToken.None).ConfigureAwait(false);
            }
        }
    }

    /// <summary>Registered as an <see cref="ITableDeltaSink"/> — fanned out to automatically by the
    /// <c>sf-table-delta</c> endpoint alongside <see cref="DaprStreamBridge"/>. One NATS message per
    /// delta entry, all sharing <see cref="TableDeltaEnvelope.Seq"/> (the envelope already carries a
    /// batch seq on this flavor, unlike Orleans — see <see cref="NatsTableDeltaMessage"/>'s doc).</summary>
    public async Task OnTableDeltaAsync(TableDeltaEnvelope envelope)
    {
        List<NatsSinkClient>? clients;
        lock (_gate)
        {
            _tableSinks.TryGetValue(envelope.Table, out var entry);
            clients = entry?.Clients;
        }

        if (clients is null or { Count: 0 })
        {
            return;
        }

        foreach (var delta in envelope.Deltas)
        {
            var message = new NatsTableDeltaMessage
            {
                Table = envelope.Table,
                Seq = envelope.Seq,
                Row = delta.Row,
                Weight = delta.Weight,
            };

            foreach (var client in clients)
            {
                await client.PublishAsync(message, CancellationToken.None).ConfigureAwait(false);
            }
        }
    }

    /// <summary>Small inline equivalent of the Orleans host's internal <c>StartupSignal</c> helper — same
    /// rationale as <see cref="StreamForge.Dapr.Host.Services.PipelineSupervisorService"/>'s identical
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
