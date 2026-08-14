using Orleans;
using Orleans.Streams;
using StreamForge.Abstractions;
using StreamForge.AppCore.Sinks;

namespace StreamForge.Host.Services;

/// <summary>
/// Plan 009 B2: the platform's first outbound sink — a SECOND, independent consumer at the exact same
/// seam <see cref="StreamBridgeService"/> already taps (pipeline results, table deltas), fire-and-forget
/// republishing each result row / delta entry to NATS per each Running entity's <c>Sinks</c> list. Read
/// <see cref="StreamBridgeService"/> first: this class mirrors its subscribe/unsubscribe pattern against
/// the SAME Orleans streams, deliberately as a second subscriber (Orleans streams support many
/// independent consumers of one <see cref="StreamId"/>) rather than any change to that file — inserting
/// here means zero changes to grains, actors or the engine, exactly as plan 009's own wording asks for.
///
/// <para><b>Cadence — why polling, not lifecycle events.</b> <see cref="StreamBridgeService"/> subscribes
/// to a pipeline/table's output stream on its "started" lifecycle event and unsubscribes on
/// "stopped"/"deleted" — but a <c>Sinks</c> EDIT (add/remove/reconfigure a sink on an already-Running
/// entity) fires no lifecycle event at all, so that trigger alone would miss "editing a sink must take
/// effect without restarting the host" (a plan 009 B2 requirement). <see cref="StreamBridgeService"/>'s
/// OWN answer to the identical problem on the source side is a 30s <see cref="PeriodicTimer"/> re-scan
/// (<c>RefreshSourceSubscriptionsAsync</c>) — this class reuses that exact cadence for pipelines/tables
/// instead of inventing a new one, and applies it uniformly (not just to sources) since there is no
/// lifecycle signal to lean on here at all.</para>
///
/// <para><b>Fire-and-forget, honestly.</b> See <see cref="NatsSinkClient"/>'s class doc for the
/// publish-level contract (never blocks past its own timeout, never throws). This service's own
/// responsibility is narrower: never let one entity's stream-item processing wait on another's, and
/// never let a stream subscription callback throw (Orleans would otherwise treat that as a delivery
/// failure). A <see cref="NatsSinkClient.PublishAsync{T}"/> call is awaited plainly, with no extra
/// try/catch around it, precisely because it already promises never to throw.</para>
/// </summary>
public sealed class NatsPublisherService(
    IClusterClient client,
    IHostApplicationLifetime lifetime,
    ILogger<NatsPublisherService> logger) : BackgroundService
{
    private static readonly TimeSpan RefreshInterval = TimeSpan.FromSeconds(30);

    private sealed record PipelineSinkState(
        string Signature, List<ISinkClient> Clients, StreamSubscriptionHandle<List<ResultEnvelope>> Handle);

    private sealed record TableSinkState(
        string Signature, List<ISinkClient> Clients, StreamSubscriptionHandle<List<TableDeltaDto>> Handle);

    private readonly Dictionary<string, PipelineSinkState> _pipelineSinks = new();
    private readonly Dictionary<string, TableSinkState> _tableSinks = new();

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await StartupSignal.WaitForApplicationStartedAsync(lifetime, stoppingToken);

        var registry = client.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);

        using var timer = new PeriodicTimer(RefreshInterval);
        do
        {
            try
            {
                await RefreshAsync(registry, stoppingToken);
            }
            catch (Exception ex)
            {
                // Best-effort, mirroring StreamBridgeService's identical refresh-loop try/catch: one bad
                // sweep (e.g. the grain is momentarily unreachable) must not kill this BackgroundService —
                // it retries on the next tick.
                logger.LogDebug(ex, "NatsPublisherService: sink refresh sweep failed — will retry next tick.");
            }
        } while (await timer.WaitForNextTickAsync(stoppingToken));

        // Shutdown: best-effort teardown so no NatsConnection is left dangling.
        foreach (var state in _pipelineSinks.Values)
        {
            await TeardownAsync(state.Clients);
        }

        foreach (var state in _tableSinks.Values)
        {
            await TeardownAsync(state.Clients);
        }
    }

    private async Task RefreshAsync(IRegistryGrain registry, CancellationToken ct)
    {
        var pipelines = await registry.GetPipelinesAsync();
        var tables = await registry.GetTablesAsync();

        await RefreshPipelinesAsync(pipelines, ct);
        await RefreshTablesAsync(tables, ct);
    }

    // ------------------------------------------------------------------
    // Pipelines
    // ------------------------------------------------------------------

    private async Task RefreshPipelinesAsync(List<PipelineDefinition> pipelines, CancellationToken ct)
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var p in pipelines)
        {
            if (p.Status != PipelineStatus.Running)
            {
                continue; // not producing output; any existing subscription is cleaned up in the sweep below.
            }

            var active = SinkSelection.Active(p.Sinks);
            if (active.Count == 0)
            {
                continue;
            }

            seen.Add(p.Id);
            var signature = SinkSelection.Signature(active);
            if (_pipelineSinks.TryGetValue(p.Id, out var existing))
            {
                if (existing.Signature == signature)
                {
                    continue; // unchanged since the last refresh.
                }

                await TeardownPipelineAsync(existing);
            }

            _pipelineSinks[p.Id] = await SubscribePipelineAsync(p.Id, active, signature);
        }

        foreach (var staleId in _pipelineSinks.Keys.Where(id => !seen.Contains(id)).ToList())
        {
            var state = _pipelineSinks[staleId];
            _pipelineSinks.Remove(staleId);
            await TeardownPipelineAsync(state);
        }
    }

    private async Task<PipelineSinkState> SubscribePipelineAsync(string pipelineId, List<SinkSpec> active, string signature)
    {
        var clients = active.Select(s => NewClient(s, "pipeline", pipelineId)).ToList();

        var stream = client.GetStreamProvider(StreamConstants.ProviderName)
            .GetStream<List<ResultEnvelope>>(StreamId.Create(StreamConstants.OutputNamespace, pipelineId));

        var handle = await stream.SubscribeAsync(async (rows, _) =>
        {
            // Plan 014: the delivered batch survives as far as the sink client, instead of being torn back
            // into one publish per row here. Materialized once — SinkFanout walks the list once per client.
            // For NATS and file clients this is the same set of PublishAsync calls in a different nesting
            // order, which is unobservable for them (see SinkFanout's own doc, which argues that at length
            // rather than asserting it); a sink whose delivery unit is a transaction sees one batch.
            var messages = rows.Select(row => new NatsPipelineRowMessage
            {
                PipelineId = row.PipelineId,
                Seq = row.Seq,
                TimestampMs = row.TimestampMs,
                Row = row.Row,
            }).ToList();

            await SinkFanout.PublishAllAsync(clients, messages, CancellationToken.None).ConfigureAwait(false);
        });

        return new PipelineSinkState(signature, clients, handle);
    }

    private static async Task TeardownPipelineAsync(PipelineSinkState state)
    {
        try
        {
            await state.Handle.UnsubscribeAsync();
        }
        catch
        {
            // best-effort, mirrors StreamBridgeService's identical unsubscribe try/catch.
        }

        await TeardownAsync(state.Clients);
    }

    // ------------------------------------------------------------------
    // Tables
    // ------------------------------------------------------------------

    private async Task RefreshTablesAsync(List<TableDefinition> tables, CancellationToken ct)
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var t in tables)
        {
            if (t.Status != PipelineStatus.Running)
            {
                continue;
            }

            var active = SinkSelection.Active(t.Sinks);
            if (active.Count == 0)
            {
                continue;
            }

            seen.Add(t.Name);
            var signature = SinkSelection.Signature(active);
            if (_tableSinks.TryGetValue(t.Name, out var existing))
            {
                if (existing.Signature == signature)
                {
                    continue;
                }

                await TeardownTableAsync(existing);
            }

            _tableSinks[t.Name] = await SubscribeTableAsync(t.Name, active, signature);
        }

        foreach (var staleName in _tableSinks.Keys.Where(name => !seen.Contains(name)).ToList())
        {
            var state = _tableSinks[staleName];
            _tableSinks.Remove(staleName);
            await TeardownTableAsync(state);
        }
    }

    private async Task<TableSinkState> SubscribeTableAsync(string tableName, List<SinkSpec> active, string signature)
    {
        var clients = active.Select(s => NewClient(s, "table", tableName)).ToList();

        var stream = client.GetStreamProvider(StreamConstants.ProviderName)
            .GetStream<List<TableDeltaDto>>(StreamId.Create(StreamConstants.TableDeltaNamespace, tableName));

        // Orleans' table-delta stream item carries no batch sequence number of its own (unlike the Dapr
        // flavor's TableDeltaEnvelope.Seq) — StreamBridgeService invents one client-side per subscription
        // for the exact same reason (see its own _tableSeq field); mirrored here via a boxed counter
        // closed over by the callback below (SubscribeAsync's callback runs on one logical thread per
        // subscription, same non-overlapping-delivery guarantee StreamBridgeService already relies on).
        var seqBox = new long[1];

        var handle = await stream.SubscribeAsync(async (deltas, _) =>
        {
            var seq = ++seqBox[0];

            // Plan 014, as in SubscribePipelineAsync above: one stream item is one batch all the way to the
            // sink client. Every delta in it shares the one invented seq, exactly as before.
            var messages = deltas.Select(delta => new NatsTableDeltaMessage
            {
                Table = tableName,
                Seq = seq,
                Row = delta.Row,
                Weight = delta.Weight,
            }).ToList();

            await SinkFanout.PublishAllAsync(clients, messages, CancellationToken.None).ConfigureAwait(false);
        });

        return new TableSinkState(signature, clients, handle);
    }

    private static async Task TeardownTableAsync(TableSinkState state)
    {
        try
        {
            await state.Handle.UnsubscribeAsync();
        }
        catch
        {
            // best-effort
        }

        await TeardownAsync(state.Clients);
    }

    // ------------------------------------------------------------------
    // Shared
    // ------------------------------------------------------------------

    /// <summary>Plan 010: the sink's KIND decides which client type this is — SinkSelection.Active only
    /// returns specs a registered transport claims, so the lookup below cannot miss.</summary>
    private ISinkClient NewClient(SinkSpec spec, string entityKind, string entityName) =>
        SinkTransports.Find(spec.Kind)!.Create(spec, entityKind, entityName, (destination, ex) => logger.LogWarning(
            ex,
            "{Kind} sink publish failed for {EntityKind} '{EntityName}' destination '{Destination}' — the {EntityKind} itself keeps running; this sink is dropping messages until the broker/credentials/destination are fixed.",
            spec.Kind, entityKind, entityName, destination, entityKind));

    private static async Task TeardownAsync(IEnumerable<ISinkClient> clients)
    {
        foreach (var c in clients)
        {
            await c.DisposeAsync();
        }
    }
}
