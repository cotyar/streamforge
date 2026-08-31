using Orleans;
using Orleans.Streams;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Environments;
using StreamsForge.AppCore.Sinks;
using StreamsForge.Host.Facades;

namespace StreamsForge.Host.Services;

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

        using var timer = new PeriodicTimer(RefreshInterval);
        do
        {
            try
            {
                await RefreshAsync(stoppingToken);
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

    /// <summary>Plan 021 environment strategy: ITERATES EVERY ENVIRONMENT (same reasoning as
    /// GeneratorSupervisorService/IngestDrainPumpService — this is a timer-driven sweep, outside any
    /// request, so the ambient is always empty), aggregated into ONE combined pipelines/tables list before
    /// either Refresh*Async runs below: both methods' own stale-subscription cleanup ("remove anything not
    /// in `seen`") assumes it is being handed the FULL catalog, the same aggregation concern
    /// IngestDrainPumpService's SweepAsync has with its RetainOnly call.</summary>
    private async Task RefreshAsync(CancellationToken ct)
    {
        var environments = await client.GetGrain<IEnvironmentRegistryGrain>(StreamConstants.EnvironmentsKey).ListAsync();

        var pipelines = new List<PipelineDefinition>();
        var tables = new List<TableDefinition>();
        foreach (var env in environments)
        {
            var registry = client.RegistryFor(EnvKeys.Normalize(env.Name));
            pipelines.AddRange(await registry.GetPipelinesAsync());
            tables.AddRange(await registry.GetTablesAsync());
        }

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

            _pipelineSinks[p.Id] = await SubscribePipelineAsync(p.Environment, p.Id, active, signature);
        }

        foreach (var staleId in _pipelineSinks.Keys.Where(id => !seen.Contains(id)).ToList())
        {
            var state = _pipelineSinks[staleId];
            _pipelineSinks.Remove(staleId);
            await TeardownPipelineAsync(state);
        }
    }

    private async Task<PipelineSinkState> SubscribePipelineAsync(string env, string pipelineId, List<SinkSpec> active, string signature)
    {
        // Plan 021 wave 2 — loopback/duplex sinks name a CATALOG ENTITY, so they are read in the
        // environment that authored them; see SinkEnvironmentScoping's class doc for the leak this closes.
        var clients = active.Select(s => NewClient(SinkEnvironmentScoping.Scope(s, env), "pipeline", pipelineId))
            .OfType<ISinkClient>().ToList();

        // Plan 021 D6 — MUST match PipelineGrain's own publish key (this.GetPrimaryKeyString(), i.e.
        // EnvKeys.Qualify(def.Environment, id)) or a non-default environment's pipeline would publish onto
        // a stream this second subscriber never hears.
        var stream = client.GetStreamProvider(StreamConstants.ProviderName)
            .GetStream<List<ResultEnvelope>>(StreamId.Create(StreamConstants.OutputNamespace, EnvKeys.Qualify(env, pipelineId)));

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

            // Plan 021 D3 — unlike a pipeline's GUID id, a table NAME is not globally unique across
            // environments, so the _tableSinks dictionary (and the sweep's `seen` set) must key on the
            // D3-qualified name, or two environments' same-named table would collide on one dictionary
            // entry and one subscription.
            var qualifiedName = EnvKeys.Qualify(t.Environment, t.Name);
            seen.Add(qualifiedName);
            var signature = SinkSelection.Signature(active);
            if (_tableSinks.TryGetValue(qualifiedName, out var existing))
            {
                if (existing.Signature == signature)
                {
                    continue;
                }

                await TeardownTableAsync(existing);
            }

            _tableSinks[qualifiedName] = await SubscribeTableAsync(qualifiedName, t.Name, active, signature);
        }

        foreach (var staleName in _tableSinks.Keys.Where(name => !seen.Contains(name)).ToList())
        {
            var state = _tableSinks[staleName];
            _tableSinks.Remove(staleName);
            await TeardownTableAsync(state);
        }
    }

    private async Task<TableSinkState> SubscribeTableAsync(string qualifiedTableName, string tableName, List<SinkSpec> active, string signature)
    {
        // Plan 021 wave 2 — see the identical call in SubscribePipelineAsync. The environment comes from
        // the qualified key rather than a second parameter: it IS the environment this table lives in.
        var env = EnvKeys.EnvOf(qualifiedTableName);
        var clients = active.Select(s => NewClient(SinkEnvironmentScoping.Scope(s, env), "table", tableName))
            .OfType<ISinkClient>().ToList();

        // Plan 021 D6 — MUST match TableGrain's own publish key (this.GetPrimaryKeyString()) or a
        // non-default environment's table would publish onto a stream this second subscriber never hears.
        var stream = client.GetStreamProvider(StreamConstants.ProviderName)
            .GetStream<List<TableDeltaDto>>(StreamId.Create(StreamConstants.TableDeltaNamespace, qualifiedTableName));

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
    /// returns specs a registered transport claims, so the lookup below cannot miss.
    ///
    /// <para>Plan 016 wave 6: <c>Create</c> can now throw SYNCHRONOUSLY — an <c>@name</c> endpoint
    /// reference this instance has no mapping for (<see cref="StreamsForge.AppCore.Discovery.NamedEndpoints.Resolve"/>,
    /// reached from <see cref="HttpSinkClient"/>'s/<see cref="NatsSinkClient"/>'s constructors). Before this
    /// wave nothing here could throw, so <c>RefreshPipelinesAsync</c>/<c>RefreshTablesAsync</c> called this
    /// inside a bare <c>.Select(...).ToList()</c>: an uncaught throw there would abort the WHOLE refresh
    /// sweep (every other entity's sinks too, not just this one), landing only on this service's own
    /// debug-level "sweep failed, will retry next tick" log — nowhere near this entity's own status. That
    /// is a materially worse outcome than "a broken sink must not break the entity" (this class's own class
    /// doc), so a resolution failure is caught HERE, at the exact same log line and throttling contract as
    /// every other sink failure, and null instead propagates out — the two call sites above filter it with
    /// <c>OfType&lt;ISinkClient&gt;()</c>, so ONE misconfigured sink drops out of an entity's client list
    /// rather than taking the rest of the sweep down with it.
    /// Retried every refresh cycle, same as before.</para></summary>
    private ISinkClient? NewClient(SinkSpec spec, string entityKind, string entityName)
    {
        try
        {
            return SinkTransports.Find(spec.Kind)!.Create(spec, entityKind, entityName, (destination, ex) => logger.LogWarning(
                ex,
                "{Kind} sink publish failed for {EntityKind} '{EntityName}' destination '{Destination}' — the {EntityKind} itself keeps running; this sink is dropping messages until the broker/credentials/destination are fixed.",
                spec.Kind, entityKind, entityName, destination, entityKind));
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "{Kind} sink could not be constructed for {EntityKind} '{EntityName}' — the {EntityKind} itself keeps running without this sink; it is retried on the next refresh sweep.",
                spec.Kind, entityKind, entityName, entityKind);
            return null;
        }
    }

    private static async Task TeardownAsync(IEnumerable<ISinkClient> clients)
    {
        foreach (var c in clients)
        {
            await c.DisposeAsync();
        }
    }
}
