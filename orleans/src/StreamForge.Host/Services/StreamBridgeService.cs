using Microsoft.AspNetCore.SignalR;
using Orleans;
using Orleans.Runtime;
using Orleans.Streams;
using StreamForge.Abstractions;
using StreamForge.Engine;
using StreamForge.Api.Hubs;

namespace StreamForge.Host.Services;

/// <summary>
/// Bridges Orleans streams to SignalR groups: pipeline lifecycle/results/metrics and raw
/// source event relays (sampled to ~20 msg/s per source).
/// </summary>
public sealed class StreamBridgeService(
    IClusterClient client,
    IHubContext<StreamHub> hub,
    IHostApplicationLifetime lifetime) : BackgroundService
{
    private const double SourceRelayMinIntervalMs = 50; // ~20 msg/s cap

    /// <summary>How long a table's deltas accumulate before one `tableDelta` message carries them all.
    /// The engine publishes one list per upstream batch, which for a bulk load (a Monte-Carlo run
    /// pushing tens of thousands of rows) is tens of thousands of publishes — and therefore tens of
    /// thousands of socket frames, each with its own SignalR envelope. That is what puts a browser
    /// minutes behind the engine: the cost is per MESSAGE, not per row. 100 ms is under the client's own
    /// 120 ms render coalescing, so this adds no perceptible latency to a table that is merely ticking,
    /// while collapsing a bulk load into a handful of frames.</summary>
    private const int TableDeltaCoalesceMs = 100;

    /// <summary>Flush early once a table's pending deltas reach this many, so memory stays bounded by
    /// (tables x this) rather than by how fast a producer can outrun a slow client. Deltas are NEVER
    /// dropped to stay under it — a dropped delta silently desynchronises the client's Z-set, which is
    /// far worse than a large frame.</summary>
    private const int TableDeltaCoalesceMaxPending = 20_000;

    private readonly Dictionary<string, StreamSubscriptionHandle<List<ResultEnvelope>>> _pipelineSubs = new();
    private readonly Dictionary<string, StreamSubscriptionHandle<EventRecord>> _sourceSubs = new();
    private readonly Dictionary<string, DateTime> _lastSourceSend = new();
    private readonly Dictionary<string, StreamSubscriptionHandle<List<TableDeltaDto>>> _tableSubs = new();
    private readonly Dictionary<string, long> _tableSeq = new();

    /// <summary>Deltas accumulated for a table since its last send. Guarded by <see cref="_tableGate"/>:
    /// Orleans delivers stream items on its own scheduler and the flush timer fires on a pool thread, so
    /// the two genuinely race — and the ORDER of deltas within a table must survive, since a retraction
    /// that overtakes its assertion corrupts the client's row set.</summary>
    private readonly Dictionary<string, List<TableDeltaDto>> _tablePending = new();
    private readonly Lock _tableGate = new();
    private Timer? _tableFlushTimer;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await StartupSignal.WaitForApplicationStartedAsync(lifetime, stoppingToken);

        var streamProvider = client.GetStreamProvider(StreamConstants.ProviderName);

        _tableFlushTimer = new Timer(
            _ => _ = FlushPendingTableDeltasAsync(),
            state: null,
            dueTime: TimeSpan.FromMilliseconds(TableDeltaCoalesceMs),
            period: TimeSpan.FromMilliseconds(TableDeltaCoalesceMs));

        var lifecycleStream = streamProvider.GetStream<LifecycleEvent>(
            StreamId.Create(StreamConstants.LifecycleNamespace, StreamConstants.LifecycleEventsKey));
        await lifecycleStream.SubscribeAsync(OnLifecycleEventAsync);

        var metricsStream = streamProvider.GetStream<PipelineMetrics>(
            StreamId.Create(StreamConstants.LifecycleNamespace, StreamConstants.MetricsKey));
        await metricsStream.SubscribeAsync(OnMetricsAsync);

        var registry = client.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);

        // Program.cs also kicks off registry seeding (EnsureInitializedAsync) from a fire-and-forget
        // ApplicationStarted callback, racing this service's own ApplicationStarted wait above — on a
        // fresh boot (no persisted catalog yet) this call can easily win the race, i.e. run BEFORE that
        // seeding has populated Sources/Pipelines/Tables and started the already-"Running" grains.
        // EnsureInitializedAsync is idempotent (no-ops once state is non-empty) and Orleans serializes
        // both callers' calls to it (RegistryGrain isn't [MayInterleave] for this method), so awaiting it
        // here — instead of just trusting Program.cs got there first — makes the race harmless: whichever
        // caller's turn runs first does the real seed+start, the other is a fast no-op, and by the time we
        // reach RefreshSourceSubscriptionsAsync/GetPipelinesAsync/GetTablesAsync below the catalog and the
        // Running grains are guaranteed to exist. Without this, Sources eventually self-heal via the 30s
        // refresh loop below, but Pipelines/Tables never do (they're only enumerated once, right here) —
        // so a losing race meant tableDelta/pipelineResult permanently never subscribed, hence never
        // relayed, for any table/pipeline that was already Running at boot.
        await registry.EnsureInitializedAsync();

        await RefreshSourceSubscriptionsAsync(registry);
        foreach (var pipeline in await registry.GetPipelinesAsync())
        {
            if (pipeline.Status == PipelineStatus.Running)
            {
                await SubscribeToPipelineOutputAsync(pipeline.Id);
            }
        }

        foreach (var table in await registry.GetTablesAsync())
        {
            if (table.Status == PipelineStatus.Running)
            {
                await SubscribeToTableOutputAsync(table.Name);
            }
        }

        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(30));
        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            try
            {
                await RefreshSourceSubscriptionsAsync(registry);
            }
            catch
            {
                // best-effort; try again next tick
            }
        }
    }

    private async Task RefreshSourceSubscriptionsAsync(IRegistryGrain registry)
    {
        var sources = await registry.GetSourcesAsync();
        foreach (var src in sources)
        {
            await SubscribeToSourceAsync(src.Name);
        }
    }

    private async Task OnLifecycleEventAsync(LifecycleEvent evt, StreamSequenceToken? token)
    {
        // Table lifecycle events reuse this same stream/type — LifecycleEvent.PipelineId holds the
        // table's Name (its grain key) in that case, not an Id. Kind is prefixed "table-" to disambiguate.
        if (evt.Kind.StartsWith("table-", StringComparison.Ordinal))
        {
            await OnTableLifecycleEventAsync(evt);
            return;
        }

        await hub.Clients.Group($"pipeline:{evt.PipelineId}").SendAsync("pipelineStatus", evt.PipelineId, evt.Status);

        switch (evt.Kind)
        {
            case "started":
                await SubscribeToPipelineOutputAsync(evt.PipelineId);
                break;
            case "stopped":
            case "deleted":
                await UnsubscribeFromPipelineOutputAsync(evt.PipelineId);
                break;
        }
    }

    private async Task OnTableLifecycleEventAsync(LifecycleEvent evt)
    {
        var tableName = evt.PipelineId;
        await hub.Clients.Group($"table:{tableName}").SendAsync("tableStatus", tableName, evt.Status);

        switch (evt.Kind)
        {
            case "table-started":
                await SubscribeToTableOutputAsync(tableName);
                break;
            case "table-stopped":
            case "table-deleted":
                await UnsubscribeFromTableOutputAsync(tableName);
                break;
        }
    }

    private async Task OnMetricsAsync(PipelineMetrics metrics, StreamSequenceToken? token)
    {
        await hub.Clients.Group("metrics").SendAsync("pipelineMetrics", metrics);
    }

    private async Task SubscribeToPipelineOutputAsync(string pipelineId)
    {
        if (_pipelineSubs.ContainsKey(pipelineId))
        {
            return;
        }

        var stream = client.GetStreamProvider(StreamConstants.ProviderName)
            .GetStream<List<ResultEnvelope>>(StreamId.Create(StreamConstants.OutputNamespace, pipelineId));

        var handle = await stream.SubscribeAsync(async (rows, _) =>
            await hub.Clients.Group($"pipeline:{pipelineId}").SendAsync("pipelineResult", pipelineId, rows));

        _pipelineSubs[pipelineId] = handle;
    }

    private async Task UnsubscribeFromPipelineOutputAsync(string pipelineId)
    {
        if (!_pipelineSubs.Remove(pipelineId, out var handle))
        {
            return;
        }

        try
        {
            await handle.UnsubscribeAsync();
        }
        catch
        {
            // best-effort
        }
    }

    private async Task SubscribeToSourceAsync(string name)
    {
        if (_sourceSubs.ContainsKey(name))
        {
            return;
        }

        var stream = client.GetStreamProvider(StreamConstants.ProviderName)
            .GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, name));

        var handle = await stream.SubscribeAsync(async (evt, _) =>
        {
            var now = DateTime.UtcNow;
            if (_lastSourceSend.TryGetValue(name, out var last) &&
                (now - last).TotalMilliseconds < SourceRelayMinIntervalMs)
            {
                return;
            }

            _lastSourceSend[name] = now;
            await hub.Clients.Group($"source:{name}").SendAsync("sourceEvent", name, evt);
        });

        _sourceSubs[name] = handle;
    }

    private async Task SubscribeToTableOutputAsync(string tableName)
    {
        if (_tableSubs.ContainsKey(tableName))
        {
            return;
        }

        var stream = client.GetStreamProvider(StreamConstants.ProviderName)
            .GetStream<List<TableDeltaDto>>(StreamId.Create(StreamConstants.TableDeltaNamespace, tableName));

        var handle = await stream.SubscribeAsync(async (deltas, _) =>
        {
            List<TableDeltaDto>? sendNow = null;
            lock (_tableGate)
            {
                if (!_tablePending.TryGetValue(tableName, out var pending))
                {
                    pending = [];
                    _tablePending[tableName] = pending;
                }
                pending.AddRange(deltas);
                if (pending.Count >= TableDeltaCoalesceMaxPending)
                {
                    sendNow = pending;
                    _tablePending.Remove(tableName);
                }
            }

            // Sending inside the lock would hold it across a network write; the cap path is the only one
            // that sends from this callback at all, and by then the batch is already detached.
            if (sendNow is not null)
            {
                await SendTableDeltasAsync(tableName, sendNow);
            }
        });

        _tableSubs[tableName] = handle;
    }

    /// <summary>Stops the coalescing timer and drains whatever it was holding, so a shutdown does not
    /// silently swallow up to one flush window of deltas from a client that is still connected.</summary>
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_tableFlushTimer is not null)
        {
            await _tableFlushTimer.DisposeAsync();
            _tableFlushTimer = null;
        }
        await FlushPendingTableDeltasAsync();
        await base.StopAsync(cancellationToken);
    }

    /// <summary>One `tableDelta` per (table, flush) rather than per engine publish, with same-key
    /// retract+assert pairs inside that flush window collapsed to their net effect (wishlist item 16's
    /// netting half). Netted BY THE ENGINE'S OWN ROW-IDENTITY RULE
    /// (<see cref="TableExecutor.CanonicalRowKeyOf"/>, the exact canonicalization TableExecutorImpl's own
    /// epoch consolidation keys by — see <see cref="NetByRowIdentity"/>'s own doc for why this bridge does
    /// not invent its own notion of "same row"), never a guess: a bridge-local guess at row identity could
    /// net two rows the engine considers distinct (silently dropping a real change) or fail to net two the
    /// engine considers identical (leaving redundant frames on the wire), and either mistake would quietly
    /// change what the client's Z-set converges to relative to the engine's own state — which is exactly
    /// the failure mode a coalescing layer must never introduce. A batch that nets to nothing (every key's
    /// weight cancelled across the window) sends NOTHING — see <see cref="NetByRowIdentity"/>'s own
    /// call site below.</summary>
    private async Task SendTableDeltasAsync(string tableName, List<TableDeltaDto> deltas)
    {
        var netted = NetByRowIdentity(deltas);
        if (netted.Count == 0)
        {
            // Every key's net weight across this flush window was exactly zero — e.g. a row asserted and
            // then retracted (or the reverse) entirely within the window. Nothing changed as far as any
            // consumer of this table's OUTPUT can tell, so there is nothing to notify: no hub send, no seq
            // bump. This is not the same thing as the "never drop a delta" rule above — that rule is about
            // never losing a delta that carries real information; a net-zero window carries none by
            // construction (the engine's own ConsolidateEpochOutput makes the identical call for exactly
            // this reason, within one epoch — see TableExecutorImpl's own doc comment on it).
            return;
        }

        long seq;
        lock (_tableGate)
        {
            seq = _tableSeq[tableName] = _tableSeq.GetValueOrDefault(tableName) + 1;
        }
        await hub.Clients.Group($"table:{tableName}").SendAsync("tableDelta", tableName, netted, seq);
    }

    /// <summary>Wishlist item 16's netting half. Collapses same-canonical-row entries within ONE flush
    /// window's deltas to their net weight, dropping any key whose net weight is exactly zero — the
    /// identical algorithm <c>TableExecutorImpl.ConsolidateEpochOutput</c> runs for one engine epoch (see
    /// that method's own doc comment: "net every delta … by canonical row key … and drop any key whose net
    /// weight is exactly zero"), applied here to a flush window's worth of already-published
    /// <see cref="TableDeltaDto"/>s instead of one epoch's raw op output. Row identity comes from
    /// <see cref="TableExecutor.CanonicalRowKeyOf"/> — the Engine's own canonicalization, added to
    /// PublicApi.cs specifically because the existing instance-shaped <c>TableExecutor.CanonicalRowKey</c>
    /// needs a live executor (and therefore a compiled <see cref="TablePlan"/>) this bridge has no reason
    /// to build. ORDER-PRESERVING: a key's SURVIVING entry keeps the first occurrence's position and Row/
    /// Evicted content — only its Weight becomes the window's net sum for that key — mirroring
    /// ConsolidateEpochOutput's own "safe to keep whichever literal row instance was seen first for a key"
    /// reasoning, which holds here for the identical reason: two deltas that canonicalize to the same key
    /// are byte-identical in content by construction.
    ///
    /// A CLIENT APPLYING THESE NETTED DELTAS MUST CONVERGE TO EXACTLY THE SAME ROWS AS BEFORE NETTING —
    /// this changes message SIZE, never the Z-set the client ends up with; see
    /// StreamBridgeTableDeltaNettingTests for the proof.</summary>
    public static List<TableDeltaDto> NetByRowIdentity(List<TableDeltaDto> deltas)
    {
        if (deltas.Count <= 1)
        {
            return deltas;
        }

        var netWeight = new Dictionary<string, long>();
        var first = new Dictionary<string, TableDeltaDto>();
        var order = new List<string>();

        foreach (var delta in deltas)
        {
            var key = TableExecutor.CanonicalRowKeyOf(delta.Row);
            if (netWeight.TryGetValue(key, out var existing))
            {
                netWeight[key] = existing + delta.Weight;
            }
            else
            {
                netWeight[key] = delta.Weight;
                first[key] = delta;
                order.Add(key);
            }
        }

        var netted = new List<TableDeltaDto>(order.Count);
        foreach (var key in order)
        {
            var weight = netWeight[key];
            if (weight == 0)
            {
                continue;
            }

            var f = first[key];
            netted.Add(new TableDeltaDto { Row = f.Row, Weight = weight, Evicted = f.Evicted });
        }
        return netted;
    }

    /// <summary>Drains every table's pending deltas. Failures are swallowed per table so one
    /// disconnecting client cannot stop the others being served — the same tolerance the rest of this
    /// bridge applies to hub sends.</summary>
    private async Task FlushPendingTableDeltasAsync()
    {
        List<(string Table, List<TableDeltaDto> Deltas)> batches;
        lock (_tableGate)
        {
            if (_tablePending.Count == 0) return;
            batches = _tablePending.Where(kv => kv.Value.Count > 0)
                .Select(kv => (kv.Key, kv.Value)).ToList();
            foreach (var (table, _) in batches) _tablePending.Remove(table);
        }

        foreach (var (table, deltas) in batches)
        {
            try
            {
                await SendTableDeltasAsync(table, deltas);
            }
            catch (Exception)
            {
                // A hub send can fail for reasons entirely outside this table (a client vanished
                // mid-write). Dropping this batch loses deltas for that table, which is why the cap
                // above exists to keep batches small rather than to make this path safe.
            }
        }
    }

    private async Task UnsubscribeFromTableOutputAsync(string tableName)
    {
        if (!_tableSubs.Remove(tableName, out var handle))
        {
            return;
        }

        try
        {
            await handle.UnsubscribeAsync();
        }
        catch
        {
            // best-effort
        }
    }
}
