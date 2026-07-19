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

    private readonly Dictionary<string, StreamSubscriptionHandle<List<ResultEnvelope>>> _pipelineSubs = new();
    private readonly Dictionary<string, StreamSubscriptionHandle<EventRecord>> _sourceSubs = new();
    private readonly Dictionary<string, DateTime> _lastSourceSend = new();
    private readonly Dictionary<string, StreamSubscriptionHandle<List<TableDeltaDto>>> _tableSubs = new();
    private readonly Dictionary<string, long> _tableSeq = new();

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await StartupSignal.WaitForApplicationStartedAsync(lifetime, stoppingToken);

        var streamProvider = client.GetStreamProvider(StreamConstants.ProviderName);

        var lifecycleStream = streamProvider.GetStream<LifecycleEvent>(
            StreamId.Create(StreamConstants.LifecycleNamespace, StreamConstants.LifecycleEventsKey));
        await lifecycleStream.SubscribeAsync(OnLifecycleEventAsync);

        var metricsStream = streamProvider.GetStream<PipelineMetrics>(
            StreamId.Create(StreamConstants.LifecycleNamespace, StreamConstants.MetricsKey));
        await metricsStream.SubscribeAsync(OnMetricsAsync);

        var registry = client.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);

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
            var seq = _tableSeq[tableName] = _tableSeq.GetValueOrDefault(tableName) + 1;
            await hub.Clients.Group($"table:{tableName}").SendAsync("tableDelta", tableName, deltas, seq);
        });

        _tableSubs[tableName] = handle;
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
