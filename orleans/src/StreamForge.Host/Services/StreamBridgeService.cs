using Microsoft.AspNetCore.SignalR;
using Orleans;
using Orleans.Runtime;
using Orleans.Streams;
using StreamForge.Abstractions;
using StreamForge.Engine;
using StreamForge.Host.Hubs;

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
}
