using Microsoft.AspNetCore.SignalR;
using StreamsForge.Abstractions;
using StreamsForge.Abstractions.Streaming;
using StreamsForge.Api.Hubs;
using StreamsForge.Dapr.Host.Streaming;
using Xunit;

namespace StreamsForge.Dapr.Tests;

/// <summary>
/// Plan 005 W5-B: proves <see cref="DaprStreamBridge"/> sends the SAME SignalR group name / event name /
/// argument shape as orleans/src/StreamsForge.Host/Services/StreamBridgeService.cs, using a fake
/// <see cref="IHubContext{StreamHub}"/> that records every <c>SendAsync</c> call instead of a real
/// SignalR hub connection — this is a routing/shape test, not a transport test.
/// </summary>
public class StreamingDaprStreamBridgeTests
{
    private sealed record SentMessage(string Method, object?[] Args);

    private sealed class FakeClientProxy : IClientProxy
    {
        public List<SentMessage> Sent { get; } = [];

        public Task SendCoreAsync(string method, object?[] args, CancellationToken cancellationToken = default)
        {
            Sent.Add(new SentMessage(method, args));
            return Task.CompletedTask;
        }
    }

    private sealed class FakeHubClients : IHubClients
    {
        public Dictionary<string, FakeClientProxy> GroupProxies { get; } = new();

        public IClientProxy Group(string groupName)
        {
            if (!GroupProxies.TryGetValue(groupName, out var proxy))
            {
                proxy = new FakeClientProxy();
                GroupProxies[groupName] = proxy;
            }

            return proxy;
        }

        public IClientProxy All => throw new NotSupportedException();
        public IClientProxy AllExcept(IReadOnlyList<string> excludedConnectionIds) => throw new NotSupportedException();
        IClientProxy IHubClients<IClientProxy>.Client(string connectionId) => throw new NotSupportedException();
        public ISingleClientProxy Client(string connectionId) => throw new NotSupportedException();
        public IClientProxy Clients(IReadOnlyList<string> connectionIds) => throw new NotSupportedException();
        public IClientProxy GroupExcept(string groupName, IReadOnlyList<string> excludedConnectionIds) => throw new NotSupportedException();
        public IClientProxy Groups(IReadOnlyList<string> groupNames) => throw new NotSupportedException();
        public IClientProxy OthersInGroup(string groupName) => throw new NotSupportedException();
        public IClientProxy User(string userId) => throw new NotSupportedException();
        public IClientProxy Users(IReadOnlyList<string> userIds) => throw new NotSupportedException();
    }

    private sealed class FakeHubContext : IHubContext<StreamHub>
    {
        public FakeHubClients ClientsImpl { get; } = new();

        IHubClients IHubContext<StreamHub>.Clients => ClientsImpl;

        public IGroupManager Groups => throw new NotSupportedException();
    }

    private static (DaprStreamBridge Bridge, FakeHubContext Hub) NewBridge()
    {
        var hub = new FakeHubContext();
        return (new DaprStreamBridge(hub), hub);
    }

    [Fact]
    public async Task OnSourceEventsAsync_SingleEvent_SendsSourceEventToSourceGroupWithNameAndEventDict()
    {
        var (bridge, hub) = NewBridge();
        var evt = new Dictionary<string, object?> { ["symbol"] = "AAPL", ["price"] = 101.5 };
        var envelope = new SourceEventsEnvelope { Source = "trades", Events = [evt] };

        await bridge.OnSourceEventsAsync(envelope);

        var proxy = Assert.Contains("source:trades", hub.ClientsImpl.GroupProxies);
        var sent = Assert.Single(proxy.Sent);
        Assert.Equal("sourceEvent", sent.Method);
        // Orleans' StreamBridgeService.SubscribeToSourceAsync sends (name, evt) — singular event, not a
        // batch — for each stream item; this asserts the Dapr side matches that exact arg shape per
        // relayed event, even though the wire envelope itself carries a batch.
        Assert.Equal(2, sent.Args.Length);
        Assert.Equal("trades", sent.Args[0]);
        Assert.Same(evt, sent.Args[1]);
    }

    [Fact]
    public async Task OnSourceEventsAsync_BatchOfEventsForSameSource_SamplesPerEventNotPerBatch()
    {
        // A tight synchronous loop over N events for the SAME source executes in sub-millisecond wall
        // time — far under SourceRateSampler's 50ms window — so only the FIRST event in the batch should
        // survive sampling. This demonstrates per-event (not per-batch) sampling deterministically
        // without needing an injected clock: if sampling were applied once per BATCH instead, either all
        // 3 events would be sent, or none would — never exactly 1.
        var (bridge, hub) = NewBridge();
        var events = new List<Dictionary<string, object?>>
        {
            new() { ["i"] = 1L },
            new() { ["i"] = 2L },
            new() { ["i"] = 3L },
        };
        var envelope = new SourceEventsEnvelope { Source = "trades", Events = events };

        await bridge.OnSourceEventsAsync(envelope);

        var proxy = hub.ClientsImpl.GroupProxies["source:trades"];
        var sent = Assert.Single(proxy.Sent);
        Assert.Same(events[0], sent.Args[1]);
    }

    [Fact]
    public async Task OnSourceEventsAsync_DifferentSources_AreSampledIndependently()
    {
        var (bridge, hub) = NewBridge();
        await bridge.OnSourceEventsAsync(new SourceEventsEnvelope { Source = "trades", Events = [new() { ["i"] = 1L }] });
        await bridge.OnSourceEventsAsync(new SourceEventsEnvelope { Source = "quotes", Events = [new() { ["i"] = 1L }] });

        Assert.Single(hub.ClientsImpl.GroupProxies["source:trades"].Sent);
        Assert.Single(hub.ClientsImpl.GroupProxies["source:quotes"].Sent);
    }

    [Fact]
    public async Task OnTableDeltaAsync_SendsTableDeltaToTableGroupWithNameDeltasAndSeq()
    {
        var (bridge, hub) = NewBridge();
        var deltas = new List<TableDeltaDto> { new() { Row = new() { ["symbol"] = "AAPL" }, Weight = 1 } };
        var envelope = new TableDeltaEnvelope { Table = "positions", Seq = 42, Deltas = deltas };

        await bridge.OnTableDeltaAsync(envelope);

        var proxy = Assert.Contains("table:positions", hub.ClientsImpl.GroupProxies);
        var sent = Assert.Single(proxy.Sent);
        Assert.Equal("tableDelta", sent.Method);
        Assert.Equal(3, sent.Args.Length);
        Assert.Equal("positions", sent.Args[0]);
        Assert.Same(deltas, sent.Args[1]);
        Assert.Equal(42L, sent.Args[2]);
    }

    [Fact]
    public async Task OnTableDeltaAsync_RelaysEnvelopeSeqVerbatim_DoesNotInventItsOwnCounter()
    {
        // Unlike Orleans (which maintains its own local _tableSeq counter per subscription), the Dapr
        // envelope already carries the table's own sequence number — the bridge must relay it, not
        // derive a new one, even across repeated calls.
        var (bridge, hub) = NewBridge();
        await bridge.OnTableDeltaAsync(new TableDeltaEnvelope { Table = "positions", Seq = 5 });
        await bridge.OnTableDeltaAsync(new TableDeltaEnvelope { Table = "positions", Seq = 5 });

        var proxy = hub.ClientsImpl.GroupProxies["table:positions"];
        Assert.Equal(2, proxy.Sent.Count);
        Assert.Equal(5L, proxy.Sent[0].Args[2]);
        Assert.Equal(5L, proxy.Sent[1].Args[2]); // same seq relayed twice, not auto-incremented to 6
    }

    [Fact]
    public async Task OnPipelineResultsAsync_SendsPipelineResultToPipelineGroupWithIdAndResultsBatch()
    {
        var (bridge, hub) = NewBridge();
        var results = new List<ResultEnvelope> { new() { PipelineId = "p1", Seq = 1, Row = new() { ["total"] = 42L } } };
        var envelope = new PipelineResultsEnvelope { PipelineId = "p1", Results = results };

        await bridge.OnPipelineResultsAsync(envelope);

        var proxy = Assert.Contains("pipeline:p1", hub.ClientsImpl.GroupProxies);
        var sent = Assert.Single(proxy.Sent);
        Assert.Equal("pipelineResult", sent.Method);
        Assert.Equal(2, sent.Args.Length);
        Assert.Equal("p1", sent.Args[0]);
        Assert.Same(results, sent.Args[1]);
    }

    [Fact]
    public async Task OnLifecycleEventAsync_PipelineKind_SendsPipelineStatusToPipelineGroup()
    {
        var (bridge, hub) = NewBridge();
        var evt = new LifecycleEvent { PipelineId = "p1", Kind = "started", Status = PipelineStatus.Running };

        await bridge.OnLifecycleEventAsync(evt);

        var proxy = Assert.Contains("pipeline:p1", hub.ClientsImpl.GroupProxies);
        var sent = Assert.Single(proxy.Sent);
        Assert.Equal("pipelineStatus", sent.Method);
        Assert.Equal(["p1", PipelineStatus.Running], sent.Args);
    }

    [Fact]
    public async Task OnLifecycleEventAsync_TableKind_SendsTableStatusToTableGroup_UsingPipelineIdFieldAsTableName()
    {
        // Mirrors StreamBridgeService.OnTableLifecycleEventAsync's documented reuse: table lifecycle
        // events share LifecycleEvent with pipeline ones, "table-" prefixed Kind disambiguates, and the
        // PipelineId field carries the table's Name (its actor id) in that case.
        var (bridge, hub) = NewBridge();
        var evt = new LifecycleEvent { PipelineId = "positions", Kind = "table-started", Status = PipelineStatus.Running };

        await bridge.OnLifecycleEventAsync(evt);

        var proxy = Assert.Contains("table:positions", hub.ClientsImpl.GroupProxies);
        var sent = Assert.Single(proxy.Sent);
        Assert.Equal("tableStatus", sent.Method);
        Assert.Equal(["positions", PipelineStatus.Running], sent.Args);
        Assert.DoesNotContain("pipeline:positions", hub.ClientsImpl.GroupProxies);
    }

    [Fact]
    public async Task OnMetricsAsync_SendsPipelineMetricsToMetricsGroup()
    {
        var (bridge, hub) = NewBridge();
        var metrics = new PipelineMetrics { PipelineId = "p1", TotalEventsIn = 100 };

        await bridge.OnMetricsAsync(metrics);

        var proxy = Assert.Contains("metrics", hub.ClientsImpl.GroupProxies);
        var sent = Assert.Single(proxy.Sent);
        Assert.Equal("pipelineMetrics", sent.Method);
        Assert.Same(metrics, Assert.Single(sent.Args));
    }
}
