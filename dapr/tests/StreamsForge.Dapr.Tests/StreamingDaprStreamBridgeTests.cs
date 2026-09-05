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
    public async Task OnSourceEventsAsync_BatchOfEventsForSameSource_PacesRatherThanDrops_AllThreeRelayedInOrder()
    {
        // Plan 025 D5/D6 (porting Orleans decision D5, plan 023): this used to assert exactly ONE relayed
        // event for a tight batch of 3 — the OLD sampler DROPPED anything arriving under 50ms after the
        // last relay. That read as data loss to an operator watching a source's live tape (a burst is the
        // NORMAL shape of a polled source's tick, not an anomaly), so the sampler now PACES instead: a
        // too-early event waits out the rest of its slot and is still relayed. This test therefore incurs
        // ~150ms of REAL wall time (0 + 50 + 100ms, per SourceRateSampler.MinIntervalMs) — DaprStreamBridge
        // constructs its pacer with the real wall clock (no clock-injection seam on the bridge itself; the
        // pacer's own decision logic is covered clock-free in StreamingSourceRateSamplerTests), so exercising
        // the bridge's actual `await Task.Delay` here is a small, deliberate, one-time real-time cost rather
        // than a flake risk — it has no deadline to race, it always finishes waiting.
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
        Assert.Equal(3, proxy.Sent.Count);
        Assert.Same(events[0], proxy.Sent[0].Args[1]);
        Assert.Same(events[1], proxy.Sent[1].Args[1]);
        Assert.Same(events[2], proxy.Sent[2].Args[1]);
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
    public async Task OnLifecycleEventAsync_SourceStartedKind_SendsNothingToAnyGroup()
    {
        // Plan 025 D6: mirrors Orleans' OnSourceLifecycleEventAsync, whose own doc comment explains why —
        // StreamHub's clients listen for pipelineStatus/tableStatus and there is no sourceStatus
        // counterpart to invent; the console learns a source's state from the REST catalog it already
        // re-reads. Unlike Orleans (which still acts on this event to subscribe/unsubscribe its own
        // per-entity stream handle), this bridge has no per-entity subscription to start or stop in the
        // first place (fixed topics — see the class doc comment), so "source-started"/"source-stopped"
        // are pure no-ops here.
        var (bridge, hub) = NewBridge();
        var evt = new LifecycleEvent { PipelineId = "trades", Kind = "source-started", Status = PipelineStatus.Running };

        await bridge.OnLifecycleEventAsync(evt);

        Assert.Empty(hub.ClientsImpl.GroupProxies);
    }

    [Fact]
    public async Task OnLifecycleEventAsync_SourceStoppedKind_SendsNothingToAnyGroup()
    {
        var (bridge, hub) = NewBridge();
        var evt = new LifecycleEvent { PipelineId = "trades", Kind = "source-stopped", Status = PipelineStatus.Stopped };

        await bridge.OnLifecycleEventAsync(evt);

        Assert.Empty(hub.ClientsImpl.GroupProxies);
    }

    [Fact]
    public async Task OnLifecycleEventAsync_UnrecognisedPrefixedKind_IsIgnored_NeverFallsThroughToPipelineBranch()
    {
        // Models.cs' own doc comment on LifecycleEvent.Kind states the rule explicitly: "a subscriber
        // that does not recognise a prefix must ignore the event, never fall through to the pipeline
        // branch: this list grows additively." A future entity kind sharing this stream (say "widget-")
        // must not be misrouted into the pipeline group just because it isn't "table-"/"source-".
        var (bridge, hub) = NewBridge();
        var evt = new LifecycleEvent { PipelineId = "w1", Kind = "widget-started", Status = PipelineStatus.Running };

        await bridge.OnLifecycleEventAsync(evt);

        Assert.Empty(hub.ClientsImpl.GroupProxies);
    }

    [Fact]
    public async Task OnLifecycleEventAsync_SourceDeleted_ForgetsThisBridgesOwnPacingState()
    {
        // The bridge's per-source pacing state (SourceRateSampler) is keyed by the SAME qualified name a
        // "source-deleted" LifecycleEvent carries in PipelineId (see OnSourceEventsAsync's own comment:
        // envelope.Source is already the qualified key). Mirrors Orleans' UnsubscribeFromSourceAsync
        // clearing _lastSourceSend/_sourcePacedStreak on delete: a source deleted and recreated under the
        // same name must not inherit the deleted one's pacing delay. Proven behaviorally, not by
        // inspecting private state: a second immediate event for "trades" normally owes a real ~50ms
        // delay (SourceRateSampler.MinIntervalMs) before relaying — after a "source-deleted" event clears
        // that state, the NEXT event for the same name relays with no such delay.
        var (bridge, hub) = NewBridge();
        await bridge.OnSourceEventsAsync(new SourceEventsEnvelope { Source = "trades", Events = [new() { ["i"] = 1L }] });

        await bridge.OnLifecycleEventAsync(new LifecycleEvent { PipelineId = "trades", Kind = "source-deleted", Status = PipelineStatus.Stopped });

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        await bridge.OnSourceEventsAsync(new SourceEventsEnvelope { Source = "trades", Events = [new() { ["i"] = 2L }] });
        stopwatch.Stop();

        // Comfortably under the 50ms slot a paced (non-forgotten) call would have to wait out — this is
        // a real-time assertion, but it has no shared deadline to lose a race against: it only has to
        // beat half of a 50ms floor, which an un-paced call clears near-instantly.
        Assert.True(stopwatch.ElapsedMilliseconds < 25, $"expected a forgotten source to relay immediately, took {stopwatch.ElapsedMilliseconds}ms");

        var proxy = hub.ClientsImpl.GroupProxies["source:trades"];
        Assert.Equal(2, proxy.Sent.Count);
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
