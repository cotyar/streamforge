using System.Diagnostics;
using System.Runtime.CompilerServices;
using StreamForge.Abstractions;
using StreamForge.AppCore.Sinks;
using StreamForge.AppCore.Transports;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// Plan 019 D2 (wave 019-B): unit tests for <see cref="DuplexSinkTransport"/>/<see cref="DuplexSinkClient"/>
/// — the stateless proxy that reaches a duplex session's outbound half by NAME through
/// <see cref="DuplexSessions"/> rather than opening a connection of its own. No mocking framework, matching
/// this repo's convention (see <see cref="TransportRegistryTests"/>'s <c>FizzTransport</c>,
/// <see cref="DuplexTransportRegistryTests"/>'s <c>FluxTransport</c>) — a hand-written <see cref="FakeSession"/>
/// stands in for a real <c>fix</c> session.
///
/// <para>Every test that publishes into <see cref="DuplexSessions"/> withdraws again in a <c>finally</c> —
/// that registry is process-global, and xUnit does not guarantee this class's methods never interleave with
/// another test class's.</para>
/// </summary>
public class DuplexSinkTests
{
    // ------------------------------------------------------------------
    // Fake session.
    // ------------------------------------------------------------------

    /// <summary>Stands in for a real duplex session (<c>FixDuplexTransport</c>'s, once wave 019-E lands).
    /// <see cref="OnSend"/> lets a test script exactly what one <see cref="SendAsync"/> call returns, or
    /// makes it hang / throw, without a second fake class per scenario.</summary>
    private sealed class FakeSession : IDuplexSession
    {
        public bool IsReady { get; set; } = true;
        public List<IReadOnlyList<Dictionary<string, object?>>> Sends { get; } = [];
        public Func<IReadOnlyList<Dictionary<string, object?>>, CancellationToken, Task<DuplexSendOutcome>>? OnSend { get; set; }

        public async IAsyncEnumerable<InboundMessage> SubscribeAsync([EnumeratorCancellation] CancellationToken ct)
        {
            // Never yields, never completes cleanly — this fake's inbound half is not under test here;
            // DuplexTransportRegistryTests already covers SubscriberCore driving a duplex session's inbound
            // half. Blocking (honoring ct) matches that file's fakes' own documented discipline.
            await Task.Delay(Timeout.Infinite, ct);
            yield break;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        public Task<DuplexSendOutcome> SendAsync(IReadOnlyList<Dictionary<string, object?>> rows, CancellationToken ct)
        {
            Sends.Add(rows);
            return OnSend is not null
                ? OnSend(rows, ct)
                : Task.FromResult(new DuplexSendOutcome(rows.Count, 0, []));
        }
    }

    /// <summary>Publishes <paramref name="session"/> under <paramref name="sourceName"/> and returns an
    /// <see cref="IDisposable"/> that withdraws it again — the RAII-ish cleanup every test below uses so a
    /// failure mid-test still leaves <see cref="DuplexSessions"/> clean for whichever test runs next.</summary>
    private static IDisposable Published(string sourceName, IDuplexSession session)
    {
        DuplexSessions.Publish(sourceName, session);
        return new Withdrawer(sourceName, session);
    }

    private sealed class Withdrawer(string sourceName, IDuplexSession session) : IDisposable
    {
        public void Dispose() => DuplexSessions.Withdraw(sourceName, session);
    }

    private static DuplexSinkClient Client(string sourceName, string entityName = "e1", Action<string, Exception>? onFailure = null) =>
        new(new DuplexSinkConfig { SourceName = sourceName }, "table", entityName, onFailure);

    private static NatsPipelineRowMessage Row(string id) =>
        new() { Row = new Dictionary<string, object?> { ["id"] = id } };

    // ------------------------------------------------------------------
    // A batch reaches a published session's SendAsync in one call.
    // ------------------------------------------------------------------

    [Fact]
    public async Task PublishBatchAsync_ReachesThePublishedSessionAsOneSendAsyncCall()
    {
        const string source = "dsx-happy";
        var session = new FakeSession();
        using var _ = Published(source, session);
        var client = Client(source);

        var payloads = new List<NatsPipelineRowMessage> { Row("a"), Row("b"), Row("c") };
        await client.PublishBatchAsync(payloads, CancellationToken.None);

        var send = Assert.Single(session.Sends);
        Assert.Equal(["a", "b", "c"], send.Select(r => r["id"]));
        Assert.Equal(3, client.Counters.Published);
        Assert.Equal(0, client.Counters.Failed);
    }

    [Fact]
    public async Task PublishAsync_SingleMessage_IsABatchOfOneToTheSameSession()
    {
        const string source = "dsx-single";
        var session = new FakeSession();
        using var _ = Published(source, session);
        var client = Client(source);

        await client.PublishAsync(Row("solo"), CancellationToken.None);

        var send = Assert.Single(session.Sends);
        var row = Assert.Single(send);
        Assert.Equal("solo", row["id"]);
        Assert.Equal(1, client.Counters.Published);
    }

    // ------------------------------------------------------------------
    // A partial DuplexSendOutcome increments the failure counter and surfaces the identified failure.
    // ------------------------------------------------------------------

    [Fact]
    public async Task PartialOutcome_IncrementsFailedAndNamesTheRejectedRow()
    {
        const string source = "dsx-partial";
        var session = new FakeSession
        {
            OnSend = (rows, _) => Task.FromResult(new DuplexSendOutcome(
                Sent: 1, Failed: 1,
                Failures: [new DuplexSendFailure(rows[1]["id"]!.ToString(), 42, "not logged on")])),
        };
        using var _ = Published(source, session);

        string? reportedSource = null;
        Exception? reportedEx = null;
        var client = Client(source, onFailure: (s, ex) => { reportedSource = s; reportedEx = ex; });

        await client.PublishBatchAsync(new List<NatsPipelineRowMessage> { Row("ord-1"), Row("ord-2") }, CancellationToken.None);

        Assert.Equal(1, client.Counters.Published);
        Assert.Equal(1, client.Counters.Failed);
        Assert.Contains("ord-2", client.Counters.LastError);
        Assert.Contains("not logged on", client.Counters.LastError);
        Assert.Equal(source, reportedSource);
        Assert.NotNull(reportedEx);
    }

    // ------------------------------------------------------------------
    // Find returning null is counted and never silently succeeds.
    // ------------------------------------------------------------------

    [Fact]
    public async Task MissingSession_IsCountedAsAFailure_NeverASilentSuccess()
    {
        // Deliberately NOT published — DuplexSessions.Find must return null for this name.
        var client = Client("dsx-no-such-source");

        await client.PublishBatchAsync(new List<NatsPipelineRowMessage> { Row("a") }, CancellationToken.None);

        Assert.Equal(0, client.Counters.Published);
        Assert.Equal(1, client.Counters.Failed);
        Assert.NotNull(client.Counters.LastError);
        Assert.Contains("no live session", client.Counters.LastError);
    }

    [Fact]
    public async Task MissingSession_FailsTheWholeBatch()
    {
        var client = Client("dsx-no-such-source-2");

        await client.PublishBatchAsync(
            new List<NatsPipelineRowMessage> { Row("a"), Row("b"), Row("c") }, CancellationToken.None);

        Assert.Equal(0, client.Counters.Published);
        Assert.Equal(3, client.Counters.Failed);
    }

    // ------------------------------------------------------------------
    // PublishAsync/PublishBatchAsync never throw, even when the session itself throws.
    // ------------------------------------------------------------------

    [Fact]
    public async Task SessionThrows_IsCaughtAndCounted_NeverPropagates()
    {
        const string source = "dsx-throws";
        var session = new FakeSession
        {
            OnSend = (_, _) => throw new InvalidOperationException("session is dead"),
        };
        using var _ = Published(source, session);
        var client = Client(source);

        // The point of the test: this does not throw.
        await client.PublishBatchAsync(new List<NatsPipelineRowMessage> { Row("a") }, CancellationToken.None);

        Assert.Equal(0, client.Counters.Published);
        Assert.Equal(1, client.Counters.Failed);
        Assert.Contains("session is dead", client.Counters.LastError);
    }

    [Fact]
    public async Task SessionThrowsSynchronouslyBeforeReturningATask_IsAlsoCaught()
    {
        const string source = "dsx-throws-sync";
        var session = new FakeSession { OnSend = null };
        // Reassign OnSend to a delegate that throws BEFORE constructing a Task — SendAsync itself throws
        // synchronously rather than returning a faulted Task, the other shape IDuplexSession.SendAsync's
        // "may throw" contract permits.
        session.OnSend = (_, _) => throw new TimeoutException("dial timed out");
        using var _ = Published(source, session);
        var client = Client(source);

        await client.PublishBatchAsync(new List<NatsPipelineRowMessage> { Row("a") }, CancellationToken.None);

        Assert.Equal(1, client.Counters.Failed);
        Assert.Contains("dial timed out", client.Counters.LastError);
    }

    // ------------------------------------------------------------------
    // The 3s budget is honoured against a session that never returns.
    // ------------------------------------------------------------------

    [Fact]
    public async Task SessionThatNeverReturns_FailsAfterThePublishTimeoutRatherThanHanging()
    {
        const string source = "dsx-hangs";
        var gate = new TaskCompletionSource();
        var session = new FakeSession
        {
            OnSend = async (_, ct) =>
            {
                // Ignores ct on purpose — this is exactly the "buggy or slow session that never observes
                // cancellation" case DuplexSinkClient's wall-clock race (not a linked CTS alone) exists to
                // survive. The gate lets the test itself finish quickly by never actually completing this
                // task on its own; the client must give up on its own copy of the wait, not on this task.
                await gate.Task;
                return new DuplexSendOutcome(1, 0, []);
            },
        };
        using var _ = Published(source, session);
        var client = Client(source);

        var sw = Stopwatch.StartNew();
        await client.PublishBatchAsync(new List<NatsPipelineRowMessage> { Row("a") }, CancellationToken.None);
        sw.Stop();

        Assert.True(sw.Elapsed >= DuplexSinkClient.PublishTimeout, $"returned too early: {sw.Elapsed}");
        Assert.True(sw.Elapsed < DuplexSinkClient.PublishTimeout + TimeSpan.FromSeconds(2), $"took far longer than the budget: {sw.Elapsed}");
        Assert.Equal(0, client.Counters.Published);
        Assert.Equal(1, client.Counters.Failed);
        Assert.Contains("did not respond within", client.Counters.LastError);

        gate.TrySetResult(); // let the abandoned task finish so it doesn't outlive the test process
    }

    // ------------------------------------------------------------------
    // A cancelled caller is not a failure.
    // ------------------------------------------------------------------

    [Fact]
    public async Task AlreadyCancelledCaller_IsNotCountedAsAFailure()
    {
        const string source = "dsx-cancelled";
        var session = new FakeSession();
        using var _ = Published(source, session);
        var client = Client(source);

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await client.PublishBatchAsync(new List<NatsPipelineRowMessage> { Row("a") }, cts.Token);

        Assert.Equal(0, client.Counters.Published);
        Assert.Equal(0, client.Counters.Failed);
        Assert.Empty(session.Sends);
    }

    [Fact]
    public async Task EmptyBatch_IsANoOp()
    {
        var client = Client("dsx-unused-source");
        await client.PublishBatchAsync(new List<NatsPipelineRowMessage>(), CancellationToken.None);
        Assert.Equal(0, client.Counters.Published);
        Assert.Equal(0, client.Counters.Failed);
    }

    // ------------------------------------------------------------------
    // SinkSelection.Active treats the kind normally, through the registry — no special-casing.
    // ------------------------------------------------------------------

    [Fact]
    public void SinkSelection_ActivatesTheDuplexKindLikeAnyOther()
    {
        var configured = new SinkSpec { Kind = SinkKinds.Duplex, Enabled = true, Duplex = new DuplexSinkConfig { SourceName = "fx1" } };
        Assert.Single(SinkSelection.Active([configured]));

        Assert.Empty(SinkSelection.Active([new SinkSpec { Kind = SinkKinds.Duplex, Enabled = false, Duplex = configured.Duplex }]));
        Assert.Empty(SinkSelection.Active([new SinkSpec { Kind = SinkKinds.Duplex, Enabled = true, Duplex = new DuplexSinkConfig() }]));
        Assert.Empty(SinkSelection.Active([new SinkSpec { Kind = SinkKinds.Duplex, Enabled = true, Duplex = null }]));
    }

    [Fact]
    public void SinkTransports_CreateReturnsADuplexSinkClient()
    {
        var spec = new SinkSpec { Kind = SinkKinds.Duplex, Enabled = true, Duplex = new DuplexSinkConfig { SourceName = "fx1" } };
        var client = SinkTransports.Find(spec.Kind)!.Create(spec, "pipeline", "p1", null);

        var duplex = Assert.IsType<DuplexSinkClient>(client);
        Assert.Equal("p1", duplex.EntityName);
        Assert.Equal("fx1", duplex.SourceName);
    }

    [Fact]
    public void SourceName_ExpandsTheNamePlaceholder()
    {
        var client = Client("target-{name}", entityName: "orders");
        Assert.Equal("target-orders", client.SourceName);
    }

    // ------------------------------------------------------------------
    // Teardown/rebuild of the client leaves the published session untouched — the load-bearing D2 claim.
    // ------------------------------------------------------------------

    [Fact]
    public async Task DisposingAClient_NeverWithdrawsTheSessionItNeverOwned()
    {
        const string source = "dsx-teardown";
        var session = new FakeSession();
        using var _ = Published(source, session);

        var first = Client(source);
        await first.PublishBatchAsync(new List<NatsPipelineRowMessage> { Row("a") }, CancellationToken.None);
        Assert.Single(session.Sends);

        // SinkSelection.Signature changing (any unrelated field on the OWNING pipeline/table's sink list)
        // tears the client down and rebuilds a fresh one — this is that rebuild, simulated directly.
        await first.DisposeAsync();

        Assert.Same(session, DuplexSessions.Find(source));

        var second = Client(source);
        await second.PublishBatchAsync(new List<NatsPipelineRowMessage> { Row("b") }, CancellationToken.None);

        Assert.Equal(2, session.Sends.Count);
        Assert.Same(session, DuplexSessions.Find(source)); // still the very same session, never re-opened
    }

    // ------------------------------------------------------------------
    // DuplexSinkTransport — IsConfigured / Validate / Describe.
    // ------------------------------------------------------------------

    [Fact]
    public void IsConfigured_RequiresANonBlankSourceName()
    {
        var transport = new DuplexSinkTransport();
        Assert.True(transport.IsConfigured(new SinkSpec { Kind = SinkKinds.Duplex, Duplex = new DuplexSinkConfig { SourceName = "fx" } }));
        Assert.False(transport.IsConfigured(new SinkSpec { Kind = SinkKinds.Duplex, Duplex = new DuplexSinkConfig { SourceName = "" } }));
        Assert.False(transport.IsConfigured(new SinkSpec { Kind = SinkKinds.Duplex, Duplex = null }));
    }

    [Fact]
    public void Validate_RequiresConfigAndANonBlankSourceName()
    {
        var transport = new DuplexSinkTransport();

        var missingConfig = new List<string>();
        transport.Validate(new SinkSpec { Kind = SinkKinds.Duplex, Duplex = null }, missingConfig);
        Assert.Contains(missingConfig, e => e.Contains("requires duplex config"));

        var blankName = new List<string>();
        transport.Validate(new SinkSpec { Kind = SinkKinds.Duplex, Duplex = new DuplexSinkConfig { SourceName = "  " } }, blankName);
        Assert.Contains(blankName, e => e.Contains("duplex.sourceName is required"));

        var ok = new List<string>();
        transport.Validate(new SinkSpec { Kind = SinkKinds.Duplex, Duplex = new DuplexSinkConfig { SourceName = "fx1" } }, ok);
        Assert.Empty(ok);
    }

    [Fact]
    public void SinkTransportsValidate_ReportsTheOffendingSinkByLabel()
    {
        var sinks = new List<SinkSpec> { new() { Kind = SinkKinds.Duplex, Name = "orders-out", Duplex = new DuplexSinkConfig() } };
        var errors = new List<string>();

        SinkTransports.Validate(sinks, errors);

        Assert.Contains(errors, e => e.Contains("orders-out") && e.Contains("duplex.sourceName is required"));
    }

    [Fact]
    public void Describe_ConfigPropertyMatchesTheSinkSpecSlot()
    {
        var descriptor = new DuplexSinkTransport().Describe();

        Assert.Equal(SinkKinds.Duplex, descriptor.Kind);
        // NOT Duplex = true — see DuplexSinkTransport.Describe()'s comment: that flag means "implements
        // IDuplexTransport", which this SINK does not; DuplexTransportRegistryTests (frozen, wave A) pins
        // it false for every registered descriptor but its own two fakes, across both catalogs.
        Assert.False(descriptor.Duplex);
        Assert.Equal("duplex", descriptor.ConfigProperty);
        Assert.Contains(descriptor.Fields, f => f.Key == "sourceName" && f.Required);
        Assert.Contains(descriptor.Fields, f => f.Key == "requireSession");
        Assert.False(string.IsNullOrWhiteSpace(descriptor.Label));

        // No field is typed "secret" — DuplexSinkConfig carries no [Secret] property (the session's
        // credentials live on the source it points at), so nothing here should claim otherwise.
        Assert.DoesNotContain(descriptor.Fields, f => f.Type == TransportFieldTypes.Secret);
    }

    [Fact]
    public void SinkKindIsRegistered()
    {
        Assert.NotNull(SinkTransports.Find(SinkKinds.Duplex));
        Assert.Contains(SinkKinds.Duplex, SinkTransports.Kinds);
    }
}
