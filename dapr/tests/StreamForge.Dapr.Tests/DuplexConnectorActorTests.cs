using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using StreamForge.Abstractions;
using StreamForge.AppCore.Connectors.Polling;
using StreamForge.AppCore.Transports;
using StreamForge.Dapr.Host.Actors;
using Xunit;

namespace StreamForge.Dapr.Tests;

/// <summary>
/// Plan 019 (wave D): the acceptance test for <see cref="ConnectorActor"/>'s duplex arming path — a fake
/// duplex kind ("duplexactor", chosen to be unlikely to collide with any other wave's fake, per the wave
/// brief) proves three claims without needing an <c>ActorHost</c> or a Dapr sidecar (this machine has no
/// <c>dapr init</c> performed — see <c>dapr/ARCHITECTURE.md</c>'s statestore-scoping caveat for why an
/// isolated actor test instance isn't an option here either):
///
/// <list type="number">
/// <item>The message-transport arming path (<c>InboundTransports.Find(def.Kind)</c> +
/// <see cref="SubscriberCore"/>, exactly what <c>ConnectorActor.StartTransportSubscriber</c> wires up)
/// needed ZERO duplex-specific code: a duplex transport is found and driven exactly like <c>nats</c>
/// is, and <c>OpenDuplex</c>'s publish / the session's own <c>DisposeAsync</c>'s withdraw are what
/// populate <see cref="DuplexSessions"/> — see <see cref="ArmingPath_FindsAndDrivesTheDuplexKindWithNoDuplexSpecificCode"/>.</item>
/// <item>Exactly one session is ever live for a source at once, including across a reconnect
/// (<see cref="Reconnect_TheSecondAttemptsSessionReplacesTheFirstAsTheOneLiveSession"/>) and across a
/// stop-then-immediate-start (<see cref="StopThenImmediateStart_TheOldSessionsBelatedDisposeCannotUnpublishTheNewOne"/>)
/// — the latter is this wave's genuinely open question from the brief: <c>ConnectorActor</c>'s own turn
/// serialization covers ITS state transitions (<c>_transportCts</c> cancel-and-replace happens inside one
/// turn), but NOT the background <see cref="SubscriberCore.RunAsync"/> task's async unwind, which runs on
/// its own thread outside any turn and can complete arbitrarily late. What actually closes that gap is
/// <see cref="DuplexSessions.Withdraw"/>'s reference-identity compare-and-remove, proven directly here by
/// forcing the race with an artificially slow <c>DisposeAsync</c>.</item>
/// <item><see cref="ConnectorBookkeeping.ToStatus"/> reads <see cref="ConnectorRuntimeStatus.DuplexReady"/>
/// off <see cref="DuplexSessions"/> without reaching into any sink layer — null for a non-duplex kind,
/// false for a duplex kind with no live session (never started, stopped, or mid-reconnect), true only when
/// a live session reports itself ready.</item>
/// </list>
/// </summary>
public class DuplexConnectorActorTests
{
    private const string DuplexActorKind = "duplexactor";

    private static readonly DuplexActorTransport Transport = new(DuplexActorKind);

    static DuplexConnectorActorTests()
    {
        DuplexTransports.Register(Transport);
    }

    // ------------------------------------------------------------------
    // The fake duplex transport — rides the existing connector.nats config slot, same convention
    // orleans/tests/StreamForge.Host.Tests/DuplexTransportRegistryTests.cs's FluxTransport uses (a real
    // duplex transport, FIX, gets its own contracts property in a later wave; adding one here would be a
    // contract change this wave does not make). UNLIKE that wave-A fake, this one actually implements the
    // Publish/Withdraw contract DuplexSessions.cs's doc says every duplex transport owes — wave A's fake
    // predates DuplexSessions ("pre-built between waves B/C/D") and only had to prove the plain inbound
    // seam, not the session rendezvous this wave is responsible for proving.
    // ------------------------------------------------------------------

    private sealed class DuplexActorTransport(string kind) : IDuplexTransport
    {
        private readonly ConcurrentDictionary<string, int> _attempts = new();

        /// <summary>Per source name: which 1-based OpenDuplex attempt(s) should end their
        /// <see cref="FakeSession.SubscribeAsync"/> immediately (a clean end) rather than block forever —
        /// how the reconnect test drives <see cref="SubscriberCore"/> into a second attempt without waiting
        /// out real backoff (a clean end reconnects at once, per that class's own doc comment).</summary>
        public ConcurrentDictionary<string, HashSet<int>> EndImmediatelyAttempts { get; } = new();

        /// <summary>Per source name: how long that source's sessions' <c>DisposeAsync</c> should sleep
        /// before withdrawing — the knob the stop/start race test uses to force a stale withdraw to arrive
        /// strictly after a newer session has already published.</summary>
        public ConcurrentDictionary<string, TimeSpan> DisposeDelayBySource { get; } = new();

        /// <summary>Per source name: whether newly-opened sessions report <see cref="IDuplexSession.IsReady"/>
        /// true (default) or false.</summary>
        public ConcurrentDictionary<string, bool> ReadyBySource { get; } = new();

        /// <summary>Every session this transport has ever created for a source, in open order — so a test
        /// can assert on session IDENTITY across attempts without racing <see cref="DuplexSessions"/>
        /// itself (which a fast, delay-free reconnect can replace before a poll observes the first one).</summary>
        public ConcurrentDictionary<string, List<FakeSession>> CreatedBySource { get; } = new();

        public string Kind => kind;

        public string FormatOf(SourceDefinition def) => FileFormats.JsonArray;

        public void Validate(SourceDefinition def, List<string> errors)
        {
            if (def.Connector?.Nats is null)
            {
                errors.Add($"kind '{kind}' requires connector.nats");
            }
        }

        // Pinned by IDuplexTransport's own doc comment: Open() delegates to OpenDuplex() so both entry
        // points return the same live object — exactly what ConnectorActor.StartTransportSubscriber relies
        // on via SubscriberCore calling plain Open().
        public IInboundSubscription Open(SourceDefinition def) => OpenDuplex(def);

        public IDuplexSession OpenDuplex(SourceDefinition def)
        {
            var attempt = _attempts.AddOrUpdate(def.Name, 1, (_, n) => n + 1);
            var endImmediately = EndImmediatelyAttempts.TryGetValue(def.Name, out var set) && set.Contains(attempt);
            var disposeDelay = DisposeDelayBySource.TryGetValue(def.Name, out var d) ? d : TimeSpan.Zero;
            var ready = !ReadyBySource.TryGetValue(def.Name, out var r) || r;

            var session = new FakeSession(def.Name, endImmediately, disposeDelay) { IsReady = ready };
            CreatedBySource.AddOrUpdate(def.Name, [session], (_, list) => { list.Add(session); return list; });

            // This is the load-bearing line for the whole wave: OpenDuplex publishes, per DuplexSessions'
            // own doc ("who publishes, and why it is not the driver directly") — ConnectorActor never
            // calls this method or DuplexSessions itself.
            DuplexSessions.Publish(def.Name, session);
            return session;
        }

        public TransportDescriptor Describe() => new()
        {
            Kind = kind,
            Label = "DuplexActor fake",
            ConfigProperty = "nats",
            Duplex = true,
            Fields = [new TransportField { Key = "url", Label = "Server URL", Required = true }],
        };
    }

    private sealed class FakeSession(string sourceName, bool endImmediately, TimeSpan disposeDelay) : IDuplexSession
    {
        public bool IsReady { get; init; } = true;

        public bool Disposed { get; private set; }

        // Plan 019 wave B2: mechanical implementation of IDuplexSession's new counters — accumulated per
        // session INSTANCE, matching the interface's documented "fresh session per OpenDuplex call, counters
        // reset on reconnect" scope. FailNextSends lets a test force SendAsync to report every offered row
        // as failed instead of accepted, without a second fake class.
        private long _sent;
        private long _failed;
        private DuplexSendFailure? _lastFailure;

        public bool FailNextSends { get; set; }

        public long SentTotal => Interlocked.Read(ref _sent);

        public long FailedTotal => Interlocked.Read(ref _failed);

        public DuplexSendFailure? LastFailure => Volatile.Read(ref _lastFailure);

        public async IAsyncEnumerable<InboundMessage> SubscribeAsync([EnumeratorCancellation] CancellationToken ct)
        {
            if (endImmediately)
            {
                yield break; // a clean end -> SubscriberCore reconnects immediately, no backoff delay
            }

            await Task.Delay(Timeout.Infinite, ct);
        }

        public async ValueTask DisposeAsync()
        {
            if (disposeDelay > TimeSpan.Zero)
            {
                await Task.Delay(disposeDelay);
            }

            Disposed = true;
            // Per DuplexSessions.cs's doc: "the session's own DisposeAsync" is where withdraw belongs —
            // never ConnectorActor.
            DuplexSessions.Withdraw(sourceName, this);
        }

        public Task<DuplexSendOutcome> SendAsync(IReadOnlyList<Dictionary<string, object?>> rows, CancellationToken ct)
        {
            if (FailNextSends)
            {
                var failures = rows
                    .Select(r => new DuplexSendFailure(r.GetValueOrDefault("id")?.ToString(), null, "simulated rejection"))
                    .ToList();
                Interlocked.Add(ref _failed, failures.Count);
                Volatile.Write(ref _lastFailure, failures[^1]);
                return Task.FromResult(new DuplexSendOutcome(0, failures.Count, failures));
            }

            Interlocked.Add(ref _sent, rows.Count);
            return Task.FromResult(new DuplexSendOutcome(rows.Count, 0, []));
        }
    }

    private static SourceDefinition DuplexSource(string name) => new()
    {
        Name = name,
        Kind = DuplexActorKind,
        Enabled = true,
        Fields = [new FieldDef("id", FieldType.String)],
        Connector = new ConnectorConfig
        {
            Nats = new NatsSubConfig { Url = "duplexactor://localhost", Subject = "orders" },
        },
    };

    private static async Task WaitUntilAsync(Func<bool> condition, int timeoutMs = 5_000, int pollMs = 5)
    {
        var sw = Stopwatch.StartNew();
        while (!condition())
        {
            if (sw.ElapsedMilliseconds > timeoutMs)
            {
                throw new TimeoutException("condition was not met within the timeout");
            }

            await Task.Delay(pollMs);
        }
    }

    // ------------------------------------------------------------------
    // 1) Arming needs no production changes: the same InboundTransports.Find + SubscriberCore composition
    //    ConnectorActor.StartTransportSubscriber already uses drives a duplex kind unmodified.
    // ------------------------------------------------------------------

    [Fact]
    public async Task ArmingPath_FindsAndDrivesTheDuplexKindWithNoDuplexSpecificCode()
    {
        var name = $"arm-{Guid.NewGuid():N}";
        try
        {
            // Exactly the line ConnectorActor.OnActivateAsync/StartAsync uses to decide whether a kind is a
            // message transport at all — no branch anywhere names "duplexactor" or IDuplexTransport.
            var transport = InboundTransports.Find(DuplexActorKind);
            Assert.Same(Transport, transport);

            var def = DuplexSource(name);
            var core = new SubscriberCore(
                def, transport!, new DedupTracker([]),
                onRows: (_, _) => Task.CompletedTask,
                onStatus: (_, _) => { });

            using var cts = new CancellationTokenSource();
            var run = core.RunAsync(cts.Token);

            await WaitUntilAsync(() => DuplexSessions.Find(name) is not null);
            var session = DuplexSessions.Find(name);
            Assert.NotNull(session);
            Assert.True(session!.IsReady);

            cts.Cancel();
            await run;

            await WaitUntilAsync(() => DuplexSessions.Find(name) is null);
        }
        finally
        {
            if (DuplexSessions.Find(name) is { } leftover)
            {
                DuplexSessions.Withdraw(name, leftover);
            }
        }
    }

    // ------------------------------------------------------------------
    // 2) Exactly one live session — across a reconnect.
    // ------------------------------------------------------------------

    [Fact]
    public async Task Reconnect_TheSecondAttemptsSessionReplacesTheFirstAsTheOneLiveSession()
    {
        var name = $"reconnect-{Guid.NewGuid():N}";
        try
        {
            // Attempt #1 ends its SubscribeAsync immediately (a clean disconnect) so SubscriberCore
            // reconnects at once with no backoff delay (SubscriberCore.RunAsync's own documented rule).
            Transport.EndImmediatelyAttempts[name] = [1];

            var def = DuplexSource(name);
            var core = new SubscriberCore(
                def, Transport, new DedupTracker([]),
                onRows: (_, _) => Task.CompletedTask,
                onStatus: (_, _) => { });

            using var cts = new CancellationTokenSource();
            var run = core.RunAsync(cts.Token);

            await WaitUntilAsync(() =>
                Transport.CreatedBySource.TryGetValue(name, out var list) && list.Count >= 2);

            var created = Transport.CreatedBySource[name];
            Assert.True(created.Count >= 2, "expected at least two connect attempts (the reconnect)");
            var first = created[0];
            var second = created[1];
            Assert.NotSame(first, second);

            // The second (blocking) attempt is the one still live — never both, never neither.
            await WaitUntilAsync(() => ReferenceEquals(DuplexSessions.Find(name), second));
            Assert.Same(second, DuplexSessions.Find(name));

            cts.Cancel();
            await run;

            await WaitUntilAsync(() => DuplexSessions.Find(name) is null);
        }
        finally
        {
            Transport.EndImmediatelyAttempts.TryRemove(name, out _);
            if (DuplexSessions.Find(name) is { } leftover)
            {
                DuplexSessions.Withdraw(name, leftover);
            }
        }
    }

    // ------------------------------------------------------------------
    // 3) The genuinely open question: a stop immediately followed by a start, where the OLD session's
    //    async DisposeAsync (and therefore its Withdraw call) completes AFTER the new session already
    //    published. ConnectorActor's own turn serialization does not prevent this — StopAsync cancels the
    //    background task's CTS and returns without waiting for the task to unwind (fire-and-forget
    //    Task.Run, exactly like production). What actually closes the gap is DuplexSessions.Withdraw's
    //    reference-identity compare-and-remove, forced to fire late here on purpose.
    // ------------------------------------------------------------------

    [Fact]
    public async Task StopThenImmediateStart_TheOldSessionsBelatedDisposeCannotUnpublishTheNewOne()
    {
        var name = $"stopstart-{Guid.NewGuid():N}";
        try
        {
            // Every session opened for this source sleeps 300ms inside DisposeAsync before withdrawing —
            // long enough that a same-test "start again" comfortably wins the race to publish first.
            Transport.DisposeDelayBySource[name] = TimeSpan.FromMilliseconds(300);

            var def = DuplexSource(name);

            // "StartAsync" #1 — mirrors ConnectorActor.StartTransportSubscriber's fire-and-forget Task.Run.
            using var cts1 = new CancellationTokenSource();
            var core1 = new SubscriberCore(
                def, Transport, new DedupTracker([]),
                onRows: (_, _) => Task.CompletedTask,
                onStatus: (_, _) => { });
            var run1 = core1.RunAsync(cts1.Token);

            await WaitUntilAsync(() => DuplexSessions.Find(name) is not null);
            var session1 = DuplexSessions.Find(name);
            Assert.NotNull(session1);

            // "StopAsync" — cancels but does NOT await run1's own unwind, exactly like
            // ConnectorActor.StopTransportSubscriberIfRunning (Cancel + Dispose the CTS, nothing more).
            cts1.Cancel();

            // "StartAsync" #2 — immediately, well before session1's 300ms-delayed DisposeAsync has run.
            using var cts2 = new CancellationTokenSource();
            var core2 = new SubscriberCore(
                def, Transport, new DedupTracker([]),
                onRows: (_, _) => Task.CompletedTask,
                onStatus: (_, _) => { });
            var run2 = core2.RunAsync(cts2.Token);

            await WaitUntilAsync(() => DuplexSessions.Find(name) is FakeSession s && !ReferenceEquals(s, session1));
            var session2 = DuplexSessions.Find(name);
            Assert.NotSame(session1, session2);

            // Let session1's belated DisposeAsync (and its Withdraw call) actually land now.
            await run1;
            Assert.True(((FakeSession)session1!).Disposed);

            // The load-bearing assertion: session1's stale withdraw, arriving after session2 already
            // published, must NOT have evicted session2.
            Assert.Same(session2, DuplexSessions.Find(name));

            cts2.Cancel();
            await run2;
            await WaitUntilAsync(() => DuplexSessions.Find(name) is null);
        }
        finally
        {
            Transport.DisposeDelayBySource.TryRemove(name, out _);
            if (DuplexSessions.Find(name) is { } leftover)
            {
                DuplexSessions.Withdraw(name, leftover);
            }
        }
    }

    // ------------------------------------------------------------------
    // 4) ConnectorRuntimeStatus.DuplexReady — read off DuplexSessions, never off a sink layer.
    // ------------------------------------------------------------------

    [Fact]
    public void ToStatus_DuplexReady_NullForANonDuplexKind()
    {
        var state = new ConnectorActorState { Def = new SourceDefinition { Name = "s1", Kind = SourceKinds.Url } };

        var status = ConnectorBookkeeping.ToStatus(state, "s1");

        Assert.Null(status.DuplexReady);
    }

    [Fact]
    public void ToStatus_DuplexReady_NullWhenTheStateHasNoDefAtAll()
    {
        var state = new ConnectorActorState(); // never started

        var status = ConnectorBookkeeping.ToStatus(state, "never-started");

        Assert.Null(status.DuplexReady);
    }

    [Fact]
    public void ToStatus_DuplexReady_FalseForADuplexKindWithNoLiveSession()
    {
        var name = $"status-nosession-{Guid.NewGuid():N}";
        var state = new ConnectorActorState { Def = DuplexSource(name) };

        var status = ConnectorBookkeeping.ToStatus(state, name);

        // null and false mean different things (ConnectorRuntimeStatus.DuplexReady's own doc comment):
        // this IS a duplex kind, it just has no session published right now (stopped/never started/
        // mid-reconnect) — an operator needs "down" here, not "not applicable".
        Assert.False(status.DuplexReady);
    }

    [Fact]
    public void ToStatus_DuplexReady_TrueWhenTheLiveSessionReportsReady()
    {
        var name = $"status-ready-{Guid.NewGuid():N}";
        try
        {
            DuplexSessions.Publish(name, new FakeSession(name, endImmediately: false, disposeDelay: TimeSpan.Zero) { IsReady = true });
            var state = new ConnectorActorState { Def = DuplexSource(name) };

            var status = ConnectorBookkeeping.ToStatus(state, name);

            Assert.True(status.DuplexReady);
        }
        finally
        {
            if (DuplexSessions.Find(name) is { } leftover)
            {
                DuplexSessions.Withdraw(name, leftover);
            }
        }
    }

    [Fact]
    public void ToStatus_DuplexReady_FalseWhenTheLiveSessionReportsNotReady()
    {
        var name = $"status-notready-{Guid.NewGuid():N}";
        try
        {
            DuplexSessions.Publish(name, new FakeSession(name, endImmediately: false, disposeDelay: TimeSpan.Zero) { IsReady = false });
            var state = new ConnectorActorState { Def = DuplexSource(name) };

            var status = ConnectorBookkeeping.ToStatus(state, name);

            Assert.False(status.DuplexReady);
        }
        finally
        {
            if (DuplexSessions.Find(name) is { } leftover)
            {
                DuplexSessions.Withdraw(name, leftover);
            }
        }
    }

    [Fact]
    public void ToStatus_DuplexSentFailedAndLastFailure_StayAtContractDefaults_ForANonDuplexKind()
    {
        // A non-duplex kind has no outbound half at all (DuplexTransports.Find(kind) is null), so
        // ConnectorBookkeeping.ToStatus never resolves a session for it — these three fields stay at the
        // record's own defaults regardless of what wave B2 wired up for genuine duplex kinds. See
        // ToStatus_DuplexCounters_ReflectTheLiveSessionsSendHistory below for the duplex-kind case, where
        // the same fields ARE populated.
        var state = new ConnectorActorState { Def = new SourceDefinition { Name = "s1", Kind = SourceKinds.Url }, EventsEmittedTotal = 5, LastStatus = "ok" };

        var status = ConnectorBookkeeping.ToStatus(state, "s1");

        Assert.Equal(0, status.DuplexSentTotal);
        Assert.Equal(0, status.DuplexFailedTotal);
        Assert.Null(status.LastDuplexFailure);
        // And every pre-existing field this wave did not touch is unaffected.
        Assert.Equal(5, status.EventsEmittedTotal);
        Assert.Equal("ok", status.LastStatus);
    }

    [Fact]
    public void ToStatus_DuplexCounters_ReflectTheLiveSessionsSendHistory()
    {
        // Plan 019 wave B2: IDuplexSession now carries its own cumulative counters (the seam both the
        // driver and the proxy sink touch), so ToStatus CAN read a send it never routed itself — straight
        // off the live session, same read-through shape DuplexReady already used.
        var name = $"status-counters-{Guid.NewGuid():N}";
        var session = new FakeSession(name, endImmediately: false, disposeDelay: TimeSpan.Zero) { IsReady = true };
        try
        {
            DuplexSessions.Publish(name, session);
            var state = new ConnectorActorState { Def = DuplexSource(name) };

            var beforeSend = ConnectorBookkeeping.ToStatus(state, name);
            Assert.Equal(0, beforeSend.DuplexSentTotal);
            Assert.Equal(0, beforeSend.DuplexFailedTotal);
            Assert.Null(beforeSend.LastDuplexFailure);

            var sendOutcome = session.SendAsync(
                [new Dictionary<string, object?> { ["id"] = "ord-1" }], CancellationToken.None).GetAwaiter().GetResult();
            Assert.Equal(1, sendOutcome.Sent);

            var afterAccepted = ConnectorBookkeeping.ToStatus(state, name);
            Assert.Equal(1, afterAccepted.DuplexSentTotal);
            Assert.Equal(0, afterAccepted.DuplexFailedTotal);
            Assert.Null(afterAccepted.LastDuplexFailure);

            session.FailNextSends = true;
            var failOutcome = session.SendAsync(
                [new Dictionary<string, object?> { ["id"] = "ord-2" }], CancellationToken.None).GetAwaiter().GetResult();
            Assert.Equal(1, failOutcome.Failed);

            var afterFailure = ConnectorBookkeeping.ToStatus(state, name);
            Assert.Equal(1, afterFailure.DuplexSentTotal);
            Assert.Equal(1, afterFailure.DuplexFailedTotal);
            Assert.NotNull(afterFailure.LastDuplexFailure);
            Assert.Contains("ord-2", afterFailure.LastDuplexFailure);
            Assert.Contains("simulated rejection", afterFailure.LastDuplexFailure);
        }
        finally
        {
            DuplexSessions.Withdraw(name, session);
        }
    }
}
