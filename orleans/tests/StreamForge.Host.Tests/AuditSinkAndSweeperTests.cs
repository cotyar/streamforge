using System.Security.Claims;
using Microsoft.Extensions.Logging.Abstractions;
using StreamForge.Abstractions;
using StreamForge.Api.Auth;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// Plan 015 wave 4-D — the four properties the whole design rests on, tested where they can actually
/// break: the queue's drop policy and its counter, audit never reaching the caller as an exception, the
/// sweeper staying inert while approvals are disabled, and the sweeper surviving a store that is not
/// there or is throwing (a sibling wave is implementing <c>SweepAsync</c> concurrently, so "the facade
/// throws NotImplementedException" is not a hypothetical).
/// </summary>
public class AuditSinkTests
{
    private static AuditChannelSink Sink(int capacity = 4, bool enabled = true) =>
        new(capacity, enabled, NullLogger.Instance);

    private static AuditEntry Entry(string id) => new()
    {
        Id = id,
        AtMs = 1,
        Actor = "alice",
        Action = Actions.PipelineWrite,
        Scope = id,
        Outcome = "allowed",
    };

    // =============================================================================================
    // The queue: bounded, drop-on-overflow, and every drop counted
    // =============================================================================================

    [Fact]
    public void TheQueueIsBoundedAndCountsEveryRowItRefuses()
    {
        var sink = Sink(capacity: 4);

        for (var i = 0; i < 10; i++)
        {
            sink.Record(Entry($"e{i}"));
        }

        Assert.Equal(10, sink.Offered);
        Assert.Equal(4, sink.Enqueued);
        Assert.Equal(6, sink.Dropped);
    }

    [Fact]
    public void RecordingIsANoOpWhenAuditIsDisabled()
    {
        var sink = Sink(capacity: 4, enabled: false);
        sink.Record(Entry("e0"));

        Assert.False(sink.Enabled);
        Assert.Equal(0, sink.Offered);
        Assert.Equal(0, sink.Dropped);
    }

    /// <summary>The drop policy's direction, which is the decision this file exists to pin: an overflow
    /// keeps the rows that were already queued (the onset of the burst) and refuses the incoming ones,
    /// so the survivors are the FIRST four and not the last four.</summary>
    [Fact]
    public async Task OverflowKeepsTheOldestRowsAndReportsTheHoleAsItsOwnAuditEntry()
    {
        var sink = Sink(capacity: 4);
        for (var i = 0; i < 10; i++)
        {
            sink.Record(Entry($"e{i}"));
        }

        var store = await DrainAsync(sink);

        var kept = store.Entries.Where(e => e.Action != "audit.dropped").Select(e => e.Id).ToList();
        Assert.Equal(new[] { "e0", "e1", "e2", "e3" }, kept);

        // Silence is never mistaken for absence: the hole is a row in the log, not only a counter.
        var report = Assert.Single(store.Entries, e => e.Action == "audit.dropped");
        Assert.Equal("failed", report.Outcome);
        Assert.Contains("6 audit entries were dropped", report.Detail);
    }

    [Fact]
    public async Task TheWriterDrainsEverythingTheQueueHolds()
    {
        var sink = Sink(capacity: 64);
        sink.Record(Entry("a"));
        sink.Record(Entry("b"));

        var store = await DrainAsync(sink);

        Assert.Equal(new[] { "a", "b" }, store.Entries.Select(e => e.Id));
        Assert.Equal(0, sink.Dropped);
    }

    /// <summary>A store that rejects every row must not stop the drain or take the host down — the rows
    /// are lost, loudly, and the process carries on.</summary>
    [Fact]
    public async Task AThrowingAuditStoreDoesNotStopTheWriter()
    {
        var sink = Sink(capacity: 64);
        sink.Record(Entry("a"));
        sink.Record(Entry("b"));

        var store = new RecordingAuditFacade { Throw = new InvalidOperationException("store is down") };
        var writer = new AuditWriterService(sink, () => store, NullLogger<AuditWriterService>.Instance);
        sink.Complete();
        await writer.DrainAsync();

        Assert.Equal(2, writer.Failed);
    }

    /// <summary>No <see cref="IAuditFacade"/> registered at all — the state of a host on which the
    /// sibling wave's store has not landed. The writer must drain and discard rather than leaving a
    /// permanently full queue behind, and must not throw.</summary>
    [Fact]
    public async Task AMissingAuditStoreIsDrainedAndDiscarded()
    {
        var sink = Sink(capacity: 4);
        for (var i = 0; i < 20; i++)
        {
            sink.Record(Entry($"e{i}"));
        }

        var writer = new AuditWriterService(sink, () => null, NullLogger<AuditWriterService>.Instance);
        sink.Complete();
        await writer.DrainAsync();

        Assert.Equal(0, writer.Failed);
    }

    /// <summary>The hosted-service plumbing itself: started for real, stopped for real, and the row
    /// arrives without the request path ever having waited for it.</summary>
    [Fact]
    public async Task TheHostedServiceDrainsWhatIsRecordedWhileItIsRunning()
    {
        var sink = Sink(capacity: 64);
        var store = new RecordingAuditFacade();
        var writer = new AuditWriterService(sink, () => store, NullLogger<AuditWriterService>.Instance);

        await writer.StartAsync(CancellationToken.None);
        sink.Record(Entry("a"));

        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (store.Count == 0 && DateTime.UtcNow < deadline)
        {
            await Task.Delay(10);
        }

        await writer.StopAsync(CancellationToken.None);
        Assert.Equal(1, store.Count);
    }

    /// <summary>Completes the queue and runs the drain to the end — the loop exactly as
    /// <c>ExecuteAsync</c> runs it, minus the BackgroundService start/stop race.</summary>
    private static async Task<RecordingAuditFacade> DrainAsync(AuditChannelSink sink)
    {
        var store = new RecordingAuditFacade();
        var writer = new AuditWriterService(sink, () => store, NullLogger<AuditWriterService>.Instance);
        sink.Complete();
        await writer.DrainAsync();
        return store;
    }

    // =============================================================================================
    // What the guard records, and what it deliberately does not
    // =============================================================================================

    private static AccessGuard Guard(
        IAuditSink audit,
        PermissionGrant[] grants,
        bool recordAllowedMutations = true,
        bool entitlements = true)
    {
        var document = PermissionResolverTests.Doc(version: 1);
        document.Users.Add(new UserAccessEntry { Username = "alice", Grants = [.. grants] });
        var resolver = new PermissionResolver(
            new CountingAccessPolicyFacade(document),
            NullLogger<PermissionResolver>.Instance,
            policyCacheSeconds: 600);

        return new AccessGuard(resolver, entitlements, audit, recordAllowedMutations);
    }

    private static ClaimsPrincipal Alice => PermissionResolverTests.Principal("alice");

    private static PermissionGrant AllowAll() => new() { Action = "*", Scope = "*" };

    [Fact]
    public async Task AnAllowedMutationIsRecordedAndAnAllowedReadIsNot()
    {
        var sink = Sink(capacity: 64);
        var guard = Guard(sink, [AllowAll()]);

        Assert.Equal(AccessDecision.Allowed, (await guard.CheckAsync(Alice, Actions.PipelineRead, "p1")).Decision);
        Assert.Equal(0, sink.Offered);

        Assert.Equal(AccessDecision.Allowed, (await guard.CheckAsync(Alice, Actions.PipelineWrite, "p1")).Decision);
        Assert.Equal(1, sink.Offered);
    }

    /// <summary>The three allowed-actions that are writes (or verbs) and are still not recorded, each
    /// for its own stated reason: the platform's hottest route, and the two coarse policy bundles the
    /// Editor/Admin door asks for.</summary>
    [Theory]
    [InlineData(Actions.SourceIngest)]
    [InlineData(Actions.CatalogWrite)]
    [InlineData(Actions.ConfigExport)]
    [InlineData(Actions.ChatUse)]
    public async Task TheHotAndCoarseActionsAreNotRecordedWhenAllowed(string action)
    {
        var sink = Sink(capacity: 64);
        var guard = Guard(sink, [AllowAll()]);

        await guard.CheckAsync(Alice, action, "*");

        Assert.Equal(0, sink.Offered);
    }

    /// <summary>…but a REFUSAL of any of them is recorded. A denial is rare by construction, and one
    /// that is not rare is itself the thing worth seeing.</summary>
    [Theory]
    [InlineData(Actions.SourceIngest)]
    [InlineData(Actions.PipelineRead)]
    [InlineData(Actions.CatalogWrite)]
    public async Task ARefusalIsAlwaysRecordedEvenForActionsThatAreQuietWhenAllowed(string action)
    {
        var sink = Sink(capacity: 64);
        var guard = Guard(sink, []);

        var result = await guard.CheckAsync(Alice, action, "*");

        Assert.Equal(AccessDecision.Denied, result.Decision);
        Assert.Equal(1, sink.Offered);
    }

    [Fact]
    public async Task ARequiresApprovalDecisionIsRecorded()
    {
        var sink = Sink(capacity: 64);
        var guard = Guard(sink, [new PermissionGrant { Action = Actions.PipelineWrite, Scope = "*", RequiresApproval = true }]);

        var result = await guard.CheckAsync(Alice, Actions.PipelineWrite, "p1");

        Assert.Equal(AccessDecision.RequiresApproval, result.Decision);
        Assert.Equal(1, sink.Offered);
    }

    [Fact]
    public async Task RecordAllowedMutationsFalseKeepsTheRefusalsAndDropsTheAllows()
    {
        var sink = Sink(capacity: 64);
        var guard = Guard(sink, [AllowAll()], recordAllowedMutations: false);
        await guard.CheckAsync(Alice, Actions.PipelineWrite, "p1");
        Assert.Equal(0, sink.Offered);

        var strict = Guard(sink, [], recordAllowedMutations: false);
        await strict.CheckAsync(Alice, Actions.PipelineWrite, "p1");
        Assert.Equal(1, sink.Offered);
    }

    /// <summary>Legacy mode enforces nothing, so it records nothing: a log full of "allowed — not
    /// enforced" would bury the rows that mean something.</summary>
    [Fact]
    public async Task LegacyModeRecordsNothing()
    {
        var sink = Sink(capacity: 64);
        var guard = Guard(sink, [], entitlements: false);

        Assert.True((await guard.CheckAsync(Alice, Actions.PipelineWrite, "p1")).IsAllowed);
        Assert.Equal(0, sink.Offered);
    }

    /// <summary><b>Audit must never make a request fail.</b> A sink that throws on every row is exactly
    /// the failure mode the design exists to survive, and the caller must not be able to tell.</summary>
    [Fact]
    public async Task AThrowingSinkNeverReachesTheCaller()
    {
        var guard = Guard(new ThrowingAuditSink(), [AllowAll()]);

        var allowed = await guard.CheckAsync(Alice, Actions.PipelineWrite, "p1");
        Assert.Equal(AccessDecision.Allowed, allowed.Decision);

        var denied = await Guard(new ThrowingAuditSink(), []).CheckAsync(Alice, Actions.PipelineWrite, "p1");
        Assert.Equal(AccessDecision.Denied, denied.Decision);
    }

    [Fact]
    public async Task AGuardWithNoSinkBehavesIdentically()
    {
        var document = PermissionResolverTests.Doc(version: 1);
        document.Users.Add(new UserAccessEntry { Username = "alice", Grants = [AllowAll()] });
        var guard = new AccessGuard(
            new PermissionResolver(new CountingAccessPolicyFacade(document), NullLogger<PermissionResolver>.Instance, 600),
            entitlementsEnabled: true);

        Assert.True((await guard.CheckAsync(Alice, Actions.PipelineWrite, "p1")).IsAllowed);
    }

    /// <summary>The guard's row names the human and carries the decision's reason — the two things a
    /// 403 and an incident review both need.</summary>
    [Fact]
    public async Task TheRecordedRowNamesTheActorTheScopeAndTheOutcome()
    {
        var sink = Sink(capacity: 64);
        var guard = Guard(sink, []);
        await guard.CheckAsync(Alice, Actions.PipelineWrite, "prod-orders");

        var writerStore2 = await DrainAsync(sink);

        var row = Assert.Single(writerStore2.Entries);
        Assert.Equal("alice", row.Actor);
        Assert.Equal(Actions.PipelineWrite, row.Action);
        Assert.Equal("prod-orders", row.Scope);
        Assert.Equal("denied", row.Outcome);
        Assert.False(string.IsNullOrWhiteSpace(row.Detail));
        Assert.Null(row.OnBehalfOf);
    }
}

/// <summary>Plan 015 wave 4-D — the escalation sweeper and the chat's approval filer.</summary>
public class ApprovalSweeperTests
{
    private static ApprovalSweeperService Sweeper(Func<IApprovalFacade?> facade, bool enabled, int sweepSeconds = 1) =>
        new(facade, new ApprovalOptions(enabled, sweepSeconds), NullLogger<ApprovalSweeperService>.Instance);

    /// <summary>Approvals ship inert. "Inert" has to mean the sweeper never touches the store at all,
    /// or the byte-identical claim is only a claim.</summary>
    [Fact]
    public async Task TheSweeperDoesNotRunWhenApprovalsAreDisabled()
    {
        var store = new RecordingApprovalFacade();
        var sweeper = Sweeper(() => store, enabled: false);

        await sweeper.StartAsync(CancellationToken.None);
        await Task.Delay(150);
        await sweeper.StopAsync(CancellationToken.None);

        Assert.Equal(0, store.Sweeps);
        Assert.Equal(0, sweeper.Sweeps);
        Assert.Equal(0, sweeper.Failures);
    }

    [Fact]
    public async Task TheSweeperCallsSweepAsyncWhenApprovalsAreEnabled()
    {
        var store = new RecordingApprovalFacade { Changed = 3 };
        var sweeper = Sweeper(() => store, enabled: true);

        await sweeper.SweepOnceAsync();

        Assert.Equal(1, store.Sweeps);
        Assert.Equal(1, sweeper.Sweeps);
        Assert.Equal(3, sweeper.LastChanged);
        Assert.True(store.LastNowMs > 0);
    }

    /// <summary>A sibling wave is implementing <c>SweepAsync</c> right now, so against some trees it
    /// throws <see cref="NotImplementedException"/>. That must cost one log line, not a host.</summary>
    [Fact]
    public async Task TheSweeperSurvivesAThrowingFacade()
    {
        var store = new RecordingApprovalFacade { Throw = new NotImplementedException() };
        var sweeper = Sweeper(() => store, enabled: true);

        await sweeper.SweepOnceAsync();
        await sweeper.SweepOnceAsync();

        Assert.Equal(0, sweeper.Sweeps);
        Assert.Equal(2, sweeper.Failures);
    }

    [Fact]
    public async Task TheSweeperSurvivesAFacadeThatIsNotRegisteredAtAll()
    {
        var sweeper = Sweeper(() => null, enabled: true);

        await sweeper.SweepOnceAsync();

        Assert.Equal(0, sweeper.Sweeps);
        Assert.Equal(1, sweeper.Failures);
    }

    /// <summary>The whole service, started and stopped, against a store that throws every time: it must
    /// keep ticking and must stop cleanly.</summary>
    [Fact]
    public async Task TheRunningSweeperKeepsGoingAfterAFailureAndStopsCleanly()
    {
        var store = new RecordingApprovalFacade { Throw = new InvalidOperationException("sidecar down") };
        var sweeper = Sweeper(() => store, enabled: true, sweepSeconds: 1);

        await sweeper.StartAsync(CancellationToken.None);
        await Task.Delay(1200);
        await sweeper.StopAsync(CancellationToken.None);

        Assert.True(sweeper.Failures >= 1, $"expected at least one attempted sweep, saw {sweeper.Failures}");
    }

    // =============================================================================================
    // The chat's approval filer
    // =============================================================================================

    private static ApprovalRequest Draft() => new()
    {
        Id = "correlation-1",
        RequestedBy = "alice",
        Action = Actions.PipelineWrite,
        Scope = "prod-orders",
        Origin = "chat",
    };

    private static ApprovalStoreChatFiler Filer(Func<IApprovalFacade?> facade, bool enabled) =>
        new(facade, new ApprovalOptions(enabled, 30), NullLogger<ApprovalStoreChatFiler>.Instance);

    /// <summary>Wave 3-C's honest behaviour, kept: filing into a feature nobody turned on would hand the
    /// model an id that looks like a promise and is not one.</summary>
    [Fact]
    public async Task TheChatFilerFilesNothingWhenApprovalsAreDisabled()
    {
        var store = new RecordingApprovalFacade();

        var id = await Filer(() => store, enabled: false).FileAsync(Draft(), CancellationToken.None);

        Assert.Null(id);
        Assert.Empty(store.Filed);
    }

    [Fact]
    public async Task TheChatFilerReturnsTheIdTheStoreAssigned()
    {
        var store = new RecordingApprovalFacade { AssignedId = "ap-42" };

        var id = await Filer(() => store, enabled: true).FileAsync(Draft(), CancellationToken.None);

        Assert.Equal("ap-42", id);
        Assert.Equal("prod-orders", Assert.Single(store.Filed).Scope);
    }

    [Fact]
    public async Task TheChatFilerReturnsNullWhenTheStoreThrowsOrIsMissing()
    {
        var throwing = new RecordingApprovalFacade { Throw = new InvalidOperationException("nope") };

        Assert.Null(await Filer(() => throwing, enabled: true).FileAsync(Draft(), CancellationToken.None));
        Assert.Null(await Filer(() => null, enabled: true).FileAsync(Draft(), CancellationToken.None));
    }
}

/// <summary>Collects what the writer drained. Stands in for wave 4's day-sharded store.</summary>
internal sealed class RecordingAuditFacade : IAuditFacade
{
    public List<AuditEntry> Entries { get; } = [];
    public Exception? Throw { get; set; }

    public int Count { get { lock (Entries) { return Entries.Count; } } }

    public Task AppendAsync(AuditEntry entry)
    {
        if (Throw is not null)
        {
            throw Throw;
        }

        lock (Entries)
        {
            Entries.Add(entry);
        }

        return Task.CompletedTask;
    }

    public Task<AuditPage> QueryAsync(string day, string? actor, string? actionPrefix, int limit, int offset) =>
        Task.FromResult(new AuditPage());

    public Task<List<string>> GetDaysAsync() => Task.FromResult(new List<string>());
}

/// <summary>The one thing an audit sink must never be able to do to a request.</summary>
internal sealed class ThrowingAuditSink : IAuditSink
{
    public void Record(AuditEntry entry) => throw new InvalidOperationException("the sink is on fire");
}

/// <summary>Stands in for wave 4's approval store, on both the sweep and the file path.</summary>
internal sealed class RecordingApprovalFacade : IApprovalFacade
{
    private int _sweeps;

    public List<ApprovalRequest> Filed { get; } = [];
    public Exception? Throw { get; set; }
    public int Changed { get; set; }
    public string? AssignedId { get; set; }
    public long LastNowMs { get; private set; }
    public int Sweeps => Volatile.Read(ref _sweeps);

    public Task<ApprovalRequest> RequestAsync(ApprovalRequest request)
    {
        if (Throw is not null)
        {
            throw Throw;
        }

        Filed.Add(request);
        return Task.FromResult(new ApprovalRequest
        {
            Id = AssignedId ?? "",
            Action = request.Action,
            Scope = request.Scope,
            RequestedBy = request.RequestedBy,
            Origin = request.Origin,
        });
    }

    public Task<int> SweepAsync(long nowMs)
    {
        Interlocked.Increment(ref _sweeps);
        LastNowMs = nowMs;

        if (Throw is not null)
        {
            throw Throw;
        }

        return Task.FromResult(Changed);
    }

    public Task<ApprovalRequest?> GetAsync(string id) => Task.FromResult<ApprovalRequest?>(null);

    public Task<List<ApprovalRequest>> ListAsync(ApprovalState? state, int limit) =>
        Task.FromResult(new List<ApprovalRequest>());

    public Task<ApprovalRequest?> VoteAsync(string id, ApprovalVote vote) => Task.FromResult<ApprovalRequest?>(null);

    public Task<ApprovalRequest?> CancelAsync(string id, string username) => Task.FromResult<ApprovalRequest?>(null);

    public Task<ApprovalRequest?> RecordOutcomeAsync(string id, bool executed, string outcome) =>
        Task.FromResult<ApprovalRequest?>(null);
}
