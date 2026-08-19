using System.Collections.Concurrent;
using System.Text.Json;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Runtime.Hosting;
using Orleans.Storage;
using StreamForge.Host.Grains;
using Orleans.TestingHost;
using StreamForge.Abstractions;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// Plan 015 W4-B test scaffolding: grain storage that keeps every write as JSON in a static map, so a
/// test can read WHAT WAS PERSISTED rather than what the live activation happens to hold.
///
/// <para>That distinction is the whole point for one test here. "A vote past the deadline expires the
/// request" is trivially true of the in-memory object — the state machine mutated it — and the bug worth
/// catching is a store that persisted only when the vote was ACCEPTED, which leaves the stored copy
/// Pending while every read of the live activation says Expired. Memory grain storage would hide that;
/// forcing a deactivation to expose it would depend on the activation collector's timing. Reading the
/// bytes does neither.</para>
///
/// <para>Static, keyed by a per-cluster TestId, in the manner PersistenceModeTestRegistry already
/// establishes in this folder.</para>
/// </summary>
internal static class RecordedGrainStorage
{
    public static readonly ConcurrentDictionary<string, ConcurrentDictionary<string, string>> Writes = new();

    public static ConcurrentDictionary<string, string> For(string testId) =>
        Writes.GetOrAdd(testId, _ => new ConcurrentDictionary<string, string>());

    /// <summary>The last persisted state whose storage key contains <paramref name="grainKey"/>, or null
    /// if nothing was ever written for it — which is itself an assertable fact.</summary>
    public static T? Read<T>(string testId, string stateName, string grainKey) where T : class
    {
        var map = For(testId);
        var hit = map.FirstOrDefault(kv => kv.Key.StartsWith(stateName + "/", StringComparison.Ordinal)
            && kv.Key.Contains(grainKey, StringComparison.Ordinal));
        return hit.Value is null ? null : JsonSerializer.Deserialize<T>(hit.Value);
    }
}

internal sealed class RecordingGrainStorage(string testId) : IGrainStorage
{
    private static string Key(string stateName, GrainId grainId) => $"{stateName}/{grainId}";

    public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        if (RecordedGrainStorage.For(testId).TryGetValue(Key(stateName, grainId), out var json))
        {
            var value = JsonSerializer.Deserialize<T>(json);
            if (value is not null)
            {
                grainState.State = value;
                grainState.RecordExists = true;
                grainState.ETag = "1";
                return Task.CompletedTask;
            }
        }

        grainState.RecordExists = false;
        return Task.CompletedTask;
    }

    public Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        RecordedGrainStorage.For(testId)[Key(stateName, grainId)] = JsonSerializer.Serialize(grainState.State);
        grainState.RecordExists = true;
        grainState.ETag = "1";
        return Task.CompletedTask;
    }

    public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        RecordedGrainStorage.For(testId).TryRemove(Key(stateName, grainId), out _);
        grainState.RecordExists = false;
        return Task.CompletedTask;
    }
}

internal sealed class ApprovalTestSiloConfigurator : ISiloConfigurator
{
    public void Configure(ISiloBuilder siloBuilder) =>
        siloBuilder.ConfigureServices(services => services.AddGrainStorage(
            StreamConstants.StorageName,
            (sp, _) => new RecordingGrainStorage(
                sp.GetRequiredService<IConfiguration>()["TestId"]
                    ?? throw new InvalidOperationException("TestId not configured — see ApprovalTestSiloConfigurator."))));
}

internal sealed class ApprovalTestClientConfigurator : IClientBuilderConfigurator
{
    public void Configure(IConfiguration configuration, IClientBuilder clientBuilder) { }
}

/// <summary>
/// Plan 015 W4-B — the Orleans approvals store, against a real TestingHost cluster (grain discovery by
/// assembly scan, exactly as the Host does it; no Program.cs involvement).
///
/// <para>These tests deliberately do NOT re-test <c>ApprovalStateMachine</c>, which has its own 42 tests
/// in a project both solutions build. What they test is the three things the GRAIN owns and could get
/// wrong on its own: that it actually consults the state machine rather than deciding for itself, that
/// it persists on <c>StateChanged</c> rather than on <c>Accepted</c>, and that it resolves voter
/// eligibility from the access policy at all. Each is a bug a passing state-machine suite would not
/// see.</para>
///
/// <para>Every test uses its own approvals grain key and its own template/action names — the real
/// deployment has exactly one activation under <see cref="StreamConstants.ApprovalsKey"/>, but the
/// ACCESS policy singleton is shared by construction (the grain looks it up by
/// <see cref="StreamConstants.AccessKey"/>), so template action patterns must not overlap or one test's
/// first-match would answer another's.</para>
/// </summary>
public sealed class ApprovalGrainTests : IAsyncLifetime
{
    private TestCluster _cluster = null!;
    private string _testId = null!;

    public async Task InitializeAsync()
    {
        _testId = Guid.NewGuid().ToString("n");
        var builder = new TestClusterBuilder(1);
        builder.ConfigureHostConfiguration(config => config.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["TestId"] = _testId,
        }));
        builder.AddSiloBuilderConfigurator<ApprovalTestSiloConfigurator>();
        builder.AddClientBuilderConfigurator<ApprovalTestClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    public async Task DisposeAsync()
    {
        await _cluster.DisposeAsync();
        RecordedGrainStorage.Writes.TryRemove(_testId, out _);
    }

    private IApprovalGrain Approvals([System.Runtime.CompilerServices.CallerMemberName] string key = "") =>
        _cluster.GrainFactory.GetGrain<IApprovalGrain>("approvals-test-" + key);

    private IAccessPolicyGrain Policy =>
        _cluster.GrainFactory.GetGrain<IAccessPolicyGrain>(StreamConstants.AccessKey);

    /// <summary>Seeds one approver group and one template covering exactly one action, so the tests
    /// cannot shadow each other through the shared policy document.</summary>
    private async Task SeedAsync(string action, string groupName, string[] members, ApprovalTemplate template)
    {
        await Policy.UpsertGroupAsync(
            new GroupDefinition { Name = groupName, Members = [.. members] },
            "test");
        template.ActionPattern = action;
        template.ApproverGroups = [groupName];
        await Policy.UpsertApprovalTemplateAsync(template, "test");
    }

    private static ApprovalRequest Draft(string action, string requestedBy, string scope = "prod-thing") => new()
    {
        Action = action,
        Scope = scope,
        Reason = "because",
        RequestedBy = requestedBy,
    };

    // ------------------------------------------------------------------------------------------
    // Filing
    // ------------------------------------------------------------------------------------------

    /// <summary>The whitelist in <c>CreateRequest</c> is only a security property if the store actually
    /// routes through it. A draft that arrives pre-approved — populated Votes, State=Approved,
    /// RequiredApprovals=0 — must come back Pending with nothing counted.</summary>
    [Fact]
    public async Task Filing_DiscardsEverythingTheCallerTriedToPreDecide()
    {
        await SeedAsync("t1.act", "t1-approvers", ["bob"], new ApprovalTemplate { Name = "t1", RequiredApprovals = 2 });

        var draft = Draft("t1.act", "alice");
        draft.State = ApprovalState.Approved;
        draft.RequiredApprovals = 0;
        draft.Votes = [new ApprovalVote { Username = "alice", Approve = true }];
        draft.Id = "id-the-caller-chose";

        var stored = await Approvals().RequestAsync(draft);

        Assert.Equal(ApprovalState.Pending, stored.State);
        Assert.Empty(stored.Votes);
        Assert.Equal(2, stored.RequiredApprovals);
        Assert.NotEqual("id-the-caller-chose", stored.Id);
        Assert.Equal(["t1-approvers"], stored.ApproverGroups);
        Assert.Equal("t1", stored.TemplateName);
    }

    /// <summary>No enabled template covering the action means there is nobody to approve it. Filing
    /// anyway would create a request with an empty approver group — one nobody can act on, which can
    /// only rot until it expires — so the store refuses instead, loudly.</summary>
    [Fact]
    public async Task Filing_WithNoMatchingTemplate_Refuses()
    {
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => Approvals().RequestAsync(Draft("t2.nobody-covers-this", "alice")));

        Assert.Contains("t2.nobody-covers-this", ex.Message, StringComparison.Ordinal);
    }

    // ------------------------------------------------------------------------------------------
    // Voting — the rule the whole plan exists for
    // ------------------------------------------------------------------------------------------

    /// <summary>
    /// THE test of this file. Alice files a request and is HERSELF in the approver group — the exact
    /// case a second pair of eyes exists for (an administrator asking for review of their own
    /// privileged action) — and her own vote must not count.
    ///
    /// <para>It proves the grain consults <c>ApprovalStateMachine</c> rather than deciding for itself,
    /// because a store that checked only "is the voter eligible?" — the obvious, wrong implementation —
    /// would approve this request on the spot: alice IS eligible. Bob's vote immediately afterwards
    /// approves it, which is what makes the refusal a rule and not a broken setup.</para>
    /// </summary>
    [Fact]
    public async Task RequestersOwnVote_IsRefused_EvenWhenSheIsAnApprover()
    {
        await SeedAsync("t3.act", "t3-approvers", ["alice", "bob"], new ApprovalTemplate { Name = "t3", RequiredApprovals = 1 });
        var filed = await Approvals().RequestAsync(Draft("t3.act", "alice"));

        var afterSelfVote = await Approvals().VoteAsync(filed.Id, new ApprovalVote { Username = "alice", Approve = true });

        Assert.NotNull(afterSelfVote);
        Assert.Equal(ApprovalState.Pending, afterSelfVote!.State);
        Assert.Empty(afterSelfVote.Votes);

        var afterBob = await Approvals().VoteAsync(filed.Id, new ApprovalVote { Username = "bob", Approve = true });
        Assert.Equal(ApprovalState.Approved, afterBob!.State);
        Assert.Single(afterBob.Votes);
    }

    /// <summary>Eligibility is resolved from the access-policy document, not assumed. Carol is a real
    /// user in no approver group: her vote is refused and nothing is recorded. <c>NotAnApprover</c> is
    /// the enum's default, so this is also the assertion that a forgotten lookup path fails closed.</summary>
    [Fact]
    public async Task VoteFromSomeoneOutsideEveryApproverGroup_IsRefused()
    {
        await SeedAsync("t4.act", "t4-approvers", ["bob"], new ApprovalTemplate { Name = "t4", RequiredApprovals = 1 });
        var filed = await Approvals().RequestAsync(Draft("t4.act", "alice"));

        var after = await Approvals().VoteAsync(filed.Id, new ApprovalVote { Username = "carol", Approve = true });

        Assert.Equal(ApprovalState.Pending, after!.State);
        Assert.Empty(after.Votes);
    }

    /// <summary>A DISABLED approver is not an approver. Eligibility is resolved through AppCore's
    /// EffectivePermissionsBuilder, which returns no groups at all for a disabled user — so disabling an
    /// account stops that person casting the deciding vote through the same mechanism that kills their
    /// token, rather than through a second rule somebody has to remember. A hand-rolled walk of
    /// <c>policy.Groups</c> would pass every other test in this file and fail this one, which is why it
    /// is here.</summary>
    [Fact]
    public async Task ADisabledApprover_IsNotAnApprover()
    {
        // "erin" and not "bob": the access document is shared by every test in this class, so disabling
        // an approver the other tests rely on would make this test's order matter.
        await SeedAsync("t12.act", "t12-approvers", ["erin"], new ApprovalTemplate { Name = "t12", RequiredApprovals = 1 });
        await Policy.UpsertUserAccessAsync(new UserAccessEntry { Username = "erin", Disabled = true }, "test");

        var filed = await Approvals().RequestAsync(Draft("t12.act", "alice"));
        var after = await Approvals().VoteAsync(filed.Id, new ApprovalVote { Username = "erin", Approve = true });

        Assert.Equal(ApprovalState.Pending, after!.State);
        Assert.Empty(after.Votes);
    }

    /// <summary>
    /// A vote arriving after the deadline is refused AND expires the request — and the store must
    /// PERSIST that, because <c>ApplyVote</c> reports <c>StateChanged</c> without reporting
    /// <c>Accepted</c>.
    ///
    /// <para>Persisting on <c>Accepted</c> is the plausible mistake, and it is invisible from the live
    /// activation: the in-memory request says Expired either way. So this reads the stored bytes back
    /// out of the storage provider. If the write had been skipped, the persisted request would still be
    /// Pending and a restart would resurrect a request the deadline had already killed — a late approval
    /// landing on an expired action, which is the one outcome this whole plan exists to prevent.</para>
    /// </summary>
    [Fact]
    public async Task VotePastTheDeadline_IsRefused_AndTheExpiryIsPersisted()
    {
        await SeedAsync(
            "t5.act",
            "t5-approvers",
            ["bob"],
            new ApprovalTemplate { Name = "t5", RequiredApprovals = 1, ExpiresAfterSeconds = 1 });

        var filed = await Approvals().RequestAsync(Draft("t5.act", "alice"));
        Assert.True(filed.ExpiresAtMs > 0);

        // The deadline is real time: ApplyVote takes the clock itself precisely so that "state is still
        // Pending" and "the deadline has not passed" cannot be confused, and there is no seam to fake it
        // through the frozen facade. One second is the smallest the template grammar expresses.
        await Task.Delay(1_300);

        var after = await Approvals().VoteAsync(filed.Id, new ApprovalVote { Username = "bob", Approve = true });

        Assert.Equal(ApprovalState.Expired, after!.State);
        Assert.Empty(after.Votes);

        var persisted = RecordedGrainStorage.Read<ApprovalGrainState>(_testId, "approvals", "VotePastTheDeadline");
        Assert.NotNull(persisted);
        var persistedRequest = Assert.Single(persisted!.Requests);
        Assert.Equal(ApprovalState.Expired, persistedRequest.State);
        Assert.Empty(persistedRequest.Votes);
    }

    // ------------------------------------------------------------------------------------------
    // Sweeping
    // ------------------------------------------------------------------------------------------

    /// <summary>
    /// Escalation widens the approver pool exactly ONCE, however many times the sweeper runs.
    ///
    /// <para>The sweeper is a <c>BackgroundService</c> on a 30s interval, so a request that escalated on
    /// every tick would grow its approver list and re-notify its inbox forever. The latch is
    /// <c>EscalatedAtMs</c>, and the assertion that matters is the SECOND sweep returning 0: a count is
    /// what the sweeper logs and what wave 5 will surface, so a repeated escalation would be visible as
    /// noise long before anyone noticed the duplicated group.</para>
    ///
    /// <para><c>nowMs</c> is a parameter of <c>SweepAsync</c>, so this test needs no delay at all — it
    /// sweeps two minutes into the future, twice.</para>
    /// </summary>
    [Fact]
    public async Task Escalation_HappensExactlyOnce_AcrossTwoSweeps()
    {
        await Policy.UpsertGroupAsync(new GroupDefinition { Name = "t6-oncall", Members = ["dave"] }, "test");
        await SeedAsync(
            "t6.act",
            "t6-approvers",
            ["bob"],
            new ApprovalTemplate
            {
                Name = "t6",
                RequiredApprovals = 1,
                ExpiresAfterSeconds = 3_600,
                EscalateAfterSeconds = 60,
                EscalationGroups = ["t6-oncall"],
            });

        var filed = await Approvals().RequestAsync(Draft("t6.act", "alice"));
        var wellPastEscalation = filed.RequestedAtMs + 120_000;

        Assert.Equal(1, await Approvals().SweepAsync(wellPastEscalation));

        var escalated = await Approvals().GetAsync(filed.Id);
        Assert.Equal(["t6-approvers", "t6-oncall"], escalated!.ApproverGroups);
        Assert.NotNull(escalated.EscalatedAtMs);
        Assert.Equal(ApprovalState.Pending, escalated.State);

        Assert.Equal(0, await Approvals().SweepAsync(wellPastEscalation + 60_000));

        var afterSecondSweep = await Approvals().GetAsync(filed.Id);
        Assert.Equal(["t6-approvers", "t6-oncall"], afterSecondSweep!.ApproverGroups);
        Assert.Equal(escalated.EscalatedAtMs, afterSecondSweep.EscalatedAtMs);
    }

    /// <summary>Escalation widens rather than replaces, so the escalated-to group can vote and so can
    /// the original one. Also the only test that a request survives escalation as Pending — an
    /// escalation that decided anything would be a control that fires itself.</summary>
    [Fact]
    public async Task AnEscalatedToApprover_MayThenVote()
    {
        await Policy.UpsertGroupAsync(new GroupDefinition { Name = "t7-oncall", Members = ["dave"] }, "test");
        await SeedAsync(
            "t7.act",
            "t7-approvers",
            ["bob"],
            new ApprovalTemplate
            {
                Name = "t7",
                RequiredApprovals = 1,
                ExpiresAfterSeconds = 3_600,
                EscalateAfterSeconds = 60,
                EscalationGroups = ["t7-oncall"],
            });

        var filed = await Approvals().RequestAsync(Draft("t7.act", "alice"));

        // Dave is not an approver yet.
        Assert.Equal(ApprovalState.Pending, (await Approvals().VoteAsync(filed.Id, new ApprovalVote { Username = "dave", Approve = true }))!.State);

        await Approvals().SweepAsync(filed.RequestedAtMs + 120_000);

        var after = await Approvals().VoteAsync(filed.Id, new ApprovalVote { Username = "dave", Approve = true });
        Assert.Equal(ApprovalState.Approved, after!.State);
    }

    /// <summary>A sweep that changes nothing writes nothing and reports nothing — the count the sweeper
    /// logs would be meaningless otherwise, and a no-op write per 30s per host is a rewrite of the whole
    /// document for no reason.</summary>
    [Fact]
    public async Task SweepThatChangesNothing_ReportsZero()
    {
        await SeedAsync("t8.act", "t8-approvers", ["bob"], new ApprovalTemplate { Name = "t8", RequiredApprovals = 1, ExpiresAfterSeconds = 3_600 });
        var filed = await Approvals().RequestAsync(Draft("t8.act", "alice"));

        Assert.Equal(0, await Approvals().SweepAsync(filed.RequestedAtMs + 1_000));
    }

    // ------------------------------------------------------------------------------------------
    // The rest of the surface
    // ------------------------------------------------------------------------------------------

    /// <summary>Cancel is the requester's alone, and an unknown id is null rather than an exception —
    /// "no such request" and "that request, unchanged" are different answers and the facade returns
    /// both.</summary>
    [Fact]
    public async Task Cancel_IsTheRequestersAlone_AndAnUnknownIdIsNull()
    {
        await SeedAsync("t9.act", "t9-approvers", ["bob"], new ApprovalTemplate { Name = "t9", RequiredApprovals = 1 });
        var filed = await Approvals().RequestAsync(Draft("t9.act", "alice"));

        Assert.Null(await Approvals().CancelAsync("no-such-id", "alice"));
        Assert.Equal(ApprovalState.Pending, (await Approvals().CancelAsync(filed.Id, "bob"))!.State);
        Assert.Equal(ApprovalState.Cancelled, (await Approvals().CancelAsync(filed.Id, "alice"))!.State);
    }

    /// <summary>An outcome may only be stamped on an APPROVED request: recording that something ran when
    /// nobody approved it is the event the plan exists to prevent, so it is refused rather than
    /// logged.</summary>
    [Fact]
    public async Task RecordOutcome_OnlyAppliesToAnApprovedRequest()
    {
        await SeedAsync("t10.act", "t10-approvers", ["bob"], new ApprovalTemplate { Name = "t10", RequiredApprovals = 1 });
        var filed = await Approvals().RequestAsync(Draft("t10.act", "alice"));

        Assert.Equal(ApprovalState.Pending, (await Approvals().RecordOutcomeAsync(filed.Id, true, "ran"))!.State);

        await Approvals().VoteAsync(filed.Id, new ApprovalVote { Username = "bob", Approve = true });
        var executed = await Approvals().RecordOutcomeAsync(filed.Id, true, "ran");

        Assert.Equal(ApprovalState.Executed, executed!.State);
        Assert.Equal("ran", executed.Outcome);
    }

    /// <summary>The inbox query: newest first, filtered by state, limited. Newest-first because every
    /// caller of this method is an inbox and nobody pages an approval queue from the bottom.</summary>
    [Fact]
    public async Task List_IsNewestFirst_AndFiltersByState()
    {
        await SeedAsync("t11.act", "t11-approvers", ["bob"], new ApprovalTemplate { Name = "t11", RequiredApprovals = 1 });
        var first = await Approvals().RequestAsync(Draft("t11.act", "alice", "one"));
        var second = await Approvals().RequestAsync(Draft("t11.act", "alice", "two"));
        await Approvals().CancelAsync(first.Id, "alice");

        var all = await Approvals().ListAsync(null, 0);
        Assert.Equal([second.Id, first.Id], all.Select(r => r.Id).ToArray());

        var pending = await Approvals().ListAsync(ApprovalState.Pending, 0);
        Assert.Equal([second.Id], pending.Select(r => r.Id).ToArray());

        Assert.Single(await Approvals().ListAsync(null, 1));
    }
}
