using System.Security.Claims;
using Microsoft.Extensions.Logging.Abstractions;
using StreamForge.Abstractions;
using StreamForge.Api.Auth;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// Plan 015 wave 2 — the caching contract of <see cref="PermissionResolver"/> and the decision shape of
/// <see cref="AccessGuard"/>.
///
/// <para>The evaluator and the flattener are pure and already tested in both suites
/// (<c>StreamForge.AppCore.Tests/Access</c>). What is NOT covered there, and is exactly what this wave
/// added, is the part with a clock and a store behind it: that the TTL gates the cheap version poll and
/// not merely the expensive document fetch (on Dapr both are sidecar round trips, which is the entire
/// reason the design polls at all), that a version bump costs exactly one refetch, that a refresh which
/// throws leaves the previous snapshot serving instead of failing every request in the cluster, and that
/// a hundred simultaneous requests on a cold cache produce one call rather than a hundred.</para>
/// </summary>
public class PermissionResolverTests
{
    // ---------------------------------------------------------------------------------------------
    // The TTL, the version poll, and the refetch
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task TheTtlGatesTheVersionPollAndNotJustTheDocumentFetch()
    {
        var facade = new CountingAccessPolicyFacade(Doc(version: 1));
        var resolver = Resolver(facade, ttlSeconds: 600);

        await resolver.GetPolicyAsync();
        await resolver.GetPolicyAsync();
        await resolver.GetPolicyAsync();

        Assert.Equal(1, facade.VersionCalls);
        Assert.Equal(1, facade.PolicyCalls);
    }

    [Fact]
    public async Task AVersionBumpCausesExactlyOneRefetch()
    {
        var facade = new CountingAccessPolicyFacade(Doc(version: 1));
        // TTL 0: every call polls, so the ONLY thing that can suppress a fetch is the version compare.
        var resolver = Resolver(facade, ttlSeconds: 0);

        await resolver.GetPolicyAsync();
        Assert.Equal(1, facade.PolicyCalls);

        // Same version, twice more: polled twice more, fetched not at all.
        await resolver.GetPolicyAsync();
        await resolver.GetPolicyAsync();
        Assert.Equal(3, facade.VersionCalls);
        Assert.Equal(1, facade.PolicyCalls);

        facade.Document = Doc(version: 2);

        await resolver.GetPolicyAsync();
        Assert.Equal(2, facade.PolicyCalls);
        Assert.Equal(2, resolver.Version);

        // …and one refetch, not one per subsequent request.
        await resolver.GetPolicyAsync();
        await resolver.GetPolicyAsync();
        Assert.Equal(2, facade.PolicyCalls);
    }

    [Fact]
    public async Task AThrowingRefreshLeavesThePreviousSnapshotServing()
    {
        var facade = new CountingAccessPolicyFacade(Doc(version: 7));
        var resolver = Resolver(facade, ttlSeconds: 0);

        var loaded = await resolver.GetPolicyAsync();
        Assert.Equal(7, loaded.Version);
        Assert.True(resolver.HasSnapshot);

        facade.Throw = new InvalidOperationException("the silo is not answering");

        // Not an exception, not an empty document: the same snapshot, three times running. A resolver
        // that threw here would turn one flaky grain call into a cluster-wide 500.
        for (var i = 0; i < 3; i++)
        {
            var served = await resolver.GetPolicyAsync();
            Assert.Equal(7, served.Version);
            Assert.Same(loaded, served);
        }

        // And it recovers on its own the moment the store does.
        facade.Throw = null;
        facade.Document = Doc(version: 8);
        Assert.Equal(8, (await resolver.GetPolicyAsync()).Version);
    }

    [Fact]
    public async Task AColdCacheThatHasNeverLoadedGrantsNothingAndSaysSo()
    {
        var facade = new CountingAccessPolicyFacade(Doc(version: 1)) { Throw = new InvalidOperationException("down") };
        var resolver = Resolver(facade, ttlSeconds: 0);

        var document = await resolver.GetPolicyAsync();

        Assert.False(resolver.HasSnapshot);
        Assert.Empty(document.Roles);
        Assert.Empty(document.Users);
    }

    [Fact]
    public async Task ConcurrentRequestsOnAColdCacheRefreshOnce()
    {
        var entered = new TaskCompletionSource();
        var release = new TaskCompletionSource();
        var facade = new CountingAccessPolicyFacade(Doc(version: 1))
        {
            OnEnter = () => entered.TrySetResult(),
            Gate = release.Task,
        };
        var resolver = Resolver(facade, ttlSeconds: 600);

        // Hold the store open with one caller inside it, so the other sixty-four arrive while a refresh
        // is genuinely in flight — the only arrangement in which "single-flight" is being tested rather
        // than "the second caller happened to be late".
        var winner = Task.Run(() => resolver.GetPolicyAsync());
        await entered.Task;

        var rest = Enumerable.Range(0, 64).Select(_ => Task.Run(() => resolver.GetPolicyAsync())).ToArray();
        await Task.Delay(100);
        release.SetResult();
        await Task.WhenAll(rest.Append(winner));

        Assert.Equal(1, facade.VersionCalls);
        Assert.Equal(1, facade.PolicyCalls);
    }

    [Fact]
    public async Task InvalidateForcesTheNextCallToPollWithoutWaitingOutTheTtl()
    {
        var facade = new CountingAccessPolicyFacade(Doc(version: 1));
        var resolver = Resolver(facade, ttlSeconds: 600);

        await resolver.GetPolicyAsync();
        await resolver.GetPolicyAsync();
        Assert.Equal(1, facade.VersionCalls);

        facade.Document = Doc(version: 2);
        resolver.Invalidate();

        Assert.Equal(2, (await resolver.GetPolicyAsync()).Version);
        Assert.Equal(2, facade.VersionCalls);
        Assert.Equal(2, facade.PolicyCalls);
    }

    // ---------------------------------------------------------------------------------------------
    // ClaimsPrincipal → EffectivePermissions
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task TheGroupsClaimIsAnInputFromDayOneEvenThoughOidcIsDeferred()
    {
        var document = Doc(version: 1);
        document.Groups.Add(new GroupDefinition
        {
            Name = "risk",
            ExternalClaimValues = ["9d1c-risk-desk"],
            Grants = [new PermissionGrant { Action = Actions.TableWrite, Scope = "*" }],
        });

        var resolver = Resolver(new CountingAccessPolicyFacade(document), ttlSeconds: 600);

        var withClaim = await resolver.ResolveAsync(Principal("dana", groups: ["9d1c-risk-desk"]));
        Assert.Equal(["risk"], withClaim.Groups);
        Assert.Contains(withClaim.Grants, g => g.Action == Actions.TableWrite);

        var withoutClaim = await resolver.ResolveAsync(Principal("dana"));
        Assert.Empty(withoutClaim.Groups);
    }

    [Fact]
    public async Task TheRoleClaimIsThePreUpgradeFallbackAndNothingMore()
    {
        var document = Doc(version: 1);
        // A pre-upgrade catalog: roles seeded, but no per-user entry yet.
        document.Users.Add(new UserAccessEntry { Username = "migrated", Roles = ["Viewer"] });

        var resolver = Resolver(new CountingAccessPolicyFacade(document), ttlSeconds: 600);

        // No entry: the token's role claim is consulted.
        var unmigrated = await resolver.ResolveAsync(Principal("legacy-alice", role: "Editor"));
        Assert.Equal(["Editor"], unmigrated.Roles);
        Assert.Contains(unmigrated.Grants, g => g.Action == Actions.CatalogWrite);

        // An entry exists: the document wins, and a 12-hour-old "Editor" claim does not restore what an
        // administrator took away.
        var migrated = await resolver.ResolveAsync(Principal("migrated", role: "Editor"));
        Assert.Equal(["Viewer"], migrated.Roles);
        Assert.DoesNotContain(migrated.Grants, g => g.Action == Actions.CatalogWrite);
    }

    [Fact]
    public async Task ADisabledUserResolvesToNothingAtAll()
    {
        var document = Doc(version: 1);
        document.Users.Add(new UserAccessEntry { Username = "gone", Disabled = true, Roles = ["Admin"] });

        var resolver = Resolver(new CountingAccessPolicyFacade(document), ttlSeconds: 600);
        var permissions = await resolver.ResolveAsync(Principal("gone", role: "Admin"));

        Assert.True(permissions.Disabled);
        Assert.Empty(permissions.Grants);
    }

    // ---------------------------------------------------------------------------------------------
    // The guard
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task InLegacyModeTheGuardAllowsAndTheResolverIsNeverConsulted()
    {
        var facade = new CountingAccessPolicyFacade(Doc(version: 1));
        var guard = new AccessGuard(Resolver(facade, ttlSeconds: 600), entitlementsEnabled: false);

        // A principal who is disabled AND has no grants at all — in entitlements mode this is the most
        // denied caller there is.
        facade.Document.Users.Add(new UserAccessEntry { Username = "nobody", Disabled = true });

        var result = await guard.CheckAsync(Principal("nobody"), Actions.PipelineWrite, "p1");

        Assert.Equal(AccessDecision.Allowed, result.Decision);
        Assert.Contains("legacy", result.Reason, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(0, facade.VersionCalls);
        Assert.Equal(0, facade.PolicyCalls);
    }

    [Fact]
    public async Task TheGuardAnswersTheThreeStatesAndKeepsRequiresApprovalOutOfThe403()
    {
        var document = Doc(version: 1);
        document.Users.Add(new UserAccessEntry
        {
            Username = "carol",
            Grants =
            [
                new PermissionGrant { Action = Actions.PipelineWrite, Scope = "dev-*" },
                new PermissionGrant { Action = Actions.PipelineWrite, Scope = "prod-*", RequiresApproval = true },
            ],
        });

        var guard = new AccessGuard(Resolver(new CountingAccessPolicyFacade(document), 600), entitlementsEnabled: true);
        var carol = Principal("carol");

        Assert.Equal(AccessDecision.Allowed,
            (await guard.CheckAsync(carol, Actions.PipelineWrite, "dev-1")).Decision);
        Assert.Equal(AccessDecision.RequiresApproval,
            (await guard.CheckAsync(carol, Actions.PipelineWrite, "prod-1")).Decision);
        Assert.Equal(AccessDecision.Denied,
            (await guard.CheckAsync(carol, Actions.PipelineDelete, "dev-1")).Decision);
    }

    [Fact]
    public void TheReadyMade403CarriesTheDecisionsReason()
    {
        var denied = new StreamForge.AppCore.Access.AccessResult(
            AccessDecision.Denied, "denied by grant pipeline.* on prod-* (frozen for the close)", null);

        var result = AccessGuard.Deny(denied);

        // Results.Json<T> exposes both through IStatusCodeHttpResult / IValueHttpResult rather than a
        // public concrete type, which is also the shape the endpoints already return.
        Assert.Equal(403, Assert.IsAssignableFrom<Microsoft.AspNetCore.Http.IStatusCodeHttpResult>(result).StatusCode);
        var body = Assert.IsType<StreamForge.Api.ErrorResponse>(
            Assert.IsAssignableFrom<Microsoft.AspNetCore.Http.IValueHttpResult>(result).Value);
        Assert.Equal(denied.Reason, body.Error);
    }

    // ---------------------------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------------------------

    private static PermissionResolver Resolver(IAccessPolicyFacade facade, int ttlSeconds) =>
        new(facade, NullLogger<PermissionResolver>.Instance, ttlSeconds);

    /// <summary>A document with the three built-in roles in it, i.e. what a migrated catalog looks
    /// like.</summary>
    internal static AccessPolicyDocument Doc(long version) => new()
    {
        Roles = StreamForge.AppCore.Access.BuiltInRoleCatalog.Create(),
        Version = version,
    };

    internal static ClaimsPrincipal Principal(string name, string? role = null, IEnumerable<string>? groups = null)
    {
        var claims = new List<Claim> { new(ClaimTypes.Name, name) };
        if (role is not null)
        {
            claims.Add(new Claim(ClaimTypes.Role, role));
        }

        foreach (var group in groups ?? [])
        {
            claims.Add(new Claim(PermissionResolver.GroupsClaimType, group));
        }

        // A non-null authenticationType is what makes IsAuthenticated true — without it every policy
        // would fail on RequireAuthenticatedUser and the tests below would pass for the wrong reason.
        return new ClaimsPrincipal(new ClaimsIdentity(claims, "TestJwt"));
    }
}

/// <summary>A hand-written <see cref="IAccessPolicyFacade"/> that counts what the resolver asked it, can
/// be made to throw, and can be held open to test single-flight. The mutators throw: nothing in wave 2's
/// resolver path writes, and a fake that silently accepted a write would hide it if one appeared.</summary>
internal sealed class CountingAccessPolicyFacade(AccessPolicyDocument document) : IAccessPolicyFacade
{
    public AccessPolicyDocument Document { get; set; } = document;
    public Exception? Throw { get; set; }
    public Task? Gate { get; set; }
    public Action? OnEnter { get; set; }

    private int _versionCalls;
    private int _policyCalls;

    public int VersionCalls => Volatile.Read(ref _versionCalls);
    public int PolicyCalls => Volatile.Read(ref _policyCalls);

    public async Task<long> GetVersionAsync()
    {
        Interlocked.Increment(ref _versionCalls);
        await WaitAsync();
        return Document.Version;
    }

    public async Task<AccessPolicyDocument> GetPolicyAsync()
    {
        Interlocked.Increment(ref _policyCalls);
        await WaitAsync();
        return Document;
    }

    private async Task WaitAsync()
    {
        OnEnter?.Invoke();

        if (Gate is not null)
        {
            await Gate;
        }

        if (Throw is not null)
        {
            throw Throw;
        }
    }

    public Task<RoleDefinition?> UpsertRoleAsync(RoleDefinition role, string actor) => throw new NotSupportedException();
    public Task<bool> DeleteRoleAsync(string name) => throw new NotSupportedException();
    public Task<GroupDefinition?> UpsertGroupAsync(GroupDefinition group, string actor) => throw new NotSupportedException();
    public Task<bool> DeleteGroupAsync(string name) => throw new NotSupportedException();
    public Task<UserAccessEntry?> UpsertUserAccessAsync(UserAccessEntry entry, string actor) => throw new NotSupportedException();
    public Task<bool> DeleteUserAccessAsync(string username) => throw new NotSupportedException();
    public Task<ApprovalTemplate?> UpsertApprovalTemplateAsync(ApprovalTemplate template, string actor) => throw new NotSupportedException();
    public Task<bool> DeleteApprovalTemplateAsync(string name) => throw new NotSupportedException();
}
