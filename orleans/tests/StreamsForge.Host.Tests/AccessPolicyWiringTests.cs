using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using StreamsForge.Abstractions;
using StreamsForge.Api;
using StreamsForge.Api.Auth;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>
/// Plan 015 wave 2 — the backward-compatibility claim, tested through the real registration rather than
/// asserted in a comment.
///
/// <para>Everything here runs against the policies <see cref="StreamsForgeApiExtensions.AddStreamsForgeApi"/>
/// actually registers, resolved out of a real container by the real
/// <see cref="IAuthorizationService"/>. That matters because the claim being tested is not "the evaluator
/// says yes" — the pure suites cover that in both flavours — but "the three policy names still mean what
/// 68 <c>RequireAuthorization</c> sites and 30 gRPC attributes assume they mean, and each is satisfiable
/// from EITHER direction". A unit test of the handler in isolation would not notice the day somebody
/// composed the policy with <c>RequireRole</c> again and turned the OR into an AND.</para>
///
/// <para>No host is started: <c>builder.Build()</c> constructs the container and stops there, which is
/// also what keeps <c>AccessBootstrapService</c> from running — pinned below, because "the migration
/// quietly wrote to a store during a unit test" is the kind of thing one notices late.</para>
/// </summary>
public class AccessPolicyWiringTests
{
    private static (IAuthorizationService Auth, CountingAccessPolicyFacade Facade) Build(
        AccessPolicyDocument document,
        params (string Key, string Value)[] settings)
    {
        var builder = WebApplication.CreateBuilder();
        var config = new Dictionary<string, string?>
        {
            ["Jwt:Key"] = new string('k', 64),
            ["Jwt:Issuer"] = "streamsforge-test",
            ["Jwt:Audience"] = "streamsforge-test",
            // Long TTL: these tests are about the decision, not the refresh cadence, and one poll per
            // container keeps the call counts below meaningful.
            ["Auth:PolicyCacheSeconds"] = "600",
        };
        foreach (var (key, value) in settings)
        {
            config[key] = value;
        }

        builder.Configuration.AddInMemoryCollection(config);
        builder.Services.AddStreamsForgeApi(builder.Configuration);

        // Registered AFTER AddStreamsForgeApi, exactly as each host's AddOrleansFacades/AddDaprFacades is
        // — which is also the reason the resolver has to be registered with a lazy factory.
        var facade = new CountingAccessPolicyFacade(document);
        builder.Services.AddSingleton<IAccessPolicyFacade>(facade);
        builder.Services.AddSingleton<IUserStoreFacade>(new UnusedUserStoreFacade());

        var app = builder.Build();
        return (app.Services.GetRequiredService<IAuthorizationService>(), facade);
    }

    private static AccessPolicyDocument Migrated()
    {
        var document = PermissionResolverTests.Doc(version: 1);
        document.Users.Add(new UserAccessEntry { Username = "alice", Roles = [BuiltInRoles.Admin] });
        document.Users.Add(new UserAccessEntry { Username = "bob", Roles = [BuiltInRoles.Editor] });
        document.Users.Add(new UserAccessEntry { Username = "vic", Roles = [BuiltInRoles.Viewer] });
        return document;
    }

    // ---------------------------------------------------------------------------------------------
    // Editor and Admin: satisfiable from EITHER direction
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task TheEditorPolicyIsSatisfiedByTheLegacyRoleAlone()
    {
        // No entry in the document at all — a token minted before the migration ran.
        var (auth, _) = Build(PermissionResolverTests.Doc(version: 1));

        Assert.True((await auth.AuthorizeAsync(PermissionResolverTests.Principal("e", role: BuiltInRoles.Editor), "Editor")).Succeeded);
        Assert.True((await auth.AuthorizeAsync(PermissionResolverTests.Principal("a", role: BuiltInRoles.Admin), "Editor")).Succeeded);
        Assert.False((await auth.AuthorizeAsync(PermissionResolverTests.Principal("v", role: BuiltInRoles.Viewer), "Editor")).Succeeded);
    }

    [Fact]
    public async Task TheEditorPolicyIsSatisfiedByTheEntitlementAlone()
    {
        var document = Migrated();
        // A custom role, no legacy role name anywhere near it, and a token whose role claim says Viewer.
        document.Roles.Add(new RoleDefinition
        {
            Name = "Curator",
            Grants = [new PermissionGrant { Action = Actions.CatalogWrite, Scope = "*" }],
        });
        document.Users.Add(new UserAccessEntry { Username = "cara", Roles = ["Curator"] });

        var (auth, _) = Build(document);

        var cara = PermissionResolverTests.Principal("cara", role: BuiltInRoles.Viewer);
        Assert.True((await auth.AuthorizeAsync(cara, "Editor")).Succeeded);
        Assert.False((await auth.AuthorizeAsync(cara, "Admin")).Succeeded);
    }

    [Fact]
    public async Task TheAdminPolicyIsSatisfiedByEitherRoute()
    {
        var document = Migrated();
        document.Users.Add(new UserAccessEntry
        {
            Username = "sec",
            Grants = [new PermissionGrant { Action = Actions.AccessWrite, Scope = "*" }],
        });

        var (auth, _) = Build(document);

        // Legacy: the role claim alone, no document entry.
        Assert.True((await auth.AuthorizeAsync(PermissionResolverTests.Principal("nobody-yet", role: BuiltInRoles.Admin), "Admin")).Succeeded);
        // Entitlement: access.write alone, and a role claim that grants nothing.
        Assert.True((await auth.AuthorizeAsync(PermissionResolverTests.Principal("sec", role: BuiltInRoles.Viewer), "Admin")).Succeeded);
        // Neither.
        Assert.False((await auth.AuthorizeAsync(PermissionResolverTests.Principal("bob", role: BuiltInRoles.Editor), "Admin")).Succeeded);
    }

    [Fact]
    public async Task AnonymousSatisfiesNothing()
    {
        var (auth, _) = Build(Migrated());
        var anonymous = new System.Security.Claims.ClaimsPrincipal(new System.Security.Claims.ClaimsIdentity());

        Assert.False((await auth.AuthorizeAsync(anonymous, "Viewer")).Succeeded);
        Assert.False((await auth.AuthorizeAsync(anonymous, "Editor")).Succeeded);
        Assert.False((await auth.AuthorizeAsync(anonymous, "Admin")).Succeeded);
    }

    [Fact]
    public async Task ADenyGrantIsNotOverriddenByAnAllowAtThePolicyGate()
    {
        var document = Migrated();
        document.Users.Add(new UserAccessEntry
        {
            Username = "frozen",
            Roles = [BuiltInRoles.Editor],
            Grants = [new PermissionGrant { Action = Actions.CatalogWrite, Scope = "*", Effect = PermissionEffect.Deny }],
        });

        var (auth, _) = Build(document);

        // The entitlement route is denied — but the legacy role claim still satisfies the policy, which
        // is the honest consequence of "OR" and exactly why wave 3 has to migrate the call sites. Pinned
        // so that the limitation is a documented test rather than a surprise.
        Assert.True((await auth.AuthorizeAsync(PermissionResolverTests.Principal("frozen", role: BuiltInRoles.Editor), "Editor")).Succeeded);
        // Without the legacy claim, the Deny stands.
        Assert.False((await auth.AuthorizeAsync(PermissionResolverTests.Principal("frozen"), "Editor")).Succeeded);
    }

    // ---------------------------------------------------------------------------------------------
    // Auth:StrictViewer — the plan's one intentional behaviour change
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task StrictViewerRefusesADisabledUserAndAUserWhoseRoleWasDeleted()
    {
        var document = Migrated();
        document.Users.Add(new UserAccessEntry { Username = "gone", Disabled = true, Roles = [BuiltInRoles.Admin] });
        document.Users.Add(new UserAccessEntry { Username = "orphan", Roles = ["DeletedRole"] });

        var (auth, _) = Build(document);

        Assert.True((await auth.AuthorizeAsync(PermissionResolverTests.Principal("vic", role: BuiltInRoles.Viewer), "Viewer")).Succeeded);
        Assert.False((await auth.AuthorizeAsync(PermissionResolverTests.Principal("gone", role: BuiltInRoles.Admin), "Viewer")).Succeeded);
        Assert.False((await auth.AuthorizeAsync(PermissionResolverTests.Principal("orphan", role: BuiltInRoles.Viewer), "Viewer")).Succeeded);
    }

    [Fact]
    public async Task StrictViewerAdmitsAUserWhoHoldsOnlyDirectGrantsOrOnlyAGroup()
    {
        var document = Migrated();
        document.Users.Add(new UserAccessEntry
        {
            Username = "granted",
            Grants = [new PermissionGrant { Action = Actions.TableRead, Scope = "tag:public" }],
        });
        document.Groups.Add(new GroupDefinition { Name = "desk", Members = ["grouped"] });

        var (auth, _) = Build(document);

        Assert.True((await auth.AuthorizeAsync(PermissionResolverTests.Principal("granted"), "Viewer")).Succeeded);
        Assert.True((await auth.AuthorizeAsync(PermissionResolverTests.Principal("grouped"), "Viewer")).Succeeded);
    }

    [Fact]
    public async Task StrictViewerStandsDownAgainstAPreUpgradeCatalog()
    {
        // No roles in the document: the migration has not landed, so every principal's role would look
        // deleted. Locking the whole cluster out is not a security improvement.
        var (auth, _) = Build(new AccessPolicyDocument { Version = 1 });

        Assert.True((await auth.AuthorizeAsync(PermissionResolverTests.Principal("anyone", role: BuiltInRoles.Viewer), "Viewer")).Succeeded);
    }

    [Fact]
    public async Task StrictViewerStandsDownWhenThePolicyStoreCannotBeReached()
    {
        var facade = new CountingAccessPolicyFacade(Migrated()) { Throw = new InvalidOperationException("silo down") };
        var builder = WebApplication.CreateBuilder();
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Jwt:Key"] = new string('k', 64),
            ["Jwt:Issuer"] = "streamsforge-test",
            ["Jwt:Audience"] = "streamsforge-test",
        });
        builder.Services.AddStreamsForgeApi(builder.Configuration);
        builder.Services.AddSingleton<IAccessPolicyFacade>(facade);
        builder.Services.AddSingleton<IUserStoreFacade>(new UnusedUserStoreFacade());
        var auth = builder.Build().Services.GetRequiredService<IAuthorizationService>();

        Assert.True((await auth.AuthorizeAsync(PermissionResolverTests.Principal("vic", role: BuiltInRoles.Viewer), "Viewer")).Succeeded);
    }

    [Fact]
    public async Task StrictViewerCanBeTurnedOffOnItsOwn()
    {
        var document = Migrated();
        document.Users.Add(new UserAccessEntry { Username = "gone", Disabled = true, Roles = [BuiltInRoles.Admin] });

        var (auth, _) = Build(document, ("Auth:StrictViewer", "false"));

        Assert.True((await auth.AuthorizeAsync(PermissionResolverTests.Principal("gone", role: BuiltInRoles.Admin), "Viewer")).Succeeded);
        // …and the entitlement half of Editor/Admin is untouched by that flag.
        Assert.False((await auth.AuthorizeAsync(PermissionResolverTests.Principal("gone"), "Editor")).Succeeded);
    }

    // ---------------------------------------------------------------------------------------------
    // Auth:Mode=legacy — byte-identical to the pre-plan behaviour
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task LegacyModeBehavesExactlyAsBeforeAndNeverConsultsTheResolver()
    {
        var document = Migrated();
        document.Users.Add(new UserAccessEntry { Username = "gone", Disabled = true, Roles = [BuiltInRoles.Admin] });
        document.Users.Add(new UserAccessEntry
        {
            Username = "cara",
            Grants = [new PermissionGrant { Action = "*", Scope = "*" }],
        });

        var (auth, facade) = Build(document, ("Auth:Mode", "legacy"));

        // Viewer == RequireAuthenticatedUser(), and nothing else: a disabled user still passes.
        Assert.True((await auth.AuthorizeAsync(PermissionResolverTests.Principal("gone", role: BuiltInRoles.Admin), "Viewer")).Succeeded);
        Assert.True((await auth.AuthorizeAsync(PermissionResolverTests.Principal("whoever"), "Viewer")).Succeeded);

        // Editor == RequireRole("Editor","Admin"); Admin == RequireRole("Admin").
        Assert.True((await auth.AuthorizeAsync(PermissionResolverTests.Principal("b", role: BuiltInRoles.Editor), "Editor")).Succeeded);
        Assert.True((await auth.AuthorizeAsync(PermissionResolverTests.Principal("a", role: BuiltInRoles.Admin), "Editor")).Succeeded);
        Assert.True((await auth.AuthorizeAsync(PermissionResolverTests.Principal("a", role: BuiltInRoles.Admin), "Admin")).Succeeded);
        Assert.False((await auth.AuthorizeAsync(PermissionResolverTests.Principal("b", role: BuiltInRoles.Editor), "Admin")).Succeeded);
        Assert.False((await auth.AuthorizeAsync(PermissionResolverTests.Principal("v", role: BuiltInRoles.Viewer), "Editor")).Succeeded);

        // "cara" holds `*` on `*` in the document and would sail through in entitlements mode. In legacy
        // she is a Viewer, because the document is not read.
        Assert.False((await auth.AuthorizeAsync(PermissionResolverTests.Principal("cara", role: BuiltInRoles.Viewer), "Editor")).Succeeded);

        // The claim the mode makes: the store was not touched once.
        Assert.Equal(0, facade.VersionCalls);
        Assert.Equal(0, facade.PolicyCalls);
    }

    [Fact]
    public async Task TheModeFlagIsSpeltLegacyAndAnythingElseMeansEntitlements()
    {
        // Not a general "unknown values are ignored" policy — a deliberate one-way default: a typo in
        // Auth:Mode leaves enforcement ON rather than silently disabling the whole feature.
        var (auth, facade) = Build(Migrated(), ("Auth:Mode", "Legacy"));
        Assert.True((await auth.AuthorizeAsync(PermissionResolverTests.Principal("x"), "Viewer")).Succeeded);
        Assert.Equal(0, facade.PolicyCalls);

        var (strict, strictFacade) = Build(Migrated(), ("Auth:Mode", "entitlments-typo"));
        Assert.False((await strict.AuthorizeAsync(PermissionResolverTests.Principal("x"), "Viewer")).Succeeded);
        Assert.Equal(1, strictFacade.PolicyCalls);
    }

    // ---------------------------------------------------------------------------------------------
    // Bootstrap
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public void BuildingTheContainerDoesNotRunTheBootstrapMigration()
    {
        // AuthorizationCoverageTests builds a WebApplication and never starts it; AddStreamsForgeApi now
        // registers a hosted service that writes to the access store. Hosted services run at StartAsync,
        // not at Build — pinned here so that stays true rather than being believed.
        var (_, facade) = Build(Migrated());

        Assert.Equal(0, facade.VersionCalls);
        Assert.Equal(0, facade.PolicyCalls);
    }
}

/// <summary>Only ever constructed so the container has something to satisfy
/// <see cref="AccessBootstrapService"/>'s dependency with; the service never starts in these tests.</summary>
internal sealed class UnusedUserStoreFacade : IUserStoreFacade
{
    public Task<UserRecord?> ValidateCredentialsAsync(string username, string password) => throw new NotSupportedException();
    public Task<List<UserRecord>> GetUsersAsync() => throw new NotSupportedException();
    public Task<bool> CreateUserAsync(string username, string displayName, string role, string password) => throw new NotSupportedException();
    public Task<bool> UpdateUserAsync(string username, string? displayName, string? role, string? password) => throw new NotSupportedException();
    public Task<bool> DeleteUserAsync(string username) => throw new NotSupportedException();
}
