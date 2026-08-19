using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using StreamForge.Abstractions;
using StreamForge.Api;
using StreamForge.Api.Auth;
using StreamForge.AppCore.Access;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// Plan 015 wave 2-C — the mirror that was never wired, and the <c>/api/auth/me</c> payload shape.
///
/// <para><b>What the mirror is for.</b> The effective role list lives in
/// <see cref="UserAccessEntry.Roles"/>, not on <see cref="UserRecord"/>, so the two have to be kept in
/// step or a role change through <c>/api/users</c> takes effect only at the next host restart (when
/// <c>AccessBootstrapService</c> mirrors everybody once) — and silently falls back to the user's
/// 12-hour token in the meantime. These tests pin the three things that must be true of the mirror: it
/// updates on a role change, it preserves the two fields a role edit must never touch, and it does not
/// write when the document already agrees.</para>
///
/// <para><b>Tested through <see cref="UsersEndpoints.MirrorUserRoleAsync"/> rather than over HTTP.</b>
/// This repo has no HTTP-level harness (015 declined <c>WebApplicationFactory</c>: it needs both
/// flavours' <c>Program.cs</c> startable without their runtimes). The mirror is therefore a public
/// static that the three handlers call, and it is exercised here against a real in-memory store that
/// implements the same upsert-replaces-the-whole-entry semantics as <c>AccessPolicyGrain</c> and its
/// Dapr twin — which is precisely where the "preserve Disabled and Grants" rule has to live, since the
/// store itself preserves nothing.</para>
/// </summary>
public class UserRoleMirrorTests
{
    private static PermissionResolver Resolver(IAccessPolicyFacade facade, int ttlSeconds = 600) =>
        new(facade, NullLogger<PermissionResolver>.Instance, ttlSeconds);

    // ---------------------------------------------------------------------------------------------
    // The mirror
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task ARoleChangeIsMirroredIntoTheAccessEntry()
    {
        var store = new MirrorAccessPolicyFacade();
        store.Document.Users.Add(new UserAccessEntry { Username = "alice", Roles = ["Editor"] });

        var stored = await UsersEndpoints.MirrorUserRoleAsync(store, Resolver(store), "alice", "Viewer", "admin");

        Assert.NotNull(stored);
        Assert.Equal(["Viewer"], stored.Roles);
        Assert.Equal("admin", stored.UpdatedBy);

        // …and in the document, which is what the resolver will read.
        var entry = Assert.Single(store.Document.Users);
        Assert.Equal(["Viewer"], entry.Roles);
    }

    [Fact]
    public async Task TheMirrorNeverClearsDisabledOrDirectGrants()
    {
        var grant = new PermissionGrant { Action = Actions.TableWrite, Scope = "prod-*", Note = "on-call rota" };

        var store = new MirrorAccessPolicyFacade();
        store.Document.Users.Add(new UserAccessEntry
        {
            Username = "bob",
            // Somebody disabled bob during an incident. A routine "make bob a Viewer" that re-enabled
            // him would be a role edit silently re-granting access — the exact failure this plan exists
            // to prevent, and the reason this assertion is the load-bearing one in the file.
            Disabled = true,
            Roles = ["Admin"],
            Grants = [grant],
        });

        var stored = await UsersEndpoints.MirrorUserRoleAsync(store, Resolver(store), "bob", "Viewer", "admin");

        Assert.NotNull(stored);
        Assert.Equal(["Viewer"], stored.Roles);
        Assert.True(stored.Disabled);
        var kept = Assert.Single(stored.Grants);
        Assert.Equal(Actions.TableWrite, kept.Action);
        Assert.Equal("prod-*", kept.Scope);
        Assert.Equal("on-call rota", kept.Note);
    }

    [Fact]
    public async Task TheMirrorCreatesAnEntryForAUserThatHasNone()
    {
        var store = new MirrorAccessPolicyFacade();

        var stored = await UsersEndpoints.MirrorUserRoleAsync(store, Resolver(store), "carol", "Editor", "admin");

        Assert.NotNull(stored);
        var entry = Assert.Single(store.Document.Users);
        Assert.Equal("carol", entry.Username);
        Assert.Equal(["Editor"], entry.Roles);
        Assert.False(entry.Disabled);
        Assert.Empty(entry.Grants);
    }

    [Fact]
    public async Task TheMirrorDoesNotWriteWhenTheDocumentAlreadyAgrees()
    {
        var store = new MirrorAccessPolicyFacade();
        store.Document.Users.Add(new UserAccessEntry { Username = "dave", Roles = ["Editor"] });
        var versionBefore = store.Document.Version;

        await UsersEndpoints.MirrorUserRoleAsync(store, Resolver(store), "dave", "Editor", "admin");

        // Not merely an optimization: every write bumps the document version and therefore invalidates
        // every replica's policy cache. A mirror that wrote unconditionally would do that on every
        // display-name edit in the system, because the PUT handler mirrors on every update.
        Assert.Equal(0, store.Upserts);
        Assert.Equal(versionBefore, store.Document.Version);
    }

    [Fact]
    public async Task TheMirrorInvalidatesTheResolverSoTheChangeIsVisibleWithoutWaitingOutTheTtl()
    {
        var store = new MirrorAccessPolicyFacade();
        store.Document.Users.Add(new UserAccessEntry { Username = "erin", Roles = ["Viewer"] });

        // A long TTL: without the eager invalidation, the replica that made the change would keep
        // serving its own pre-change snapshot for the next ten minutes.
        var resolver = Resolver(store, ttlSeconds: 600);
        await resolver.GetPolicyAsync();
        var versionBefore = resolver.Version;

        await UsersEndpoints.MirrorUserRoleAsync(store, resolver, "erin", "Admin", "admin");

        var after = await resolver.GetPolicyAsync();
        Assert.True(after.Version > versionBefore);
        Assert.Equal(["Admin"], after.Users.Single(u => u.Username == "erin").Roles);
    }

    [Fact]
    public async Task AFailingAccessWriteThrowsRatherThanReportingSuccess()
    {
        // The handler catches this and answers 500 naming the split state (the user IS written, the
        // mirror is not). What is pinned here is only that the mirror does not swallow it: a mirror that
        // returned quietly would make the 200 a lie about whether a role change is in force.
        var store = new MirrorAccessPolicyFacade { Throw = new InvalidOperationException("the silo is not answering") };

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => UsersEndpoints.MirrorUserRoleAsync(store, Resolver(store), "frank", "Viewer", "admin"));
    }

    // ---------------------------------------------------------------------------------------------
    // The /api/auth/me payload
    // ---------------------------------------------------------------------------------------------

    /// <summary>ASP.NET's own defaults, which is what <c>ConfigureHttpJsonOptions</c> starts from —
    /// camelCase property names and nothing else configured that touches these fields.</summary>
    private static readonly JsonSerializerOptions Web = new(JsonSerializerDefaults.Web);

    [Fact]
    public void TheFourOriginalUserInfoFieldsSurviveUntouchedAndNothingElseIsSerialized()
    {
        // GET /api/users still builds this form, and Auth:Mode=legacy makes /api/auth/me build it too.
        var json = JsonSerializer.Serialize(new UserInfo("alice", "Alice", "Admin", 1234), Web);

        Assert.Equal("""{"username":"alice","displayName":"Alice","role":"Admin","createdAtMs":1234}""", json);

        // Spelled out because it is the whole rolling-deploy contract: the SPA reads a MISSING
        // permissions[] as "an old server" and falls back to ordinal Viewer < Editor < Admin. A
        // serialized `"permissions": null` is present, not missing, so the fallback would misfire.
        Assert.DoesNotContain("permissions", json, StringComparison.Ordinal);
        Assert.DoesNotContain("null", json, StringComparison.Ordinal);
    }

    [Fact]
    public void TheEntitlementFieldsAppearOnlyWhenPopulatedAndDoNotDisturbTheOriginalFour()
    {
        var permissions = new EffectivePermissions
        {
            Username = "alice",
            Disabled = false,
            Roles = ["Admin"],
            Groups = ["oncall"],
            Grants = [new PermissionGrant { Action = "*", Scope = "*" }],
            Version = 42,
        };

        var json = JsonSerializer.Serialize(
            new UserInfo(
                "alice", "Alice", "Admin", 1234,
                permissions.Grants, permissions.Roles, permissions.Groups, permissions.Disabled, permissions.Version),
            Web);

        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        Assert.Equal("alice", root.GetProperty("username").GetString());
        Assert.Equal("Alice", root.GetProperty("displayName").GetString());
        Assert.Equal("Admin", root.GetProperty("role").GetString());
        Assert.Equal(1234, root.GetProperty("createdAtMs").GetInt64());

        Assert.Equal(1, root.GetProperty("permissions").GetArrayLength());
        Assert.Equal("*", root.GetProperty("permissions")[0].GetProperty("action").GetString());
        Assert.Equal("Admin", root.GetProperty("roles")[0].GetString());
        Assert.Equal("oncall", root.GetProperty("groups")[0].GetString());
        Assert.False(root.GetProperty("disabled").GetBoolean());
        Assert.Equal(42, root.GetProperty("policyVersion").GetInt64());

        // The camelCase names have to be exactly these — web/src/api/types.ts declares them and the
        // client's fallback keys off `permissions` by name.
        Assert.Equal(
            ["username", "displayName", "role", "createdAtMs", "permissions", "roles", "groups", "disabled", "policyVersion"],
            root.EnumerateObject().Select(p => p.Name).ToArray());
    }

    // ---------------------------------------------------------------------------------------------
    // Plan 015 wave 6: the OTHER direction the two stores can drift — disabling a login.
    //
    // Tested against the statics rather than over HTTP for the reason this file's remarks already give.
    // Two facts, and the bug lived in the gap between them: the builder suppresses the role-claim
    // fallback the moment an entry EXISTS, and the disable route used to create one carrying nothing.
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public void AnEntryWithNoRolesIsNotTheSameAsNoEntryAtAll()
    {
        var document = new AccessPolicyDocument { Roles = BuiltInRoleCatalog.Create(), Version = 1 };

        // No entry: the token's role claim is consulted, which is what carries a pre-migration user.
        var withoutEntry = EffectivePermissionsBuilder.Build(document, "alice", roleClaim: "Editor");
        Assert.Equal(["Editor"], withoutEntry.Roles);
        Assert.NotEmpty(withoutEntry.Grants);

        // An entry that exists and names no role: the fallback is gone and so is everything else. This
        // is the state a disable/enable round trip used to leave behind — a login that works and can do
        // nothing, which is a demotion nobody asked for and nothing announces.
        document.Users.Add(new UserAccessEntry { Username = "alice" });
        var withEmptyEntry = EffectivePermissionsBuilder.Build(document, "alice", roleClaim: "Editor");
        Assert.Empty(withEmptyEntry.Roles);
        Assert.Empty(withEmptyEntry.Grants);
    }

    [Fact]
    public async Task DisablingAUserWithNoEntrySeedsTheRoleFromTheCredentialRecord()
    {
        var store = new SeedUserStore(new UserRecord { Username = "alice", Role = "Editor" });

        Assert.Equal(["Editor"], await AccessEndpoints.SeedRolesFromCredentialAsync(store, "alice"));
    }

    [Fact]
    public async Task SeedingNeverThrowsAndNeverInventsARole()
    {
        // No such account, and a store that is down. Both leave the entry incomplete rather than
        // failing the disable: refusing to disable a login because a completeness lookup broke would be
        // strictly worse than the incomplete entry it was trying to avoid.
        Assert.Empty(await AccessEndpoints.SeedRolesFromCredentialAsync(new SeedUserStore(), "nobody"));
        Assert.Empty(await AccessEndpoints.SeedRolesFromCredentialAsync(new BrokenUserStore(), "alice"));
    }

    private sealed class SeedUserStore(params UserRecord[] users) : IUserStoreFacade
    {
        public Task<UserRecord?> ValidateCredentialsAsync(string username, string password) => throw new NotSupportedException();
        public Task<List<UserRecord>> GetUsersAsync() => Task.FromResult(users.ToList());
        public Task<bool> CreateUserAsync(string username, string displayName, string role, string password) => throw new NotSupportedException();
        public Task<bool> UpdateUserAsync(string username, string? displayName, string? role, string? password) => throw new NotSupportedException();
        public Task<bool> DeleteUserAsync(string username) => throw new NotSupportedException();
    }

    private sealed class BrokenUserStore : IUserStoreFacade
    {
        public Task<UserRecord?> ValidateCredentialsAsync(string username, string password) => throw new NotSupportedException();
        public Task<List<UserRecord>> GetUsersAsync() => throw new InvalidOperationException("store unreachable");
        public Task<bool> CreateUserAsync(string username, string displayName, string role, string password) => throw new NotSupportedException();
        public Task<bool> UpdateUserAsync(string username, string? displayName, string? role, string? password) => throw new NotSupportedException();
        public Task<bool> DeleteUserAsync(string username) => throw new NotSupportedException();
    }
}

/// <summary>An in-memory <see cref="IAccessPolicyFacade"/> with real upsert semantics for the members
/// the mirror uses — replace-the-whole-entry and bump the version, exactly as <c>AccessPolicyGrain</c>
/// and the Dapr <c>AccessPolicyStore</c> do. It stamps and version-bumps because the assertions above
/// are about what the mirror hands the store and what comes back, and a fake that did neither would let
/// a mirror that forgot to preserve a field pass.
///
/// <para>Separate from <c>CountingAccessPolicyFacade</c> in PermissionResolverTests: that one throws on
/// every mutator on purpose, because nothing on the resolver's path writes.</para></summary>
internal sealed class MirrorAccessPolicyFacade : IAccessPolicyFacade
{
    public AccessPolicyDocument Document { get; } = new();
    public Exception? Throw { get; set; }
    public int Upserts { get; private set; }

    public Task<AccessPolicyDocument> GetPolicyAsync() =>
        Throw is not null ? Task.FromException<AccessPolicyDocument>(Throw) : Task.FromResult(Document);

    public Task<long> GetVersionAsync() =>
        Throw is not null ? Task.FromException<long>(Throw) : Task.FromResult(Document.Version);

    public Task<UserAccessEntry?> UpsertUserAccessAsync(UserAccessEntry entry, string actor)
    {
        if (Throw is not null)
        {
            return Task.FromException<UserAccessEntry?>(Throw);
        }

        if (string.IsNullOrWhiteSpace(entry.Username))
        {
            return Task.FromResult<UserAccessEntry?>(null);
        }

        Upserts++;
        entry.UpdatedAtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        entry.UpdatedBy = actor;

        var idx = Document.Users.FindIndex(u => string.Equals(u.Username, entry.Username, StringComparison.Ordinal));
        if (idx >= 0)
        {
            Document.Users[idx] = entry;
        }
        else
        {
            Document.Users.Add(entry);
        }

        Document.Version++;
        return Task.FromResult<UserAccessEntry?>(entry);
    }

    public Task<bool> DeleteUserAccessAsync(string username)
    {
        if (Throw is not null)
        {
            return Task.FromException<bool>(Throw);
        }

        var idx = Document.Users.FindIndex(u => string.Equals(u.Username, username, StringComparison.Ordinal));
        if (idx < 0)
        {
            return Task.FromResult(false);
        }

        Document.Users.RemoveAt(idx);
        Document.Version++;
        return Task.FromResult(true);
    }

    // Not on the mirror's path. Throwing keeps it that way.
    public Task<RoleDefinition?> UpsertRoleAsync(RoleDefinition role, string actor) => throw new NotSupportedException();
    public Task<bool> DeleteRoleAsync(string name) => throw new NotSupportedException();
    public Task<GroupDefinition?> UpsertGroupAsync(GroupDefinition group, string actor) => throw new NotSupportedException();
    public Task<bool> DeleteGroupAsync(string name) => throw new NotSupportedException();
    public Task<ApprovalTemplate?> UpsertApprovalTemplateAsync(ApprovalTemplate template, string actor) => throw new NotSupportedException();
    public Task<bool> DeleteApprovalTemplateAsync(string name) => throw new NotSupportedException();
}
