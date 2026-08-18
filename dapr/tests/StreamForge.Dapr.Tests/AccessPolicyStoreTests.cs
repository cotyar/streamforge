using StreamForge.Abstractions;
using StreamForge.Dapr.Host.Access;
using Xunit;

namespace StreamForge.Dapr.Tests;

/// <summary>
/// Plan 015 W1: unit tests for the actor-framework-free access-policy logic (see
/// <see cref="AccessPolicyStore"/>'s class doc). No Dapr sidecar, no actor runtime, no Redis —
/// AccessPolicyStore is a plain class over an in-memory <see cref="AccessPolicyDocument"/>, which is exactly
/// why it was factored out of <see cref="StreamForge.Dapr.Host.Actors.AccessPolicyActor"/> this way, the
/// same split CatalogStore/RegistryActor already uses.
///
/// <para>What these tests are really pinning is the VERSION: every replica's permission resolver polls it on
/// a TTL and refetches the document only when it moves, so "bumped exactly once per mutation that happened,
/// and never for one that didn't" is the difference between revocation landing in ~10s and a cluster
/// refetching the policy in a loop. The rules here are shared with Orleans' AccessPolicyGrain — a divergence
/// between the two is a policy that means different things on two hosts.</para>
/// </summary>
public class AccessPolicyStoreTests
{
    private static (AccessPolicyDocument Doc, AccessPolicyStore Store) NewStore()
    {
        var doc = new AccessPolicyDocument();
        return (doc, new AccessPolicyStore(doc));
    }

    private static RoleDefinition Role(string name) => new() { Name = name, Description = "d" };

    [Fact]
    public void FreshDocument_IsEmptyAtVersionZero()
    {
        // An empty document is a legitimate state, not a bug: seeding the built-ins is
        // LegacyRoleMigration's job, and a pre-upgrade catalog looks exactly like this.
        var (doc, store) = NewStore();

        Assert.Equal(0, store.Version);
        Assert.Empty(doc.Roles);
        Assert.Empty(doc.Groups);
        Assert.Empty(doc.Users);
        Assert.Empty(doc.ApprovalTemplates);
    }

    [Fact]
    public void UpsertRole_CreatesStampsAndBumpsVersionByOne()
    {
        var (doc, store) = NewStore();

        var stored = store.UpsertRole(Role("Ops"), "admin");

        Assert.NotNull(stored);
        Assert.Equal("Ops", stored!.Name);
        Assert.Equal("admin", stored.UpdatedBy);
        Assert.True(stored.UpdatedAtMs > 0);
        Assert.Equal(1, store.Version);
        Assert.Equal(stored.UpdatedAtMs, doc.UpdatedAtMs);
        Assert.Single(doc.Roles);
    }

    [Fact]
    public void UpsertRole_ReturnsTheStoredCopyNotTheCallersObject()
    {
        // The caller must not keep a handle on the policy: mutating what it handed in after the write would
        // otherwise change the stored document with no write and — worse — no version bump, so no replica
        // would ever learn about it.
        var (doc, store) = NewStore();
        var input = Role("Ops");

        var stored = store.UpsertRole(input, "admin");
        input.Name = "Hijacked";
        input.Grants.Add(new PermissionGrant { Action = "*", Scope = "*" });

        Assert.NotSame(input, stored);
        Assert.Equal("Ops", doc.Roles[0].Name);
        Assert.Empty(doc.Roles[0].Grants);
    }

    [Fact]
    public void UpsertRole_ExistingNameReplacesInPlaceAndKeepsListPosition()
    {
        var (doc, store) = NewStore();
        store.UpsertRole(Role("A"), "admin");
        store.UpsertRole(Role("B"), "admin");

        var updated = Role("A");
        updated.Description = "changed";
        store.UpsertRole(updated, "editor");

        Assert.Equal(2, doc.Roles.Count);
        Assert.Equal("A", doc.Roles[0].Name);
        Assert.Equal("changed", doc.Roles[0].Description);
        Assert.Equal("editor", doc.Roles[0].UpdatedBy);
        Assert.Equal(3, store.Version);
    }

    [Fact]
    public void UpsertRole_MatchesNameOrdinalAndCaseSensitively()
    {
        // "Editor" and "editor" are two different strings in a token's role claim, so they are two
        // different roles here.
        var (doc, store) = NewStore();

        store.UpsertRole(Role("Editor"), "admin");
        store.UpsertRole(Role("editor"), "admin");

        Assert.Equal(2, doc.Roles.Count);
    }

    [Fact]
    public void UpsertRole_IdenticalContentStillBumpsVersion()
    {
        // Deliberate, and marked with a ponytail: comment on AccessPolicyStore.Bump — the cost is one
        // spurious refetch per replica, and the alternative is a structural comparison on every write.
        var (_, store) = NewStore();
        store.UpsertRole(Role("Ops"), "admin");

        store.UpsertRole(Role("Ops"), "admin");

        Assert.Equal(2, store.Version);
    }

    [Fact]
    public void UpsertRole_EmptyNameIsRejectedAndChangesNothing()
    {
        var (doc, store) = NewStore();

        Assert.Null(store.UpsertRole(Role(""), "admin"));
        Assert.Null(store.UpsertRole(Role("   "), "admin"));

        Assert.Empty(doc.Roles);
        Assert.Equal(0, store.Version);
    }

    [Theory]
    [InlineData(BuiltInRoles.Admin)]
    [InlineData(BuiltInRoles.Editor)]
    [InlineData(BuiltInRoles.Viewer)]
    public void DeleteRole_RefusesABuiltInAndChangesNothing(string name)
    {
        // Deleting Viewer would strand every token minted before the upgrade.
        var (doc, store) = NewStore();
        store.UpsertRole(Role(name), "admin");
        var versionBefore = store.Version;

        Assert.False(store.DeleteRole(name));
        Assert.Single(doc.Roles);
        Assert.Equal(versionBefore, store.Version);
    }

    [Fact]
    public void DeleteRole_UnknownNameReturnsFalseWithoutBumping()
    {
        var (_, store) = NewStore();

        Assert.False(store.DeleteRole("nope"));
        Assert.Equal(0, store.Version);
    }

    [Fact]
    public void DeleteRole_CustomRoleIsRemovedAndBumps()
    {
        var (doc, store) = NewStore();
        store.UpsertRole(Role("Ops"), "admin");

        Assert.True(store.DeleteRole("Ops"));
        Assert.Empty(doc.Roles);
        Assert.Equal(2, store.Version);
    }

    [Fact]
    public void UpsertGroup_StampsCreatedAtOnCreateAndKeepsItOnUpdate()
    {
        // CreatedAtMs is server-owned: a caller round-tripping a DTO that never carried it must not be able
        // to zero it, and must not be able to rewrite when the group came into existence.
        var (doc, store) = NewStore();

        var created = store.UpsertGroup(new GroupDefinition { Name = "traders" }, "admin");
        Assert.NotNull(created);
        Assert.True(created!.CreatedAtMs > 0);

        var updated = store.UpsertGroup(
            new GroupDefinition { Name = "traders", CreatedAtMs = 0, Members = ["alice"] }, "editor");

        Assert.Equal(created.CreatedAtMs, updated!.CreatedAtMs);
        Assert.Equal(["alice"], updated.Members);
        Assert.Equal("editor", updated.UpdatedBy);
        Assert.Single(doc.Groups);
        Assert.Equal(2, store.Version);
    }

    [Fact]
    public void DeleteGroup_RemovesTheGroupAndRewritesNoUserRecord()
    {
        // Membership only ever lived on the group, so there is no dangling reference to clean up — and a
        // delete that fanned out into the user list would be the second whole-list-rewrite path this design
        // exists to avoid.
        var (doc, store) = NewStore();
        store.UpsertGroup(new GroupDefinition { Name = "traders", Members = ["alice"] }, "admin");
        var alice = store.UpsertUserAccess(new UserAccessEntry { Username = "alice", Roles = ["Viewer"] }, "admin");

        Assert.True(store.DeleteGroup("traders"));

        Assert.Empty(doc.Groups);
        Assert.Single(doc.Users);
        Assert.Equal(alice!.UpdatedAtMs, doc.Users[0].UpdatedAtMs);
        Assert.Equal(["Viewer"], doc.Users[0].Roles);
    }

    [Fact]
    public void DeleteGroup_UnknownNameReturnsFalseWithoutBumping()
    {
        var (_, store) = NewStore();

        Assert.False(store.DeleteGroup("nope"));
        Assert.Equal(0, store.Version);
    }

    [Fact]
    public void UpsertUserAccess_CreatesOnFirstWriteThenUpdatesInPlace()
    {
        // This is the path the user store uses to MIRROR UserRecord.Role on every create/update — which is
        // what makes a role change take effect within the resolver's TTL instead of at the next login.
        var (doc, store) = NewStore();

        store.UpsertUserAccess(new UserAccessEntry { Username = "alice", Roles = ["Viewer"] }, "admin");
        var updated = store.UpsertUserAccess(new UserAccessEntry { Username = "alice", Roles = ["Editor"], Disabled = true }, "admin");

        Assert.Single(doc.Users);
        Assert.Equal(["Editor"], updated!.Roles);
        Assert.True(doc.Users[0].Disabled);
        Assert.Equal(2, store.Version);
    }

    [Fact]
    public void UpsertUserAccess_EmptyUsernameIsRejectedAndChangesNothing()
    {
        var (doc, store) = NewStore();

        Assert.Null(store.UpsertUserAccess(new UserAccessEntry { Username = "" }, "admin"));

        Assert.Empty(doc.Users);
        Assert.Equal(0, store.Version);
    }

    [Fact]
    public void DeleteUserAccess_AbsentIsFalsePresentIsTrue()
    {
        var (doc, store) = NewStore();
        store.UpsertUserAccess(new UserAccessEntry { Username = "alice" }, "admin");

        Assert.False(store.DeleteUserAccess("bob"));
        Assert.Equal(1, store.Version);

        Assert.True(store.DeleteUserAccess("alice"));
        Assert.Empty(doc.Users);
        Assert.Equal(2, store.Version);
    }

    [Fact]
    public void UpsertApprovalTemplate_UpsertsByNameAndDeleteReportsAbsence()
    {
        var (doc, store) = NewStore();

        var created = store.UpsertApprovalTemplate(
            new ApprovalTemplate { Name = "prod-writes", ActionPattern = "pipeline.*", ScopePattern = "prod-*" }, "admin");
        Assert.NotNull(created);
        Assert.Equal(1, store.Version);

        store.UpsertApprovalTemplate(new ApprovalTemplate { Name = "prod-writes", RequiredApprovals = 2 }, "admin");
        Assert.Single(doc.ApprovalTemplates);
        Assert.Equal(2, doc.ApprovalTemplates[0].RequiredApprovals);

        Assert.False(store.DeleteApprovalTemplate("nope"));
        Assert.Equal(2, store.Version);

        Assert.True(store.DeleteApprovalTemplate("prod-writes"));
        Assert.Empty(doc.ApprovalTemplates);
        Assert.Equal(3, store.Version);
    }

    [Fact]
    public void UpsertApprovalTemplate_EmptyNameIsRejectedAndChangesNothing()
    {
        var (doc, store) = NewStore();

        Assert.Null(store.UpsertApprovalTemplate(new ApprovalTemplate { Name = "" }, "admin"));

        Assert.Empty(doc.ApprovalTemplates);
        Assert.Equal(0, store.Version);
    }

    [Fact]
    public void EveryMutationBumpsByExactlyOneAndRefusalsNeverDo()
    {
        // The resolver refetches the whole document whenever this number moves, so a bump for nothing is a
        // cluster-wide refetch for nothing.
        var (_, store) = NewStore();

        store.UpsertRole(Role("Ops"), "admin");                                              // 1
        store.UpsertGroup(new GroupDefinition { Name = "traders" }, "admin");                // 2
        store.UpsertUserAccess(new UserAccessEntry { Username = "alice" }, "admin");         // 3
        store.UpsertApprovalTemplate(new ApprovalTemplate { Name = "t" }, "admin");          // 4
        store.DeleteRole("Ops");                                                             // 5
        store.DeleteGroup("traders");                                                        // 6
        store.DeleteUserAccess("alice");                                                     // 7
        store.DeleteApprovalTemplate("t");                                                   // 8

        Assert.Equal(8, store.Version);

        store.DeleteRole(BuiltInRoles.Viewer);
        store.DeleteRole("missing");
        store.DeleteGroup("missing");
        store.DeleteUserAccess("missing");
        store.DeleteApprovalTemplate("missing");
        store.UpsertRole(Role(""), "admin");
        store.UpsertGroup(new GroupDefinition { Name = "" }, "admin");
        store.UpsertUserAccess(new UserAccessEntry { Username = "" }, "admin");
        store.UpsertApprovalTemplate(new ApprovalTemplate { Name = "" }, "admin");

        Assert.Equal(8, store.Version);
    }

    [Fact]
    public void Version_ReadHasNoSideEffects()
    {
        // Called by every replica every Auth:PolicyCacheSeconds — on this flavour that is a sidecar round
        // trip, which is exactly why the plan refused a per-request store lookup.
        var (doc, store) = NewStore();
        store.UpsertRole(Role("Ops"), "admin");
        var stampBefore = doc.UpdatedAtMs;

        for (var i = 0; i < 5; i++)
        {
            Assert.Equal(1, store.Version);
        }

        Assert.Equal(stampBefore, doc.UpdatedAtMs);
        Assert.Single(doc.Roles);
    }
}
