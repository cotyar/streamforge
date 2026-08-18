using StreamForge.Abstractions;
using StreamForge.AppCore.Access;
using Xunit;

namespace StreamForge.AppCore.Tests.Access;

/// <summary>Plan 015 wave 1 — the once-per-data-dir migration that makes a pre-upgrade catalog
/// answerable. Pure, so none of this needs a store, a silo or a sidecar.</summary>
public class LegacyRoleMigrationTests
{
    private static readonly UserRecord[] LegacyUsers =
    [
        new() { Username = "admin", Role = "Admin" },
        new() { Username = "editor", Role = "Editor" },
        new() { Username = "viewer", Role = "Viewer" },
    ];

    [Fact]
    public void AnEmptyDocumentGetsTheThreeBuiltInsAndOneMirroredEntryPerUser()
    {
        var result = LegacyRoleMigration.Apply(new AccessPolicyDocument(), LegacyUsers, nowMs: 1000, actor: "system", out var changed);

        Assert.True(changed);
        Assert.Equal(BuiltInRoles.All.Order(), result.Roles.Select(r => r.Name).Order());
        Assert.All(result.Roles, r => Assert.True(r.BuiltIn));

        Assert.Equal(["admin", "editor", "viewer"], result.Users.Select(u => u.Username).Order());
        Assert.Equal(["Admin"], result.Users.Single(u => u.Username == "admin").Roles);
        Assert.Equal(["Editor"], result.Users.Single(u => u.Username == "editor").Roles);
        Assert.Equal(["Viewer"], result.Users.Single(u => u.Username == "viewer").Roles);
        Assert.All(result.Users, u => Assert.Equal(1000, u.UpdatedAtMs));
        Assert.All(result.Users, u => Assert.Equal("system", u.UpdatedBy));
        Assert.Equal(1000, result.UpdatedAtMs);
    }

    [Fact]
    public void TheMigratedDocumentAnswersWhatTheLegacyRoleAnswered()
    {
        // The end-to-end point of the whole migration, in one assertion: a seeded legacy data dir,
        // migrated, evaluated — and the editor can still edit while the viewer still cannot.
        var result = LegacyRoleMigration.Apply(new AccessPolicyDocument(), LegacyUsers, nowMs: 1, actor: "system", out _);

        var editor = EffectivePermissionsBuilder.Build(result, "editor");
        var viewer = EffectivePermissionsBuilder.Build(result, "viewer");

        Assert.Equal(AccessDecision.Allowed, PermissionEvaluator.Evaluate(editor, Actions.PipelineWrite, "p1").Decision);
        Assert.Equal(AccessDecision.Allowed, PermissionEvaluator.Evaluate(viewer, Actions.PipelineRead, "p1").Decision);
        Assert.Equal(AccessDecision.Denied, PermissionEvaluator.Evaluate(viewer, Actions.PipelineWrite, "p1").Decision);
        Assert.Equal(AccessDecision.Denied, PermissionEvaluator.Evaluate(editor, Actions.UserWrite, "*").Decision);
    }

    [Fact]
    public void RunningItTwiceChangesNothingTheSecondTime()
    {
        // Not cosmetic: the caller writes only when something changed, and a needless write would bump
        // Version and invalidate every replica's policy cache on every host restart.
        var once = LegacyRoleMigration.Apply(new AccessPolicyDocument(), LegacyUsers, nowMs: 1000, actor: "system", out _);
        var twice = LegacyRoleMigration.Apply(once, LegacyUsers, nowMs: 2000, actor: "someone-else", out var changed);

        Assert.False(changed);
        Assert.Equal(once.Roles.Count, twice.Roles.Count);
        Assert.Equal(once.Users.Count, twice.Users.Count);
        Assert.Equal(1000, twice.UpdatedAtMs);                       // untouched, because nothing changed
        Assert.All(twice.Users, u => Assert.Equal("system", u.UpdatedBy));
        Assert.All(twice.Users, u => Assert.Equal(1000, u.UpdatedAtMs));
    }

    [Fact]
    public void AnEntryAnAdministratorHasAlreadyCustomisedIsNeverTouched()
    {
        var document = new AccessPolicyDocument();
        document.Users.Add(new UserAccessEntry
        {
            Username = "editor",
            Roles = ["Auditor"],
            Grants = [new PermissionGrant { Action = Actions.AuditRead }],
            UpdatedAtMs = 5,
            UpdatedBy = "admin",
        });

        var result = LegacyRoleMigration.Apply(document, LegacyUsers, nowMs: 1000, actor: "system", out var changed);

        // changed is true — the built-ins and the two other users were added — but this entry is not.
        Assert.True(changed);
        var entry = result.Users.Single(u => u.Username == "editor");
        Assert.Equal(["Auditor"], entry.Roles);
        Assert.Equal("admin", entry.UpdatedBy);
        Assert.Equal(5, entry.UpdatedAtMs);
    }

    [Fact]
    public void AnEntryWithNoRolesIsMirroredIntoWithoutLosingDisabledOrItsOwnGrants()
    {
        // Such an entry exists when somebody disabled a user, or granted them something directly,
        // before the mirror existed. Clearing Disabled here would re-enable a disabled account.
        var document = new AccessPolicyDocument();
        document.Users.Add(new UserAccessEntry
        {
            Username = "viewer",
            Disabled = true,
            Grants = [new PermissionGrant { Action = Actions.TableRead, Scope = "prod-*" }],
        });

        var result = LegacyRoleMigration.Apply(document, LegacyUsers, nowMs: 1000, actor: "system", out var changed);

        Assert.True(changed);
        var entry = result.Users.Single(u => u.Username == "viewer");
        Assert.Equal(["Viewer"], entry.Roles);
        Assert.True(entry.Disabled);
        Assert.Equal("prod-*", Assert.Single(entry.Grants).Scope);

        // ...and it is still idempotent afterwards.
        LegacyRoleMigration.Apply(result, LegacyUsers, nowMs: 2000, actor: "system", out var changedAgain);
        Assert.False(changedAgain);
    }

    [Fact]
    public void AnExistingRoleOfTheSameNameIsLeftAloneEvenThoughItIsABuiltIn()
    {
        // Built-ins may be edited; only deleting them is refused. A migration that re-seeded them would
        // silently revert an administrator's carve-back of the Admin role on every restart.
        var document = new AccessPolicyDocument();
        document.Roles.Add(new RoleDefinition
        {
            Name = BuiltInRoles.Admin,
            BuiltIn = true,
            Grants = [new PermissionGrant { Action = Actions.TableRead }],
        });

        var result = LegacyRoleMigration.Apply(document, [], nowMs: 1000, actor: "system", out var changed);

        Assert.True(changed);   // Editor and Viewer were still missing
        Assert.Equal(3, result.Roles.Count);
        Assert.Equal([Actions.TableRead], result.Roles.Single(r => r.Name == BuiltInRoles.Admin).Grants.Select(g => g.Action));
    }

    [Fact]
    public void AUserWithABlankRoleIsSkippedRatherThanGettingAnEmptyEntry()
    {
        // An empty entry would also break idempotency: the next run would see no roles and try again.
        var result = LegacyRoleMigration.Apply(
            new AccessPolicyDocument(), [new UserRecord { Username = "ghost", Role = "" }], nowMs: 1, actor: "system", out _);

        Assert.Empty(result.Users);
    }

    [Fact]
    public void TheInputDocumentIsNotMutated()
    {
        // A caller that decides not to write (changed == false, or a failed store round trip) must be
        // left holding the snapshot it read.
        var document = new AccessPolicyDocument();

        LegacyRoleMigration.Apply(document, LegacyUsers, nowMs: 1000, actor: "system", out _);

        Assert.Empty(document.Roles);
        Assert.Empty(document.Users);
        Assert.Equal(0, document.UpdatedAtMs);
    }

    [Fact]
    public void VersioningIsLeftToTheStore()
    {
        var document = new AccessPolicyDocument { Version = 17 };

        var result = LegacyRoleMigration.Apply(document, LegacyUsers, nowMs: 1000, actor: "system", out _);

        Assert.Equal(17, result.Version);
    }
}
