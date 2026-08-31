using StreamsForge.Abstractions;
using StreamsForge.AppCore.Access;
using Xunit;

namespace StreamsForge.AppCore.Tests.Access;

/// <summary>Plan 015 wave 1 — flattening. Including the OIDC group seam, which lands here ahead of OIDC
/// itself precisely so that the mapping is implemented and tested before anyone is under deadline to
/// ship a login flow (015 §OIDC).</summary>
public class EffectivePermissionsBuilderTests
{
    private static PermissionGrant Allow(string action, string scope = "*") =>
        new() { Action = action, Scope = scope };

    private static AccessPolicyDocument Document() => new()
    {
        Version = 42,
        Roles =
        [
            new RoleDefinition { Name = "Reader", Grants = [Allow(Actions.TableRead)] },
            new RoleDefinition { Name = "Releaser", Grants = [Allow(Actions.PipelineControl, "prod-*")] },
        ],
    };

    [Fact]
    public void UserGroupAndRoleGrantsAllMergeIntoOneFlatList()
    {
        var doc = Document();
        doc.Groups.Add(new GroupDefinition
        {
            Name = "quants",
            Members = ["alice"],
            Roles = ["Releaser"],
            Grants = [Allow(Actions.SourceRead)],
        });
        doc.Users.Add(new UserAccessEntry { Username = "alice", Roles = ["Reader"], Grants = [Allow(Actions.ConfigExport)] });

        var effective = EffectivePermissionsBuilder.Build(doc, "alice");

        Assert.Equal("alice", effective.Username);
        Assert.False(effective.Disabled);
        Assert.Equal(42, effective.Version);
        Assert.Equal(["quants"], effective.Groups);
        Assert.Equal(["Reader", "Releaser"], effective.Roles);
        Assert.Equal(
            [Actions.ConfigExport, Actions.SourceRead, Actions.TableRead, Actions.PipelineControl],
            effective.Grants.Select(g => g.Action));
    }

    [Fact]
    public void MembershipComesFromTheGroupsMemberList()
    {
        var doc = Document();
        doc.Groups.Add(new GroupDefinition { Name = "ops", Members = ["bob"], Grants = [Allow(Actions.TableControl)] });

        Assert.Equal(["ops"], EffectivePermissionsBuilder.Build(doc, "bob").Groups);
        Assert.Empty(EffectivePermissionsBuilder.Build(doc, "alice").Groups);
    }

    [Fact]
    public void MembershipAlsoComesFromAnOidcGroupsClaim()
    {
        // The synthetic claim values the plan asks for from day one: an IdP hands over opaque directory
        // identifiers, and the group definition says which of them map onto it.
        var doc = Document();
        doc.Groups.Add(new GroupDefinition
        {
            Name = "risk",
            ExternalClaimValues = ["cn=risk,ou=groups,dc=corp", "8f2a-risk"],
            Grants = [Allow(Actions.AuditRead)],
        });

        var effective = EffectivePermissionsBuilder.Build(doc, "carol", groupClaimValues: ["8f2a-risk", "8f2a-unrelated"]);

        Assert.Equal(["risk"], effective.Groups);
        Assert.Equal([Actions.AuditRead], effective.Grants.Select(g => g.Action));
    }

    [Fact]
    public void AnEmptyOrAbsentGroupsClaimLeavesLocalMembershipUntouched()
    {
        var doc = Document();
        doc.Groups.Add(new GroupDefinition { Name = "risk", ExternalClaimValues = ["8f2a-risk"] });
        doc.Groups.Add(new GroupDefinition { Name = "ops", Members = ["dave"] });

        Assert.Equal(["ops"], EffectivePermissionsBuilder.Build(doc, "dave").Groups);
        Assert.Equal(["ops"], EffectivePermissionsBuilder.Build(doc, "dave", groupClaimValues: []).Groups);
    }

    [Fact]
    public void ClaimValuesAreComparedExactlyAndCaseSensitively()
    {
        var doc = Document();
        doc.Groups.Add(new GroupDefinition { Name = "risk", ExternalClaimValues = ["8f2a-risk"] });

        Assert.Empty(EffectivePermissionsBuilder.Build(doc, "carol", groupClaimValues: ["8F2A-RISK"]).Groups);
        Assert.Empty(EffectivePermissionsBuilder.Build(doc, "carol", groupClaimValues: ["8f2a-risk-2"]).Groups);
    }

    [Fact]
    public void AGroupMemberIsNotCountedTwiceWhenTheClaimAlsoMatches()
    {
        var doc = Document();
        doc.Groups.Add(new GroupDefinition
        {
            Name = "risk",
            Members = ["carol"],
            ExternalClaimValues = ["8f2a-risk"],
            Grants = [Allow(Actions.AuditRead)],
        });

        var effective = EffectivePermissionsBuilder.Build(doc, "carol", groupClaimValues: ["8f2a-risk"]);

        Assert.Equal(["risk"], effective.Groups);
        Assert.Single(effective.Grants);
    }

    [Fact]
    public void AnUnknownRoleNameIsSkippedInSilenceRatherThanThrowing()
    {
        // A policy document that references a deleted role must not take down every request in the
        // cluster. It still SHOWS in Roles, so an admin screen can say "this user references a role that
        // no longer exists" instead of quietly hiding the stale reference.
        var doc = Document();
        doc.Users.Add(new UserAccessEntry { Username = "alice", Roles = ["Reader", "DeletedRole"] });

        var effective = EffectivePermissionsBuilder.Build(doc, "alice");

        Assert.Equal(["Reader", "DeletedRole"], effective.Roles);
        Assert.Equal([Actions.TableRead], effective.Grants.Select(g => g.Action));
    }

    [Fact]
    public void AnUnknownRoleNameOnAGroupIsSkippedToo()
    {
        var doc = Document();
        doc.Groups.Add(new GroupDefinition { Name = "ops", Members = ["alice"], Roles = ["DeletedRole"] });

        var effective = EffectivePermissionsBuilder.Build(doc, "alice");

        Assert.Empty(effective.Grants);
        Assert.Equal(["DeletedRole"], effective.Roles);
    }

    [Fact]
    public void TheRoleClaimIsUsedOnlyWhenTheDocumentHasNoEntryForTheUser()
    {
        // A pre-upgrade catalog: LegacyRoleMigration has not run, so the token's role is all there is.
        var doc = Document();

        var effective = EffectivePermissionsBuilder.Build(doc, "alice", roleClaim: "Reader");

        Assert.Equal(["Reader"], effective.Roles);
        Assert.Equal([Actions.TableRead], effective.Grants.Select(g => g.Action));
    }

    [Fact]
    public void TheRoleClaimIsIgnoredOnceAnEntryExistsEvenIfItsRoleListIsEmpty()
    {
        // Otherwise "revoke every role from alice" would be silently undone by whatever her 12-hour-old
        // token still claims — the exact opposite of "revocation lands in ~10s".
        var doc = Document();
        doc.Users.Add(new UserAccessEntry { Username = "alice", Roles = [] });

        var effective = EffectivePermissionsBuilder.Build(doc, "alice", roleClaim: "Reader");

        Assert.Empty(effective.Roles);
        Assert.Empty(effective.Grants);
    }

    [Fact]
    public void ADisabledUserComesBackWithNoGrantsAtAll()
    {
        var doc = Document();
        doc.Groups.Add(new GroupDefinition { Name = "quants", Members = ["alice"], Grants = [Allow("*")] });
        doc.Users.Add(new UserAccessEntry { Username = "alice", Disabled = true, Roles = ["Reader"], Grants = [Allow("*")] });

        var effective = EffectivePermissionsBuilder.Build(doc, "alice");

        Assert.True(effective.Disabled);
        Assert.Empty(effective.Grants);
        Assert.Equal(42, effective.Version);
        Assert.Equal(AccessDecision.Denied, PermissionEvaluator.Evaluate(effective, Actions.TableRead, "t1").Decision);
    }

    [Fact]
    public void AnUnknownUserWithNoGroupsAndNoRoleClaimGetsNothing()
    {
        var effective = EffectivePermissionsBuilder.Build(Document(), "nobody");

        Assert.Empty(effective.Grants);
        Assert.Empty(effective.Roles);
        Assert.Empty(effective.Groups);
        Assert.False(effective.Disabled);
    }

    [Fact]
    public void UsernamesAreMatchedOrdinally()
    {
        var doc = Document();
        doc.Users.Add(new UserAccessEntry { Username = "alice", Roles = ["Reader"] });

        Assert.Empty(EffectivePermissionsBuilder.Build(doc, "Alice").Roles);
    }
}
