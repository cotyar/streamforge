using StreamForge.Abstractions;
using StreamForge.AppCore.Access;
using Xunit;

namespace StreamForge.AppCore.Tests.Access;

/// <summary>Plan 015 wave 1 — the authorization decision. These run in BOTH solutions (the project is
/// listed in each), which is the point: a decision that differed between the Orleans and the Dapr
/// flavour would be a security bug no single-flavour suite could see.</summary>
public class PermissionEvaluatorTests
{
    private static EffectivePermissions With(params PermissionGrant[] grants) =>
        new() { Username = "alice", Grants = [.. grants], Version = 7 };

    private static PermissionGrant Allow(string action, string scope = "*", bool approval = false, string? note = null) =>
        new() { Action = action, Scope = scope, Effect = PermissionEffect.Allow, RequiresApproval = approval, Note = note };

    private static PermissionGrant Deny(string action, string scope = "*", bool approval = false) =>
        new() { Action = action, Scope = scope, Effect = PermissionEffect.Deny, RequiresApproval = approval };

    // ------------------------------------------------------------------ the three rules

    [Fact]
    public void ADisabledUserIsDeniedEverythingBeforeAnyGrantIsRead()
    {
        var permissions = With(Allow("*"));
        permissions.Disabled = true;

        var result = PermissionEvaluator.Evaluate(permissions, Actions.PipelineRead, "p1");

        Assert.Equal(AccessDecision.Denied, result.Decision);
        Assert.Null(result.MatchedGrant);
        Assert.Contains("disabled", result.Reason);
        Assert.Contains("alice", result.Reason);
    }

    [Fact]
    public void NoMatchingGrantIsDeniedWithAReasonNamingWhatWasAsked()
    {
        var result = PermissionEvaluator.Evaluate(With(Allow(Actions.TableRead)), Actions.PipelineWrite, "p1");

        Assert.Equal(AccessDecision.Denied, result.Decision);
        Assert.Null(result.MatchedGrant);
        Assert.Contains("pipeline.write", result.Reason);
        Assert.Contains("p1", result.Reason);
    }

    [Fact]
    public void AMatchingAllowIsAllowedAndReportsTheGrantThatDecided()
    {
        var grant = Allow(Actions.PipelineWrite, "prod-*", note: "release team");
        var result = PermissionEvaluator.Evaluate(With(grant), Actions.PipelineWrite, "prod-eu");

        Assert.Equal(AccessDecision.Allowed, result.Decision);
        Assert.True(result.IsAllowed);
        Assert.Same(grant, result.MatchedGrant);
        Assert.Contains("prod-*", result.Reason);
        Assert.Contains("release team", result.Reason);   // the Note is why an operator wrote the grant
    }

    [Theory]
    [InlineData(true)]   // deny listed after the allow
    [InlineData(false)]  // ...and before it
    public void DenyOverridesEvenAWildlyMoreSpecificAllow(bool denyLast)
    {
        var allow = Allow(Actions.PipelineWrite, "prod-eu-1");
        var deny = Deny("*", "*");
        var permissions = denyLast ? With(allow, deny) : With(deny, allow);

        var result = PermissionEvaluator.Evaluate(permissions, Actions.PipelineWrite, "prod-eu-1");

        Assert.Equal(AccessDecision.Denied, result.Decision);
        Assert.Same(deny, result.MatchedGrant);
        Assert.Contains("denied by grant", result.Reason);
    }

    [Fact]
    public void RequiresApprovalOnADenyIsIgnoredRatherThanProducingAFourthState()
    {
        var result = PermissionEvaluator.Evaluate(
            With(Allow(Actions.TableDelete), Deny(Actions.TableDelete, approval: true)),
            Actions.TableDelete,
            "t1");

        Assert.Equal(AccessDecision.Denied, result.Decision);
    }

    [Fact]
    public void EveryMatchingAllowRequiringApprovalMeansRequiresApproval()
    {
        var grant = Allow(Actions.ConfigReplace, approval: true);
        var result = PermissionEvaluator.Evaluate(With(grant), Actions.ConfigReplace, "*");

        Assert.Equal(AccessDecision.RequiresApproval, result.Decision);
        Assert.Same(grant, result.MatchedGrant);
        Assert.Contains("requires approval", result.Reason);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void OneUnconditionalAllowBeatsAnyNumberOfApprovalGatedOnes(bool unconditionalLast)
    {
        // "alice may deploy to prod-*, and separately alice may deploy anywhere with an approval" must
        // not force alice through an approval for prod.
        var gated = Allow(Actions.PipelineControl, "*", approval: true);
        var direct = Allow(Actions.PipelineControl, "prod-*");
        var permissions = unconditionalLast ? With(gated, direct) : With(direct, gated);

        var result = PermissionEvaluator.Evaluate(permissions, Actions.PipelineControl, "prod-eu");

        Assert.Equal(AccessDecision.Allowed, result.Decision);
        Assert.Same(direct, result.MatchedGrant);
    }

    // ------------------------------------------------------------------ action matching

    [Theory]
    [InlineData("pipeline.write", "pipeline.write", true)]
    [InlineData("pipeline.read", "pipeline.write", false)]
    [InlineData("pipeline.*", "pipeline.write", true)]
    [InlineData("*", "pipeline.write", true)]
    [InlineData("*", "anything-at-all", true)]
    // The documented boundary: `*` crosses dots, so a future third segment stays covered...
    [InlineData("pipeline.*", "pipeline.write.sql", true)]
    // ...but the pattern still demands its literal dot, so the bare prefix is NOT covered.
    [InlineData("pipeline.*", "pipeline", false)]
    [InlineData("pipeline.*", "pipelines.write", false)]
    // Half-filled grants grant nothing: PermissionGrant.Action defaults to "".
    [InlineData("", "pipeline.write", false)]
    // Case-sensitive, ordinal.
    [InlineData("Pipeline.Write", "pipeline.write", false)]
    // Multiple wildcards, because the matcher backtracks rather than special-casing prefixes.
    [InlineData("*.write", "pipeline.write", true)]
    [InlineData("pipe*.wri*", "pipeline.write", true)]
    public void ActionPatternsAreGlobsWhereStarCrossesDots(string pattern, string action, bool expected)
    {
        var result = PermissionEvaluator.Evaluate(With(Allow(pattern)), action, "*");

        Assert.Equal(expected ? AccessDecision.Allowed : AccessDecision.Denied, result.Decision);
    }

    // ------------------------------------------------------------------ scope matching

    [Theory]
    [InlineData("*", "prod-eu", true)]
    [InlineData("prod-eu", "prod-eu", true)]
    [InlineData("prod-eu", "prod-us", false)]
    [InlineData("prod-*", "prod-eu", true)]
    [InlineData("prod-*", "dev-eu", false)]
    [InlineData("prod-*", "prod-", true)]
    [InlineData("prod-*", "prod", false)]
    // Case-sensitive: an entitlement must never widen itself onto a differently-cased name.
    [InlineData("prod-*", "PROD-eu", false)]
    // Asking "…anywhere?" is answered only by a *-scoped grant.
    [InlineData("prod-*", "*", false)]
    [InlineData("*", "*", true)]
    public void ScopesAreExactOrPrefixOrEverything(string scopePattern, string scope, bool expected)
    {
        var result = PermissionEvaluator.Evaluate(With(Allow(Actions.TableRead, scopePattern)), Actions.TableRead, scope);

        Assert.Equal(expected ? AccessDecision.Allowed : AccessDecision.Denied, result.Decision);
    }

    [Fact]
    public void ATagScopeMatchesWhenTheResourceCarriesTheTag()
    {
        var permissions = With(Allow(Actions.TableRead, "tag:finance"));

        Assert.Equal(
            AccessDecision.Allowed,
            PermissionEvaluator.Evaluate(permissions, Actions.TableRead, "t1", ["ops", "finance"]).Decision);
        Assert.Equal(
            AccessDecision.Denied,
            PermissionEvaluator.Evaluate(permissions, Actions.TableRead, "t1", ["ops"]).Decision);
    }

    [Fact]
    public void ATagScopeAgainstACallerThatSuppliedNoTagsIsAMissNotAMatch()
    {
        // Otherwise every call site that forgot to pass the resource's tags would silently widen every
        // tag-scoped entitlement into an unscoped one.
        var permissions = With(Allow(Actions.TableRead, "tag:finance"));

        Assert.Equal(AccessDecision.Denied, PermissionEvaluator.Evaluate(permissions, Actions.TableRead, "t1").Decision);
        Assert.Equal(AccessDecision.Denied, PermissionEvaluator.Evaluate(permissions, Actions.TableRead, "t1", []).Decision);
    }

    [Fact]
    public void ATagScopeIsGlobbedToo()
    {
        var permissions = With(Allow(Actions.TableRead, "tag:pii-*"));

        Assert.Equal(AccessDecision.Allowed, PermissionEvaluator.Evaluate(permissions, Actions.TableRead, "t1", ["pii-eu"]).Decision);
        Assert.Equal(AccessDecision.Denied, PermissionEvaluator.Evaluate(permissions, Actions.TableRead, "t1", ["public"]).Decision);
    }

    [Fact]
    public void ATagScopedDenyStillOverridesAnUnscopedAllow()
    {
        var permissions = With(Allow(Actions.TableRead, "*"), Deny(Actions.TableRead, "tag:restricted"));

        Assert.Equal(AccessDecision.Allowed, PermissionEvaluator.Evaluate(permissions, Actions.TableRead, "t1", ["public"]).Decision);
        Assert.Equal(AccessDecision.Denied, PermissionEvaluator.Evaluate(permissions, Actions.TableRead, "t2", ["restricted"]).Decision);
    }

    [Fact]
    public void AnEmptyGrantListIsDenied()
    {
        Assert.Equal(AccessDecision.Denied, PermissionEvaluator.Evaluate(With(), Actions.SourceRead, "s1").Decision);
    }
}
