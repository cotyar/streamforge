using StreamsForge.Abstractions;
using StreamsForge.AppCore.Access;
using Xunit;

namespace StreamsForge.AppCore.Tests.Access;

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
    public void AliceScenario_HerNarrowProdAllowBeatsHerBroadApprovalGatedOne(bool unconditionalLast)
    {
        // Renamed in wave 8 (was OneUnconditionalAllowBeatsAnyNumberOfApprovalGatedOnes). Every
        // assertion below is UNCHANGED — the old name stated a rule that no longer holds in general
        // ("one unconditional Allow beats any number of gated ones"), but this case still answers the
        // same way, now because prod-* is MORE SPECIFIC than *, not because it is unconditional.
        //
        // "alice may deploy to prod-*, and separately alice may deploy anywhere with an approval" must
        // not force alice through an approval for prod.
        var gated = Allow(Actions.PipelineControl, "*", approval: true);
        var direct = Allow(Actions.PipelineControl, "prod-*");
        var permissions = unconditionalLast ? With(gated, direct) : With(direct, gated);

        var result = PermissionEvaluator.Evaluate(permissions, Actions.PipelineControl, "prod-eu");

        Assert.Equal(AccessDecision.Allowed, result.Decision);
        Assert.Same(direct, result.MatchedGrant);

        // ...and the other half of the scenario, which the old rule got right too: outside prod, only
        // the gated grant matches at all.
        Assert.Equal(
            AccessDecision.RequiresApproval,
            PermissionEvaluator.Evaluate(permissions, Actions.PipelineControl, "dev-web").Decision);
    }

    // ------------------------------------------------------ specificity on the approval axis (wave 8)

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void OperatorScenario_ANarrowApprovalGrantBeatsARolesBlanketAllow(bool approvalLast)
    {
        // 015 finding 1, observed live: an editor holding the built-in Editor role (unconditional
        // pipeline.delete on *) PLUS {pipeline.delete, dev-*, requiresApproval} deleted the pipeline
        // outright. The natural way an operator expresses "deletes in this area need a second pair of
        // eyes" must not silently do nothing.
        var role = Allow(Actions.PipelineDelete, "*");
        var gate = Allow(Actions.PipelineDelete, "dev-*", approval: true, note: "second pair of eyes");
        var permissions = approvalLast ? With(role, gate) : With(gate, role);

        var inDev = PermissionEvaluator.Evaluate(permissions, Actions.PipelineDelete, "dev-thing");
        Assert.Equal(AccessDecision.RequiresApproval, inDev.Decision);
        Assert.Same(gate, inDev.MatchedGrant);
        // The reason still names the grant that decided — it feeds a 403 body and an audit row.
        Assert.Contains("dev-*", inDev.Reason);
        Assert.Contains("second pair of eyes", inDev.Reason);

        // ...and outside dev-* the role's blanket Allow is still the only match, so nothing else got
        // gated as a side effect.
        var inProd = PermissionEvaluator.Evaluate(permissions, Actions.PipelineDelete, "prod-thing");
        Assert.Equal(AccessDecision.Allowed, inProd.Decision);
        Assert.Same(role, inProd.MatchedGrant);
    }

    [Fact]
    public void ANarrowerPrefixBeatsABroaderOneAtTheSameTier()
    {
        // Nested prefixes are how an operator carves an exception out of an area: "prod-* needs an
        // approval, except the sandbox". Without the literal-length tiebreak these two would tie and
        // the safe-side tie-break would gate the sandbox too.
        var broad = Allow(Actions.TableDelete, "prod-*", approval: true);
        var narrow = Allow(Actions.TableDelete, "prod-sandbox-*");
        var permissions = With(broad, narrow);

        Assert.Equal(
            AccessDecision.Allowed,
            PermissionEvaluator.Evaluate(permissions, Actions.TableDelete, "prod-sandbox-1").Decision);
        Assert.Equal(
            AccessDecision.RequiresApproval,
            PermissionEvaluator.Evaluate(permissions, Actions.TableDelete, "prod-orders").Decision);
    }

    [Fact]
    public void AnExactScopeBeatsAPrefixWhicheverCarriesTheApproval()
    {
        var exactGated = Allow(Actions.TableDelete, "prod-orders", approval: true);
        var prefixPlain = Allow(Actions.TableDelete, "prod-*");
        Assert.Equal(
            AccessDecision.RequiresApproval,
            PermissionEvaluator.Evaluate(With(prefixPlain, exactGated), Actions.TableDelete, "prod-orders").Decision);

        // ...and symmetrically: the exact grant wins even when IT is the unconditional one.
        var exactPlain = Allow(Actions.TableDelete, "prod-orders");
        var prefixGated = Allow(Actions.TableDelete, "prod-*", approval: true);
        Assert.Equal(
            AccessDecision.Allowed,
            PermissionEvaluator.Evaluate(With(prefixGated, exactPlain), Actions.TableDelete, "prod-orders").Decision);
    }

    [Fact]
    public void ATagScopeOutranksAStarAndLosesToANameScope()
    {
        // Documented placement: * < tag: < prefix < exact. A tag matches a set the grant's author
        // neither enumerated nor bounded — anyone who can edit a resource can add the tag later — so it
        // must not outrank the forms whose membership the author wrote down.
        var starPlain = Allow(Actions.TableWrite, "*");
        var tagGated = Allow(Actions.TableWrite, "tag:finance", approval: true);
        Assert.Equal(
            AccessDecision.RequiresApproval,
            PermissionEvaluator.Evaluate(With(starPlain, tagGated), Actions.TableWrite, "ledger", ["finance"]).Decision);

        // ...but a name-scoped Allow beats it. This is the cost of the placement, pinned so it is a
        // decision and not a surprise: gate by name, or use a Deny.
        var prefixPlain = Allow(Actions.TableWrite, "prod-*");
        Assert.Equal(
            AccessDecision.Allowed,
            PermissionEvaluator.Evaluate(With(prefixPlain, tagGated), Actions.TableWrite, "prod-ledger", ["finance"]).Decision);
    }

    [Fact]
    public void AnEqualSpecificityTieGoesToRequiresApproval()
    {
        // Mirrored tiers and equal literal counts: `table.*` (6) on the exact name `prod-orders` (11)
        // scores exactly what `table.delete` (12) on the prefix `prod-*` (5) does. Nobody types
        // requiresApproval by accident, so the tie goes the safe way — and, because it is a tie-break on
        // a score rather than on position, document order does not decide it.
        var gated = Allow("table.*", "prod-orders", approval: true);
        var plain = Allow("table.delete", "prod-*");

        Assert.Equal(
            PermissionEvaluator.Evaluate(With(gated, plain), Actions.TableDelete, "prod-orders").Decision,
            PermissionEvaluator.Evaluate(With(plain, gated), Actions.TableDelete, "prod-orders").Decision);
        Assert.Equal(
            AccessDecision.RequiresApproval,
            PermissionEvaluator.Evaluate(With(gated, plain), Actions.TableDelete, "prod-orders").Decision);
    }

    [Fact]
    public void AMoreSpecificActionBeatsABroaderOneOnTheSameScope()
    {
        var broad = Allow("table.*", "*");
        var exact = Allow(Actions.TableDelete, "*", approval: true);

        Assert.Equal(
            AccessDecision.RequiresApproval,
            PermissionEvaluator.Evaluate(With(broad, exact), Actions.TableDelete, "t1").Decision);
        // ...and the broad one still answers every other action in its family, ungated.
        Assert.Equal(
            AccessDecision.Allowed,
            PermissionEvaluator.Evaluate(With(broad, exact), Actions.TableWrite, "t1").Decision);
    }

    [Fact]
    public void SpecificityNeverOutranksADenyHoweverSpecificTheAllowIs()
    {
        // The deliberate narrowing of the upgrade path this wave implemented: the ladder applies ONLY
        // to the approval axis. An exact-scope Allow cannot punch a hole in a guardrail Deny — narrow
        // the Deny's own scope instead.
        var permissions = With(Allow(Actions.PipelineDelete, "prod-orders"), Deny("pipeline.*", "prod-*"));

        var result = PermissionEvaluator.Evaluate(permissions, Actions.PipelineDelete, "prod-orders");

        Assert.Equal(AccessDecision.Denied, result.Decision);
        Assert.Contains("denied by grant", result.Reason);
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
