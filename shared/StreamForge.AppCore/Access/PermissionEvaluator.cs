using StreamForge.Abstractions;

namespace StreamForge.AppCore.Access;

/// <summary>
/// Plan 015 — the authorization decision itself, and nothing else.
///
/// <para>This type is deliberately a pure static function of (flattened permissions, action, scope,
/// resource tags). It has no store, no clock, no logger and no runtime types, which is what lets its
/// tests live in <c>StreamForge.AppCore.Tests</c> — a project listed in BOTH solutions. An
/// authorization decision that differed between the Orleans and the Dapr flavour would be a security
/// bug that no single-flavour suite could see; the only way to make that structurally impossible is for
/// there to be exactly one implementation, tested once, running in both. Plan 005 established the shape
/// with <c>PasswordHasher</c>.</para>
///
/// <para><b>The three rules, in the order they apply.</b>
/// <list type="number">
///   <item>A disabled user is denied everything, before a single grant is read. This is the cheap 90% of
///   token revocation (015 D:"Disabled users fall out of the same machinery"): the resolver hands over
///   an <see cref="EffectivePermissions"/> with <c>Disabled = true</c> and the answer is no, whatever
///   the grant list happens to contain.</item>
///   <item><b>Deny overrides, absolutely.</b> Any matching grant with <see cref="PermissionEffect.Deny"/>
///   wins outright — it is NOT part of the specificity ladder below, on purpose. See the comment in
///   <see cref="Evaluate"/> for what that costs and what to do instead.</item>
///   <item>Among matching Allows the <b>most specific one decides the approval axis</b> — see
///   <see cref="Specificity"/> for the score and why it is shaped that way. On a tie,
///   <see cref="PermissionGrant.RequiresApproval"/> wins.</item>
/// </list></para>
///
/// <para><b>Why a reason string is not optional.</b> The same result object feeds an audit row and a 403
/// body. "Denied" with no explanation is the failure mode that makes an entitlement system unusable —
/// an operator staring at a 403 needs to know whether they are missing a grant or tripping over a Deny
/// somebody wrote three months ago, and that is exactly the difference between the two reason strings
/// this produces.</para>
/// </summary>
public static class PermissionEvaluator
{
    /// <summary>Evaluate one action against one resource.</summary>
    /// <param name="permissions">The caller's flattened view, from <see cref="EffectivePermissionsBuilder"/>.</param>
    /// <param name="action">A concrete action, never a pattern — one of the <see cref="Actions"/> constants.</param>
    /// <param name="scope">The resource's id or name, or <c>"*"</c> to ask "…anywhere?" for an operation
    /// that has no single resource (a config replace, a user create). Note that asking with <c>"*"</c> is
    /// answered only by a <c>*</c>-scoped grant: a caller holding <c>prod-*</c> cannot do the global
    /// thing, which is the correct reading of a scoped entitlement.</param>
    /// <param name="resourceTags">The resource's <c>Tags</c>, so <c>tag:finance</c> scopes can match. All
    /// three entity types already carry them, which is why tag scoping cost nothing to add.</param>
    public static AccessResult Evaluate(
        EffectivePermissions permissions,
        string action,
        string scope,
        IReadOnlyCollection<string>? resourceTags = null)
    {
        ArgumentNullException.ThrowIfNull(permissions);

        if (permissions.Disabled)
        {
            // Before any grant is consulted. The resolver already empties the grant list for a disabled
            // user; this is the second lock on the same door, because a hand-built EffectivePermissions
            // (a test fake, a future cache path) could carry grants and a disabled flag at once.
            return new AccessResult(
                AccessDecision.Denied,
                $"user '{permissions.Username}' is disabled",
                null);
        }

        // DENY IS ABSOLUTE, AND SPECIFICITY APPLIES ONLY ON THE APPROVAL AXIS. Wave 8 (015 finding 1)
        // replaced the old "any unconditional Allow beats any approval Allow" rule with the specificity
        // ladder below, because that old rule made the natural way to write "deletes in this area need a
        // second pair of eyes" — a narrow requiresApproval grant on top of a role's broad Allow —
        // silently do nothing. But the upgrade path as originally written ("…with Deny breaking ties")
        // would have let a specific Allow outrank a broad Deny, and that is deliberately NOT built: a
        // guardrail `Deny pipeline.* on prod-*` would then be defeated by any older, narrower
        // `Allow pipeline.delete on prod-orders` that nobody remembers writing. The reported bug does
        // not need it, so it does not get it.
        //
        // What that costs, stated plainly: you cannot carve an Allow out of a broad Deny — "deny writes
        // to prod-*, except this service account may write prod-eu-1" is inexpressible. The workaround
        // is the one the original note already gave: narrow the Deny's own scope instead.
        //
        // ponytail: the ladder is two tiers plus a literal count, summed — no lattice, no "does pattern A
        // subsume pattern B" containment test. Ceiling: two grants that overlap without either containing
        // the other tie on score and resolve to RequiresApproval, and the tag tier is placed by fiat
        // rather than derived. Upgrade path if that ever bites: compute real subsumption (A ⊇ B) and use
        // the score only when neither pattern subsumes the other — a refinement of this order, not a
        // replacement, so it keeps every decision this version makes.
        PermissionGrant? bestAllow = null;
        var bestScore = -1;

        foreach (var grant in permissions.Grants)
        {
            if (!Matches(grant, action, scope, resourceTags))
            {
                continue;
            }

            if (grant.Effect == PermissionEffect.Deny)
            {
                // RequiresApproval on a Deny is meaningless and is ignored here rather than inventing a
                // fourth state — AccessModels.cs says so on the field itself.
                return new AccessResult(
                    AccessDecision.Denied,
                    $"denied by grant {Describe(grant)}",
                    grant);
            }

            var score = Specificity(grant);

            // Strictly greater wins; on an exact tie the approval-gated grant wins, which is both the
            // safer answer and the one an operator ADDING a grant is more likely to have meant (nobody
            // types requiresApproval by accident). Note the tie-break also settles document order out of
            // the decision entirely: the answer no longer depends on which grant a role happened to
            // contribute first.
            if (score > bestScore
                || (score == bestScore && grant.RequiresApproval && bestAllow is { RequiresApproval: false }))
            {
                bestAllow = grant;
                bestScore = score;
            }

            // No early return on an Allow: a Deny later in the list still overrides it, so the whole
            // list has to be walked. Grant lists are tens of entries at most and this runs per request,
            // so the walk is deliberately not indexed or cached.
        }

        if (bestAllow is not null)
        {
            return bestAllow.RequiresApproval
                ? new AccessResult(
                    AccessDecision.RequiresApproval,
                    $"grant {Describe(bestAllow)} requires approval",
                    bestAllow)
                : new AccessResult(
                    AccessDecision.Allowed,
                    $"allowed by grant {Describe(bestAllow)}",
                    bestAllow);
        }

        return new AccessResult(
            AccessDecision.Denied,
            $"no grant matches '{action}' on '{scope}'",
            null);
    }

    /// <summary>Does one grant cover this (action, scope) pair? Both axes are ordinal and
    /// case-SENSITIVE. Justification, since the brief demands one: the action vocabulary is generated
    /// from the <see cref="Actions"/> constants and is lower-case by construction, so folding case buys
    /// nothing there; on the scope axis it actively costs — an entitlement written <c>prod-*</c> would
    /// silently start covering a table somebody named <c>PROD-Sandbox</c>, and an entitlement widening
    /// itself is the one direction of surprise an authorization system must not have.</summary>
    public static bool Matches(PermissionGrant grant, string action, string scope, IReadOnlyCollection<string>? resourceTags = null)
        => GlobMatch(grant.Action, action) && ScopeMatches(grant.Scope, scope, resourceTags);

    /// <summary>
    /// How specific a grant is — the score that decides, among grants that ALL match, which one gets to
    /// say whether the action needs an approval. Higher is more specific. It is a total order (an int),
    /// and it is deliberately a pure function of the two patterns, so it does not depend on document
    /// order, on which role contributed the grant, or on anything the caller passed.
    ///
    /// <para><b>Both axes are scored the same way and summed.</b> Each of <c>Action</c> and <c>Scope</c>
    /// gets a tier times <see cref="TierStep"/>, plus its literal-character count as a within-tier
    /// tiebreak:
    /// <list type="bullet">
    ///   <item><b>tier 0 — no literals at all</b> (<c>*</c>, or the empty pattern): the grant names
    ///   nothing, it just says "everything". This is the tier a role's blanket entitlement lands in, and
    ///   it must lose to anything an operator actually typed out.</item>
    ///   <item><b>tier 1 — a <c>tag:</c> scope</b> (scope axis only; there is no tag form on the action
    ///   axis).</item>
    ///   <item><b>tier 2 — a pattern with a literal part and a <c>*</c></b>: <c>prod-*</c>,
    ///   <c>pipeline.*</c>, <c>tag:</c>-free globs of any shape.</item>
    ///   <item><b>tier 3 — an exact literal</b>, no wildcard: one action, one named resource.</item>
    /// </list>
    /// Within a tier the longer literal wins, which is what makes <c>prod-eu-*</c> beat <c>prod-*</c> —
    /// nested prefixes are the commonest way an operator carves a narrower area out of a broader one,
    /// and without this they would tie and always resolve to "needs approval".</para>
    ///
    /// <para><b>Where <c>tag:</c> sits, and why.</b> Below an exact name and below a prefix; above
    /// <c>*</c>. A tag scope matches a set its author did not enumerate AND cannot see the boundary of:
    /// <c>prod-*</c> at least constrains the names it will ever cover, whereas <c>tag:finance</c> covers
    /// whatever anyone holding <c>*.write</c> has tagged <c>finance</c> since — the set is editable by
    /// people who are not editing entitlements at all. Letting the least predictable form outrank the
    /// forms whose membership the grant's author wrote down themselves is the wrong default for the axis
    /// this score governs. It is still far above <c>*</c>, because it does name a category.</para>
    ///
    /// <para>The cost of that placement, stated so nobody has to rediscover it: "everything tagged
    /// finance needs an approval" is defeated by any plain name-scoped Allow that also matches — the
    /// approval-gated <c>tag:finance</c> grant loses to a <c>prod-*</c> Allow on a resource that is both.
    /// Express such a gate on the same axis as the grants it must beat (scope it by name), or use a
    /// <c>Deny</c>, which outranks everything unconditionally.</para>
    ///
    /// <para><b>Why sum the two axes rather than rank one above the other.</b> Neither axis is
    /// obviously the senior one — "this action, anywhere" and "any action, this resource" are both
    /// legitimate ways to be specific — and inventing a priority between them would decide cases nobody
    /// has asked about. Summing lets them tie, and a tie is a defined answer: RequiresApproval wins. The
    /// literal counts are clamped below <see cref="TierStep"/> so a long name can never climb a tier.</para>
    /// </summary>
    internal static int Specificity(PermissionGrant grant) =>
        AxisScore(grant.Action, tagsAllowed: false) + AxisScore(grant.Scope, tagsAllowed: true);

    /// <summary>One tier is worth more than any literal count, so tiers dominate and literals only
    /// break ties inside one.</summary>
    private const int TierStep = 1000;

    private static int AxisScore(string pattern, bool tagsAllowed)
    {
        var literals = 0;
        foreach (var c in pattern)
        {
            if (c != '*')
            {
                literals++;
            }
        }

        if (literals == 0)
        {
            // `*`, `**`, or "" — nothing was named. Tier 0, and no tiebreak either.
            return 0;
        }

        var tier = tagsAllowed && pattern.StartsWith(TagPrefix, StringComparison.Ordinal) ? 1
            : pattern.Contains('*') ? 2
            : 3;

        return (tier * TierStep) + Math.Min(literals, TierStep - 1);
    }

    /// <summary>The four scope forms, in the order they are cheapest to distinguish: <c>tag:finance</c>
    /// (matches when the resource carries that tag), and then everything else — <c>*</c>, an exact
    /// id/name, and a prefix like <c>prod-*</c> — which are all one glob against the resource's id.
    /// Three of the four forms therefore need no code of their own, which is why the grammar was chosen
    /// this way.</summary>
    /// <summary>Internal rather than private since plan 015 wave 4: <see cref="ApprovalTemplate.ScopePattern"/>
    /// shares this exact grammar, and an approval template whose <c>tag:prod</c> meant something different
    /// from the entitlement it guards would be the worst kind of wrong — one operator writing one string
    /// twice and getting two behaviours. One copy, one meaning.</summary>
    internal static bool ScopeMatches(string pattern, string scope, IReadOnlyCollection<string>? resourceTags)
    {
        if (pattern.StartsWith(TagPrefix, StringComparison.Ordinal))
        {
            if (resourceTags is null || resourceTags.Count == 0)
            {
                // A tag-scoped grant against a caller that passed no tags is a miss, not a match. The
                // alternative — treating "no tags supplied" as "unknown, so allow" — would make every
                // call site that forgot to pass tags silently widen every tag entitlement.
                return false;
            }

            var tagPattern = pattern[TagPrefix.Length..];
            foreach (var tag in resourceTags)
            {
                // Globbed, not compared: `tag:pii-*` falls out for free and cost a `foreach`.
                if (GlobMatch(tagPattern, tag))
                {
                    return true;
                }
            }

            return false;
        }

        return GlobMatch(pattern, scope);
    }

    private const string TagPrefix = "tag:";

    /// <summary>
    /// The one matcher behind both axes: <c>*</c> stands for any run of characters, <b>including
    /// dots</b>. So <c>pipeline.*</c> covers <c>pipeline.write</c> and would also cover a future
    /// <c>pipeline.write.sql</c>, while <c>pipeline</c> alone is NOT covered by <c>pipeline.*</c>
    /// (the pattern demands the dot). Dot-crossing is the deliberate choice: the alternative — a
    /// segment-bounded <c>*</c> — means the day someone adds a third segment, every existing
    /// <c>x.*</c> entitlement silently stops covering it, and an entitlement that silently narrows is
    /// how a production incident starts at 3am. The boundary case is pinned in the tests.
    /// <para>Empty pattern matches nothing: <see cref="PermissionGrant.Action"/> defaults to <c>""</c>,
    /// and a half-filled grant must grant nothing.</para>
    /// </summary>
    internal static bool GlobMatch(string pattern, string value)
    {
        // Textbook iterative glob with backtracking — linear in practice, no allocation, no Regex (a
        // Regex per grant per request would be the single most expensive thing on this path).
        int p = 0, v = 0, lastStar = -1, resumeAt = 0;

        while (v < value.Length)
        {
            if (p < pattern.Length && pattern[p] == '*')
            {
                lastStar = p++;
                resumeAt = v;
            }
            else if (p < pattern.Length && pattern[p] == value[v])
            {
                p++;
                v++;
            }
            else if (lastStar >= 0)
            {
                p = lastStar + 1;
                v = ++resumeAt;
            }
            else
            {
                return false;
            }
        }

        while (p < pattern.Length && pattern[p] == '*')
        {
            p++;
        }

        return p == pattern.Length;
    }

    private static string Describe(PermissionGrant grant) =>
        string.IsNullOrEmpty(grant.Note)
            ? $"{grant.Action} on {grant.Scope}"
            : $"{grant.Action} on {grant.Scope} ({grant.Note})";
}

/// <summary>What <see cref="PermissionEvaluator.Evaluate"/> answers with. <see cref="Reason"/> ends up
/// in an audit row and in a 403 body, and <see cref="MatchedGrant"/> is what an admin UI needs to show
/// WHICH entitlement decided — null only when nothing matched at all, or when the user is disabled.</summary>
public sealed record AccessResult(AccessDecision Decision, string Reason, PermissionGrant? MatchedGrant)
{
    public bool IsAllowed => Decision == AccessDecision.Allowed;
    public bool IsDenied => Decision == AccessDecision.Denied;
}
