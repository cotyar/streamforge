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
///   <item><b>Deny overrides.</b> Any matching grant with <see cref="PermissionEffect.Deny"/> wins
///   outright — see the ponytail note on <see cref="Evaluate"/>.</item>
///   <item>Among matching Allows the <b>most permissive wins on the approval axis</b>: one
///   unconditional Allow is enough to answer <see cref="AccessDecision.Allowed"/>; only when every
///   matching Allow carries <see cref="PermissionGrant.RequiresApproval"/> is the answer
///   <see cref="AccessDecision.RequiresApproval"/>. "Alice may deploy to prod-*, and separately alice
///   may deploy anywhere with an approval" must not force alice through an approval for prod.</item>
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

        // ponytail: deny-overrides, flat — the FIRST matching Deny wins, with no specificity ladder and
        // no ordering rules. Ceiling: you cannot express "deny writes to prod-*, except this one service
        // account may write prod-eu-1", because the broad Deny beats the narrow Allow every time; the
        // workaround is to narrow the Deny's own scope instead. Upgrade path, if that ever bites: score
        // each matching grant by pattern specificity (literal characters outside wildcards, tag < prefix
        // < exact) and let the highest score win with Deny breaking ties. That is a strictly larger rule
        // set that keeps every decision this version makes, so it can land later without a migration —
        // which is the whole reason it is safe to not build it now.
        PermissionGrant? unconditionalAllow = null;
        PermissionGrant? approvalAllow = null;

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

            if (grant.RequiresApproval)
            {
                approvalAllow ??= grant;
            }
            else
            {
                unconditionalAllow ??= grant;
            }

            // No early return on an Allow: a Deny later in the list still overrides it, so the whole
            // list has to be walked. Grant lists are tens of entries at most and this runs per request,
            // so the walk is deliberately not indexed or cached.
        }

        if (unconditionalAllow is not null)
        {
            return new AccessResult(
                AccessDecision.Allowed,
                $"allowed by grant {Describe(unconditionalAllow)}",
                unconditionalAllow);
        }

        if (approvalAllow is not null)
        {
            return new AccessResult(
                AccessDecision.RequiresApproval,
                $"grant {Describe(approvalAllow)} requires approval",
                approvalAllow);
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
