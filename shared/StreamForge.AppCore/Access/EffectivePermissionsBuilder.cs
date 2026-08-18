using StreamForge.Abstractions;

namespace StreamForge.AppCore.Access;

/// <summary>
/// Plan 015 — flattening the policy document into the one thing the evaluator reads.
///
/// <para>Three sources of grants merge into one flat list, and the flattening is where every
/// interesting decision hides: the user's own <see cref="UserAccessEntry.Grants"/>, the grants of every
/// role the user effectively holds, and the grants of every group the user is in (plus the roles those
/// groups carry). The result is version-stamped from the document so a caller — or a bug report — can
/// tell which snapshot answered.</para>
///
/// <para><b>Group membership comes from two places at once, from day one.</b>
/// <see cref="GroupDefinition.Members"/> is the local list, and
/// <see cref="GroupDefinition.ExternalClaimValues"/> maps values of an OIDC <c>groups</c> claim onto the
/// same group. OIDC itself is deferred to its own plan (015 §OIDC lists the five reasons why), but the
/// mapping lands and is unit-tested here with synthetic claim values, so that when OIDC arrives the IdP
/// group story is already implemented rather than being designed under deadline.</para>
///
/// <para><b>The role claim is a fallback, not an input.</b> Since the effective role list lives in
/// <see cref="UserAccessEntry.Roles"/> and the user store mirrors <c>UserRecord.Role</c> there on every
/// create/update, the JWT's <c>ClaimTypes.Role</c> is consulted ONLY when the document has no entry for
/// this user at all — i.e. against a pre-upgrade catalog that <see cref="LegacyRoleMigration"/> has not
/// run over yet. If it were consulted whenever the entry's role list was empty, then "revoke every role
/// from alice" would silently restore whatever her 12-hour-old token still claims, which is the exact
/// opposite of "revocation lands in ~10s".</para>
///
/// <para><b>Unknown names are skipped in silence.</b> A role or group name that no longer resolves is a
/// stale reference, not a crisis: throwing would let one deleted role take down every request in the
/// cluster until somebody edited the document, and the deleted role granted nothing anyway. The
/// permissive-looking choice is the strictly safer one here — skipping can only ever remove grants.</para>
/// </summary>
public static class EffectivePermissionsBuilder
{
    /// <summary>Flatten one user's view of one policy document.</summary>
    /// <param name="document">The snapshot the resolver is holding.</param>
    /// <param name="username">The authenticated principal's name.</param>
    /// <param name="groupClaimValues">Values of the OIDC <c>groups</c> claim, or empty/null for a local
    /// login. Compared to <see cref="GroupDefinition.ExternalClaimValues"/> ordinally and exactly — an
    /// IdP group value is an opaque identifier (often a directory GUID), so globbing it would be
    /// meaningless and case-folding it would be guessing.</param>
    /// <param name="roleClaim">The JWT's <c>ClaimTypes.Role</c>, used only when the document has no
    /// entry for <paramref name="username"/>. See the type remarks.</param>
    public static EffectivePermissions Build(
        AccessPolicyDocument document,
        string username,
        IReadOnlyCollection<string>? groupClaimValues = null,
        string? roleClaim = null)
    {
        ArgumentNullException.ThrowIfNull(document);

        var entry = document.Users.FirstOrDefault(u => string.Equals(u.Username, username, StringComparison.Ordinal));

        var result = new EffectivePermissions
        {
            Username = username,
            Disabled = entry?.Disabled ?? false,
            Version = document.Version,
        };

        if (result.Disabled)
        {
            // 015 D:"the resolver returns an empty grant set for Disabled == true". Returning early with
            // nothing is both the decision and the belt to the evaluator's braces: whatever path this
            // object travels down, there is nothing in it to allow anything.
            return result;
        }

        var groups = document.Groups
            .Where(g => g.Members.Contains(username, StringComparer.Ordinal)
                        || (groupClaimValues is { Count: > 0 }
                            && g.ExternalClaimValues.Any(v => groupClaimValues.Contains(v, StringComparer.Ordinal))))
            .ToList();

        // Role names, in the order they were asked for, deduped. Unknown ones stay in this list on
        // purpose even though they resolve to no grants: an admin screen that showed only the roles that
        // still exist would hide the fact that a user references a deleted one.
        var roleNames = new List<string>();

        if (entry is not null)
        {
            AddDistinct(roleNames, entry.Roles);
        }
        else if (!string.IsNullOrWhiteSpace(roleClaim))
        {
            AddDistinct(roleNames, [roleClaim]);
        }

        foreach (var group in groups)
        {
            AddDistinct(roleNames, group.Roles);
        }

        // Merge order is user → group → role. It is documented rather than load-bearing: deny-overrides
        // and most-permissive-Allow make the evaluator's answer independent of grant order, which is
        // half the reason those two rules were chosen over a specificity ladder.
        if (entry is not null)
        {
            result.Grants.AddRange(entry.Grants);
        }

        foreach (var group in groups)
        {
            result.Grants.AddRange(group.Grants);
        }

        foreach (var roleName in roleNames)
        {
            var role = document.Roles.FirstOrDefault(r => string.Equals(r.Name, roleName, StringComparison.Ordinal));
            if (role is null)
            {
                continue;   // stale reference — see the type remarks
            }

            result.Grants.AddRange(role.Grants);
        }

        result.Roles = roleNames;
        result.Groups = groups.Select(g => g.Name).ToList();
        return result;
    }

    private static void AddDistinct(List<string> target, IEnumerable<string> names)
    {
        foreach (var name in names)
        {
            if (!string.IsNullOrWhiteSpace(name) && !target.Contains(name, StringComparer.Ordinal))
            {
                target.Add(name);
            }
        }
    }
}
