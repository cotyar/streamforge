using StreamForge.Abstractions;

namespace StreamForge.AppCore.Access;

/// <summary>
/// Plan 015 — turning a pre-upgrade catalog into one the evaluator can answer from, without a flag day.
///
/// <para>Two things have to be true before the entitlement path can carry a request: the three built-in
/// roles have to exist in the document, and every existing user has to have a
/// <see cref="UserAccessEntry"/> whose <see cref="UserAccessEntry.Roles"/> mirrors the
/// <c>UserRecord.Role</c> they have today. The mirror is the piece the plan's wave-0 notes call out as
/// newly owned by wave 1: because the effective role list lives in the access document rather than on
/// the credential record, a role *change* only takes effect within the resolver's TTL if somebody keeps
/// the two in step. The user store does that from here on for create/update; this does it once, for the
/// users who already existed.</para>
///
/// <para><b>Pure on purpose.</b> It takes a document and a list of users and returns the document that
/// should be stored, so it is testable without a store, without a silo and without a sidecar — the same
/// reason <see cref="PermissionEvaluator"/> is pure. Wiring it into each host's startup is a separate
/// job; both flavours call the same function, so neither can drift.</para>
///
/// <para><b>Idempotent, and it never overwrites an administrator.</b> A second run changes nothing and
/// reports <c>changed = false</c> — which matters more than it looks: the caller writes only when
/// something changed, and a needless write would bump <see cref="AccessPolicyDocument.Version"/> and
/// invalidate every replica's policy cache on every host restart. An entry that already exists with a
/// non-empty role list is left completely alone: by the time this runs a second time an administrator
/// may have replaced alice's <c>Editor</c> with <c>Auditor</c>, and re-mirroring the stale
/// <c>UserRecord.Role</c> over the top would be a privilege change performed by a migration.</para>
/// </summary>
public static class LegacyRoleMigration
{
    /// <summary>Returns the document that should be stored.</summary>
    /// <param name="document">The document as read from the store. Not mutated: the returned document
    /// has its own lists, so a caller that decides not to write is left holding an untouched snapshot.</param>
    /// <param name="users">Every <see cref="UserRecord"/> in the user store.</param>
    /// <param name="nowMs">Stamped on whatever this creates. Passed in rather than read from the clock —
    /// that is the difference between a testable function and one that needs a fake clock.</param>
    /// <param name="actor">Who to record as having made the change; "system" from a startup migration.</param>
    /// <param name="changed">False when the returned document is equivalent to the one passed in, so the
    /// caller can skip the write and the version bump.</param>
    public static AccessPolicyDocument Apply(
        AccessPolicyDocument document,
        IReadOnlyCollection<UserRecord> users,
        long nowMs,
        string actor,
        out bool changed)
    {
        ArgumentNullException.ThrowIfNull(document);
        ArgumentNullException.ThrowIfNull(users);

        changed = false;

        // Shallow copy: new lists, same element instances. Deep-cloning would need a hand-written copy
        // of every model in AccessModels.cs and would go stale the first time one gains a field; the
        // elements this function does change, it replaces rather than mutates, which gets the same
        // guarantee for the price of two `new`s.
        var result = new AccessPolicyDocument
        {
            Roles = [.. document.Roles],
            Groups = [.. document.Groups],
            Users = [.. document.Users],
            ApprovalTemplates = [.. document.ApprovalTemplates],
            Version = document.Version,          // versioning belongs to the store, not to the migration
            UpdatedAtMs = document.UpdatedAtMs,
        };

        foreach (var builtIn in BuiltInRoleCatalog.Create(nowMs, actor))
        {
            // Seeded only if ABSENT. An existing role of the same name is an administrator's, even when
            // it started life as one of these — built-ins may be edited, only deleting them is refused.
            if (result.Roles.Any(r => string.Equals(r.Name, builtIn.Name, StringComparison.Ordinal)))
            {
                continue;
            }

            result.Roles.Add(builtIn);
            changed = true;
        }

        foreach (var user in users)
        {
            if (string.IsNullOrWhiteSpace(user.Username) || string.IsNullOrWhiteSpace(user.Role))
            {
                // A blank role has nothing to mirror. Creating an empty entry for it would also break
                // idempotency: the next run would see an entry with no roles and try to mirror again.
                continue;
            }

            var index = result.Users.FindIndex(e => string.Equals(e.Username, user.Username, StringComparison.Ordinal));

            if (index < 0)
            {
                result.Users.Add(new UserAccessEntry
                {
                    Username = user.Username,
                    Roles = [user.Role],
                    UpdatedAtMs = nowMs,
                    UpdatedBy = actor,
                });
                changed = true;
                continue;
            }

            var existing = result.Users[index];
            if (existing.Roles.Count > 0)
            {
                continue;   // an administrator's, or already mirrored
            }

            // An entry with no roles at all: created for its Disabled flag or its direct grants before
            // the mirror existed. Fill in the role, keep everything else exactly as it was — especially
            // Disabled, which a migration must never clear.
            result.Users[index] = new UserAccessEntry
            {
                Username = existing.Username,
                Disabled = existing.Disabled,
                Roles = [user.Role],
                Grants = [.. existing.Grants],
                UpdatedAtMs = nowMs,
                UpdatedBy = actor,
            };
            changed = true;
        }

        if (changed)
        {
            result.UpdatedAtMs = nowMs;
        }

        return result;
    }
}
