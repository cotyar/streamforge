using Orleans;
using Orleans.Runtime;
using StreamsForge.Abstractions;

namespace StreamsForge.Host.Grains;

/// <summary>Plan 015 W1 — the access-policy singleton (key = <see cref="StreamConstants.AccessKey"/>),
/// persisted in the same "definitions" store as the catalog and the user store.
///
/// <para><b>Why this is not part of UserStoreGrain.</b> Credentials are rewritten on every password
/// change; policy is read on every request and changes rarely. Splitting them is what lets the
/// per-request resolver cache policy aggressively while never holding a password hash in memory
/// (015 D:"Storage is a NEW singleton"), and it is why <c>Disabled</c> and the effective role list live
/// in <see cref="UserAccessEntry"/> here rather than on <see cref="UserRecord"/> — a resolver that had
/// to read the user store to learn a user is disabled would be caching exactly the thing the split
/// exists to avoid.</para>
///
/// <para><b>This grain is dumb storage and nothing else.</b> It stores, stamps and versions; it never
/// evaluates. Wildcard matching, deny-overrides, group flattening and the tri-state decision live in
/// AppCore's pure <c>PermissionEvaluator</c> so that both flavours share one tested implementation. It
/// does not seed either: an empty document is a legitimate state (every pre-upgrade data dir starts
/// here), and the built-in roles arrive from a pure <c>LegacyRoleMigration</c>.</para>
///
/// <para><b>Version is the whole performance story.</b> Every mutation bumps <c>Version</c> by exactly
/// one, and every replica polls <see cref="GetVersionAsync"/> on a TTL (<c>Auth:PolicyCacheSeconds</c>,
/// default 10) and refetches the document only when the number moves. That is one tiny grain call per
/// 10s per replica instead of a store lookup on every read — and it is what makes a revocation land in
/// ~10s instead of at the 12h token expiry.</para>
///
/// <para>The Dapr twin (<c>AccessPolicyActor</c> over a pure <c>AccessPolicyStore</c>) implements the
/// same <see cref="IAccessPolicyFacade"/> with the same semantics, member for member. Any change to the
/// rules below has to land on both sides in the same wave.</para></summary>
public sealed class AccessPolicyGrain(
    [PersistentState("access", StreamConstants.StorageName)] IPersistentState<AccessPolicyDocument> state)
    : Grain, IAccessPolicyGrain
{
    /// <summary>A grain that has never been written returns a fresh empty document with
    /// <c>Version = 0</c>, never null — <see cref="IPersistentState{T}"/> already gives us exactly that
    /// (a default-constructed <see cref="AccessPolicyDocument"/>), so "never seeded" and "seeded with
    /// nothing" are indistinguishable to every caller and no call site needs a null branch.</summary>
    public Task<AccessPolicyDocument> GetPolicyAsync() => Task.FromResult(state.State);

    /// <summary>Called by every replica every <c>Auth:PolicyCacheSeconds</c>, so it is the hottest call
    /// on this grain by two orders of magnitude. It reads one field of already-activated state and has
    /// no side effects whatsoever — in particular it does NOT touch storage: Orleans deserialized the
    /// document once at activation and the in-memory copy is authoritative for this activation, so the
    /// per-poll cost is a grain-call round trip and a long, not a store read.
    ///
    /// <para>ponytail: the ceiling is that a poll still ACTIVATES this grain if it had been collected,
    /// which deserializes the whole document to answer with a long. That is one deserialization per
    /// idle period per silo, not per poll, so it is not worth a separate version-only key; if it ever
    /// shows up in a profile, the upgrade path is to persist <c>Version</c> in its own tiny state
    /// object under a second <see cref="PersistentStateAttribute"/>.</para></summary>
    public Task<long> GetVersionAsync() => Task.FromResult(state.State.Version);

    // ------------------------------------------------------------------------------------------
    // Mutations. Every one of them bumps Version by exactly 1, stamps the document and the mutated
    // record, persists, and returns the STORED object rather than the caller's — so a caller can never
    // walk away believing a field it sent survived when the store overrode it (CreatedAtMs below is
    // precisely that case).
    //
    // ponytail: an upsert whose content is byte-identical to what is already stored still bumps the
    // version, which costs every replica one spurious document refetch within its TTL. Deliberately not
    // optimised: the ceiling is a config-import or a role-mirroring write loop that re-writes the whole
    // document repeatedly, and the upgrade path is a structural-equality check in Upsert* before the
    // bump — cheap to add later, and wrong to add now without a benchmark that says which comparison
    // (reference, JSON, per-field) is actually correct for a record that carries its own UpdatedAtMs.
    // ------------------------------------------------------------------------------------------

    private static long NowMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    /// <summary>The one place the version moves. Called after the in-memory mutation and before the
    /// caller sees anything, so a failed write throws with the version un-bumped in storage.</summary>
    private async Task CommitAsync(long nowMs)
    {
        state.State.Version++;
        state.State.UpdatedAtMs = nowMs;
        await state.WriteStateAsync();
    }

    /// <summary>Upsert matching is <b>ordinal and case-sensitive</b> on <c>Name</c> here and on
    /// <c>Username</c> for user entries — the same comparison the catalog uses for entity names, and the
    /// same one the Dapr twin must use. Case-insensitive matching would make "Editor" and "editor" the
    /// same role on one flavour and two on the other the moment either side reached for a different
    /// default.</summary>
    private static int IndexOfName(List<RoleDefinition> list, string name) =>
        list.FindIndex(r => string.Equals(r.Name, name, StringComparison.Ordinal));

    public async Task<RoleDefinition?> UpsertRoleAsync(RoleDefinition role, string actor)
    {
        // A nameless record is unaddressable — it could never be read back, updated or deleted — so it
        // is rejected outright rather than stored: null, nothing changed, no version bump. (A bump here
        // would make every replica refetch to observe a document that did not move.)
        if (string.IsNullOrWhiteSpace(role.Name))
        {
            return null;
        }

        var now = NowMs();
        role.UpdatedAtMs = now;
        role.UpdatedBy = actor;

        // NOTE: BuiltIn is stored exactly as sent. The delete guard below keys off the NAME, not this
        // flag, so a caller cannot make Viewer deletable by clearing it — the flag is informational
        // (it drives the SPA's disabled delete button), and deriving it here would be a rule the Dapr
        // twin does not have.
        var idx = IndexOfName(state.State.Roles, role.Name);
        if (idx >= 0)
        {
            state.State.Roles[idx] = role;
        }
        else
        {
            state.State.Roles.Add(role);
        }

        await CommitAsync(now);
        return role;
    }

    /// <summary>Refuses a built-in by NAME: deleting Viewer would strand every pre-upgrade token, whose
    /// only claim is a role string. Built-ins may be EDITED (an operator narrowing Editor is a
    /// legitimate, reversible act) but never removed. Absent name → false, and in both refusal cases
    /// nothing changes and the version does not move.</summary>
    public async Task<bool> DeleteRoleAsync(string name)
    {
        if (BuiltInRoles.All.Contains(name, StringComparer.Ordinal))
        {
            return false;
        }

        var idx = IndexOfName(state.State.Roles, name);
        if (idx < 0)
        {
            return false;
        }

        state.State.Roles.RemoveAt(idx);
        await CommitAsync(NowMs());
        return true;
    }

    public async Task<GroupDefinition?> UpsertGroupAsync(GroupDefinition group, string actor)
    {
        if (string.IsNullOrWhiteSpace(group.Name))
        {
            return null;
        }

        var now = NowMs();
        group.UpdatedAtMs = now;
        group.UpdatedBy = actor;

        var idx = state.State.Groups.FindIndex(g => string.Equals(g.Name, group.Name, StringComparison.Ordinal));
        if (idx >= 0)
        {
            // CreatedAtMs belongs to the group, not to this write: an update carries the stored value
            // forward and ignores whatever the caller sent, so a PUT built from a partially-filled DTO
            // cannot silently reset a group's age. Returning the stored object (below) is what makes
            // that override visible to the caller instead of a surprise on the next GET.
            group.CreatedAtMs = state.State.Groups[idx].CreatedAtMs;
            state.State.Groups[idx] = group;
        }
        else
        {
            group.CreatedAtMs = now;
            state.State.Groups.Add(group);
        }

        await CommitAsync(now);
        return group;
    }

    /// <summary>Removes the group and <b>does not rewrite a single user record</b>. Membership lives on
    /// the group (015 D:"Groups carry roles and grants"), so there is nothing dangling to clean up — the
    /// evaluator flattens groups by walking the group list, and a group that is gone contributes
    /// nothing. Rewriting users here would be the second whole-list-rewrite path the decision exists to
    /// avoid.</summary>
    public async Task<bool> DeleteGroupAsync(string name)
    {
        var idx = state.State.Groups.FindIndex(g => string.Equals(g.Name, name, StringComparison.Ordinal));
        if (idx < 0)
        {
            return false;
        }

        state.State.Groups.RemoveAt(idx);
        await CommitAsync(NowMs());
        return true;
    }

    /// <summary>Upsert, because this is also the call the user store makes on every create/update to
    /// mirror <c>UserRecord.Role</c> into <see cref="UserAccessEntry.Roles"/> — the mirror is what makes
    /// a role change take effect within the resolver's TTL instead of at the user's next login.</summary>
    public async Task<UserAccessEntry?> UpsertUserAccessAsync(UserAccessEntry entry, string actor)
    {
        if (string.IsNullOrWhiteSpace(entry.Username))
        {
            return null;
        }

        var now = NowMs();
        entry.UpdatedAtMs = now;
        entry.UpdatedBy = actor;

        var idx = state.State.Users.FindIndex(u => string.Equals(u.Username, entry.Username, StringComparison.Ordinal));
        if (idx >= 0)
        {
            state.State.Users[idx] = entry;
        }
        else
        {
            state.State.Users.Add(entry);
        }

        await CommitAsync(now);
        return entry;
    }

    /// <summary>Deleting the access entry is NOT deleting the user — the credential record in the user
    /// store is untouched. What it removes is the per-user policy overlay, after which the evaluator
    /// sees a user with no entry and falls back to the token's role claim, exactly as it does against a
    /// pre-upgrade catalog. Absent username → false.</summary>
    public async Task<bool> DeleteUserAccessAsync(string username)
    {
        var idx = state.State.Users.FindIndex(u => string.Equals(u.Username, username, StringComparison.Ordinal));
        if (idx < 0)
        {
            return false;
        }

        state.State.Users.RemoveAt(idx);
        await CommitAsync(NowMs());
        return true;
    }

    public async Task<ApprovalTemplate?> UpsertApprovalTemplateAsync(ApprovalTemplate template, string actor)
    {
        if (string.IsNullOrWhiteSpace(template.Name))
        {
            return null;
        }

        // ApprovalTemplate carries no UpdatedAtMs/UpdatedBy of its own (see AccessModels.cs) — templates
        // ship seeded but inert and are edited rarely, so the document's own UpdatedAtMs is the whole
        // audit trail here. The per-record stamp rule simply has nothing to stamp on this type; the
        // audit log (wave 4) is where "who changed a template" actually gets answered.
        var now = NowMs();

        var idx = state.State.ApprovalTemplates.FindIndex(t => string.Equals(t.Name, template.Name, StringComparison.Ordinal));
        if (idx >= 0)
        {
            state.State.ApprovalTemplates[idx] = template;
        }
        else
        {
            state.State.ApprovalTemplates.Add(template);
        }

        await CommitAsync(now);
        return template;
    }

    public async Task<bool> DeleteApprovalTemplateAsync(string name)
    {
        var idx = state.State.ApprovalTemplates.FindIndex(t => string.Equals(t.Name, name, StringComparison.Ordinal));
        if (idx < 0)
        {
            return false;
        }

        state.State.ApprovalTemplates.RemoveAt(idx);
        await CommitAsync(NowMs());
        return true;
    }
}
