using System.Text.Json;
using StreamsForge.Abstractions;

namespace StreamsForge.Dapr.Host.Access;

/// <summary>
/// Plan 015 W1: actor-framework-free access-policy logic behind <see cref="Actors.AccessPolicyActor"/> —
/// the same split <see cref="Catalog.CatalogStore"/> has behind <see cref="Actors.RegistryActor"/>, and
/// for the same reason: a plain class over an in-memory <see cref="AccessPolicyDocument"/> is unit-testable
/// without a Dapr sidecar, an actor runtime, or a Redis state store (see
/// dapr/tests/StreamsForge.Dapr.Tests/AccessPolicyStoreTests.cs). The actor is the thin shell that loads and
/// saves the document; every rule about what a mutation does lives here.
///
/// <para><b>This is dumb storage on purpose.</b> It does not seed the built-in roles, does not know what
/// <c>Viewer</c> means, and never evaluates anything — a pure <c>LegacyRoleMigration</c> seeds and migrates
/// (wired up in wave 2) and a pure <c>PermissionEvaluator</c> decides. An EMPTY document is therefore a
/// completely legitimate, non-crashing state: it is what a store that has never been written looks like,
/// and it is what the resolver sees on a pre-upgrade catalog before the migration has run.</para>
///
/// <para><b>Why <see cref="AccessPolicyDocument.Version"/> matters more than anything else here.</b> The
/// per-request permission resolver on every replica polls <c>GetVersionAsync()</c> on a TTL
/// (<c>Auth:PolicyCacheSeconds</c>, default 10) and refetches the document only when the number moves —
/// which on THIS flavour is what keeps authorization off the sidecar: a full store lookup per request would
/// be a Dapr round trip on every read (015 D:"Permissions resolve server-side per request"). So the version
/// is bumped by exactly one on every mutation, it is the only thing a version read touches, and reading it
/// has no side effects at all.</para>
///
/// <para><b>The semantics below are pinned across both flavours</b> — Orleans' <c>AccessPolicyGrain</c>
/// implements the identical rules, because the whole point of a runtime-neutral
/// <see cref="IAccessPolicyFacade"/> is that a policy behaves the same on either host.</para>
/// </summary>
public sealed class AccessPolicyStore(AccessPolicyDocument document)
{
    /// <summary>The live document. The actor persists THIS object, so callers get the stored state, not a
    /// snapshot of it — which is also why every mutation below returns the stored record rather than the
    /// caller's: a caller that kept mutating the object it handed in must not be able to edit the policy
    /// after the fact, without a write and without a version bump.</summary>
    public AccessPolicyDocument Document => document;

    /// <summary>Reads the number and nothing else — see the class doc. Every replica calls this on a timer.</summary>
    public long Version => document.Version;

    // ---------------------------------------------------------------------------------------------
    // Roles
    // ---------------------------------------------------------------------------------------------

    /// <summary>Upsert by <see cref="RoleDefinition.Name"/>, ordinal and case-sensitive: "Editor" and
    /// "editor" are two different roles, exactly as they are two different strings in a token's role claim.
    /// An empty name is rejected (null, nothing changed, no version bump) — a nameless role can never be
    /// referenced by a user or a group, so storing one would only be a way to burn versions.</summary>
    public RoleDefinition? UpsertRole(RoleDefinition role, string actor)
    {
        if (string.IsNullOrWhiteSpace(role.Name))
        {
            return null;
        }

        var now = NowMs();
        var stored = Copy(role);
        stored.UpdatedAtMs = now;
        stored.UpdatedBy = actor;
        Put(document.Roles, document.Roles.FindIndex(r => r.Name == role.Name), stored);
        Bump(now);
        return stored;
    }

    /// <summary>Refuses a built-in by NAME rather than by <see cref="RoleDefinition.BuiltIn"/>: deleting
    /// Viewer would strand every token minted before the upgrade, and a flag the caller can clear on an
    /// upsert is not a lock. Built-ins may still be EDITED — that is how a deployment tightens what Editor
    /// means without inventing a new role name that nothing in the catalog references yet.</summary>
    public bool DeleteRole(string name)
    {
        if (BuiltInRoles.All.Contains(name, StringComparer.Ordinal))
        {
            return false;
        }

        return Remove(document.Roles, r => r.Name == name);
    }

    // ---------------------------------------------------------------------------------------------
    // Groups
    // ---------------------------------------------------------------------------------------------

    /// <summary>Upsert by <see cref="GroupDefinition.Name"/>. Membership lives on the group (015 D:"Groups
    /// carry roles and grants"), so this one write is the whole membership change — no user record is
    /// touched, and the hottest singleton in the system keeps exactly one whole-list-rewrite path.
    ///
    /// <para><see cref="GroupDefinition.CreatedAtMs"/> is server-owned: stamped on create, and carried over
    /// from the stored record on update so a caller cannot rewrite when the group came into existence (nor
    /// zero it by round-tripping a DTO that never carried it).</para></summary>
    public GroupDefinition? UpsertGroup(GroupDefinition group, string actor)
    {
        if (string.IsNullOrWhiteSpace(group.Name))
        {
            return null;
        }

        var now = NowMs();
        var index = document.Groups.FindIndex(g => g.Name == group.Name);
        var stored = Copy(group);
        stored.UpdatedAtMs = now;
        stored.UpdatedBy = actor;
        stored.CreatedAtMs = index >= 0 ? document.Groups[index].CreatedAtMs : now;
        Put(document.Groups, index, stored);
        Bump(now);
        return stored;
    }

    /// <summary>Removes the group and NOTHING else. A user record is deliberately not rewritten: membership
    /// only ever lived on the group, so there is no dangling reference to clean up — and a delete that
    /// fanned out into the user list would be the second whole-list-rewrite path this design exists to
    /// avoid.</summary>
    public bool DeleteGroup(string name) => Remove(document.Groups, g => g.Name == name);

    // ---------------------------------------------------------------------------------------------
    // Per-user access
    // ---------------------------------------------------------------------------------------------

    /// <summary>Upsert by <see cref="UserAccessEntry.Username"/>. Created on first write, which is also how
    /// the user store MIRRORS <c>UserRecord.Role</c> into this document on every user create/update — the
    /// mirror is what makes a role change take effect within the resolver's TTL instead of at the user's
    /// next login (see AccessModels.cs on UserAccessEntry.Roles).</summary>
    public UserAccessEntry? UpsertUserAccess(UserAccessEntry entry, string actor)
    {
        if (string.IsNullOrWhiteSpace(entry.Username))
        {
            return null;
        }

        var now = NowMs();
        var stored = Copy(entry);
        stored.UpdatedAtMs = now;
        stored.UpdatedBy = actor;
        Put(document.Users, document.Users.FindIndex(u => u.Username == entry.Username), stored);
        Bump(now);
        return stored;
    }

    /// <summary>Drops one user's policy. NOT the same thing as disabling them: an absent entry makes the
    /// evaluator fall back to the token's role claim (the pre-upgrade path), whereas
    /// <see cref="UserAccessEntry.Disabled"/> is what actually kills a live token. Deleting the entry of a
    /// user you meant to disable would do the opposite of what it looks like — which is why disablement is
    /// a field on the entry and not the absence of one.</summary>
    public bool DeleteUserAccess(string username) => Remove(document.Users, u => u.Username == username);

    // ---------------------------------------------------------------------------------------------
    // Approval templates
    // ---------------------------------------------------------------------------------------------

    /// <summary>Upsert by <see cref="ApprovalTemplate.Name"/>. Templates ship seeded but inert
    /// (<c>Approvals:Enabled=false</c>), so storing one changes no behaviour until the feature is switched
    /// on — which is what keeps an existing deployment byte-identical after the upgrade.</summary>
    public ApprovalTemplate? UpsertApprovalTemplate(ApprovalTemplate template, string actor)
    {
        if (string.IsNullOrWhiteSpace(template.Name))
        {
            return null;
        }

        // ponytail: ApprovalTemplate carries no UpdatedAtMs/UpdatedBy of its own (AccessModels.cs is
        // frozen), so only the document-level stamp records that this write happened. Ceiling: you cannot
        // tell from the document who last edited a template. Upgrade path is additive — [Id(9)]/[Id(10)]
        // on ApprovalTemplate plus two lines here — and until then the audit log (wave 4) is where "who
        // changed this template" is answered.
        var stored = Copy(template);
        var index = document.ApprovalTemplates.FindIndex(t => t.Name == template.Name);
        Put(document.ApprovalTemplates, index, stored);
        Bump(NowMs());
        return stored;
    }

    public bool DeleteApprovalTemplate(string name) => Remove(document.ApprovalTemplates, t => t.Name == name);

    // ---------------------------------------------------------------------------------------------
    // Internals
    // ---------------------------------------------------------------------------------------------

    private static long NowMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    /// <summary>Deep copy via a JSON round trip.
    ///
    /// <para>ponytail: one line instead of four hand-written clone methods that would silently go stale the
    /// next time AccessModels.cs grows a field — and these types are already JSON-serialized on the way
    /// into the state store, so the round trip is provably lossless for exactly the shapes that reach here.
    /// Ceiling: reflection-based serialization on every mutation. That is fine because policy "is read on
    /// every request and changes rarely" (015 D:"Storage is a NEW singleton"); if mutations ever became hot,
    /// the upgrade path is a source-generated JsonSerializerContext, not hand-written clones.</para></summary>
    private static T Copy<T>(T value) => JsonSerializer.Deserialize<T>(JsonSerializer.Serialize(value))!;

    private static void Put<T>(List<T> list, int index, T value)
    {
        if (index >= 0)
        {
            list[index] = value;
        }
        else
        {
            list.Add(value);
        }
    }

    private bool Remove<T>(List<T> list, Predicate<T> match)
    {
        if (list.RemoveAll(match) == 0)
        {
            return false;
        }

        Bump(NowMs());
        return true;
    }

    /// <summary>The one place the version moves: +1, exactly, per mutation that actually happened.
    ///
    /// <para>ponytail: an upsert whose content is byte-identical to what is already stored still bumps,
    /// which costs every replica one spurious document refetch (~10s later, one per replica). Deliberately
    /// not optimised — the alternative is a structural comparison of the record on every write, which is
    /// more code, more to get subtly wrong, and buys nothing on a document that "changes rarely". Ceiling:
    /// a client that PUTs an unchanged role in a loop makes every replica refetch in a loop. Upgrade path:
    /// compare the serialized <see cref="Copy{T}"/> output against the stored record's and return the
    /// stored one without bumping — one <c>if</c>, once someone can show a workload that needs it.</para></summary>
    private void Bump(long now)
    {
        document.Version++;
        document.UpdatedAtMs = now;
    }
}
