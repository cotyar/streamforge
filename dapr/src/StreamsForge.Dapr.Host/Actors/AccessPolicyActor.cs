using Dapr.Actors.Runtime;
using StreamsForge.Abstractions;
using StreamsForge.Dapr.Host.Access;

namespace StreamsForge.Dapr.Host.Actors;

/// <summary>
/// Plan 015 W1: Dapr counterpart of Orleans' <c>AccessPolicyGrain</c> — singleton actor, id =
/// <see cref="StreamConstants.AccessKey"/> ("access"), one state entry of the same name holding the whole
/// <see cref="AccessPolicyDocument"/>.
///
/// <para><b>Thin by design.</b> Every rule about what a mutation does — the version bump, the UpdatedBy
/// stamp, the built-in-role refusal, the empty-name rejection — lives in
/// <see cref="AccessPolicyStore"/>, a plain class with no Dapr dependency, which is what makes those rules
/// testable without a sidecar (dapr/tests/StreamsForge.Dapr.Tests/AccessPolicyStoreTests.cs). This actor
/// contributes exactly two things the store cannot: loading the document on activation and persisting it
/// after a write. Same shape as <see cref="RegistryActor"/> over <see cref="Catalog.CatalogStore"/>.</para>
///
/// <para><b>Persist only when something changed.</b> Every mutation below writes state only when the store
/// says the mutation actually happened (non-null / true). A refused write — a built-in role, an unknown
/// name, an empty name — must leave the version alone, and a version that moved for nothing would send
/// every replica in the cluster off to refetch the document (see <see cref="GetVersionAsync"/>).</para>
///
/// <para><b>Reentrancy:</b> this actor calls nothing — no other actor, no proxy, no orchestrator. Default
/// (non-reentrant) turn-based concurrency, and it should stay that way: the resolver's version poll is the
/// hottest call in the system and it must never queue behind a turn that is waiting on somebody else.</para>
///
/// <para><b>No seeding here.</b> Unlike <see cref="UserStoreActor.EnsureInitializedAsync"/>, this actor has
/// no ensure-initialized: an empty document is a legitimate state (a pre-upgrade catalog looks exactly like
/// it), and the built-in roles are seeded by the pure <c>LegacyRoleMigration</c> so that both flavours seed
/// identically from one implementation instead of two.</para>
/// </summary>
public sealed class AccessPolicyActor(ActorHost host) : Actor(host), IAccessPolicyActor
{
    private const string StateName = StreamConstants.AccessKey;

    private AccessPolicyDocument _document = new();
    private AccessPolicyStore _store = null!;

    protected override async Task OnActivateAsync()
    {
        var existing = await StateManager.TryGetStateAsync<AccessPolicyDocument>(StateName);
        _document = existing.HasValue ? existing.Value : new AccessPolicyDocument();
        _store = new AccessPolicyStore(_document);
    }

    private Task SaveAsync() => StateManager.SetStateAsync(StateName, _document);

    public Task<AccessPolicyDocument> GetPolicyAsync() => Task.FromResult(_store.Document);

    public Task<long> GetVersionAsync() => Task.FromResult(_store.Version);

    public async Task<RoleDefinition?> UpsertRoleAsync(UpsertRoleActorRequest request)
    {
        var stored = _store.UpsertRole(request.Role, request.Actor);
        if (stored is not null)
        {
            await SaveAsync();
        }

        return stored;
    }

    public Task<bool> DeleteRoleAsync(string name) => SaveIfAsync(_store.DeleteRole(name));

    public async Task<GroupDefinition?> UpsertGroupAsync(UpsertGroupActorRequest request)
    {
        var stored = _store.UpsertGroup(request.Group, request.Actor);
        if (stored is not null)
        {
            await SaveAsync();
        }

        return stored;
    }

    public Task<bool> DeleteGroupAsync(string name) => SaveIfAsync(_store.DeleteGroup(name));

    public async Task<UserAccessEntry?> UpsertUserAccessAsync(UpsertUserAccessActorRequest request)
    {
        var stored = _store.UpsertUserAccess(request.Entry, request.Actor);
        if (stored is not null)
        {
            await SaveAsync();
        }

        return stored;
    }

    public Task<bool> DeleteUserAccessAsync(string username) => SaveIfAsync(_store.DeleteUserAccess(username));

    public async Task<ApprovalTemplate?> UpsertApprovalTemplateAsync(UpsertApprovalTemplateActorRequest request)
    {
        var stored = _store.UpsertApprovalTemplate(request.Template, request.Actor);
        if (stored is not null)
        {
            await SaveAsync();
        }

        return stored;
    }

    public Task<bool> DeleteApprovalTemplateAsync(string name) => SaveIfAsync(_store.DeleteApprovalTemplate(name));

    /// <summary>The delete half of "persist only when something changed" — the store has already mutated
    /// (or not) by the time this is called, so the bool is both the answer and the dirty flag.</summary>
    private async Task<bool> SaveIfAsync(bool removed)
    {
        if (removed)
        {
            await SaveAsync();
        }

        return removed;
    }
}
