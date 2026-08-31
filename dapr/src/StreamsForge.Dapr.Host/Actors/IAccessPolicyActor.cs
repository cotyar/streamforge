using Dapr.Actors;
using StreamsForge.Abstractions;

namespace StreamsForge.Dapr.Host.Actors;

// Plan 015 W1. Request payloads for the multi-argument IAccessPolicyFacade members: a Dapr actor method
// takes 0 or 1 parameters (unlike an Orleans grain method, which allows arbitrary parameter lists via
// Orleans' own serializer), so every `(record, actor)` upsert is wrapped in a record here — the same
// mechanism, and the same reason, as IUserStoreActor's ValidateCredentialsRequest/CreateUserActorRequest.
// `Actor` is the principal the write is attributed to; it lands in the stored record's UpdatedBy.

public sealed record UpsertRoleActorRequest(RoleDefinition Role, string Actor);

public sealed record UpsertGroupActorRequest(GroupDefinition Group, string Actor);

public sealed record UpsertUserAccessActorRequest(UserAccessEntry Entry, string Actor);

public sealed record UpsertApprovalTemplateActorRequest(ApprovalTemplate Template, string Actor);

/// <summary>
/// Actor-invocation surface for the access-policy singleton actor (id =
/// <see cref="StreamConstants.AccessKey"/>, "access") — the Dapr counterpart of Orleans'
/// <c>IAccessPolicyGrain</c>. A SEPARATE singleton from <see cref="IUserStoreActor"/> on purpose (015
/// D:"Storage is a NEW singleton"): credentials are rewritten on every password change, policy is read on
/// every request and cached hard, and the split is what lets the resolver cache policy aggressively while
/// never holding a password hash.
///
/// <para>All the rules live in the actor-framework-free <see cref="Access.AccessPolicyStore"/>;
/// <see cref="AccessPolicyActor"/> is the thin load/save shell, and
/// <see cref="Facades.DaprAccessPolicyFacade"/> is the adapter that turns
/// <see cref="IAccessPolicyFacade"/> calls into these. No <see cref="ActorResult{T}"/> anywhere on this
/// interface: nothing here fails in a way that needs an error string — a refused mutation is a null or a
/// false, exactly as it is on <see cref="IUserStoreActor"/> and on Orleans' own store.</para>
/// </summary>
public interface IAccessPolicyActor : IActor
{
    /// <summary>The whole document. A store that has never been written answers with a fresh, empty
    /// <see cref="AccessPolicyDocument"/> at <c>Version = 0</c> — never null. An empty document is a
    /// legitimate state: seeding the built-in roles is <c>LegacyRoleMigration</c>'s job, not this
    /// actor's.</summary>
    Task<AccessPolicyDocument> GetPolicyAsync();

    /// <summary>The version and nothing else. Called by every replica's resolver every
    /// <c>Auth:PolicyCacheSeconds</c> (default 10) — on this flavour that is a sidecar round trip, which is
    /// precisely why the plan refused a per-request store lookup — so it must stay side-effect-free and must
    /// never do more work than reading the number.</summary>
    Task<long> GetVersionAsync();

    Task<RoleDefinition?> UpsertRoleAsync(UpsertRoleActorRequest request);

    /// <summary>False for a built-in (see <see cref="Access.AccessPolicyStore.DeleteRole"/>) and false for
    /// a name that isn't there.</summary>
    Task<bool> DeleteRoleAsync(string name);

    Task<GroupDefinition?> UpsertGroupAsync(UpsertGroupActorRequest request);
    Task<bool> DeleteGroupAsync(string name);

    Task<UserAccessEntry?> UpsertUserAccessAsync(UpsertUserAccessActorRequest request);
    Task<bool> DeleteUserAccessAsync(string username);

    Task<ApprovalTemplate?> UpsertApprovalTemplateAsync(UpsertApprovalTemplateActorRequest request);
    Task<bool> DeleteApprovalTemplateAsync(string name);
}
