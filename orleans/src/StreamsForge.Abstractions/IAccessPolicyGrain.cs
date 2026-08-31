namespace StreamsForge.Abstractions;

// ============================================================================
// Plan 015 (RBAC → entitlements) W1: the Orleans half of the access-policy singleton.
//
// Its own file rather than another block in GrainInterfaces.cs because the Dapr twin
// (AccessPolicyActor over a pure AccessPolicyStore) has to agree with this one member for member, and
// a reviewer comparing the two flavours should be able to open one file per side.
// ============================================================================

/// <summary>Singleton (key = <see cref="StreamConstants.AccessKey"/>, storage
/// <see cref="StreamConstants.StorageName"/>). Plan 005's seam rule applies unchanged: every member
/// lives on the runtime-neutral <see cref="IAccessPolicyFacade"/>, so shared/StreamsForge.Api depends on
/// the facade and never on this interface — and, unlike <see cref="IRegistryGrain"/> and
/// <see cref="IUserStoreGrain"/>, this one adds <b>nothing at all</b>.
///
/// <para>There is deliberately no <c>EnsureInitializedAsync</c>. Seeding the built-in roles and
/// migrating a pre-upgrade catalog belong to a pure <c>LegacyRoleMigration</c> (wave 1 sibling, wired up
/// in wave 2): this grain is dumb storage, and an EMPTY document is a legitimate, non-crashing state —
/// which is also the state every existing data dir starts this upgrade in. Putting a seed here would
/// mean two seeders (one per flavour) that must stay byte-identical forever, which is exactly the bug
/// the pure-migration split exists to avoid.</para>
///
/// <para>The store never evaluates anything. Wildcards, deny-overrides, group flattening and the
/// tri-state decision all live in AppCore's pure <c>PermissionEvaluator</c>, so the semantics are
/// tested once, in both suites, instead of once per runtime.</para></summary>
public interface IAccessPolicyGrain : IAccessPolicyFacade, IGrainWithStringKey
{
}
