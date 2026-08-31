namespace StreamsForge.Abstractions;

/// <summary>Plan 021 wave 1, track A (Orleans) — the environment directory grain. Singleton (key =
/// StreamConstants.EnvironmentsKey). Inherits <see cref="IEnvironmentFacade"/> exactly the way
/// <see cref="IRegistryGrain"/> inherits <c>ICatalogFacade</c> (see GrainInterfaces.cs's own class doc for
/// the same reasoning): a grain reference already satisfies the facade type with zero adapter code, and a
/// Dapr host instead registers a thin actor-proxy adapter implementing <see cref="IEnvironmentFacade"/>
/// directly.
///
/// <para>No members beyond what <see cref="IEnvironmentFacade"/> already declares — unlike
/// <see cref="IRegistryGrain"/>, there is no Orleans-boot-only <c>EnsureInitializedAsync</c> here: the
/// environment directory needs no seeding (<c>default</c> is synthesised, never stored — see
/// <c>EnvironmentRegistryGrain</c>) and nothing about the directory itself depends on silo-start ordering
/// the way generator/pipeline/table resumption does.</para></summary>
public interface IEnvironmentRegistryGrain : IEnvironmentFacade, IGrainWithStringKey
{
}
