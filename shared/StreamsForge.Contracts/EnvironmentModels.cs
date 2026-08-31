using Orleans;

namespace StreamsForge.Abstractions;

/// <summary>
/// Plan 021 wave 0 — the environment vocabulary, pre-built by the orchestrator so that wave 1's three
/// concurrent tracks (Orleans runtime, Dapr runtime, REST surface) meet on a shape none of them owns.
///
/// <para>An environment is a PARTITION INSIDE ONE RUNNING SERVER: its own sources, pipelines and tables,
/// its own name uniqueness, its own SQL namespace, its own grain/actor/stream keys. It is deliberately
/// <b>not</b> a security boundary (any authenticated Editor can point a header at any environment until
/// plan 015's per-resource grants are scoped to one — see the plan's D9), <b>not</b> a resource boundary
/// (one process, one heap: a runaway pipeline in <c>staging</c> starves <c>prod</c> exactly as much as it
/// does today), and <b>not</b> a second cluster, silo or sidecar.</para>
/// </summary>
[GenerateSerializer]
public sealed class EnvironmentRecord
{
    /// <summary>The name as a human types it. The default environment is spelled <c>"default"</c> HERE and
    /// in every other API-facing place, and as the empty string in every runtime key — see
    /// <c>EnvKeys.Default</c>, whose doc comment holds the whole reason.</summary>
    [Id(0)] public string Name { get; set; } = "";

    [Id(1)] public string Description { get; set; } = "";

    [Id(2)] public long CreatedAtMs { get; set; }

    /// <summary>Username, matching the <c>UpdatedBy</c> convention plan 015 established on the three
    /// definition types. Empty for the default environment, which nobody created.</summary>
    [Id(3)] public string CreatedBy { get; set; } = "";

    /// <summary>How many sources + pipelines + tables live in it. Filled by the registry on read, never
    /// stored: it is a fact about another grain's state and would go stale the moment it were persisted.
    /// −1 means "not counted" (the environment's catalog was not consulted for this response).</summary>
    [Id(4)] public int EntityCount { get; set; } = -1;
}

/// <summary>
/// Plan 021 — the environment directory, one singleton per flavour (Orleans
/// <c>IEnvironmentRegistryGrain</c> inherits this so a grain reference IS-A facade, exactly as
/// <c>IRegistryGrain</c>/<c>ICatalogFacade</c> already do; Dapr registers a thin actor-proxy adapter).
///
/// <para><b>Creation is deliberate; a typo is a 404</b> (D7). Implicit creation on first use would make
/// <c>X-StreamsForge-Environment: stagng</c> a successful deploy into a new empty environment nobody meant
/// to make. <c>default</c> always exists, cannot be created, cannot be deleted, and cannot be renamed —
/// and neither can any other environment, for the same reason plan 011 D2 refuses renaming a sharded
/// table: the name is in every key.</para>
/// </summary>
public interface IEnvironmentFacade
{
    /// <summary>Every environment, <c>default</c> first and the rest name-ordered. <c>default</c> is
    /// synthesised, not stored, so it is present on a brand-new instance and on every instance that
    /// predates this plan.</summary>
    Task<List<EnvironmentRecord>> ListAsync();

    /// <summary>Whether an environment exists and may be addressed. True for
    /// <c>EnvKeys.Default</c>/<c>"default"</c> always. This is the call the middleware makes on every
    /// request that names an environment, so it must be cheap — the implementations answer from the
    /// singleton's already-activated state, with no catalog access.</summary>
    Task<bool> ExistsAsync(string name);

    /// <summary>Creates an environment. Throws <see cref="InvalidOperationException"/> (409-style, the
    /// convention <c>ValidateUniqueTableName</c> established) if the name already exists, and
    /// <see cref="ArgumentException"/> (400-style) if it fails <c>EnvKeys.IsValidName</c> — including the
    /// reserved names and <c>default</c> itself.</summary>
    Task<EnvironmentRecord> CreateAsync(string name, string description, string createdBy);

    /// <summary>Deletes an environment and, when <paramref name="force"/> is set, everything in it.
    /// Refuses a non-empty environment without <paramref name="force"/>
    /// (<see cref="InvalidOperationException"/>), and refuses <c>default</c> outright, always. Returns
    /// false when the environment does not exist. This is the one genuinely destructive operation the plan
    /// adds: it deletes the catalog AND the runtime state of everything in it.</summary>
    Task<bool> DeleteAsync(string name, bool force);
}
