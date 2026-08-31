using StreamsForge.Abstractions;
using StreamsForge.AppCore.Environments;

namespace StreamsForge.Dapr.Host.Actors;

/// <summary>Persisted shape of the "environments" singleton actor's state — deliberately holds only the
/// NAMED (non-default) environments. <see cref="EnvKeys.Default"/> is synthesised on every read (see
/// <see cref="EnvironmentRegistryStore.ListWithDefault"/>), never stored — see
/// <see cref="EnvironmentRecord"/>'s own class doc for why: it must be present on an instance that
/// predates this plan, which has no row for it anywhere.</summary>
public sealed class EnvironmentRegistryState
{
    public List<EnvironmentRecord> Environments { get; set; } = [];
}

/// <summary>
/// Plan 021 (environment isolation), Dapr track: pure, actor-framework-free logic behind
/// <see cref="EnvironmentRegistryActor"/> — mirrors the <see cref="Catalog.CatalogStore"/>/
/// <see cref="RegistryActor"/> split (see that class's own doc comment for the rationale) so the name/
/// duplicate/reserved-word validation is unit-testable without a Dapr sidecar.
///
/// <para><b>What stays here vs. what the actor does:</b> name legality (<see cref="EnvKeys.IsValidName"/>)
/// and duplicate detection are pure — no I/O needed to answer them. Whether an environment is EMPTY (so it
/// can be deleted without <c>force</c>) requires asking that environment's own catalog, which needs an
/// <see cref="ICatalogFacadeFactory"/> and therefore I/O — that decision, and the force-delete teardown
/// itself, live in <see cref="EnvironmentRegistryActor.DeleteAsync"/>, not here.</para>
/// </summary>
public sealed class EnvironmentRegistryStore(EnvironmentRegistryState state)
{
    /// <summary><see cref="EnvKeys.Default"/> first, then every stored environment name-ordered — the
    /// list shape <see cref="IEnvironmentFacade.ListAsync"/> promises. <see cref="EnvironmentRecord.EntityCount"/>
    /// is left at its -1 default here (no catalog access from a pure method) — the actor fills it in.</summary>
    public List<EnvironmentRecord> ListWithDefault() =>
    [
        new EnvironmentRecord { Name = EnvKeys.DefaultDisplayName, Description = "", CreatedAtMs = 0, CreatedBy = "" },
        .. state.Environments.OrderBy(e => e.Name, StringComparer.Ordinal),
    ];

    /// <summary>True for the default environment (spelled either way) or any stored one. Cheap — no I/O,
    /// answered from already-loaded state, per <see cref="IEnvironmentFacade.ExistsAsync"/>'s own
    /// "must be cheap" contract.</summary>
    public bool Exists(string normalizedName) =>
        normalizedName == EnvKeys.Default || state.Environments.Any(e => e.Name == normalizedName);

    /// <summary>Throws <see cref="ArgumentException"/> on a name <see cref="EnvKeys.IsValidName"/> rejects
    /// (this also catches "default" and every reserved word — see that method's own doc), and
    /// <see cref="InvalidOperationException"/> on a duplicate — the same two-exception convention
    /// <see cref="Catalog.CatalogStore"/> uses (<c>ValidateUniqueTableName</c> is the 409-style
    /// precedent).</summary>
    public EnvironmentRecord Create(string name, string description, string createdBy, long nowMs)
    {
        if (!EnvKeys.IsValidName(name))
        {
            throw new ArgumentException(
                $"'{name}' is not a legal environment name — lower-case letters, digits and hyphens only, 1-32 characters, and not one of the reserved words ({string.Join(", ", EnvKeys.Reserved)}).");
        }

        if (Exists(name))
        {
            throw new InvalidOperationException($"environment '{name}' already exists.");
        }

        var record = new EnvironmentRecord
        {
            Name = name,
            Description = description,
            CreatedAtMs = nowMs,
            CreatedBy = createdBy,
        };
        state.Environments.Add(record);
        return record;
    }

    /// <summary>Removes the stored record. Callers must already have decided the environment is deletable
    /// (not default, exists, empty-or-forced) — this method itself does none of that validation, on
    /// purpose: it is the one piece of this store that needs no I/O AND the one piece the actor cannot
    /// safely lift into a pure method (default/existence are pure checks the actor still makes first, but
    /// "empty" is not).</summary>
    public bool Delete(string normalizedName) => state.Environments.RemoveAll(e => e.Name == normalizedName) > 0;
}
