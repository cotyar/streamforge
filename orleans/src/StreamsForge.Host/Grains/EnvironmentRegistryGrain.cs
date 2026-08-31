using Orleans;
using Orleans.Runtime;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Environments;

namespace StreamsForge.Host.Grains;

public sealed class EnvironmentRegistryState
{
    /// <summary>Every environment EXCEPT <c>default</c> — <c>default</c> is synthesised on every read
    /// (see <see cref="EnvironmentRegistryGrain.ListAsync"/>) rather than stored here, so a brand-new
    /// instance and an instance that predates this plan both report it present with zero persisted state.
    /// <see cref="EnvironmentRecord.Name"/> here is always the DISPLAY form — see <c>EnvKeys.Display</c> —
    /// which for every entry in this list is identical to the internal form, because only <c>default</c>
    /// differs between the two and <c>default</c> never appears here.</summary>
    public List<EnvironmentRecord> Environments { get; set; } = [];
}

/// <summary>Plan 021 wave 1, track A — singleton grain (key = <see cref="StreamConstants.EnvironmentsKey"/>)
/// holding the environment directory. See <see cref="IEnvironmentRegistryGrain"/>'s class doc for the
/// facade-inheritance shape and <see cref="EnvKeys"/>'s class doc for why <c>default</c> is the empty
/// string internally and why creation/deletion/rename are as restricted as they are (D7 in plan 021).
///
/// <para><b>Not [Reentrant] and not [MayInterleave]</b> — unlike <see cref="RegistryGrain"/>, nothing here
/// calls back into this same grain from a callee it invoked (the per-environment count in
/// <see cref="ListAsync"/> and the teardown in <see cref="DeleteAsync"/> only ever call INTO an
/// <see cref="IRegistryGrain"/>, never back into this activation), so there is no cycle to allow.</para></summary>
public sealed class EnvironmentRegistryGrain(
    [PersistentState("environments", StreamConstants.StorageName)] IPersistentState<EnvironmentRegistryState> state)
    : Grain, IEnvironmentRegistryGrain
{
    /// <summary>The synthesised <c>default</c> entry — never read from or written to
    /// <see cref="EnvironmentRegistryState"/>. <see cref="EnvironmentRecord.CreatedBy"/> stays empty
    /// (nobody created it) and <see cref="EnvironmentRecord.CreatedAtMs"/> stays 0 (it has no creation
    /// moment this grain knows about).</summary>
    private static EnvironmentRecord SynthesizeDefault() => new()
    {
        Name = EnvKeys.DefaultDisplayName,
        Description = "The environment every pre-plan-021 catalog already lives in.",
        CreatedAtMs = 0,
        CreatedBy = "",
        EntityCount = -1,
    };

    /// <summary><c>default</c> first, then the rest name-ordered — see the interface doc. Fills
    /// <see cref="EnvironmentRecord.EntityCount"/> by asking each environment's own
    /// <see cref="IRegistryGrain"/> for its sources+pipelines+tables count; a registry that has never been
    /// activated answers with empty lists (activating it here, as a side effect of listing, is harmless —
    /// it holds no state to lose), and a registry call that genuinely fails leaves the count at -1 rather
    /// than failing the whole listing for every other environment.</summary>
    public async Task<List<EnvironmentRecord>> ListAsync()
    {
        var result = new List<EnvironmentRecord> { await WithEntityCountAsync(SynthesizeDefault()) };
        foreach (var e in state.State.Environments.OrderBy(e => e.Name, StringComparer.Ordinal))
        {
            result.Add(await WithEntityCountAsync(Clone(e)));
        }
        return result;
    }

    private async Task<EnvironmentRecord> WithEntityCountAsync(EnvironmentRecord rec)
    {
        try
        {
            var registry = RegistryFor(EnvKeys.Normalize(rec.Name));
            var sources = await registry.GetSourcesAsync();
            var pipelines = await registry.GetPipelinesAsync();
            var tables = await registry.GetTablesAsync();
            rec.EntityCount = sources.Count + pipelines.Count + tables.Count;
        }
        catch
        {
            // Best-effort — see the class doc on ListAsync. -1 is "not counted", which is exactly true here.
            rec.EntityCount = -1;
        }
        return rec;
    }

    private static EnvironmentRecord Clone(EnvironmentRecord e) => new()
    {
        Name = e.Name,
        Description = e.Description,
        CreatedAtMs = e.CreatedAtMs,
        CreatedBy = e.CreatedBy,
        EntityCount = e.EntityCount,
    };

    /// <summary>Cheap by construction: no <c>await</c>, no catalog access — exactly what the interface doc
    /// demands for a call made on every request that names an environment.</summary>
    public Task<bool> ExistsAsync(string name)
    {
        var env = EnvKeys.Normalize(name);
        if (env == EnvKeys.Default)
        {
            return Task.FromResult(true);
        }
        return Task.FromResult(state.State.Environments.Any(e => string.Equals(e.Name, env, StringComparison.Ordinal)));
    }

    public async Task<EnvironmentRecord> CreateAsync(string name, string description, string createdBy)
    {
        // EnvKeys.IsValidName already rejects "", "default" and every other Reserved name, so `default`
        // itself can never reach the Add below — its un-creatability falls out of the validator, not a
        // separate check here.
        if (!EnvKeys.IsValidName(name))
        {
            throw new ArgumentException(
                $"'{name}' is not a valid environment name — lower-case letters, digits and hyphens only, " +
                "1-32 characters, and not one of the reserved names (default, catalog, users, access, " +
                "approvals, audit, events, metrics).", nameof(name));
        }

        if (state.State.Environments.Any(e => string.Equals(e.Name, name, StringComparison.Ordinal)))
        {
            throw new InvalidOperationException($"Environment '{name}' already exists.");
        }

        var rec = new EnvironmentRecord
        {
            Name = name,
            Description = description ?? "",
            CreatedAtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            CreatedBy = createdBy ?? "",
            EntityCount = -1,
        };
        state.State.Environments.Add(rec);
        await state.WriteStateAsync();
        return Clone(rec);
    }

    /// <summary>Refuses <c>default</c> outright (always, force or not). Refuses a non-empty environment
    /// without <paramref name="force"/>. With <paramref name="force"/>, deletes every source, pipeline and
    /// table the environment's own <see cref="IRegistryGrain"/> holds — via that grain's OWN
    /// Delete*Async methods (which already stop the running entity and tear down its history/shard
    /// tiers — see <see cref="ICatalogFacade.DeleteTableAsync"/>/<c>DeleteSourceAsync</c>/
    /// <c>DeletePipelineAsync</c>), never by touching another grain's persisted state directly — then
    /// removes the environment record itself.
    ///
    /// <para><b>Tables delete in a fixed-point loop, not a topological sort.</b> A currently-Running table
    /// that another Running table still lists in <c>TableInputs</c> refuses to delete
    /// (<c>RegistryGrain.ThrowIfRunningDependents</c>) — exactly the same guard a manual delete hits. Rather
    /// than duplicating <c>RegistryGrain</c>'s private topological sort here, this repeatedly asks the
    /// registry to delete whatever is left, catching the refusal and retrying next pass, until either
    /// nothing remains or a full pass makes no progress (which, for an acyclic <c>TableInputs</c> graph —
    /// the only kind the platform can construct — only happens once every deletable table is gone).</para>
    ///
    /// <para><b>What force-delete does NOT clean up.</b> (1) The environment's own <see cref="IRegistryGrain"/>
    /// persisted state file is left on disk holding an EMPTY catalog, not physically removed — Orleans grain
    /// storage has no "delete this grain's file" operation this code can reach, only "the grain's own state
    /// object is now empty". Its <c>FieldNumberMaps</c> dictionary entries for the deleted entities are left
    /// behind too, dead weight that a same-named environment created later would inherit (harmless: field
    /// numbers are meant to persist forever anyway — see the repo's hard rule 5). (2) Delete*Async's own
    /// teardown of history/shard-router/shard grains is BEST-EFFORT (each wrapped in its own try/catch, same
    /// as a manual delete) — a teardown call that throws leaves that tier's persisted state orphaned under
    /// the old key, exactly as it would for a single manual delete outside this path. Neither gap is new here;
    /// force-delete inherits both from the exact same Delete*Async calls a human clicking "delete" one entity
    /// at a time would also hit.</para></summary>
    public async Task<bool> DeleteAsync(string name, bool force)
    {
        var env = EnvKeys.Normalize(name);
        if (env == EnvKeys.Default)
        {
            throw new InvalidOperationException("The default environment cannot be deleted.");
        }

        var idx = state.State.Environments.FindIndex(e => string.Equals(e.Name, env, StringComparison.Ordinal));
        if (idx < 0)
        {
            return false;
        }

        var registry = RegistryFor(env);
        var sources = await registry.GetSourcesAsync();
        var pipelines = await registry.GetPipelinesAsync();
        var tables = await registry.GetTablesAsync();
        var nonEmpty = sources.Count > 0 || pipelines.Count > 0 || tables.Count > 0;

        if (nonEmpty && !force)
        {
            throw new InvalidOperationException(
                $"Environment '{env}' is not empty ({sources.Count} source(s), {pipelines.Count} pipeline(s), " +
                $"{tables.Count} table(s)) — pass force=true to delete it and everything in it.");
        }

        if (force && nonEmpty)
        {
            await DeleteTablesFixedPointAsync(registry, tables.Select(t => t.Id).ToList());

            foreach (var p in pipelines)
            {
                try
                {
                    await registry.DeletePipelineAsync(p.Id);
                }
                catch
                {
                    // best-effort — see the class doc's "what force-delete does NOT clean up".
                }
            }

            foreach (var s in sources)
            {
                try
                {
                    await registry.DeleteSourceAsync(s.Name);
                }
                catch
                {
                    // best-effort
                }
            }
        }

        state.State.Environments.RemoveAt(idx);
        await state.WriteStateAsync();
        return true;
    }

    /// <summary>See <see cref="DeleteAsync"/>'s class doc for why this is a fixed-point retry rather than a
    /// topological sort. Any table that still cannot be deleted after a pass that made zero progress is left
    /// in place — that can only happen on a genuinely cyclic <c>TableInputs</c> graph, which nothing in this
    /// platform can construct today, so in practice this always converges to empty.</summary>
    private static async Task DeleteTablesFixedPointAsync(IRegistryGrain registry, List<string> remainingIds)
    {
        var remaining = new List<string>(remainingIds);
        var progress = true;
        while (remaining.Count > 0 && progress)
        {
            progress = false;
            foreach (var id in remaining.ToList())
            {
                try
                {
                    await registry.DeleteTableAsync(id);
                    remaining.Remove(id);
                    progress = true;
                }
                catch (InvalidOperationException)
                {
                    // Still has a running dependent that hasn't been deleted yet — retry next pass.
                }
            }
        }
    }

    private IRegistryGrain RegistryFor(string env) =>
        GrainFactory.GetGrain<IRegistryGrain>(EnvKeys.Qualify(env, StreamConstants.RegistryKey));
}
