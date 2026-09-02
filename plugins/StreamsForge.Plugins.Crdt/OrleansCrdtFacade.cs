using Orleans;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Environments;
using StreamsForge.Connectors.Crdt;

namespace StreamsForge.Host.Facades;

/// <summary>Plan 020 wave B-2: Orleans-side <see cref="ICrdtFacade"/> — the CRDT counterpart of
/// <c>OrleansConnectorStatusFacade</c> (StreamsForge.Host's own <c>OrleansFacades.cs</c>), same "resolve,
/// check kind, forward to the grain" shape. <see cref="ICrdtFacade.MergeAsync"/>/
/// <see cref="ICrdtFacade.GetStatusAsync"/>'s own doc comments say null means "no source of that name
/// exists or it is not crdt-kind" — checked HERE, once, so <see cref="CrdtDocGrain"/> itself never has to
/// (D5: it trusts the def stamped on it at <c>StartAsync</c>, not a fresh registry read).
///
/// <para><b>Moved here from StreamsForge.Host verbatim</b> (the CRDT-as-a-plugin work): the one behavior
/// change is <see cref="ResolveCrdtSourceAsync"/>, which used to call the Host's own internal
/// <c>IClusterClient.RegistryFor</c> extension (<c>RegistryGrainKeys.cs</c>) — an <c>internal</c> helper
/// this plugin project cannot see, since it does not (and must not) reference the Host assembly. Inlined
/// to the one line that extension was: <c>GetGrain&lt;IRegistryGrain&gt;(EnvKeys.Qualify(env,
/// StreamConstants.RegistryKey))</c>, byte-identical to what the extension did.</para></summary>
internal sealed class OrleansCrdtFacade(IClusterClient client) : ICrdtFacade
{
    public bool Enabled => true;

    public async Task<CrdtMergeResult?> MergeAsync(string sourceName, IReadOnlyList<byte[]> updates)
    {
        var def = await ResolveCrdtSourceAsync(sourceName);
        if (def is null)
        {
            return null;
        }

        return await client.GetGrain<ICrdtDocGrain>(EnvKeys.Qualify(EnvironmentAmbient.Current, sourceName)).MergeAsync(updates);
    }

    /// <summary>Plan 020 wave D, finding 3 — same "resolve, check kind, forward" shape as
    /// <see cref="MergeAsync"/>, forwarding to <see cref="ICrdtDocGrain.MergeAttributedAsync"/> instead.</summary>
    public async Task<CrdtMergeResult?> MergeAttributedAsync(string sourceName, IReadOnlyList<byte[]> updates, string actor)
    {
        var def = await ResolveCrdtSourceAsync(sourceName);
        if (def is null)
        {
            return null;
        }

        return await client.GetGrain<ICrdtDocGrain>(EnvKeys.Qualify(EnvironmentAmbient.Current, sourceName)).MergeAttributedAsync(updates, actor);
    }

    public async Task<CrdtDocStatus?> GetStatusAsync(string sourceName)
    {
        var def = await ResolveCrdtSourceAsync(sourceName);
        if (def is null)
        {
            return null;
        }

        return await client.GetGrain<ICrdtDocGrain>(EnvKeys.Qualify(EnvironmentAmbient.Current, sourceName)).GetStatusAsync();
    }

    public async Task<CrdtMergeResult?> ReplayAsync(string sourceName)
    {
        var def = await ResolveCrdtSourceAsync(sourceName);
        if (def is null)
        {
            return null;
        }

        return await client.GetGrain<ICrdtDocGrain>(EnvKeys.Qualify(EnvironmentAmbient.Current, sourceName)).ReplayAsync();
    }

    // Plan 020 wave D: pure delegation. This plugin already references StreamsForge.Connectors.Crdt (the
    // grain projects documents with it), so the decode costs nothing new HERE — the point of routing it
    // through the facade is that StreamsForge.Api does not have to.
    public CrdtUpdateInspection Inspect(SourceDefinition source, byte[] update) =>
        CrdtUpdateInspector.Inspect(update, source.Connector?.Crdt ?? new CrdtSourceConfig());

    /// <summary>Plan 020 wave F — same "resolve, check kind, forward" shape as <see cref="MergeAsync"/>,
    /// forwarding to <see cref="ICrdtDocGrain.RebalanceAsync"/> instead.</summary>
    public async Task<EscrowRebalanceResult?> RebalanceAsync(string sourceName, string from, string to, long amount)
    {
        var def = await ResolveCrdtSourceAsync(sourceName);
        if (def is null)
        {
            return null;
        }

        return await client.GetGrain<ICrdtDocGrain>(EnvKeys.Qualify(EnvironmentAmbient.Current, sourceName)).RebalanceAsync(from, to, amount);
    }

    private async Task<SourceDefinition?> ResolveCrdtSourceAsync(string sourceName)
    {
        // Plan 021 D4 — a facade answering one request reads the ambient. Inlined RegistryFor (see this
        // class's own doc comment for why).
        var registry = client.GetGrain<IRegistryGrain>(EnvKeys.Qualify(EnvironmentAmbient.Current, StreamConstants.RegistryKey));
        var def = await registry.GetSourceAsync(sourceName);
        return def is null || def.Kind != SourceKinds.Crdt ? null : def;
    }
}
