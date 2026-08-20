using Orleans;
using StreamForge.Abstractions;
using StreamForge.AppCore.Environments;

namespace StreamForge.Host.Facades;

/// <summary>Plan 021 D1/D3 — the ONE place <c>StreamForge.Host</c> turns "which environment" into "which
/// <see cref="IRegistryGrain"/>". Every one of this repo's
/// <c>GetGrain&lt;IRegistryGrain&gt;(StreamConstants.RegistryKey)</c> call sites becomes a call to
/// <see cref="RegistryFor(IClusterClient, string)"/> or <see cref="RegistryFor(IGrainFactory, string)"/>
/// instead, so there is exactly one place that composes the qualified key
/// (<see cref="EnvKeys.Qualify"/>) — the same discipline <see cref="EnvKeys"/>'s own class doc asks every
/// OTHER runtime key in this codebase to follow.
///
/// <para><b>Which environment a caller passes is the decision that matters and is made AT THE CALL SITE,
/// not here</b> — see this wave's own report for the full breakdown, but in short: a facade/gRPC service
/// answering one request passes <see cref="EnvironmentAmbient.Current"/>; a grain acting on its own
/// already-loaded <c>TableDefinition</c>/<c>PipelineDefinition</c> passes that definition's
/// <c>Environment</c> field (plan 021 D5 — the runtime never reads the ambient, it reads the definition);
/// a background service that enumerates every environment passes each one in turn.</para></summary>
internal static class RegistryGrainKeys
{
    public static IRegistryGrain RegistryFor(this IClusterClient client, string env) =>
        client.GetGrain<IRegistryGrain>(EnvKeys.Qualify(env, StreamConstants.RegistryKey));

    public static IRegistryGrain RegistryFor(this IGrainFactory factory, string env) =>
        factory.GetGrain<IRegistryGrain>(EnvKeys.Qualify(env, StreamConstants.RegistryKey));
}
