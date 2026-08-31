using StreamsForge.Abstractions;
using StreamsForge.AppCore.Environments;
using StreamsForge.Dapr.Host.Actors;

namespace StreamsForge.Dapr.Host.Facades;

/// <summary>
/// Plan 021 (environment isolation), Dapr track. Every <see cref="ICatalogFacade"/> consumer in this
/// project falls into one of two shapes: a request-scoped one that wants "the environment THIS REQUEST
/// selected" (<see cref="EnvironmentAmbient.Current"/> — see <c>DaprFacadesExtensions.AddDaprFacades</c>'s
/// <c>AddTransient</c> registration, which is the only place that reads the ambient to build one of these),
/// and a background sweep that must act on EVERY environment's catalog, never the request-local one that
/// happens to be empty (default) outside a request (plan D5). This factory is what lets the second kind
/// exist at all without becoming a facade-per-environment DI registration explosion: it hands out a
/// <see cref="ICatalogFacade"/> for an EXPLICIT environment name, on demand, as many times as asked.
///
/// <para>Registered as a singleton (<c>DaprFacadesExtensions.AddDaprFacades</c>) — it holds no per-call
/// state itself, only the actor-proxy machinery every <see cref="DaprCatalogFacade"/> instance already
/// used to construct inline before this plan (a <c>RegistryActor</c> proxy at
/// <c>EnvKeys.Qualify(environment, StreamConstants.RegistryKey)</c>), so sharing one factory instance
/// costs nothing.</para>
/// </summary>
public interface ICatalogFacadeFactory
{
    /// <summary><paramref name="environment"/> is the INTERNAL spelling (<see cref="EnvKeys.Default"/> —
    /// the empty string — for the default environment, never the display string <c>"default"</c>). Callers
    /// that hold an <see cref="Abstractions.EnvironmentRecord"/> or a raw header value must
    /// <see cref="EnvKeys.Normalize"/> it first — this method does not normalize, so a stray
    /// <c>"default"</c> literal here would (correctly, if surprisingly) address a NAMED environment
    /// literally called "default", which <see cref="EnvKeys.IsValidName"/> already refuses to let anyone
    /// create, so in practice the mistake surfaces as "that environment doesn't exist" rather than a silent
    /// cross-environment mix-up.</summary>
    ICatalogFacade For(string environment);
}

internal sealed class DaprCatalogFacadeFactory : ICatalogFacadeFactory
{
    public ICatalogFacade For(string environment) => new DaprCatalogFacade(environment);
}
