using StreamForge.Abstractions;

namespace StreamForge.Host.Tests;

/// <summary>Plan 021 wave 2 — <c>StreamHub</c> takes an <see cref="IServiceProvider"/> rather than an
/// <see cref="ICatalogFacade"/>, because a hub is activated OUTSIDE any HTTP request: a
/// constructor-injected facade resolves <c>EnvironmentAmbient.Current</c> at that moment and is therefore
/// bound to the DEFAULT environment for every connection, whatever environment the connection selected.
/// The group names would still be right (they come from the connection's own HttpContext), but the
/// entitlement check would run against the wrong environment's tags. See
/// <c>StreamHub.ReadCatalogAsync</c>'s doc comment for the whole argument.
///
/// <para>This hands back one catalog whatever environment is current, which is exactly what the hub tests
/// want: none of them exercises more than one environment's catalog, and the ones that care about the
/// environment assert on the GROUP NAME, which does not come through here.</para></summary>
internal sealed class SingleCatalogServiceProvider(ICatalogFacade catalog) : IServiceProvider
{
    public object? GetService(Type serviceType) =>
        serviceType == typeof(ICatalogFacade) ? catalog : null;
}
