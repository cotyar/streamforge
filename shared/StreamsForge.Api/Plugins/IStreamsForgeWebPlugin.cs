using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using StreamsForge.AppCore.Plugins;

namespace StreamsForge.Api.Plugins;

/// <summary>
/// The optional second tier of a server plugin. <see cref="IStreamsForgePlugin.Register"/> is enough for a
/// plugin that only adds transports or SQL functions (process-wide registries, no host involvement).
/// Implement THIS interface only when the plugin needs the host itself: a DI service to replace a
/// facade the core registers as disabled, or HTTP endpoints of its own. Both hooks are default no-ops,
/// so a plugin overrides the one it needs. Lives in StreamsForge.Api (not AppCore) because the hooks
/// take ASP.NET types, and AppCore deliberately has no ASP.NET dependency. Hook order per host:
/// <c>Register()</c> (at load) → <c>ConfigureServices</c> (before Build) → <c>MapEndpoints</c> (after
/// the core API is mapped, so a plugin route can never shadow a core one).
/// </summary>
public interface IStreamsForgeWebPlugin : IStreamsForgePlugin
{
    /// <summary>Runs before the host builds its container. Registering a service the core already
    /// registered replaces it for single-instance resolution (last registration wins).</summary>
    void ConfigureServices(IServiceCollection services, IConfiguration configuration)
    {
    }

    /// <summary>Runs after the core API is mapped. Routes should live under <c>/api/plugins/{name}/…</c>
    /// unless they deliberately implement a core-documented route.</summary>
    void MapEndpoints(IEndpointRouteBuilder endpoints)
    {
    }
}

/// <summary>Host-side driver of the hooks above — both Program.cs files call these two, in this order.</summary>
public static class StreamsForgePluginHosting
{
    public static void ConfigureServices(IServiceCollection services, IConfiguration configuration)
    {
        foreach (var loaded in StreamsForgePlugins.Loaded)
        {
            if (loaded.Plugin is IStreamsForgeWebPlugin web)
            {
                web.ConfigureServices(services, configuration);
            }
        }
    }

    public static void MapPluginEndpoints(this WebApplication app)
    {
        foreach (var loaded in StreamsForgePlugins.Loaded)
        {
            if (loaded.Plugin is IStreamsForgeWebPlugin web)
            {
                web.MapEndpoints(app);
            }
        }
    }
}
