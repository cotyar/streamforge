using Microsoft.AspNetCore.Builder;

namespace StreamForge.Api;

/// <summary>
/// Plan 015: REST over the access policy — roles, groups, per-user entitlements, approval templates.
/// The routes themselves land in wave 2; this file exists from wave 1's commit so that the map site in
/// <see cref="StreamForgeApiExtensions"/> and the endpoints can be written by two different agents at
/// the same time without either waiting on the other to compile.
/// </summary>
public static class AccessEndpoints
{
    public static void MapAccessEndpoints(this WebApplication app)
    {
        // Intentionally empty until wave 2-C. An empty map call adds no routes, so the endpoint-metadata
        // coverage test sees exactly what it saw before.
    }
}
