using Microsoft.AspNetCore.Builder;

namespace StreamForge.Api;

/// <summary>Config export/import endpoints (plan 006 W3C — decisions D-I/D-J).
/// GET /api/config/export (Viewer) and POST /api/config/import (Editor; replace mode Admin).</summary>
public static class ConfigEndpoints
{
    public static void MapConfigEndpoints(this WebApplication app)
    {
        // ponytail: stub — wave 3C fills this in; mounted early so the shared mount list stays
        // orchestrator-owned.
    }
}
