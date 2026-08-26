using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;

namespace StreamForge.Api;

/// <summary>
/// Console UI plugins: a library that adds a source/sink kind can ship a specialized editor for it as one
/// ES module, and the console loads it at boot (see <c>web/src/plugins/registry.tsx</c> for the contract).
///
/// <para>The directory is <c>ui-plugins/</c> next to the host's binaries by default, overridable with
/// <c>Ui:PluginsPath</c>. That default is the whole point: a connector package that declares its
/// <c>ui-plugins/mykind.js</c> as content copied to the output directory is installed by a
/// <c>PackageReference</c> alone — nothing in StreamForge is edited, rebuilt or configured.</para>
///
/// <para>Anonymous, like <c>GET /api/meta/instance</c>: the SPA loads plugins before anyone has logged in,
/// and this serves the console's own front-end assets — filenames and browser code, never catalog data.
/// It follows that a plugin file is world-readable to anything that can reach the console; put nothing in
/// one that isn't already shipped to every browser.</para>
/// </summary>
public static class UiPluginsEndpoints
{
    public static void MapUiPluginsEndpoints(this WebApplication app)
    {
        var dir = app.Configuration["Ui:PluginsPath"] is { Length: > 0 } configured
            ? Path.GetFullPath(configured)
            : Path.Combine(AppContext.BaseDirectory, "ui-plugins");

        app.MapGet("/api/ui-plugins", () => Results.Ok(PluginFiles(dir).Select(f => $"/api/ui-plugins/{Path.GetFileName(f)}")))
            .AllowAnonymous();

        app.MapGet("/api/ui-plugins/{file}", (string file) =>
        {
            // Name only, no traversal: the requested name must match a file the listing above would have
            // returned. Comparing resolved paths rather than filtering the string is what makes "../"
            // and an absolute path both simply not match.
            var resolved = Path.GetFullPath(Path.Combine(dir, file));
            return PluginFiles(dir).Contains(resolved)
                ? Results.File(resolved, "text/javascript")
                : Results.NotFound();
        }).AllowAnonymous();
    }

    private static string[] PluginFiles(string dir) =>
        Directory.Exists(dir)
            ? Directory.EnumerateFiles(dir)
                .Where(f => Path.GetExtension(f) is ".js" or ".mjs")
                .Select(Path.GetFullPath)
                .Order(StringComparer.Ordinal)
                .ToArray()
            : [];
}
