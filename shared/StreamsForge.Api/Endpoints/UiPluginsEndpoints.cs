using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using StreamsForge.AppCore.Plugins;

namespace StreamsForge.Api;

/// <summary>
/// Console UI plugins: a library that adds a source/sink kind can ship a specialized editor for it as one
/// ES module, and the console loads it at boot (see <c>web/src/plugins/registry.tsx</c> for the contract).
///
/// <para>The directory is <c>ui-plugins/</c> next to the host's binaries by default, overridable with
/// <c>Ui:PluginsPath</c>. That default is the whole point: a connector package that declares its
/// <c>ui-plugins/mykind.js</c> as content copied to the output directory is installed by a
/// <c>PackageReference</c> alone — nothing in StreamsForge is edited, rebuilt or configured.</para>
///
/// <para>A module can also travel INSIDE a server plugin DLL, as an embedded <c>ui-plugins/*.js</c>
/// resource (<see cref="StreamsForgePlugins.UiModules"/>) — one file instead of a DLL plus a loose
/// <c>.js</c>. The list below is the UNION of the directory and the embedded modules; a same-named file on
/// disk wins, so an operator can override a bundled module without rebuilding the plugin. The directory is
/// rescanned on every request (nothing here is cached); the embedded set is fixed for the process's
/// lifetime — it is populated once, at plugin load, and nothing short of a restart changes which plugin
/// DLLs are loaded.</para>
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

        app.MapGet("/api/ui-plugins", () => Results.Ok(ModuleNames(dir).Select(f => $"/api/ui-plugins/{f}")))
            .AllowAnonymous();

        app.MapGet("/api/ui-plugins/{file}", (string file) =>
        {
            // Name only, no traversal: the requested name must match a file the disk listing above would
            // have returned. Comparing resolved paths rather than filtering the string is what makes
            // "../" and an absolute path both simply not match.
            var resolved = Path.GetFullPath(Path.Combine(dir, file));
            if (PluginFiles(dir).Contains(resolved))
            {
                return Results.File(resolved, "text/javascript");
            }

            // Disk lost (or never had it) — fall back to an embedded module of the same name. Exact
            // filename match only, same as the disk path: no traversal surface to speak of since a
            // resource name isn't a path.
            var module = StreamsForgePlugins.UiModules.FirstOrDefault(m => m.FileName == file);
            var stream = module is not null ? module.Assembly.GetManifestResourceStream(module.ResourceName) : null;
            return stream is not null ? Results.File(stream, "text/javascript") : Results.NotFound();
        }).AllowAnonymous();
    }

    private static IEnumerable<string> ModuleNames(string dir) =>
        PluginFiles(dir).Select(f => Path.GetFileName(f)!)
            .Concat(StreamsForgePlugins.UiModules.Select(m => m.FileName))
            .Distinct(StringComparer.Ordinal)
            .Order(StringComparer.Ordinal);

    private static string[] PluginFiles(string dir) =>
        Directory.Exists(dir)
            ? Directory.EnumerateFiles(dir)
                .Where(f => Path.GetExtension(f) is ".js" or ".mjs")
                .Select(Path.GetFullPath)
                .Order(StringComparer.Ordinal)
                .ToArray()
            : [];
}
