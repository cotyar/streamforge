using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using StreamsForge.AppCore.Plugins;

namespace StreamsForge.Api;

/// <summary>
/// Console UI plugins: a library that adds a source/sink kind can ship a specialized editor for it as one
/// ES module or TypeScript file, and the console loads it at boot (see
/// <c>web/src/plugins/registry.tsx</c> for the contract).
///
/// <para>The directory is <c>ui-plugins/</c> next to the host's binaries by default, overridable with
/// <c>Ui:PluginsPath</c>. That default is the whole point: a connector package that declares its
/// <c>ui-plugins/mykind.tsx</c> as content copied to the output directory is installed by a
/// <c>PackageReference</c> alone — nothing in StreamsForge is edited, rebuilt or configured.</para>
///
/// <para>A module can also travel INSIDE a server plugin DLL, as an embedded <c>ui-plugins/*</c> resource
/// (<see cref="StreamsForgePlugins.UiModules"/>) — one file instead of a DLL plus a loose file. The list
/// below is the UNION of the directory and the embedded modules; a same-named file on disk wins, so an
/// operator can override a bundled module without rebuilding the plugin. The directory is rescanned on
/// every request (nothing here is cached); the embedded set is fixed for the process's lifetime — it is
/// populated once, at plugin load, and nothing short of a restart changes which plugin DLLs are
/// loaded.</para>
///
/// <para><b>Cache-busting.</b> The listing response carries <c>Cache-Control: no-store</c> — the console
/// fetches it with <c>{cache: 'no-store'}</c> so it never sees a stale module list — and each listed URL
/// carries its own <c>?v=&lt;version&gt;</c>: the file's <c>LastWriteTimeUtc</c> ticks for a module on
/// disk, the owning assembly's module-version GUID for an embedded one. Browsers cache a dynamic
/// <c>import()</c> by URL, so without a versioned URL a plain reload after editing a module would keep
/// importing the OLD cached module; the query string is not a route parameter (the file route below
/// ignores it entirely), so it exists purely to change the URL, not to select a version server-side.</para>
///
/// <para><b>TypeScript modules.</b> A <c>.ts</c>/<c>.tsx</c> file is served <c>text/plain; charset=utf-8</c>
/// — plain source, verbatim — rather than as JavaScript; the console fetches it as text and transpiles it
/// client-side (a lazy <c>sucrase</c> import) before importing the result. A <c>.js</c>/<c>.mjs</c> module
/// keeps the original <c>text/javascript</c> content type and is imported directly, no transpile step.</para>
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

        app.MapGet("/api/ui-plugins", (HttpContext ctx) =>
        {
            ctx.Response.Headers.CacheControl = "no-store";
            return Results.Ok(Modules(dir).Select(m => $"/api/ui-plugins/{m.Name}?v={m.Version}"));
        }).AllowAnonymous();

        app.MapGet("/api/ui-plugins/{file}", (string file) =>
        {
            // Name only, no traversal: the requested name must match a file the disk listing above would
            // have returned. Comparing resolved paths rather than filtering the string is what makes
            // "../" and an absolute path both simply not match. The route has no {v} segment — the query
            // string a listed URL carries is not consulted here at all, only the file name is.
            var resolved = Path.GetFullPath(Path.Combine(dir, file));
            if (PluginFiles(dir).Contains(resolved))
            {
                return Results.File(resolved, ContentTypeOf(resolved));
            }

            // Disk lost (or never had it) — fall back to an embedded module of the same name. Exact
            // filename match only, same as the disk path: no traversal surface to speak of since a
            // resource name isn't a path.
            var module = StreamsForgePlugins.UiModules.FirstOrDefault(m => m.FileName == file);
            var stream = module is not null ? module.Assembly.GetManifestResourceStream(module.ResourceName) : null;
            return stream is not null ? Results.File(stream, ContentTypeOf(file)) : Results.NotFound();
        }).AllowAnonymous();
    }

    /// <summary>Every module name paired with its cache-busting version, disk first (so
    /// <see cref="Enumerable.DistinctBy{TSource,TKey}(IEnumerable{TSource},Func{TSource,TKey})"/> keeps the
    /// disk entry when the same name exists both on disk and embedded), ordered ordinal by name.</summary>
    private static IEnumerable<(string Name, string Version)> Modules(string dir) =>
        PluginFiles(dir)
            .Select(f => (Name: Path.GetFileName(f)!, Version: File.GetLastWriteTimeUtc(f).Ticks.ToString()))
            .Concat(StreamsForgePlugins.UiModules.Select(m =>
                (Name: m.FileName, Version: m.Assembly.ManifestModule.ModuleVersionId.ToString("N"))))
            .DistinctBy(m => m.Name)
            .OrderBy(m => m.Name, StringComparer.Ordinal);

    private static string[] PluginFiles(string dir) =>
        Directory.Exists(dir)
            ? Directory.EnumerateFiles(dir)
                .Where(f => StreamsForgePlugins.UiModuleExtensions.Contains(Path.GetExtension(f), StringComparer.OrdinalIgnoreCase))
                .Select(Path.GetFullPath)
                .Order(StringComparer.Ordinal)
                .ToArray()
            : [];

    private static string ContentTypeOf(string file)
    {
        var ext = Path.GetExtension(file);
        return ext.Equals(".ts", StringComparison.OrdinalIgnoreCase) || ext.Equals(".tsx", StringComparison.OrdinalIgnoreCase)
            ? "text/plain; charset=utf-8"
            : "text/javascript";
    }
}
