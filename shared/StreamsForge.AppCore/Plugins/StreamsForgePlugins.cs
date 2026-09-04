using System.Reflection;
using System.Runtime.Loader;

namespace StreamsForge.AppCore.Plugins;

/// <summary>
/// What an out-of-tree connector assembly implements to install itself: one parameterless class whose
/// <see cref="Register"/> calls <c>InboundTransports.Register</c> / <c>PolledTransports.Register</c> /
/// <c>SinkTransports.Register</c> (and/or <c>DuplexTransports</c>) for the kinds it brings.
///
/// <para>Registration happens once, at host startup, before anything can start a source — which is the
/// contract those registries already state. Implementations must not open connections or read a database
/// here; <c>Open()</c> on the transport is where a connection belongs.</para>
/// </summary>
public interface IStreamsForgePlugin
{
    /// <summary>Human-readable name for the startup log line. Not an identity — kinds are the identity,
    /// and the registries reject a duplicate one.</summary>
    string Name { get; }

    /// <summary>Registers this plugin's transports. Throwing here fails only THIS plugin (the loader
    /// reports it and continues), so a broken plugin cannot keep the host from starting.</summary>
    void Register();
}

/// <summary>One plugin instance the loader activated, with the assembly it came from — the assembly is
/// what a runtime needs to add the plugin's own types (Orleans grains, serializers) to its manifest.</summary>
public sealed record LoadedPlugin(IStreamsForgePlugin Plugin, Assembly Assembly);

/// <summary>One console UI module a plugin assembly carries as an embedded resource — the DLL-plus-loose-
/// file install becomes one file. <see cref="ResourceName"/> is the manifest resource name
/// (<c>ui-plugins/&lt;FileName&gt;</c>); <see cref="Assembly"/> is where to read it back from with
/// <see cref="Assembly.GetManifestResourceStream(string)"/>. <see cref="FileName"/>'s extension is one of
/// <see cref="StreamsForgePlugins.UiModuleExtensions"/>.</summary>
public sealed record UiModule(string FileName, Assembly Assembly, string ResourceName);

/// <summary>
/// Loads <see cref="IStreamsForgePlugin"/> implementations out of a directory of assemblies, so a
/// connector that cannot live in this repo installs by being copied next to the host rather than by being
/// referenced from it. The console-side counterpart is the <c>ui-plugins/</c> directory (one ES module or
/// TypeScript file per specialized editor) — a full out-of-tree kind is normally one DLL here and one
/// module there, and the two are independent: a server plugin with no UI module gets the generic
/// descriptor-driven form.
///
/// <para><b>Why a directory scan when the registries deliberately are not DI discovery.</b> Their doc
/// comments reject assembly SCANNING FOR TRANSPORT TYPES — inferring registration from the type graph, so
/// that what runs depends on what happens to be linked. This is the opposite: the plugin declares itself
/// with an explicit <see cref="IStreamsForgePlugin.Register"/> call, and the only thing discovered is which
/// FILES an operator put in the directory. What gets registered still reads as one explicit call.</para>
///
/// <para><b>Default load context, deliberately.</b> Plugins are loaded into the host's own context so they
/// share its <c>StreamsForge.AppCore</c>/<c>StreamsForge.Contracts</c> types — a transport in an isolated
/// context would implement a DIFFERENT <c>IInboundTransport</c> and could not register at all. The cost is
/// the usual plugin ceiling: a plugin's dependency versions are not isolated from the host's, and on a
/// conflict the host's copy wins — <see cref="DescribeVersionConflicts"/> is the diagnostic for the one
/// direction that actually bites (a plugin built against a NEWER version than the host is running).</para>
///
/// <para><b>Two passes, deliberately.</b> Pass 1 loads every <c>*.dll</c> in the directory with
/// <see cref="AssemblyLoadContext.LoadFromAssemblyPath"/> — a plugin's own dependency DLLs sit in the same
/// directory and must all be resident before pass 2 reflects over any of them, or a plugin whose assembly
/// happens to sort before its dependency (ordinal order) would fail to resolve a type it needs. Pass 2 then
/// scans only the assemblies that actually loaded, so one native or corrupt DLL is reported once (as
/// "skipped, not a loadable managed assembly") and does not also show up as a scan failure.</para>
/// </summary>
public static class StreamsForgePlugins
{
    /// <summary>Where a host looks when nothing is configured: <c>plugins/</c> next to its binaries. Same
    /// convention as the console's <c>ui-plugins/</c> — a NuGet package that copies content to the output
    /// directory installs itself.</summary>
    public static string DefaultDirectory => Path.Combine(AppContext.BaseDirectory, "plugins");

    /// <summary>Console UI module file extensions the loader/endpoint recognize: ES modules (<c>.js</c>/
    /// <c>.mjs</c>) served to the browser verbatim, or TypeScript (<c>.ts</c>/<c>.tsx</c>) served as plain
    /// text and transpiled client-side. Convention: <c>&lt;EmbeddedResource Include="ui-plugins/*"
    /// LogicalName="ui-plugins/%(Filename)%(Extension)" /&gt;</c> in the plugin's csproj — the wildcard
    /// covers any of these extensions (and anything else dropped in the directory is simply ignored)
    /// without the csproj needing to list them one by one.</summary>
    public static readonly string[] UiModuleExtensions = [".js", ".mjs", ".ts", ".tsx"];

    /// <summary>Loads and registers every plugin in <paramref name="directory"/>, returning one line per
    /// outcome for the caller to log — this assembly takes no logging dependency, and a host that
    /// swallowed these lines would leave "my kind is missing" undiagnosable.
    ///
    /// <para>A missing directory is not an error (the common case: no plugins installed). An assembly that
    /// fails to load, a plugin whose constructor throws, and a <see cref="IStreamsForgePlugin.Register"/>
    /// that throws are all reported and skipped — including the duplicate-kind
    /// <see cref="InvalidOperationException"/> the registries raise, which is exactly how a plugin shipped
    /// twice (two versions of the same DLL in the directory) surfaces.</para></summary>
    public static IReadOnlyList<string> LoadFrom(string? directory = null)
    {
        var dir = string.IsNullOrWhiteSpace(directory) ? DefaultDirectory : Path.GetFullPath(directory);
        if (!Directory.Exists(dir))
        {
            return [];
        }

        var report = new List<string>();

        // Pass 1: get every assembly file resident in the default load context first, independent of
        // whether it turns out to carry a plugin type — see the class doc for why this has to be a
        // separate pass from the scan below.
        var assemblies = new List<Assembly>();
        foreach (var file in Directory.EnumerateFiles(dir, "*.dll").Order(StringComparer.Ordinal))
        {
            try
            {
                assemblies.Add(AssemblyLoadContext.Default.LoadFromAssemblyPath(file));
            }
            catch (Exception ex)
            {
                // BadImageFormat (a native DLL a plugin shipped alongside itself), a missing dependency —
                // neither is this host's problem to fix, and neither is worth refusing to start over.
                report.Add($"plugin assembly '{Path.GetFileName(file)}' skipped (not a loadable managed assembly): {ex.Message}");
            }
        }

        // Pass 2: reflect over the assemblies that actually loaded, looking for plugin types.
        foreach (var assembly in assemblies)
        {
            try
            {
                report.AddRange(ScanAssembly(assembly));
            }
            catch (Exception ex)
            {
                report.Add($"plugin assembly '{AssemblyDisplayName(assembly)}' could not be scanned for plugins: {Describe(ex)}");
            }
        }

        return report;
    }

    private static string AssemblyDisplayName(Assembly assembly) =>
        string.IsNullOrEmpty(assembly.Location) ? assembly.GetName().Name ?? "?" : Path.GetFileName(assembly.Location);

    /// <summary>A <see cref="ReflectionTypeLoadException"/> carries one loader exception per type that
    /// failed to reflect over — often dozens for one bad assembly, nearly all duplicates of the same root
    /// cause. Up to 3 distinct messages is enough for an operator to act on without flooding the log.</summary>
    private static string Describe(Exception ex) =>
        ex is ReflectionTypeLoadException { LoaderExceptions.Length: > 0 } rtle
            ? string.Join("; ", rtle.LoaderExceptions
                .Where(e => e is not null)
                .Select(e => e!.Message)
                .Distinct(StringComparer.Ordinal)
                .Take(3))
            : ex.Message;

    private static List<string> ScanAssembly(Assembly assembly)
    {
        var report = new List<string>();
        string? lastRegisteredName = null;

        // Not finding a plugin type is silence on purpose: the directory holds a plugin's DEPENDENCIES
        // too, and reporting each of them as "no plugin here" would bury the lines that matter.
        foreach (var type in assembly.GetExportedTypes()
                     .Where(t => typeof(IStreamsForgePlugin).IsAssignableFrom(t) && t is { IsAbstract: false, IsInterface: false })
                     .OrderBy(t => t.FullName, StringComparer.Ordinal))
        {
            var (line, registeredName) = Activate(type);
            report.Add(line);
            if (registeredName is not null)
            {
                lastRegisteredName = registeredName;
                report.AddRange(DescribeVersionConflicts(
                    registeredName,
                    assembly.GetReferencedAssemblies(),
                    AssemblyLoadContext.Default.Assemblies.Select(a => a.GetName())));
            }
        }

        // Embedded ui-plugins/* resources count only from an assembly that registered at least one plugin
        // here — the directory also holds a plugin's plain dependency DLLs, and scanning every one of
        // THOSE for a same-shaped resource would attribute a UI module to an assembly that never opted
        // into being a StreamsForge plugin at all.
        if (lastRegisteredName is not null)
        {
            report.AddRange(ScanUiModules(assembly, lastRegisteredName));
        }

        return report;
    }

    private static readonly List<LoadedPlugin> _loaded = [];
    private static readonly List<UiModule> _uiModules = [];

    /// <summary>Every plugin <see cref="LoadFrom"/> activated so far, in load order. Hosts read this after
    /// loading to run the plugin's optional hooks (<c>IStreamsForgeWebPlugin</c> in StreamsForge.Api) and
    /// to register plugin assemblies with the runtime.</summary>
    public static IReadOnlyList<LoadedPlugin> Loaded => _loaded;

    /// <summary>Every console UI module found embedded in a plugin assembly so far — see
    /// <see cref="UiModule"/>. <c>UiPluginsEndpoints</c> unions these with the on-disk <c>ui-plugins/</c>
    /// directory, disk winning on a same-named file.</summary>
    public static IReadOnlyList<UiModule> UiModules => _uiModules;

    private static (string Line, string? RegisteredName) Activate(Type type)
    {
        try
        {
            var plugin = (IStreamsForgePlugin)Activator.CreateInstance(type)!;
            plugin.Register();
            _loaded.Add(new LoadedPlugin(plugin, type.Assembly));
            return ($"plugin '{plugin.Name}' ({type.FullName}) registered", plugin.Name);
        }
        catch (Exception ex)
        {
            // TargetInvocationException wraps whatever the constructor actually threw; the inner message is
            // the one an operator can act on ("an inbound transport for kind 'orion' is already
            // registered"), so unwrap it rather than reporting the wrapper.
            var cause = ex is TargetInvocationException { InnerException: { } inner } ? inner : ex;
            return ($"plugin '{type.FullName}' failed to register: {cause.Message}", null);
        }
    }

    /// <summary>The default load context is the one place both the host and every plugin's own
    /// dependencies live, so a plugin whose csproj references a NEWER version of something the host
    /// already ships (e.g. Newtonsoft.Json) silently gets the host's OLDER copy at runtime — assembly
    /// resolution in one context always keeps whatever loaded first. That mismatch is invisible until the
    /// plugin calls a member the older copy doesn't have, which surfaces as a
    /// <see cref="TypeInitializationException"/> or <see cref="MissingMethodException"/> far from its real
    /// cause. This is pure and side-effect free so it can be unit-tested without loading an assembly at
    /// all.
    ///
    /// <para>Only the "plugin expects newer than the host has" direction is worth a line: the opposite (a
    /// plugin built against an older version than the host) resolves to something with at least as much as
    /// the plugin asked for, which is the ordinary, harmless case.</para></summary>
    public static IEnumerable<string> DescribeVersionConflicts(
        string pluginName,
        IEnumerable<AssemblyName> referenced,
        IEnumerable<AssemblyName> loaded)
    {
        var loadedVersions = new Dictionary<string, Version?>(StringComparer.OrdinalIgnoreCase);
        foreach (var name in loaded)
        {
            if (name.Name is not null)
            {
                loadedVersions.TryAdd(name.Name, name.Version); // first wins
            }
        }

        return referenced
            .Where(r => r.Name is not null
                        && r.Version is not null
                        && loadedVersions.TryGetValue(r.Name, out var lv)
                        && lv is not null
                        && lv < r.Version)
            .OrderBy(r => r.Name, StringComparer.Ordinal)
            .Select(r => $"plugin '{pluginName}' references {r.Name} {r.Version} but the host has {loadedVersions[r.Name!]} loaded — the host copy wins; a TypeInitializationException at first use means this");
    }

    /// <summary>Convention: <c>&lt;EmbeddedResource Include="ui-plugins/*" LogicalName="ui-plugins/%(Filename)%(Extension)" /&gt;</c>
    /// in the plugin's csproj — no member on <see cref="IStreamsForgePlugin"/> needed, since the resource is
    /// discoverable from the assembly alone. Only files whose extension is in
    /// <see cref="UiModuleExtensions"/> count; anything else under <c>ui-plugins/</c> in the assembly is
    /// ignored.</summary>
    private static List<string> ScanUiModules(Assembly assembly, string pluginName)
    {
        var report = new List<string>();
        foreach (var resourceName in assembly.GetManifestResourceNames().Order(StringComparer.Ordinal))
        {
            if (!resourceName.StartsWith("ui-plugins/", StringComparison.Ordinal))
            {
                continue;
            }

            var ext = Path.GetExtension(resourceName);
            if (!UiModuleExtensions.Contains(ext, StringComparer.OrdinalIgnoreCase))
            {
                continue;
            }

            var fileName = resourceName["ui-plugins/".Length..];
            _uiModules.Add(new UiModule(fileName, assembly, resourceName));
            report.Add($"plugin '{pluginName}' provides ui module '{fileName}'");
        }

        return report;
    }
}
