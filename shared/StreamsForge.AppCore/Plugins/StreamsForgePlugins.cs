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
/// <c>.js</c> install becomes one file. <see cref="ResourceName"/> is the manifest resource name
/// (<c>ui-plugins/&lt;FileName&gt;</c>); <see cref="Assembly"/> is where to read it back from with
/// <see cref="Assembly.GetManifestResourceStream(string)"/>.</summary>
public sealed record UiModule(string FileName, Assembly Assembly, string ResourceName);

/// <summary>
/// Loads <see cref="IStreamsForgePlugin"/> implementations out of a directory of assemblies, so a
/// connector that cannot live in this repo installs by being copied next to the host rather than by being
/// referenced from it. The console-side counterpart is the <c>ui-plugins/</c> directory (one ES module per
/// specialized editor) — a full out-of-tree kind is normally one DLL here and one module there, and the
/// two are independent: a server plugin with no UI module gets the generic descriptor-driven form.
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
/// conflict the host's copy wins.</para>
/// </summary>
public static class StreamsForgePlugins
{
    /// <summary>Where a host looks when nothing is configured: <c>plugins/</c> next to its binaries. Same
    /// convention as the console's <c>ui-plugins/</c> — a NuGet package that copies content to the output
    /// directory installs itself.</summary>
    public static string DefaultDirectory => Path.Combine(AppContext.BaseDirectory, "plugins");

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
        foreach (var file in Directory.EnumerateFiles(dir, "*.dll").Order(StringComparer.Ordinal))
        {
            try
            {
                report.AddRange(LoadAssembly(file));
            }
            catch (Exception ex)
            {
                // BadImageFormat (a native DLL a plugin shipped alongside itself), a missing dependency, a
                // type that cannot be reflected over — none of them are this host's problem to fix, and
                // none of them are worth refusing to start over.
                report.Add($"plugin assembly '{Path.GetFileName(file)}' could not be loaded: {ex.Message}");
            }
        }

        return report;
    }

    private static List<string> LoadAssembly(string file)
    {
        var assembly = AssemblyLoadContext.Default.LoadFromAssemblyPath(file);
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
            }
        }

        // Embedded ui-plugins/*.js|*.mjs resources count only from an assembly that registered at least
        // one plugin here — the directory also holds a plugin's plain dependency DLLs, and scanning every
        // one of THOSE for a same-shaped resource would attribute a UI module to an assembly that never
        // opted into being a StreamsForge plugin at all.
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

    /// <summary>Convention: <c>&lt;EmbeddedResource Include="ui-plugins/*.js" LogicalName="ui-plugins/%(Filename)%(Extension)" /&gt;</c>
    /// (also allow <c>.mjs</c>) in the plugin's csproj — no member on <see cref="IStreamsForgePlugin"/>
    /// needed, since the resource is discoverable from the assembly alone.</summary>
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
            if (ext is not (".js" or ".mjs"))
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
