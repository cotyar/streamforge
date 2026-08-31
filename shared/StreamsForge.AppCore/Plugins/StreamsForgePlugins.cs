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

        // Not finding a plugin type is silence on purpose: the directory holds a plugin's DEPENDENCIES
        // too, and reporting each of them as "no plugin here" would bury the lines that matter.
        return
        [
            .. assembly.GetExportedTypes()
                .Where(t => typeof(IStreamsForgePlugin).IsAssignableFrom(t) && t is { IsAbstract: false, IsInterface: false })
                .OrderBy(t => t.FullName, StringComparer.Ordinal)
                .Select(Activate),
        ];
    }

    private static string Activate(Type type)
    {
        try
        {
            var plugin = (IStreamsForgePlugin)Activator.CreateInstance(type)!;
            plugin.Register();
            return $"plugin '{plugin.Name}' ({type.FullName}) registered";
        }
        catch (Exception ex)
        {
            // TargetInvocationException wraps whatever the constructor actually threw; the inner message is
            // the one an operator can act on ("an inbound transport for kind 'orion' is already
            // registered"), so unwrap it rather than reporting the wrapper.
            var cause = ex is TargetInvocationException { InnerException: { } inner } ? inner : ex;
            return $"plugin '{type.FullName}' failed to register: {cause.Message}";
        }
    }
}
