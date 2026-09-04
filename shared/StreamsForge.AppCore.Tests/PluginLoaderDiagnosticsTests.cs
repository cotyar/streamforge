using System.Reflection;
using StreamsForge.AppCore.Plugins;
using Xunit;

namespace StreamsForge.AppCore.Tests;

/// <summary>
/// Pure-function coverage for <see cref="StreamsForgePlugins.DescribeVersionConflicts"/> — the loader's
/// diagnostic line for a plugin whose own dependency is a NEWER version than what the host's default load
/// context already has resident. Assembly resolution in one context always keeps whichever copy loaded
/// first, so the host's older copy silently wins; the line exists so a later
/// <see cref="TypeInitializationException"/>/<see cref="MissingMethodException"/> at first use has an
/// explanation on record from load time, rather than being debugged cold.
///
/// <para>Exercised directly against hand-built <see cref="AssemblyName"/>s — no assembly needs to be
/// loaded on disk for this, since the function is pure.</para>
/// </summary>
public class PluginLoaderDiagnosticsTests
{
    [Fact]
    public void A_plugin_referencing_a_newer_version_than_the_host_loaded_is_reported_with_the_exact_line()
    {
        var referenced = new[] { Named("Newtonsoft.Json", "13.0.3") };
        var loaded = new[] { Named("Newtonsoft.Json", "13.0.1") };

        var lines = StreamsForgePlugins.DescribeVersionConflicts("orion", referenced, loaded).ToList();

        Assert.Equal(
            [
                "plugin 'orion' references Newtonsoft.Json 13.0.3 but the host has 13.0.1 loaded — the host copy wins; a TypeInitializationException at first use means this",
            ],
            lines);
    }

    [Fact]
    public void Equal_versions_are_silent()
    {
        var referenced = new[] { Named("Newtonsoft.Json", "13.0.1") };
        var loaded = new[] { Named("Newtonsoft.Json", "13.0.1") };

        Assert.Empty(StreamsForgePlugins.DescribeVersionConflicts("orion", referenced, loaded));
    }

    [Fact]
    public void A_host_copy_newer_than_the_reference_is_silent()
    {
        var referenced = new[] { Named("Newtonsoft.Json", "13.0.1") };
        var loaded = new[] { Named("Newtonsoft.Json", "13.0.3") };

        Assert.Empty(StreamsForgePlugins.DescribeVersionConflicts("orion", referenced, loaded));
    }

    [Fact]
    public void A_referenced_assembly_the_host_never_loaded_is_silent()
    {
        var referenced = new[] { Named("Some.Dependency.Only.The.Plugin.Has", "1.2.3") };
        var loaded = new[] { Named("Newtonsoft.Json", "13.0.1") };

        Assert.Empty(StreamsForgePlugins.DescribeVersionConflicts("orion", referenced, loaded));
    }

    [Fact]
    public void Names_are_compared_case_insensitively()
    {
        var referenced = new[] { Named("newtonsoft.json", "13.0.3") };
        var loaded = new[] { Named("Newtonsoft.Json", "13.0.1") };

        var lines = StreamsForgePlugins.DescribeVersionConflicts("orion", referenced, loaded).ToList();

        var line = Assert.Single(lines);
        Assert.Contains("newtonsoft.json", line, StringComparison.Ordinal);
        Assert.Contains("13.0.1", line, StringComparison.Ordinal);
    }

    [Fact]
    public void A_null_version_on_the_referenced_side_is_silent()
    {
        var referenced = new[] { new AssemblyName { Name = "Newtonsoft.Json" } }; // no Version set
        var loaded = new[] { Named("Newtonsoft.Json", "13.0.1") };

        Assert.Empty(StreamsForgePlugins.DescribeVersionConflicts("orion", referenced, loaded));
    }

    [Fact]
    public void A_null_version_on_the_loaded_side_is_silent()
    {
        var referenced = new[] { Named("Newtonsoft.Json", "13.0.3") };
        var loaded = new[] { new AssemblyName { Name = "Newtonsoft.Json" } }; // no Version set

        Assert.Empty(StreamsForgePlugins.DescribeVersionConflicts("orion", referenced, loaded));
    }

    private static AssemblyName Named(string name, string version) => new(name) { Version = new Version(version) };
}
