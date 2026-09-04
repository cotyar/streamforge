using System.Net.Http.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using StreamsForge.Api;
using StreamsForge.AppCore.Plugins;
using Xunit;

namespace StreamsForge.AppCore.Tests;

/// <summary>
/// <c>UiPluginsEndpoints</c> exercised over real HTTP, same "bare <c>WebApplication</c>, real Kestrel
/// listener on a dynamic port" pattern the host's own endpoint tests use — this route needs nothing off
/// <c>MapStreamsForgeApi</c>'s big facade graph (no auth, no catalog), just <c>app.Configuration</c>, so a
/// full host bootstrap would only add noise.
///
/// <para>IN THE SHARED test project on purpose, same reasoning as <see cref="OutOfTreeKindTests"/>:
/// <c>UiPluginsEndpoints</c> lives in <c>StreamsForge.Api</c>, which both runtime flavors ship unchanged,
/// so one test here covers both rather than two near-duplicates under <c>orleans/tests</c> and
/// <c>dapr/tests</c>.</para>
///
/// <para>Embedded-module coverage reuses the same <c>StreamsForge.AppCore.Tests.PluginFixture</c> DLL
/// <see cref="OutOfTreeKindTests"/> loads for the loader-side assertions — <c>StreamsForgePlugins.UiModules</c>
/// is a process-global, append-only list (like the transport registries), so loading it once here is
/// enough for every test in this class within the same test run.</para>
/// </summary>
public sealed class UiPluginsEndpointsTests : IAsyncDisposable
{
    private readonly List<string> _tempDirs = [];
    private WebApplication? _app;

    public async ValueTask DisposeAsync()
    {
        if (_app is not null)
        {
            await _app.StopAsync();
            await _app.DisposeAsync();
        }

        foreach (var dir in _tempDirs)
        {
            if (Directory.Exists(dir))
            {
                Directory.Delete(dir, recursive: true);
            }
        }
    }

    [Fact]
    public async Task Listing_includes_a_disk_file_and_serves_its_bytes()
    {
        var dir = NewTempDir();
        File.WriteAllText(Path.Combine(dir, "disk-only.js"), "export default { apiVersion: 2 };");

        using var client = await StartAsync(dir);

        var list = await client.GetFromJsonAsync<string[]>("/api/ui-plugins") ?? [];
        var url = Assert.Single(list, u => u.StartsWith("/api/ui-plugins/disk-only.js?v=", StringComparison.Ordinal));

        var body = await client.GetStringAsync(url);
        Assert.Equal("export default { apiVersion: 2 };", body);
    }

    [Fact]
    public async Task Listing_is_no_store_and_a_url_changes_when_the_file_changes()
    {
        var dir = NewTempDir();
        var path = Path.Combine(dir, "versioned.js");
        File.WriteAllText(path, "export default { apiVersion: 2 };");
        var oldWrite = File.GetLastWriteTimeUtc(path);

        using var client = await StartAsync(dir);

        var response = await client.GetAsync("/api/ui-plugins");
        Assert.True(response.Headers.CacheControl!.NoStore);
        var before = await response.Content.ReadFromJsonAsync<string[]>() ?? [];
        var beforeUrl = Assert.Single(before, u => u.StartsWith("/api/ui-plugins/versioned.js?v=", StringComparison.Ordinal));

        // Rewrite the file AND bump its last-write-time explicitly — filesystem timestamp resolution can
        // be coarser than the test, so relying on the write alone to change the ticks would be flaky.
        File.WriteAllText(path, "export default { apiVersion: 3 };");
        File.SetLastWriteTimeUtc(path, oldWrite.AddSeconds(1));

        var after = await client.GetFromJsonAsync<string[]>("/api/ui-plugins") ?? [];
        var afterUrl = Assert.Single(after, u => u.StartsWith("/api/ui-plugins/versioned.js?v=", StringComparison.Ordinal));

        Assert.NotEqual(beforeUrl, afterUrl);

        var body = await client.GetStringAsync(afterUrl);
        Assert.Equal("export default { apiVersion: 3 };", body);
    }

    [Fact]
    public async Task A_tsx_file_is_listed_and_served_as_plain_text()
    {
        var dir = NewTempDir();
        File.WriteAllText(Path.Combine(dir, "editor.tsx"), "export default function Editor() { return null; }");

        using var client = await StartAsync(dir);

        var list = await client.GetFromJsonAsync<string[]>("/api/ui-plugins") ?? [];
        var url = Assert.Single(list, u => u.StartsWith("/api/ui-plugins/editor.tsx?v=", StringComparison.Ordinal));

        var response = await client.GetAsync(url);
        Assert.Equal("text/plain", response.Content.Headers.ContentType!.MediaType);
        Assert.Equal("utf-8", response.Content.Headers.ContentType!.CharSet);
        Assert.Equal("export default function Editor() { return null; }", await response.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task An_unknown_file_name_is_not_found()
    {
        using var client = await StartAsync(NewTempDir());

        var response = await client.GetAsync("/api/ui-plugins/nope.js");

        Assert.Equal(System.Net.HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task An_embedded_module_from_a_loaded_plugin_is_listed_and_served_from_the_assembly()
    {
        LoadFixturePlugin();

        using var client = await StartAsync(NewTempDir()); // empty disk directory — nothing to collide with.

        var list = await client.GetFromJsonAsync<string[]>("/api/ui-plugins") ?? [];
        var url = Assert.Single(list, u => u.StartsWith("/api/ui-plugins/test-kind.js?v=", StringComparison.Ordinal));

        var body = await client.GetStringAsync(url);
        Assert.Contains("registerTransportEditor('ui-fixture-kind'", body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task An_embedded_tsx_module_is_listed()
    {
        LoadFixturePlugin();

        using var client = await StartAsync(NewTempDir());

        var list = await client.GetFromJsonAsync<string[]>("/api/ui-plugins") ?? [];
        var url = Assert.Single(list, u => u.StartsWith("/api/ui-plugins/test-kind-ts.tsx?v=", StringComparison.Ordinal));

        var body = await client.GetStringAsync(url);
        Assert.Contains("registerTransportEditor('ui-fixture-kind-ts'", body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_same_named_file_on_disk_wins_over_the_embedded_module()
    {
        LoadFixturePlugin();

        var dir = NewTempDir();
        File.WriteAllText(Path.Combine(dir, "test-kind.js"), "/* operator override */");

        using var client = await StartAsync(dir);

        // Listed exactly once — the union de-duplicates by name, it does not offer two entries.
        var list = await client.GetFromJsonAsync<string[]>("/api/ui-plugins") ?? [];
        var url = Assert.Single(list, u => u.StartsWith("/api/ui-plugins/test-kind.js?v=", StringComparison.Ordinal));

        var body = await client.GetStringAsync(url);
        Assert.Equal("/* operator override */", body); // disk content, not the embedded module's.
    }

    // ------------------------------------------------------------------
    // Plumbing.
    // ------------------------------------------------------------------

    private string NewTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), $"sf-ui-plugins-{Guid.NewGuid():N}");
        Directory.CreateDirectory(dir);
        _tempDirs.Add(dir);
        return dir;
    }

    /// <summary>Loads the same build fixture <see cref="OutOfTreeKindTests"/> uses, into THIS test
    /// process's <c>StreamsForgePlugins.UiModules</c> — a fresh temp copy each call so re-loading it from
    /// several tests in this class never collides on assembly identity.</summary>
    private void LoadFixturePlugin()
    {
        var dir = NewTempDir();
        File.Copy(FixturePluginDllPath(), Path.Combine(dir, $"test-kind-fixture-{Guid.NewGuid():N}.dll"));
        StreamsForgePlugins.LoadFrom(dir);
    }

    /// <summary>Same fixture-locating logic as <c>OutOfTreeKindTests.FixturePluginDllPath</c> — an
    /// independent copy rather than reaching into a file this test class does not own, same convention
    /// this repo's other wave-scoped test files already follow (see <c>PluginRequirementGateTests</c>).</summary>
    private static string FixturePluginDllPath()
    {
        // "bin" only — see OutOfTreeKindTests.FixturePluginDllPath for why "obj/**/ref[int]" must be
        // excluded (same simple name, metadata-only, throws "Reference assemblies cannot be loaded for
        // execution" if picked up instead).
        var fixtureBinDir = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "StreamsForge.AppCore.Tests.PluginFixture", "bin"));
        return Directory.EnumerateFiles(fixtureBinDir, "StreamsForge.AppCore.Tests.PluginFixture.dll", SearchOption.AllDirectories)
                   .OrderByDescending(File.GetLastWriteTimeUtc)
                   .FirstOrDefault()
               ?? throw new FileNotFoundException($"PluginFixture DLL not found under '{fixtureBinDir}' — build the solution first.");
    }

    private async Task<HttpClient> StartAsync(string uiPluginsDir)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Ui:PluginsPath"] = uiPluginsDir,
        });

        _app = builder.Build();
        _app.MapUiPluginsEndpoints();

        await _app.StartAsync();
        var address = _app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First();
        return new HttpClient { BaseAddress = new Uri(address) };
    }
}
