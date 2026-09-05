using System.Net.Http.Json;
using Xunit;

namespace StreamsForge.Dapr.Live.Tests;

/// <summary>
/// Plan 025 D3.5: the open cross-flavor requirement <c>dapr/PARITY.md</c> §3 names explicitly ("Plan 021
/// environments — the one open cross-flavor requirement from the most recent plan. Create staging,
/// create a same-named table in both environments, confirm two separate Redis keys rather than two
/// filtered reads, force-delete, confirm no re-seed on restart"). Verifies live, against a real Dapr
/// instance for the first time, what <c>dapr/ARCHITECTURE.md</c>'s "Environment isolation" section only
/// described: <c>RegistryActor</c>'s actor id is <c>EnvKeys.Qualify(environment, "catalog")</c> — literally
/// <c>"catalog"</c> for the default environment and <c>"staging.catalog"</c> for an environment named
/// <c>staging</c> — so two environments are two ACTORS with two independently-persisted Redis entries,
/// never one registry filtered by an environment column.
/// </summary>
[Collection(DaprLiveTestCollection.Name)]
public sealed class EnvironmentIsolationTests : IAsyncLifetime
{
    private const string EnvHeader = "X-StreamsForge-Environment";

    private DaprHostProcess? _host;
    private string? _skipReason;

    public async Task InitializeAsync()
    {
        _skipReason = DaprHostProcess.Preflight();
        if (_skipReason is not null)
        {
            return;
        }

        await DaprHostProcess.ResetAsync();
        _host = new DaprHostProcess("env-isolation");
    }

    public async Task DisposeAsync()
    {
        if (_host is not null)
        {
            await _host.DisposeAsync();
        }
    }

    [Fact]
    public async Task Environments_are_separate_redis_keys_and_a_force_deleted_environment_does_not_reseed()
    {
        Assert.True(_skipReason is null, $"skipped: {_skipReason}");
        var host = _host!;

        host.Start();
        await host.WaitHealthyAsync();

        using var client = await host.LoginAsync();

        var createEnv = await client.PostAsJsonAsync($"{host.BaseUrl}/api/environments", new { name = "staging" });
        Assert.True(createEnv.IsSuccessStatusCode, $"POST /api/environments -> {(int)createEnv.StatusCode}");

        // A same-named source+table in EACH environment — default already has plenty of seeded sources,
        // but staging starts with an EMPTY catalog (seeding is default-environment-only), so it needs its
        // own source for the table's SQL to compile against. Writing a table (not just a source) into
        // each environment's catalog is what the plan brief specifically asks to check, and either write
        // is enough to force that environment's RegistryActor to activate and persist for the first time.
        await CreateSourceAndTableAsync(client, host.BaseUrl, env: null);
        await CreateSourceAndTableAsync(client, host.BaseUrl, env: "staging");

        // Live check, not a description taken on faith: dapr/ARCHITECTURE.md says RegistryActor's id is
        // EnvKeys.Qualify(environment, "catalog") — "catalog" for default, "staging.catalog" for staging
        // — which composes into the Redis actor-state key format observed everywhere else in this
        // project: "<appid>||RegistryActor||<actor-id>||catalog".
        var registryKeys = await DaprHostProcess.ScanTestRedisKeysAsync(
            $"{DaprHostProcess.AppId}||RegistryActor||*");
        Assert.Contains($"{DaprHostProcess.AppId}||RegistryActor||catalog||catalog", registryKeys);
        Assert.Contains($"{DaprHostProcess.AppId}||RegistryActor||staging.catalog||catalog", registryKeys);
        // Exactly two — not one registry actor being read twice through two "filtered" views, which is
        // the wrong shape this test exists to rule out.
        Assert.Equal(2, registryKeys.Count);

        // Force-delete staging; both its own entities and the environment record itself go.
        var delete = await client.DeleteAsync($"{host.BaseUrl}/api/environments/staging?force=true");
        Assert.True(delete.IsSuccessStatusCode, $"DELETE /api/environments/staging?force=true -> {(int)delete.StatusCode}");

        await host.RestartAsync();
        using var clientAfter = await host.LoginAsync();

        var envs = await clientAfter.GetFromJsonAsync<List<EnvironmentDto>>($"{host.BaseUrl}/api/environments");
        Assert.NotNull(envs);
        Assert.DoesNotContain(envs!, e => e.Name == "staging");

        // Not re-seeded: selecting the (now nonexistent) "staging" environment 404s rather than
        // resurrecting an empty-or-reseeded catalog under that name — sf-env's own documented rule that
        // an unknown environment is refused before the request reaches its handler.
        var afterDeleteReq = new HttpRequestMessage(HttpMethod.Get, $"{host.BaseUrl}/api/tables");
        afterDeleteReq.Headers.Add(EnvHeader, "staging");
        afterDeleteReq.Headers.Authorization = clientAfter.DefaultRequestHeaders.Authorization;
        using var rawClient = new HttpClient();
        var afterDelete = await rawClient.SendAsync(afterDeleteReq);
        Assert.Equal(System.Net.HttpStatusCode.NotFound, afterDelete.StatusCode);
    }

    private static async Task CreateSourceAndTableAsync(HttpClient client, string baseUrl, string? env)
    {
        var sourceReq = new HttpRequestMessage(HttpMethod.Post, $"{baseUrl}/api/sources")
        {
            Content = JsonContent.Create(new
            {
                name = "iso_src",
                description = "environment isolation test source",
                kind = "file",
                enabled = false,
                fields = new object[] { new { name = "id", type = "Long" } },
                connector = new
                {
                    schedule = new { intervalMs = 5000 },
                    file = new { path = "/tmp/does-not-exist-iso.ndjson", format = "ndjson" },
                    mapping = new
                    {
                        itemsPath = "$",
                        dedupKeyField = "id",
                        fields = new object[] { new { field = new { name = "id", type = "Long" } } },
                    },
                },
            }),
        };
        if (env is not null)
        {
            sourceReq.Headers.Add("X-StreamsForge-Environment", env);
        }
        var sourceResp = await client.SendAsync(sourceReq);
        var sourceText = await sourceResp.Content.ReadAsStringAsync();
        Assert.True(sourceResp.IsSuccessStatusCode, $"POST api/sources (env={env ?? "default"}) -> {(int)sourceResp.StatusCode}: {sourceText}");

        var tableReq = new HttpRequestMessage(HttpMethod.Post, $"{baseUrl}/api/tables")
        {
            Content = JsonContent.Create(new
            {
                name = "iso_check",
                description = "environment isolation test table",
                sql = "SELECT id FROM iso_src LATEST BY (id)",
            }),
        };
        if (env is not null)
        {
            tableReq.Headers.Add("X-StreamsForge-Environment", env);
        }
        var tableResp = await client.SendAsync(tableReq);
        var tableText = await tableResp.Content.ReadAsStringAsync();
        Assert.True(tableResp.IsSuccessStatusCode, $"POST api/tables (env={env ?? "default"}) -> {(int)tableResp.StatusCode}: {tableText}");
    }

    private sealed record EnvironmentDto(string Name);
}
