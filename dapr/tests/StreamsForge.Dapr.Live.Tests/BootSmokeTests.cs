using System.Net.Http.Json;
using Xunit;

namespace StreamsForge.Dapr.Live.Tests;

/// <summary>
/// Plan 025 D3.1: the baseline every other test in this project assumes. Boots a fresh isolated
/// instance (Redis database 1 flushed first — <see cref="DaprHostProcess.ResetAsync"/>) and checks the
/// four cheapest, most load-bearing facts about it: it answers healthz, it logs an admin in, its seeded
/// catalog is EXACTLY the demo catalog's shape (6 sources / 7 pipelines / 5 tables — the same seed
/// counts the dev instance and the Orleans flavor both produce from an empty data store), and one of
/// those seeded tables actually fills with rows rather than sitting at zero. <c>/api/meta/instance</c>
/// reporting <c>flavor == "dapr"</c> is the same assertion the Orleans Chain tests would make with
/// <c>"orleans"</c> — proof this instance is answering as itself, not as a stale process on the wrong
/// port left over from something else.
/// </summary>
[Collection(DaprLiveTestCollection.Name)]
public sealed class BootSmokeTests : IAsyncLifetime
{
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
        _host = new DaprHostProcess("boot-smoke");
    }

    public async Task DisposeAsync()
    {
        if (_host is not null)
        {
            await _host.DisposeAsync();
        }
    }

    [Fact]
    public async Task Boot_smoke()
    {
        Assert.True(_skipReason is null, $"skipped: {_skipReason}");
        var host = _host!;

        host.Start();
        await host.WaitHealthyAsync();

        // healthz reports the flavor as part of the same live check WaitHealthyAsync already used, so
        // re-fetching here also exercises the JSON shape a real health-checking operator would parse.
        using var anon = host.NewClient();
        var health = await anon.GetFromJsonAsync<HealthzDto>($"{host.BaseUrl}/api/healthz");
        Assert.NotNull(health);
        Assert.Equal("dapr", health!.Flavor);

        using var client = await host.LoginAsync();

        var meta = await anon.GetFromJsonAsync<MetaInstanceDto>($"{host.BaseUrl}/api/meta/instance");
        Assert.NotNull(meta);
        Assert.Equal("dapr", meta!.Flavor);
        Assert.NotNull(meta.CatalogCounts);
        Assert.Equal(6, meta.CatalogCounts!.Sources);
        Assert.Equal(7, meta.CatalogCounts.Pipelines);
        Assert.Equal(5, meta.CatalogCounts.Tables);

        // "positions" is one of the seeded demo tables (same name on both flavors) fed by seeded
        // generators that tick continuously — within 60s it must hold more than zero rows.
        // GET /api/tables returns TableDefinition (no row count on it — that lives on TableMetrics,
        // see StreamsForge.Contracts/Models.cs), so the id is resolved once and rows are read via
        // /{id}/metrics, the same route RestartResumeTests uses for both Status and RowCount together.
        var tables = await client.GetFromJsonAsync<List<TableSummaryDto>>($"{host.BaseUrl}/api/tables");
        var positionsId = tables?.FirstOrDefault(t => t.Name == "positions")?.Id;
        Assert.False(string.IsNullOrEmpty(positionsId), "seeded table 'positions' not found in the catalog");

        var metrics = await PollUntilAsync(
            () => client.GetFromJsonAsync<TableMetricsDto>($"{host.BaseUrl}/api/tables/{positionsId}/metrics"),
            m => m is not null && m.RowCount > 0,
            TimeSpan.FromSeconds(60));
        Assert.NotNull(metrics);
        Assert.True(metrics!.RowCount > 0, "seeded table 'positions' never accumulated any rows within 60s");
    }

    private static async Task<T?> PollUntilAsync<T>(Func<Task<T?>> poll, Func<T?, bool> until, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        T? last = default;
        while (DateTime.UtcNow < deadline)
        {
            last = await poll();
            if (until(last))
            {
                return last;
            }
            await Task.Delay(500);
        }
        return last;
    }

    private sealed record HealthzDto(string Flavor);

    private sealed record MetaInstanceDto(string Flavor, CatalogCountsDto? CatalogCounts);

    private sealed record CatalogCountsDto(int Sources, int Pipelines, int Tables);

    private sealed record TableSummaryDto(string Id, string Name);

    private sealed record TableMetricsDto(string Status, long RowCount);
}
