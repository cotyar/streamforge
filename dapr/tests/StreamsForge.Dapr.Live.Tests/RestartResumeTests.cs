using System.Net.Http.Json;
using Xunit;

namespace StreamsForge.Dapr.Live.Tests;

/// <summary>
/// Plan 025 D3.7: the boot-order/late-consumer tests
/// <c>StreamBridgeSourceLifecycleTests</c>/<c>SourceLateConsumerClusterTests</c>/
/// <c>StreamsForge.Chain.Tests.HostRestartTests</c> exercise on Orleans are explicitly OUT of scope here
/// (plan 025's own brief: "come in the next wave once that code lands — leave TODO-free"; see
/// <c>dapr/PARITY.md</c> D6, which records that this flavor's boot order across its four independent
/// supervisor sweeps is not coordinated the way <c>RegistryGrain.EnsureInitializedAsync</c>'s single
/// latched resume pass is). What THIS test proves is the plainer, still-unverified-until-now claim: a
/// restarted Dapr-flavor instance actually resumes each seeded table at the SAME status it had before
/// the restart — Running stays Running, Stopped stays Stopped, neither direction silently flips — and
/// that a Running table keeps CONSUMING new deltas afterward — i.e. the seeded generators'
/// <c>GeneratorActor</c>s and the resumed <c>TableActor</c>s both actually come back to life, not just
/// the HTTP layer answering healthz.
///
/// <para><b>Not every seeded table is Running — this was checked against <c>SeedCatalog.cs</c>, not
/// assumed.</b> Of the five demo tables, <c>positions</c>/<c>leg_exposure</c>/<c>order_states</c> seed
/// <c>Running</c> and <c>gold_tier_orders</c>/<c>hot_symbols</c> seed <c>Stopped</c> on purpose (the
/// demo intentionally ships a couple of tables an operator would start by hand). An earlier draft of
/// this test asserted every table reports <c>Running</c> after the restart and failed against
/// <c>gold_tier_orders</c> — correctly: that failure was this test's own wrong premise, not a product
/// bug, so the assertion below is "unchanged from its pre-restart status", not "Running".</para>
///
/// <para><b>"Keeps filling" is measured as <c>deltasIn</c> growth on <see cref="TableMetricsDto"/>, not
/// <c>rowCount</c> growth — this was also checked live, not assumed.</b> An earlier draft polled
/// <c>rowCount</c> and it failed even on a healthy, actively-updating instance: <c>positions</c> is
/// <c>SELECT symbol, ... FROM trades GROUP BY symbol</c>, so its row count is bounded by the fixed
/// number of distinct symbols in the seed data (8) and legitimately never grows, while
/// <c>deltasIn</c>/<c>deltasOut</c> climbed by dozens every few seconds in a manual check both before
/// and after a restart — confirmed live (<c>rowCount</c> pinned at 8, <c>deltasIn</c> 57→180 in 15s)
/// against the SAME restarted process this test itself drives. <c>deltasIn</c> is what actually answers
/// "is this table still receiving live traffic", which is what this test is about; a row-count-based
/// assertion would have been a false negative on a perfectly healthy resume.</para>
/// <para>Uses the SAME app-id and Redis database across the restart (<see cref="DaprHostProcess.RestartAsync"/>
/// — no <see cref="DaprHostProcess.ResetAsync"/> in between), which is the whole point: a reset would
/// prove nothing about resume, only about re-seeding.</para>
/// </summary>
[Collection(DaprLiveTestCollection.Name)]
public sealed class RestartResumeTests : IAsyncLifetime
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
        _host = new DaprHostProcess("restart-resume");
    }

    public async Task DisposeAsync()
    {
        if (_host is not null)
        {
            await _host.DisposeAsync();
        }
    }

    [Fact]
    public async Task Restart_resumes_running_tables_and_they_keep_filling()
    {
        Assert.True(_skipReason is null, $"skipped: {_skipReason}");
        var host = _host!;

        host.Start();
        await host.WaitHealthyAsync();

        using var client = await host.LoginAsync();

        var tables = await client.GetFromJsonAsync<List<TableSummaryDto>>($"{host.BaseUrl}/api/tables");
        Assert.NotNull(tables);
        Assert.Equal(5, tables!.Count); // the seeded demo catalog's table count, same as Boot_smoke

        var positionsId = tables.First(t => t.Name == "positions").Id;

        // WaitHealthyAsync only waits for the catalog's DEFINITIONS to be seeded (catalogCounts.sources
        // > 0) — a seeded-Running table's actor is started moments later by TableSupervisorService's own
        // sweep, not synchronously with seeding. Observed live: capturing statuses immediately after
        // WaitHealthyAsync once caught "positions" still 'Stopped' (a boot-race, not a real status),
        // flipping to 'Running' by the time this test read it again after the restart — which would have
        // been reported as a false "status changed" finding. Settling on "positions" (one of the tables
        // that must end up Running) before snapshotting anything makes the snapshot below a stable
        // post-boot-sweep read for every table, not a race with it.
        await PollUntilTableStatusAsync(client, host.BaseUrl, positionsId, "Running", TimeSpan.FromSeconds(30));

        // Capture every table's PRE-restart status — see the class doc for why this is not simply
        // "assert Running": two of the five seeded tables (gold_tier_orders, hot_symbols) start Stopped
        // by design.
        var statusesBefore = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var t in tables)
        {
            var metrics = await client.GetFromJsonAsync<TableMetricsDto>($"{host.BaseUrl}/api/tables/{t.Id}/metrics");
            Assert.NotNull(metrics);
            statusesBefore[t.Id] = metrics!.Status;
        }

        // "positions" is one of the Running ones — capture its pre-restart deltasIn so the post-restart
        // assertion is "grew from here", not just "is nonzero" (which BootSmokeTests' rowCount check
        // already covers and which alone wouldn't distinguish "resumed and still filling" from "resumed
        // once, then stuck"). deltasIn, not rowCount — see the class doc's paragraph on why.
        var before = await PollUntilDeltasInAboveZeroAsync(client, host.BaseUrl, positionsId, TimeSpan.FromSeconds(60));
        Assert.True(before > 0, "seeded table 'positions' never received any deltas before the restart");

        await host.RestartAsync();

        using var clientAfter = await host.LoginAsync();

        // Same boot-race settling as before the restart, on the same table for the same reason.
        await PollUntilTableStatusAsync(clientAfter, host.BaseUrl, positionsId, "Running", TimeSpan.FromSeconds(30));

        var tablesAfter = await clientAfter.GetFromJsonAsync<List<TableSummaryDto>>($"{host.BaseUrl}/api/tables");
        Assert.NotNull(tablesAfter);
        Assert.Equal(5, tablesAfter!.Count);
        foreach (var t in tablesAfter)
        {
            var metrics = await clientAfter.GetFromJsonAsync<TableMetricsDto>($"{host.BaseUrl}/api/tables/{t.Id}/metrics");
            Assert.NotNull(metrics);
            Assert.True(
                statusesBefore.TryGetValue(t.Id, out var before1) && before1 == metrics!.Status,
                $"table {t.Name} ({t.Id}) was '{(statusesBefore.TryGetValue(t.Id, out var b) ? b : "<unknown>")}' "
              + $"before the restart and is '{metrics!.Status}' after it");
        }

        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(60);
        long after = 0;
        while (DateTime.UtcNow < deadline)
        {
            var metrics = await clientAfter.GetFromJsonAsync<TableMetricsDto>($"{host.BaseUrl}/api/tables/{positionsId}/metrics");
            after = metrics?.DeltasIn ?? 0;
            if (after > before)
            {
                break;
            }
            await Task.Delay(500);
        }
        Assert.True(
            after > before,
            $"seeded table 'positions' received no new deltas within 60s after the restart (before={before}, after={after})\n"
          + $"--- host log tail ---\n{host.LogTailText()}");
    }

    private static async Task PollUntilTableStatusAsync(HttpClient client, string baseUrl, string tableId, string status, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            var metrics = await client.GetFromJsonAsync<TableMetricsDto>($"{baseUrl}/api/tables/{tableId}/metrics");
            if (metrics?.Status == status)
            {
                return;
            }
            await Task.Delay(500);
        }
        // Not fatal here by design — the caller's own subsequent assertions (deltasIn growth, or the
        // status-unchanged comparison) will fail with a clearer message if this table genuinely never
        // reached the expected status, and this helper exists to settle a boot race, not to gate on it.
    }

    private static async Task<long> PollUntilDeltasInAboveZeroAsync(HttpClient client, string baseUrl, string tableId, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            var metrics = await client.GetFromJsonAsync<TableMetricsDto>($"{baseUrl}/api/tables/{tableId}/metrics");
            if (metrics is { DeltasIn: > 0 })
            {
                return metrics.DeltasIn;
            }
            await Task.Delay(500);
        }
        return 0;
    }

    private sealed record TableSummaryDto(string Id, string Name);

    private sealed record TableMetricsDto(string Status, long RowCount, long DeltasIn);
}
