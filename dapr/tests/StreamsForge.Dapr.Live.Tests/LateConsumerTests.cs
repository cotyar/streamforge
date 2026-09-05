using Xunit;

namespace StreamsForge.Dapr.Live.Tests;

/// <summary>
/// Plan 025 D3, live: <b>late-consumer replay on the Dapr flavor</b> — a consumer created AFTER its
/// source has already polled still gets every row the source ever emitted.
///
/// <para><b>What this proves, and why the setup order is the OPPOSITE of
/// <c>SourceExactCountTests</c>'.</b> That class deliberately creates its source <c>enabled:false</c>,
/// stands the table up, and only THEN enables the source — because its subject is exact counts and it
/// must not race the subscription. This class's subject IS the race: the source here is created
/// <c>enabled:true</c> with nothing listening, and the test waits until
/// <c>GET /api/sources/{name}/status</c> reports <c>eventsEmittedTotal == 300</c> — i.e. until every row
/// has been published into a topic that has NO subscriber — before the consumer is created at all.
/// Before plan 025 that consumer came up empty on this flavor (<c>dapr/PARITY.md</c> D2/D6): Dapr
/// pub/sub has no replay, exactly as Orleans memory streams have none, so rows published before a
/// subscriber attached were simply gone. What closes it is
/// <c>IConnectorActor.BeginAttachAsync</c>/<c>EndAttachAsync</c> — the consumer's own actor turn asks
/// the connector to HOLD publishing, receives the replay ring's snapshot, and the connector flushes what
/// it held once the consumer says it is attached.</para>
///
/// <para><b>300 rows, not 30 000, and that is a real boundary.</b> The replay ring
/// (<c>SourceReplayBuffer.Capacity</c>) is 10 000 rows. A late consumer attaching to a source that has
/// emitted MORE than that gets the most recent 10 000 and a log line saying so — by design, not by
/// accident. This class deliberately stays well inside the ring, because "everything, exactly once" is
/// only a promise inside it; a test at 15 000 rows would be asserting a different (and weaker)
/// contract, and pinning the eviction boundary belongs with the ring's own unit tests, not to a live
/// test that would take minutes to reach it.</para>
///
/// <para><b>The append leg is what separates replay from a lucky first poll.</b> After the late table
/// has caught up to 300, another 200 rows are appended to the same file: those arrive through the
/// ORDINARY live path, with the subscription already in place. Reaching exactly 500 therefore proves the
/// hand-off worked — the connector did not stay stuck holding, and did not re-deliver the first 300 a
/// second time on top of the replay (which would read as 800, or as a mangled id set).</para>
///
/// <para><b>What this class does NOT prove.</b> Nothing here says anything about a consumer that
/// attaches to a source on ANOTHER host — a federated <c>grpc</c> source's producing side is a live
/// stream with no ring behind it, and <see cref="FederationTests"/> is written to avoid depending on
/// replay for exactly that reason. It also says nothing about ordering: <c>LATEST BY (id)</c> and a
/// pipeline's row count are both order-insensitive, so a replay that arrived interleaved with live rows
/// would still pass. Ordering across the hold/flush seam is not a promise this platform makes.</para>
///
/// <para>ONE host instance serves both facts (each with its own uniquely-named entities) — see
/// <see cref="LiveHostFixture"/> for why that needs an <c>IClassFixture</c> and is not what
/// implementing <c>IAsyncLifetime</c> on the test class itself would give.</para>
/// </summary>
public sealed class LateConsumerFixture : LiveHostFixture
{
    protected override string Label => "late-consumer";
}

[Collection(DaprLiveTestCollection.Name)]
public sealed class LateConsumerTests(LateConsumerFixture fixture) : IClassFixture<LateConsumerFixture>
{
    private DaprHostProcess? _host => fixture.Host;
    private string? _skipReason => fixture.SkipReason;
    private string _scratchDir => fixture.ScratchDir;

    // ================================================================================================
    // 1. table — the 300-then-500 scenario
    // ================================================================================================

    [Fact]
    public async Task Table_created_after_the_source_already_polled_gets_every_row_then_keeps_up()
    {
        Assert.True(_skipReason is null, $"skipped: {_skipReason}");
        var host = _host!;
        var name = "late_file_" + Guid.NewGuid().ToString("n")[..8];
        var filePath = Path.Combine(_scratchDir, name + ".ndjson");
        await File.WriteAllTextAsync(filePath, LiveRest.Ndjson(Enumerable.Range(0, 300)));

        using var client = await host.LoginAsync();

        await LiveRest.PostOkAsync(client, $"{host.BaseUrl}/api/sources", EnabledFileSource(name, filePath));

        // Nothing is subscribed yet. Wait until the connector has genuinely emitted all 300 into the
        // void — this is the precondition the whole test rests on, so it is asserted, not slept for.
        await WaitForEmittedAsync(client, host, name, 300, TimeSpan.FromSeconds(60));

        var tableId = await CreateAndStartTableAsync(client, host, "tbl_" + name, name);

        var rows = await LiveRest.SettledRowsAsync(
            client, host.BaseUrl, tableId, 300, TimeSpan.FromSeconds(60), host.LogTailText);
        Assert.Equal(300, rows.TotalRows);
        LiveRest.AssertIdSet(rows, Enumerable.Range(0, 300));

        // >=1s so the file ledger's mtime comparison actually sees a change — the same requirement
        // SourceExactCountTests documents for its own append leg.
        await Task.Delay(1200);
        await File.AppendAllTextAsync(filePath, LiveRest.Ndjson(Enumerable.Range(300, 200)));

        rows = await LiveRest.SettledRowsAsync(
            client, host.BaseUrl, tableId, 500, TimeSpan.FromSeconds(60), host.LogTailText);
        Assert.Equal(500, rows.TotalRows);
        LiveRest.AssertIdSet(rows, Enumerable.Range(0, 500));
    }

    // ================================================================================================
    // 2. pipeline — the same gap on the other consumer kind
    // ================================================================================================

    /// <summary>
    /// The pipeline half of the same protocol. <c>PipelineActor</c> got its own
    /// <c>RegisterRouterAndAttachToSourcesAsync</c> in plan 025 for the identical reason
    /// <c>TableActor</c> did, and this fact is the live proof that it works — a pipeline written after
    /// its source had already polled used to start empty.
    ///
    /// <para><b>The count is read from <c>/metrics</c>, not <c>/results</c>, and that is forced.</b>
    /// <c>GET /api/pipelines/{id}/results</c> is served from
    /// <c>PipelineActor</c>'s <c>_recentResults</c>, a ring of <c>RecentResultsCapacity = 100</c>
    /// (<c>dapr/src/StreamsForge.Dapr.Host/Actors/PipelineActor.cs:66</c>, matching
    /// <c>PipelineGrain</c>'s own 100) — a read cache, never persisted, deliberately bounded. Asking it
    /// for an exact count of 300 is asking a question it structurally cannot answer: it would return
    /// 100 on a perfectly healthy replay AND on a broken one that only delivered the last 100 rows, so
    /// the assertion would be unable to fail for the right reason. <c>totalRowsOut</c> on
    /// <c>GET /api/pipelines/{id}/metrics</c> is an unbounded counter of rows actually emitted, which is
    /// the count of rows actually emitted. <c>/results</c> is still read once, for the weaker fact it
    /// CAN carry: that the endpoint answers and its envelopes really do hold this pipeline's
    /// columns.</para>
    ///
    /// <para><b>But <c>totalRowsOut</c> is asserted as AT LEAST 300, not exactly 300 — and that is a
    /// finding, not a hedge.</b> An earlier draft asserted equality and was observed returning
    /// <b>600</b> in a whole-suite run (and 300 in the run before it). The replay is re-delivered on
    /// every (re)start of the pipeline: <c>PipelineActor.StartAsync</c> AND
    /// <c>PipelineActor.OnActivateAsync</c>'s self-heal branch both call
    /// <c>RegisterRouterAndAttachToSourcesAsync</c>, which takes a fresh snapshot of the connector's ring
    /// and feeds it through <c>ProcessEventsAsync</c> again — while <c>_totalRowsOut</c> is an instance
    /// field that a second <c>StartAsync</c> on a live activation does NOT reset. A
    /// <c>PipelineSupervisorService</c> repair sweep (~15 s) landing inside this test's window is enough
    /// to double it. Emission is therefore at-least-once by construction on this path, so a
    /// count-based equality is not a property this platform offers.</para>
    ///
    /// <para><b>Exactness is recovered downstream instead.</b> A <c>LATEST BY (id)</c> table over the
    /// pipeline is idempotent under re-delivery, so "exactly 300 distinct ids" is assertable there and
    /// says the thing that actually matters — nothing was LOST. The table is created and started while
    /// the pipeline is still <c>Stopped</c>, because a table attaching to a pipeline gets no replay
    /// (AGENTS.md hard rule 6); starting the pipeline last is what sets the rows moving.</para>
    /// </summary>
    [Fact]
    public async Task Pipeline_created_after_the_source_already_polled_gets_every_row()
    {
        Assert.True(_skipReason is null, $"skipped: {_skipReason}");
        var host = _host!;
        var name = "late_pipe_src_" + Guid.NewGuid().ToString("n")[..8];
        var pipelineName = "late_pipe_" + Guid.NewGuid().ToString("n")[..8];
        var filePath = Path.Combine(_scratchDir, name + ".ndjson");
        await File.WriteAllTextAsync(filePath, LiveRest.Ndjson(Enumerable.Range(0, 300)));

        using var client = await host.LoginAsync();

        await LiveRest.PostOkAsync(client, $"{host.BaseUrl}/api/sources", EnabledFileSource(name, filePath));
        await WaitForEmittedAsync(client, host, name, 300, TimeSpan.FromSeconds(60));

        string pipelineId;
        using (var created = await LiveRest.PostOkAsync(client, $"{host.BaseUrl}/api/pipelines", new
        {
            name = pipelineName,
            description = "late-consumer pipeline",
            sql = $"SELECT id, seq, value FROM {name}",
        }))
        {
            pipelineId = created.RootElement.GetProperty("id").GetString()!;
        }

        // The exactness instrument — see the class doc. Started BEFORE the pipeline is, because a table
        // over a pipeline gets no replay.
        var tableId = await CreateAndStartTableOverPipelineAsync(client, host, "tbl_" + pipelineName, pipelineName);

        (await client.PostAsync($"{host.BaseUrl}/api/pipelines/{pipelineId}/start", content: null))
            .EnsureSuccessStatusCode();

        long totalRowsOut = 0;
        await LiveRest.PollAsync(
            TimeSpan.FromSeconds(60),
            async () =>
            {
                using var doc = await LiveRest.GetJsonAsync(client, $"{host.BaseUrl}/api/pipelines/{pipelineId}/metrics");
                totalRowsOut = doc.RootElement.GetProperty("totalRowsOut").GetInt64();
                return totalRowsOut >= 300;
            },
            () => $"pipeline {pipelineName} never reached 300 totalRowsOut (last seen: {totalRowsOut})",
            host.LogTailText);

        // Every row emitted at least once (no loss on the pipeline's own counter) …
        Assert.True(
            totalRowsOut >= 300,
            $"pipeline emitted {totalRowsOut} rows for a 300-row replay — rows were LOST");

        // … and exactly 300 distinct ids downstream, where re-delivery is idempotent.
        var rows = await LiveRest.SettledRowsAsync(
            client, host.BaseUrl, tableId, 300, TimeSpan.FromSeconds(60), host.LogTailText);
        Assert.Equal(300, rows.TotalRows);
        LiveRest.AssertIdSet(rows, Enumerable.Range(0, 300));

        // The weaker fact /results CAN carry — see this fact's doc comment.
        using var results = await LiveRest.GetJsonAsync(client, $"{host.BaseUrl}/api/pipelines/{pipelineId}/results?limit=100");
        var envelopes = results.RootElement.EnumerateArray().ToList();
        Assert.NotEmpty(envelopes);
        Assert.True(
            envelopes.Count <= 100,
            $"/results returned {envelopes.Count} envelopes for limit=100 — the ring is capacity 100");
        Assert.True(
            envelopes[0].GetProperty("row").TryGetProperty("id", out _),
            "a /results envelope carried no 'id' column — the pipeline's own SELECT list is id, seq, value");
    }

    // ---- shared helpers ----

    private static object EnabledFileSource(string name, string filePath) => new
    {
        name,
        description = "late-consumer file source (enabled before any consumer exists)",
        kind = "file",
        enabled = true,
        fields = LiveRest.Fields(),
        connector = new
        {
            schedule = new { intervalMs = 1000 },
            file = new { path = filePath, format = "ndjson" },
            mapping = LiveRest.Mapping(),
        },
    };

    /// <summary>Waits until the source's own status says it has emitted exactly this many events.
    /// <c>eventsEmittedTotal</c> is the PRODUCER's counter, entirely independent of whether anything
    /// consumed them — which is precisely why it is the right precondition for a late-consumer
    /// test.</summary>
    private static async Task WaitForEmittedAsync(
        HttpClient client, DaprHostProcess host, string sourceName, long expected, TimeSpan timeout)
    {
        long seen = -1;
        await LiveRest.PollAsync(
            timeout,
            async () =>
            {
                using var doc = await LiveRest.GetJsonAsync(client, $"{host.BaseUrl}/api/sources/{sourceName}/status");
                seen = doc.RootElement.TryGetProperty("eventsEmittedTotal", out var v) ? v.GetInt64() : -1;
                return seen == expected;
            },
            () => $"source {sourceName} never reported eventsEmittedTotal == {expected} (last seen: {seen})",
            host.LogTailText);
    }

    /// <summary>Same as <see cref="CreateAndStartTableAsync"/> — the relation named is a PIPELINE rather
    /// than a source, which the SQL cannot tell apart and neither can this helper; it exists separately
    /// only so each call site reads as what it is.</summary>
    private static Task<string> CreateAndStartTableOverPipelineAsync(
        HttpClient client, DaprHostProcess host, string tableName, string pipelineName) =>
        CreateAndStartTableAsync(client, host, tableName, pipelineName);

    private static async Task<string> CreateAndStartTableAsync(
        HttpClient client, DaprHostProcess host, string tableName, string sourceName)
    {
        using var created = await LiveRest.PostOkAsync(client, $"{host.BaseUrl}/api/tables", new
        {
            name = tableName,
            description = "late-consumer table",
            sql = $"SELECT id, seq, value FROM {sourceName} LATEST BY (id)",
        });
        var tableId = created.RootElement.GetProperty("id").GetString()!;
        (await client.PostAsync($"{host.BaseUrl}/api/tables/{tableId}/start", content: null))
            .EnsureSuccessStatusCode();
        return tableId;
    }
}
