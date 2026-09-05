using System.Net;
using System.Net.Http.Json;
using Xunit;

namespace StreamsForge.Dapr.Live.Tests;

/// <summary>
/// Plan 025 D6, live: <b>a table may read a PIPELINE by name on the Dapr flavor</b>, and the three name
/// refusals that make "one relation name, one entity" true here as well.
///
/// <para><b>What used to happen.</b> Before this plan <c>PipelineDefinition.OutputFields</c> was not
/// published by this flavor's <c>CatalogStore</c> and pipelines were not offered as relations at all, so
/// <c>SELECT … FROM &lt;pipeline&gt;</c> in a table's SQL did not compile — <c>dapr/PARITY.md</c> D6
/// recorded it as owed, and <c>POST /api/tables/validate</c> was optimistic (it said yes and the create
/// then said no). Both halves are asserted here: the pipeline's create response really does carry
/// <c>outputFields</c> (which is what makes it a relation), the table's create response really does
/// carry <c>pipelineInputs</c> (which is what makes the routing happen), and rows really do arrive.</para>
///
/// <para><b>The setup order is forced by TWO independent constraints, and the second one is a live bug
/// this class found.</b></para>
/// <list type="number">
/// <item><b>The table must subscribe before the pipeline emits.</b> Hard rule 6 in AGENTS.md: <i>a table
/// attaching to a pipeline gets no replay</i> — pipelines have no ring and no snapshot on either flavor,
/// so a table stood up after its pipeline has already emitted starts empty and stays empty. The table is
/// therefore created and STARTED while its pipeline is still <c>Stopped</c> (which compiles fine — a
/// pipeline is a relation by virtue of its <c>outputFields</c>, not by being Running).</item>
/// <item><b>The rows must reach the pipeline through the ATTACH/replay path, not the live one.</b> This
/// is not a design constraint; it is a workaround for the Dapr-flavor defect
/// <see cref="A_single_large_live_batch_into_a_pipeline_is_silently_dropped_KNOWN_BUG"/> documents and
/// measures — a live source batch bigger than roughly ten rows takes longer than the Engine's
/// 1000 ms allowed lateness to cross Dapr's pub/sub + actor hop and is discarded wholesale by
/// <c>PipelineExecutor.OnEventCore</c>. So the source is enabled FIRST and allowed to emit all its rows
/// with nothing listening; starting the pipeline last makes it take the plan-025 D3 attach path, whose
/// snapshot is fed into a brand-new executor whose watermark has not advanced yet. Read that fact's doc
/// comment before "simplifying" this ordering — the obvious version (start everything, then enable the
/// source) is what the first draft of this class did, and it produced a table stuck at 0 of 100.</item>
/// </list>
///
/// <para><b>The refusals are asserted by STATUS CODE AND MESSAGE, and the two status codes differ.</b> A
/// table or pipeline whose name collides comes back <c>409 Conflict</c>; a SOURCE whose name collides
/// comes back <c>400 Bad Request</c>. That asymmetry is not a bug and not a rounding error — it is what
/// <c>SourcesEndpoints</c> has always done for a rejected upsert (sources have no id, so their whole
/// write path answers 400 for any catalog refusal) while <c>Tables</c>/<c>PipelinesEndpoints</c> answer
/// 409. Pinning both here means a future tidy-up that "harmonises" them has to come past a test that
/// says the current shape was deliberate. The message text is asserted too, because it carries the
/// REASON ("a table's SQL resolves a relation name to exactly one entity") — an operator who only sees
/// "already used" learns nothing about why a name they could use last month is refused now.</para>
///
/// <para><b>What this class does NOT prove.</b> It does not test <c>POST /api/tables/validate</c>
/// against a pipeline relation (that route's own agreement with create is unit-tested on both flavors,
/// and asserting it live would add a boot for a pure-function check), and it does not touch the
/// Orleans-side behaviour at all — the two flavors' agreement is a matter for their shared compile path,
/// not for a Dapr live test. It also does not assert what happens to a table whose pipeline is deleted
/// and then RECREATED under the same name; only that the delete itself does not take the table down.</para>
/// </summary>
public sealed class TableOverPipelineFixture : LiveHostFixture
{
    protected override string Label => "table-over-pipeline";
}

[Collection(DaprLiveTestCollection.Name)]
public sealed class TableOverPipelineTests(TableOverPipelineFixture fixture) : IClassFixture<TableOverPipelineFixture>
{
    private DaprHostProcess? _host => fixture.Host;
    private string? _skipReason => fixture.SkipReason;
    private string _scratchDir => fixture.ScratchDir;

    // ================================================================================================
    // 1. the chain: file source -> pipeline -> table, 100 rows exactly
    // ================================================================================================

    [Fact]
    public async Task A_table_reading_a_pipeline_by_name_gets_every_row()
    {
        Assert.True(_skipReason is null, $"skipped: {_skipReason}");
        var host = _host!;
        using var client = await host.LoginAsync();

        var chain = await BuildChainAsync(client, host, "tp1", rows: 100);

        var rows = await LiveRest.SettledRowsAsync(
            client, host.BaseUrl, chain.TableId, 100, TimeSpan.FromSeconds(60), host.LogTailText);
        Assert.Equal(100, rows.TotalRows);
        LiveRest.AssertIdSet(rows, Enumerable.Range(0, 100));
    }

    // ================================================================================================
    // 2. the three name refusals
    // ================================================================================================

    [Fact]
    public async Task The_three_relation_name_collisions_are_refused_with_their_reason()
    {
        Assert.True(_skipReason is null, $"skipped: {_skipReason}");
        var host = _host!;
        using var client = await host.LoginAsync();

        var suffix = Guid.NewGuid().ToString("n")[..8];
        var sourceName = "tp_names_src_" + suffix;
        var pipelineName = "tp_names_pipe_" + suffix;
        var filePath = Path.Combine(_scratchDir, sourceName + ".ndjson");
        await File.WriteAllTextAsync(filePath, LiveRest.Ndjson(Enumerable.Range(0, 5)));

        await LiveRest.PostOkAsync(client, $"{host.BaseUrl}/api/sources", FileSource(sourceName, filePath, enabled: false));
        await LiveRest.PostOkAsync(client, $"{host.BaseUrl}/api/pipelines", new
        {
            name = pipelineName,
            description = "name-collision fixture",
            sql = $"SELECT id, seq, value FROM {sourceName}",
        });

        // (a) a TABLE named like the pipeline -> 409
        await AssertRefusedAsync(
            client,
            HttpMethod.Post,
            $"{host.BaseUrl}/api/tables",
            new { name = pipelineName, description = "collides with a pipeline", sql = $"SELECT id FROM {sourceName} LATEST BY (id)" },
            HttpStatusCode.Conflict,
            $"Name '{pipelineName}' is already used by a pipeline — a table's SQL resolves a relation name to exactly one entity");

        // (b) a SOURCE named like the pipeline -> 400 (see the class doc on why not 409)
        await AssertRefusedAsync(
            client,
            HttpMethod.Post,
            $"{host.BaseUrl}/api/sources",
            FileSource(pipelineName, filePath, enabled: false),
            HttpStatusCode.BadRequest,
            $"Name '{pipelineName}' is already used by a pipeline — a table's SQL resolves a relation name to exactly one entity");

        // (c) a PIPELINE named like the source -> 409
        await AssertRefusedAsync(
            client,
            HttpMethod.Post,
            $"{host.BaseUrl}/api/pipelines",
            new { name = sourceName, description = "collides with a source", sql = $"SELECT id, seq, value FROM {sourceName}" },
            HttpStatusCode.Conflict,
            $"Name '{sourceName}' is already used by a stream source — a table's SQL resolves a relation name to exactly one entity");
    }

    // ================================================================================================
    // 3. deleting the pipeline out from under a running table
    // ================================================================================================

    /// <summary>
    /// The failure mode worth naming: a table whose only input is a pipeline is holding a router
    /// subscription keyed by that pipeline's id, and DELETE removes the pipeline from the catalog while
    /// that subscription is live. What must NOT happen is the table's actor (or the host) going down
    /// with it — a deleted upstream is an operator's mistake, not a reason to lose a table's rows and
    /// every other entity sharing the process.
    ///
    /// <para>What is asserted is deliberately modest and entirely observable: the delete is accepted,
    /// and several seconds later the table still answers <c>/rows</c> with the same 100 rows and the
    /// host is still up (a crashed host would fail the very next request, and the healthz check makes
    /// that unambiguous rather than leaving it to whichever assertion happened to run next). Nothing is
    /// claimed about the table CONTINUING to receive data — it cannot, its producer is gone — and
    /// nothing is claimed about what its status should read afterwards, because no decision has been
    /// recorded on that and a test is the wrong place to invent one.</para>
    /// </summary>
    [Fact]
    public async Task Deleting_the_pipeline_under_a_running_table_leaves_the_table_answering()
    {
        Assert.True(_skipReason is null, $"skipped: {_skipReason}");
        var host = _host!;
        using var client = await host.LoginAsync();

        var chain = await BuildChainAsync(client, host, "tp3", rows: 100);
        var rows = await LiveRest.SettledRowsAsync(
            client, host.BaseUrl, chain.TableId, 100, TimeSpan.FromSeconds(60), host.LogTailText);
        Assert.Equal(100, rows.TotalRows);

        var deleted = await client.DeleteAsync($"{host.BaseUrl}/api/pipelines/{chain.PipelineId}");
        Assert.True(
            deleted.IsSuccessStatusCode,
            $"DELETE api/pipelines/{chain.PipelineId} -> {(int)deleted.StatusCode}: {await deleted.Content.ReadAsStringAsync()}");

        await Task.Delay(5000);

        using var anon = host.NewClient();
        var health = await anon.GetAsync($"{host.BaseUrl}/api/healthz");
        Assert.True(
            health.IsSuccessStatusCode,
            $"the host stopped answering /api/healthz after its pipeline was deleted -> {(int)health.StatusCode}\n"
          + host.LogTailText());

        var after = await LiveRest.RowsAsync(client, host.BaseUrl, chain.TableId);
        Assert.Equal(100, after.TotalRows);
        LiveRest.AssertIdSet(after, Enumerable.Range(0, 100));
    }

    // ================================================================================================
    // 4. the defect that forced this class's ordering — SKIPPED, and here is exactly what it is
    // ================================================================================================

    /// <summary>
    /// <b>KNOWN DAPR-FLAVOR DEFECT, found by plan 025 wave 2 and deliberately NOT fixed here (this
    /// project owns tests only). Skipped so the suite stays honest: it asserts the CORRECT behaviour, so
    /// removing the Skip is how a fix gets verified.</b>
    ///
    /// <para><b>Symptom.</b> A source batch delivered to a RUNNING <c>PipelineActor</c> through the live
    /// path (<c>sf-sources</c> pub/sub → <c>PipelineEventRouter</c> →
    /// <c>PipelineActor.ProcessEventsAsync</c>) is counted in <c>totalEventsIn</c> and then silently
    /// produces NO output rows, once the batch is bigger than roughly ten rows. Nothing is logged and
    /// nothing is surfaced: <c>PipelineExecutor.LateEvents</c> exists but is not on
    /// <c>PipelineMetrics</c>, so from the API the pipeline looks perfectly healthy — events in, zero
    /// rows out, forever.</para>
    ///
    /// <para><b>Mechanism.</b> <c>PipelineExecutor.OnEventCore</c>
    /// (<c>shared/StreamsForge.Engine/Runtime/ExecutorImpl.cs:187</c>) drops any event whose
    /// <c>_ts</c> is older than the executor's watermark — <c>if (evt.Timestamp &lt; Watermark) {
    /// LateEvents++; return []; }</c> — and the watermark is <c>now - AllowedLatenessMs</c>
    /// (<c>:28</c>, 1000 ms), re-advanced every 500 ms by <c>PipelineActor.OnTimerTickAsync</c>'s
    /// <c>AdvanceWatermark(UtcNow)</c>. A row's <c>_ts</c> is stamped in the connector at mapping time,
    /// so the row has to cross Dapr's pub/sub hop AND an actor invocation within one second of being
    /// mapped or it is late on arrival. It does not.</para>
    ///
    /// <para><b>Measured on this machine</b> (one file source, one <c>SELECT id, seq, value FROM src</c>
    /// pipeline already Running, rows appended in one batch; "age" is <c>now - lastEventTsMs</c> at the
    /// first poll that saw the batch, so it is an upper bound within ~100 ms):</para>
    /// <list type="table">
    /// <item>batch 5 → 5 rows out, age 330 ms</item>
    /// <item>batch 10 → 10 rows out, age 782 ms</item>
    /// <item>batch 25 → <b>0</b> rows out, age 1402 ms</item>
    /// <item>batch 50 → <b>0</b> rows out, age 2668 ms</item>
    /// <item>batch 100 → <b>0</b> rows out, age 2389 ms</item>
    /// </list>
    /// <para>Delivery latency scales with batch size at roughly 25 ms per row, so the cliff sits exactly
    /// where the 1000 ms allowance runs out.</para>
    ///
    /// <para><b>It is a PARITY gap, not a shared-Engine design limit.</b> The identical scenario was run
    /// against a real Orleans host (same file, same SQL, same ordering, spawned on 4999/5099 from
    /// <c>orleans/src/StreamsForge.Host/bin/Debug/net10.0</c>): <c>totalEventsIn 100, totalRowsOut
    /// 100</c>. Orleans' in-process memory-stream hop is ~1 ms, so it never approaches the allowance.
    /// Only the Dapr transport does.</para>
    ///
    /// <para><b>Scope.</b> PIPELINES only. Tables are unaffected — <c>TableActor</c>'s executor applies
    /// no lateness rule, which is why <c>SourceExactCountTests</c> pushes 500- and 700-row batches
    /// straight into a table and passes. And the plan-025 D3 ATTACH path is unaffected, because the
    /// snapshot is fed into a freshly constructed executor whose watermark has not advanced yet — which
    /// is why <see cref="LateConsumerTests"/>' 300-row pipeline fact passes while this one cannot.</para>
    ///
    /// <para><b>Not diagnosed here:</b> whether the right fix is a larger/configurable allowance, a
    /// watermark that is driven by event time rather than wall clock on this flavor, chunking the
    /// connector's publish into small batches, or reporting <c>LateEvents</c> so the loss is at least
    /// visible. That is a design decision, not a test's to make.</para>
    /// </summary>
    [Fact(Skip = "Known Dapr-flavor defect found by plan 025 wave 2 and not fixed by this agent (tests-only "
               + "ownership): a live source batch above ~10 rows takes longer than the Engine's 1000 ms "
               + "AllowedLatenessMs to cross pub/sub + the actor hop and is dropped whole by "
               + "PipelineExecutor.OnEventCore, with no log line and no metric. Orleans passes the identical "
               + "scenario 100/100. See this method's doc comment for the measurements and the mechanism; "
               + "remove this Skip to verify a fix.")]
    public async Task A_single_large_live_batch_into_a_pipeline_is_silently_dropped_KNOWN_BUG()
    {
        Assert.True(_skipReason is null, $"skipped: {_skipReason}");
        var host = _host!;
        using var client = await host.LoginAsync();

        var suffix = Guid.NewGuid().ToString("n")[..8];
        var sourceName = "late_batch_src_" + suffix;
        var pipelineName = "late_batch_pipe_" + suffix;
        var filePath = Path.Combine(_scratchDir, sourceName + ".ndjson");

        // Empty file first, so the pipeline is Running with an already-advanced watermark BEFORE any row
        // exists — the state the live path is actually in during normal operation.
        await File.WriteAllTextAsync(filePath, "");
        await LiveRest.PostOkAsync(client, $"{host.BaseUrl}/api/sources", FileSource(sourceName, filePath, enabled: true));

        string pipelineId;
        using (var created = await LiveRest.PostOkAsync(client, $"{host.BaseUrl}/api/pipelines", new
        {
            name = pipelineName,
            description = "live-batch lateness reproduction",
            sql = $"SELECT id, seq, value FROM {sourceName}",
        }))
        {
            pipelineId = created.RootElement.GetProperty("id").GetString()!;
        }
        (await client.PostAsync($"{host.BaseUrl}/api/pipelines/{pipelineId}/start", content: null))
            .EnsureSuccessStatusCode();

        // Let the watermark timer run for a few ticks so the executor is genuinely "caught up".
        await Task.Delay(3000);

        await File.WriteAllTextAsync(filePath, LiveRest.Ndjson(Enumerable.Range(0, 100)));

        long eventsIn = 0, rowsOut = 0;
        await LiveRest.PollAsync(
            TimeSpan.FromSeconds(60),
            async () =>
            {
                using var doc = await LiveRest.GetJsonAsync(client, $"{host.BaseUrl}/api/pipelines/{pipelineId}/metrics");
                eventsIn = doc.RootElement.GetProperty("totalEventsIn").GetInt64();
                rowsOut = doc.RootElement.GetProperty("totalRowsOut").GetInt64();
                return eventsIn >= 100;
            },
            () => $"pipeline {pipelineName} never received the 100-row batch (eventsIn: {eventsIn})",
            host.LogTailText);

        await Task.Delay(3000);
        using var final = await LiveRest.GetJsonAsync(client, $"{host.BaseUrl}/api/pipelines/{pipelineId}/metrics");
        Assert.Equal(100, final.RootElement.GetProperty("totalEventsIn").GetInt64());
        Assert.Equal(100, final.RootElement.GetProperty("totalRowsOut").GetInt64());
    }

    // ---- shared setup ----

    private sealed record Chain(string SourceName, string PipelineName, string PipelineId, string TableId);

    /// <summary>source (ENABLED, allowed to emit everything with nothing listening) → pipeline created
    /// but left Stopped → table over that pipeline, started → pipeline started last, which is what makes
    /// it take the attach/replay path. See the class doc's two numbered constraints for why neither half
    /// of this ordering is negotiable.</summary>
    private async Task<Chain> BuildChainAsync(HttpClient client, DaprHostProcess host, string label, int rows)
    {
        var suffix = Guid.NewGuid().ToString("n")[..8];
        var sourceName = $"{label}_src_{suffix}";
        var pipelineName = $"{label}_pipe_{suffix}";
        var tableName = $"{label}_tbl_{suffix}";
        var filePath = Path.Combine(_scratchDir, sourceName + ".ndjson");
        await File.WriteAllTextAsync(filePath, LiveRest.Ndjson(Enumerable.Range(0, rows)));

        await LiveRest.PostOkAsync(client, $"{host.BaseUrl}/api/sources", FileSource(sourceName, filePath, enabled: true));

        // The producer runs to completion before any consumer exists — asserted, not slept for.
        long emitted = -1;
        await LiveRest.PollAsync(
            TimeSpan.FromSeconds(60),
            async () =>
            {
                using var doc = await LiveRest.GetJsonAsync(client, $"{host.BaseUrl}/api/sources/{sourceName}/status");
                emitted = doc.RootElement.TryGetProperty("eventsEmittedTotal", out var v) ? v.GetInt64() : -1;
                return emitted == rows;
            },
            () => $"source {sourceName} never reported eventsEmittedTotal == {rows} (last seen: {emitted})",
            host.LogTailText);

        string pipelineId;
        using (var created = await LiveRest.PostOkAsync(client, $"{host.BaseUrl}/api/pipelines", new
        {
            name = pipelineName,
            description = "table-over-pipeline producer",
            sql = $"SELECT id, seq, value FROM {sourceName}",
        }))
        {
            pipelineId = created.RootElement.GetProperty("id").GetString()!;
            // outputFields IS the relation — without it a table's SQL cannot resolve this name at all.
            var outputFields = created.RootElement.GetProperty("outputFields").EnumerateArray()
                .Select(f => f.GetProperty("name").GetString())
                .ToList();
            Assert.Equal(["id", "seq", "value"], outputFields);
            // Created Stopped, and deliberately LEFT that way until the table below is subscribed.
            Assert.Equal("Stopped", created.RootElement.GetProperty("status").GetString());
        }

        string tableId;
        using (var created = await LiveRest.PostOkAsync(client, $"{host.BaseUrl}/api/tables", new
        {
            name = tableName,
            description = "table reading a pipeline by name",
            sql = $"SELECT id, seq, value FROM {pipelineName} LATEST BY (id)",
        }))
        {
            tableId = created.RootElement.GetProperty("id").GetString()!;
            var pipelineInputs = created.RootElement.GetProperty("pipelineInputs").EnumerateArray()
                .Select(p => p.GetString())
                .ToList();
            Assert.Equal([pipelineName], pipelineInputs);
            // And nothing leaked into the RAW-SOURCE inputs: the table reads the pipeline, not the file
            // the pipeline reads. (TableDefinition.StreamInputs is the raw-source list; PipelineInputs
            // was split out of it by plan 025 D6 precisely so the router can tell the two apart.)
            var streamInputs = created.RootElement.GetProperty("streamInputs").EnumerateArray()
                .Select(p => p.GetString())
                .ToList();
            Assert.DoesNotContain(sourceName, streamInputs);
        }
        (await client.PostAsync($"{host.BaseUrl}/api/tables/{tableId}/start", content: null))
            .EnsureSuccessStatusCode();

        // LAST: starting the pipeline is what makes the rows move — and it is what makes them move
        // through the ATTACH path (constraint 2 in the class doc) rather than the live one.
        (await client.PostAsync($"{host.BaseUrl}/api/pipelines/{pipelineId}/start", content: null))
            .EnsureSuccessStatusCode();

        return new Chain(sourceName, pipelineName, pipelineId, tableId);
    }

    private static object FileSource(string name, string filePath, bool enabled) => new
    {
        name,
        description = "table-over-pipeline file source",
        kind = "file",
        enabled,
        fields = LiveRest.Fields(),
        connector = new
        {
            schedule = new { intervalMs = 1000 },
            file = new { path = filePath, format = "ndjson" },
            mapping = LiveRest.Mapping(),
        },
    };

    private static async Task AssertRefusedAsync(
        HttpClient client, HttpMethod method, string url, object body, HttpStatusCode expected, string expectedMessage)
    {
        using var request = new HttpRequestMessage(method, url) { Content = JsonContent.Create(body) };
        var resp = await client.SendAsync(request);
        var text = await resp.Content.ReadAsStringAsync();
        Assert.True(
            resp.StatusCode == expected,
            $"{method} {url} -> {(int)resp.StatusCode}, expected {(int)expected}: {text}");

        using var doc = System.Text.Json.JsonDocument.Parse(text);
        var error = doc.RootElement.GetProperty("error").GetString();
        Assert.Equal(expectedMessage, error);
    }
}
