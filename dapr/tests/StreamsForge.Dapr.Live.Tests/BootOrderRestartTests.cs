using System.Net;
using System.Text;
using Xunit;

namespace StreamsForge.Dapr.Live.Tests;

/// <summary>
/// Plan 025 D4, live: the Dapr twin of
/// <c>orleans/tests/StreamsForge.Chain.Tests/HostRestartTests.Url_source_rows_re_land_after_a_host_restart</c>
/// — a <c>url</c> source and a <c>LATEST BY</c> table over it, taken to exactly 200 rows, the whole
/// instance restarted, and the table refilled to exactly 200 rows: <b>the SECOND 200 ids</b>, never 400.
///
/// <para><b>Why the dataset GROWS across the restart, and why "the same 200 rows come back" is not a
/// test anyone can write.</b> Three facts compose:</para>
/// <list type="number">
/// <item>A resuming table is reset to EMPTY. <c>TableActor</c> treats a non-empty persisted snapshot as
/// "this is a restart", clears it and marks itself rebuilding — operator internal state cannot be
/// rebuilt from an output snapshot, so the rows have to arrive again over the wire (same decision
/// <c>TableGrain.StartClassicAsync</c> makes on the other flavor).</item>
/// <item>A polled source's dedup keys are PERSISTED with its connector state, so a row whose dedup key
/// was already seen before the restart is never re-emitted.</item>
/// <item>Dapr pub/sub has no replay and the connector's in-memory replay ring dies with the process.</item>
/// </list>
/// <para>Serve the same 200 rows after the restart and the source emits nothing at all, so the
/// reset-to-empty table can never refill — the test would fail identically with and without a working
/// boot pass, which is the one outcome that teaches nothing. The Orleans twin's own class doc reaches
/// this conclusion at length and deviates from its plan's literal wording for exactly this reason; the
/// same deviation is made here, and the first draft of THIS class did it the naive way and sat at 0 of
/// 200. Growing the dataset keeps the intent — the first post-restart poll is the only chance those ids
/// will ever have — and the final <c>totalRows == 200</c> with ids <c>200..399</c> is itself the
/// evidence that the persisted dedup keys suppressed the original batch.</para>
///
/// <para><b>Why a restart is the only way to test the boot pass at all.</b> D4 replaced four independent
/// supervisor sweeps with ONE coordinated resume — Running pipelines, then Running tables (topologically
/// by table input), then enabled sources, per environment — behind a <c>BootGate</c> the sweeps await.
/// That code runs exactly once per process, at startup, against whatever the catalog already holds. A
/// fresh boot only ever resumes the demo seed; a boot over a catalog a TEST built is the only way to
/// watch it resume something whose correct end state the test itself knows. Hence
/// <see cref="DaprHostProcess.RestartAsync"/>: same app-id, same <c>DataDir</c>, Redis database 1
/// deliberately NOT flushed — a reset in the middle would prove re-seeding, not resume.</para>
///
/// <para><b>An in-test <c>HttpListener</c>, not a file, and that is the point.</b> A file source could
/// re-read its file after a restart from the file itself; a <c>url</c> source can only get its rows by
/// making a real outbound HTTP request from the restarted process. The listener therefore doubles as
/// evidence: the request counter it keeps is what tells a failure apart into "the source never resumed"
/// (no post-restart request at all) versus "it resumed and the rows did not land" (requests arrive,
/// table stays empty). It also lets the served body be SWAPPED while the host is down, which is how the
/// dataset grows across the restart.</para>
///
/// <para><b>Not affected by the live-batch lateness defect</b>
/// <see cref="TableOverPipelineTests.A_single_large_live_batch_into_a_pipeline_is_silently_dropped_KNOWN_BUG"/>
/// documents, even though 200 rows arrive in one batch: that defect is a PIPELINE executor rule, and
/// this chain is source → table with no pipeline in it. <c>SourceExactCountTests</c> pushes 700-row
/// batches into a table on this same flavor and passes.</para>
///
/// <para><b>What the log assertions can and cannot say — read this before "fixing" them.</b> This
/// fact asserts (a) the restarted process logged its boot-resume line, naming a non-zero source count,
/// and (b) no supervisor logged the "boot resume pass did not complete" line, which is the ONLY log
/// evidence that the gate was already open when the first sweep ran — i.e. that the supervisors really
/// were gated behind the pass rather than racing it
/// (<c>dapr/src/StreamsForge.Dapr.Host/Services/BootResume.cs</c>, <c>BootGateWait.AwaitBootPassAsync</c>).
/// It deliberately does NOT assert "the boot line appears before the first source poll line", which is
/// how this test was originally specified, for two independent reasons found while writing it:
/// <list type="number">
/// <item>There is no such line to order against. <c>ConnectorActor</c> logs NOTHING at Information for
/// a poll cycle — its only two poll-path log statements are <c>LogWarning</c>s on failure
/// (<c>dapr/src/StreamsForge.Dapr.Host/Actors/ConnectorActor.cs:404</c> and <c>:486</c>), which is
/// correct behaviour for a loop that runs every second.</item>
/// <item>Even if there were, the ordering would be the reverse of the guarantee. The boot-resume line is
/// emitted AFTER the source phase of the pass (<c>CatalogInitializationService.cs:136</c>, after the
/// <c>foreach (var src in phases.Sources)</c> loop), so a source resumed by the pass can legitimately
/// poll BEFORE the line is printed. A test asserting the opposite order would fail against correct
/// behaviour.</item>
/// </list>
/// What D4 actually promises is an order between PHASES inside one pass — consumers before producers —
/// and that promise is asserted here the way it is observable: by the outcome (every row lands after the
/// restart), not by log archaeology.</para>
/// </summary>
[Collection(DaprLiveTestCollection.Name)]
public sealed class BootOrderRestartTests : IAsyncLifetime
{
    private const int FirstBatch = 200;
    private const int SecondBatch = 200;

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
        _host = new DaprHostProcess("boot-order-restart");
        _host.Start();
        await _host.WaitHealthyAsync();
    }

    public async Task DisposeAsync()
    {
        if (_host is not null)
        {
            await _host.DisposeAsync();
        }
    }

    [Fact]
    public async Task Url_source_rows_re_land_after_a_restart_and_the_boot_pass_ran()
    {
        Assert.True(_skipReason is null, $"skipped: {_skipReason}");
        var host = _host!;

        using var server = new CountingJsonArrayServer(BuildJsonArray(0, FirstBatch));
        var suffix = Guid.NewGuid().ToString("n")[..8];
        var sourceName = "boot_url_" + suffix;
        var tableName = "boot_tbl_" + suffix;

        using var client = await host.LoginAsync();

        // Source first (disabled), then the subscribed table, then enable — the ordering
        // SourceExactCountTests documents. This test is about the RESTART, so the pre-restart leg must
        // not itself depend on replay.
        await LiveRest.PostOkAsync(client, $"{host.BaseUrl}/api/sources", UrlSource(sourceName, server.Port, enabled: false));

        string tableId;
        using (var created = await LiveRest.PostOkAsync(client, $"{host.BaseUrl}/api/tables", new
        {
            name = tableName,
            description = "boot-order restart table",
            sql = $"SELECT id, seq, value FROM {sourceName} LATEST BY (id)",
        }))
        {
            tableId = created.RootElement.GetProperty("id").GetString()!;
        }
        (await client.PostAsync($"{host.BaseUrl}/api/tables/{tableId}/start", content: null)).EnsureSuccessStatusCode();

        var putResp = await client.PutAsync(
            $"{host.BaseUrl}/api/sources/{sourceName}",
            System.Net.Http.Json.JsonContent.Create(UrlSource(sourceName, server.Port, enabled: true)));
        Assert.True(
            putResp.IsSuccessStatusCode,
            $"PUT api/sources/{sourceName} -> {(int)putResp.StatusCode}: {await putResp.Content.ReadAsStringAsync()}");

        var before = await LiveRest.SettledRowsAsync(
            client, host.BaseUrl, tableId, FirstBatch, TimeSpan.FromSeconds(60), host.LogTailText);
        Assert.Equal(FirstBatch, before.TotalRows);
        LiveRest.AssertIdSet(before, Enumerable.Range(0, FirstBatch));

        // ---- restart: same app-id, same DataDir, Redis database 1 untouched ----
        // RestartAsync() spelled out, so ClearLogTail() can sit between the two halves: the drained tail
        // is ONE list across a stop/start pair, and the first boot logged a boot-resume line of its own.
        // Without the clear, the log assertions below would be satisfied by the pre-restart line and
        // could not fail.
        var requestsBeforeRestart = server.RequestCount;
        await host.StopAsync();

        // The dataset GROWS, and the swap happens while the host is DOWN — see the class doc. Doing it a
        // moment earlier would let the running source deliver ids 200..399 before the restart, which
        // would put the table at 400 and leave the post-restart leg with nothing new to receive: the
        // test would then measure the pre-restart poll, not the resume.
        server.SetBody(BuildJsonArray(0, FirstBatch + SecondBatch));

        host.ClearLogTail();
        host.Start();
        await host.WaitHealthyAsync();

        using var clientAfter = await host.LoginAsync();

        // 90s, not 60: the post-restart leg waits for the resumed connector's FIRST poll, and under
        // whole-solution load an outbound fetch that loses its race with the CPU-starved in-test
        // listener takes a backoff before retrying. Exactly the reasoning AGENTS.md records for the
        // Orleans twin of this test, which had its deadline widened for the same cause.
        //
        // Exactly SecondBatch rows, and exactly ids 200..399 — never 400. That the first 200 do NOT come
        // back is the evidence that the persisted dedup keys suppressed them, i.e. that these rows had
        // exactly one chance to land and took it.
        var after = await LiveRest.SettledRowsAsync(
            clientAfter, host.BaseUrl, tableId, SecondBatch, TimeSpan.FromSeconds(90), host.LogTailText);
        Assert.Equal(SecondBatch, after.TotalRows);
        LiveRest.AssertIdSet(after, Enumerable.Range(FirstBatch, SecondBatch));

        Assert.True(
            server.RequestCount > requestsBeforeRestart,
            $"the restarted host never fetched the url source again (requests: {requestsBeforeRestart} before, "
          + $"{server.RequestCount} after) — the rows in the table would then be a stale snapshot, not a resume");

        // ---- the boot pass itself, in the RESTARTED process's own drained output ----
        var log = host.LogTailText();
        var bootLine = log.Split('\n').FirstOrDefault(l => l.Contains("boot resume for environment", StringComparison.Ordinal));
        Assert.True(
            bootLine is not null,
            "the restarted process never logged its boot-resume line — the coordinated D4 pass did not run.\n"
          + $"--- host log tail ---\n{log}");
        // It must have resumed SOMETHING: the test's own url source is enabled and the demo seed's
        // generators are too, so "0 source(s)" would mean the pass ran over an empty view of the catalog
        // — a pass that logs itself while resuming nothing is the failure this guards against.
        Assert.DoesNotContain(" 0 source(s)", bootLine!, StringComparison.Ordinal);
        Assert.Contains("source(s)", bootLine!, StringComparison.Ordinal);

        Assert.DoesNotContain(
            "the boot resume pass did not complete within",
            log,
            StringComparison.Ordinal);
    }

    private static object UrlSource(string name, int port, bool enabled) => new
    {
        name,
        description = "boot-order restart url source",
        kind = "url",
        enabled,
        fields = LiveRest.Fields(),
        connector = new
        {
            schedule = new { intervalMs = 1000 },
            url = new { url = $"http://127.0.0.1:{port}/rows", format = "json" },
            mapping = LiveRest.Mapping(),
        },
    };

    private static string BuildJsonArray(int firstId, int count) =>
        "[" + string.Join(",", Enumerable.Range(firstId, count).Select(LiveRest.NdjsonLine)) + "]";

    /// <summary>A JSON endpoint whose body can be swapped AND that COUNTS the requests it served. The
    /// counter is the whole reason this is not a straight copy of <c>SourceExactCountTests</c>'
    /// <c>VolatileJsonArrayServer</c>: after a restart, "did the source resume at all" and "did it resume
    /// but lose the rows" are different failures with different causes, and only the request count
    /// separates them.</summary>
    private sealed class CountingJsonArrayServer : IDisposable
    {
        private readonly HttpListener _listener;
        private readonly Thread _thread;
        private volatile string _body;
        private int _requestCount;

        public int Port { get; }

        public int RequestCount => Volatile.Read(ref _requestCount);

        public CountingJsonArrayServer(string body)
        {
            _body = body;
            Port = GetFreePort();
            _listener = new HttpListener();
            _listener.Prefixes.Add($"http://127.0.0.1:{Port}/");
            _listener.Start();
            _thread = new Thread(Loop) { IsBackground = true };
            _thread.Start();
        }

        public void SetBody(string body) => _body = body;

        private void Loop()
        {
            while (true)
            {
                HttpListenerContext ctx;
                try
                {
                    ctx = _listener.GetContext();
                }
                catch (Exception)
                {
                    return; // listener stopped/disposed
                }

                Interlocked.Increment(ref _requestCount);
                try
                {
                    var bytes = Encoding.UTF8.GetBytes(_body);
                    ctx.Response.ContentType = "application/json";
                    ctx.Response.ContentLength64 = bytes.Length;
                    ctx.Response.OutputStream.Write(bytes, 0, bytes.Length);
                    ctx.Response.OutputStream.Close();
                }
                catch (Exception)
                {
                    // best-effort — racing Dispose() closing the response stream is not this test's concern
                }
            }
        }

        private static int GetFreePort()
        {
            var probe = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0);
            probe.Start();
            var port = ((System.Net.IPEndPoint)probe.LocalEndpoint).Port;
            probe.Stop();
            return port;
        }

        public void Dispose()
        {
            try
            {
                _listener.Stop();
                _listener.Close();
            }
            catch
            {
                // best-effort
            }
        }
    }
}
