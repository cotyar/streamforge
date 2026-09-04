using System.Diagnostics;
using System.Net;
using System.Net.Http.Json;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using Xunit;

namespace StreamsForge.Chain.Tests;

/// <summary>
/// Plan "glittery-brewing-feather" Track A / E.6: the boot-order test. A <c>url</c> source and a table
/// over it are created on one host, the host is killed, and the SAME data directory is started again
/// on the SAME ports — and the table must be full again.
///
/// <para><b>Why this is meaningful, and what exactly it asserts.</b> Three facts about resume make the
/// row count after a restart a direct read-out of boot ORDER:</para>
/// <list type="number">
/// <item>A resuming table is reset to EMPTY. <c>TableGrain.StartClassicAsync</c> detects a non-empty
/// persisted snapshot, marks <c>_rebuilding</c> and clears it — operator internal state cannot be
/// rebuilt from a snapshot, so the snapshot is not a shortcut back to full. The rows have to arrive
/// again, over the stream.</item>
/// <item>Orleans memory streams have no replay, and the in-memory replay ring this plan adds for late
/// consumers dies with the process. So the ONLY rows a resumed table can receive are rows its source
/// publishes AFTER the table has subscribed.</item>
/// <item>A polled source's dedup keys and ledger are persisted (<c>ConnectorGrainState.DedupKeys</c>,
/// written every cycle), so a row whose dedup key was already seen before the restart is never
/// re-emitted. The source's dataset therefore GROWS across the restart in this test — the second
/// batch of ids has never been seen, so the first post-restart poll is the one and only chance for
/// them to reach the table.</item>
/// </list>
/// <para>Put together: if the registry resumes the source before the table, the source's first poll
/// (due immediately — a resuming connector's overdue timer fires at once) publishes the new rows into
/// a stream nobody is listening to, and they are gone permanently. The table then sits at zero
/// forever. If consumers resume first, the table is subscribed when that poll lands and ends up
/// holding exactly the new ids. The assertion is precisely "the table subscribed before its source
/// polled, and nothing reset it afterwards".</para>
///
/// <para><b>Deviation from the plan's literal shape, and why.</b> Plan E.6 specifies serving the SAME
/// 200 rows before and after the restart and asserting the table returns to 200. That cannot pass on
/// any version of the code, because of point 3 above: those 200 dedup keys are persisted, so the
/// post-restart source emits nothing at all and the (reset-to-empty) table can never refill — the
/// test would fail identically with and without the fix, which is the one outcome that teaches
/// nothing. Growing the dataset is the smallest change that keeps the plan's intent (the first
/// post-restart poll is the only chance) while being satisfiable. The final assertion is
/// <c>totalRows == 200</c> AND the id set <c>200..399</c>, never 400 — which is itself the evidence
/// that the persisted dedup keys did suppress the original batch.</para>
///
/// <para><b>Honesty note — this is not a deterministic reproduction of the pre-fix bug.</b> Before the
/// section-E change, <c>RegistryGrain.EnsureInitializedAsync</c> started sources before pipelines and
/// tables AND ran twice at boot with no latch. But the window is only as wide as the gap between the
/// two loops, and the poll interval is 1000ms (<c>ScheduleCalc.MinIntervalMs</c> — the floor; anything
/// smaller is invalid), so on a fast machine both boot passes routinely complete inside a single poll
/// interval and the pre-fix code passes this test too. Measured in this worktree against the reverted
/// <c>RegistryGrain</c> (only that file reverted, host rebuilt, test run three times):
/// <b>3 of 3 passed</b> — i.e. the pre-fix code passed every time here. What this test IS, therefore,
/// is a regression guard on the fixed ordering at the process level (where the boot path actually
/// runs — the grain-reactivation tests that could probe it in a <c>TestCluster</c> are among the six
/// known-broken <c>CodecNotFoundException</c> tests), not a demonstration that the old code was
/// always wrong. Reported as observed rather than claimed.</para>
///
/// <para>Ports 9799/9899 REST/gRPC and 11799/30799 silo — disjoint from the two-host chain fixture's,
/// but in the same xunit collection so only one class's hosts are alive at a time.</para>
/// </summary>
[Collection(ChainHostCollection.Name)]
public sealed class HostRestartTests : IAsyncLifetime
{
    private const int HttpPort = 9799;
    private const int GrpcPort = 9899;
    private const int SiloPort = 11799;
    private const int GatewayPort = 30799;

    private const string SourceName = "restart_src";
    private const string TableName = "restart_tbl";
    private const int FirstBatch = 200;
    private const int SecondBatch = 200;

    private HostProcess? _host;
    private HttpListener? _listener;
    private Task? _listenerLoop;
    private int _listenerPort;
    private string? _skipReason;

    /// <summary>The rows the in-test HTTP server is currently serving, as a whole JSON array body.
    /// Volatile because the listener loop reads it on its own thread while the test swaps it.</summary>
    private volatile string _body = "[]";

    public Task InitializeAsync()
    {
        _skipReason = HostProcess.Preflight(HttpPort, GrpcPort, SiloPort, GatewayPort);
        if (_skipReason is not null)
        {
            return Task.CompletedTask;
        }

        _listenerPort = GetFreePort();
        _body = RowsJson(0, FirstBatch);
        try
        {
            _listener = new HttpListener();
            _listener.Prefixes.Add($"http://127.0.0.1:{_listenerPort}/");
            _listener.Start();
            _listenerLoop = Task.Run(ServeAsync);
        }
        catch (Exception ex)
        {
            _skipReason = $"could not start the in-test HTTP server on 127.0.0.1:{_listenerPort}: {ex.Message}";
            return Task.CompletedTask;
        }

        _host = new HostProcess("restart", HttpPort, GrpcPort, SiloPort, GatewayPort);
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        if (_host is not null)
        {
            await _host.DisposeAsync();
        }
        try
        {
            _listener?.Stop();
            _listener?.Close();
        }
        catch
        {
            // best-effort
        }
        if (_listenerLoop is not null)
        {
            try
            {
                await _listenerLoop.WaitAsync(TimeSpan.FromSeconds(5));
            }
            catch
            {
                // best-effort
            }
        }
    }

    [Fact]
    public async Task Url_source_rows_re_land_after_a_host_restart()
    {
        Assert.True(_skipReason is null, $"skipped: {_skipReason}");
        var host = _host!;

        host.Start();
        await host.WaitHealthyAsync();

        string tableId;
        using (var client = await host.LoginAsync())
        {
            await PostOkAsync(client, "api/sources", new
            {
                name = SourceName,
                description = "restart test url source",
                kind = "url",
                enabled = true,
                fields = FieldsJson(),
                connector = new
                {
                    schedule = new { intervalMs = 1000 },
                    url = new { url = $"http://127.0.0.1:{_listenerPort}/rows" },
                    mapping = new
                    {
                        itemsPath = "$",
                        dedupKeyField = "id",
                        fields = MappingFieldsJson(),
                    },
                },
            });

            using var created = await PostOkAsync(client, "api/tables", new
            {
                name = TableName,
                description = "restart test table",
                sql = $"SELECT id, seq, value FROM {SourceName} LATEST BY (id)",
            });
            tableId = created.RootElement.GetProperty("id").GetString()!;

            (await client.PostAsync($"api/tables/{tableId}/start", content: null)).EnsureSuccessStatusCode();

            await PollAsync(
                TimeSpan.FromSeconds(45),
                async () => await TotalAsync(client, tableId) == FirstBatch,
                $"table {TableName} never reached {FirstBatch} rows before the restart",
                host);
        }

        // The dataset GROWS across the restart: ids 200..399 have never been seen, so the persisted
        // dedup keys do not suppress them and the first post-restart poll is their only delivery. See
        // the class doc's point 3 — with an unchanged dataset there would be nothing at all for a
        // resumed table to receive, and the test could not distinguish any boot order from any other.
        _body = RowsJson(0, FirstBatch + SecondBatch);

        await host.StopAsync();
        host.Start();
        await host.WaitHealthyAsync();

        using (var client = await host.LoginAsync())
        {
            JsonDocument? rows = null;
            try
            {
                // 90 s, not 30: under whole-solution load the first post-restart poll can lose its HTTP
                // fetch to a CPU-starved in-test listener, which is an Error and a 30 s backoff before the
                // next try. The deadline is pure waiting; the assertion below is what the test is about.
                await PollAsync(
                    TimeSpan.FromSeconds(90),
                    async () =>
                    {
                        rows?.Dispose();
                        rows = await GetJsonAsync(client, $"api/tables/{tableId}/rows?limit=5000");
                        return rows.RootElement.GetProperty("totalRows").GetInt32() == SecondBatch;
                    },
                    $"table {TableName} did not refill to {SecondBatch} rows after the restart — the source "
                  + "polled before the table subscribed, and memory streams have no replay",
                    host);

                var ids = rows!.RootElement.GetProperty("rows").EnumerateArray()
                    .Select(r => r.GetProperty("row").GetProperty("id").GetInt64())
                    .ToHashSet();
                Assert.Equal(SecondBatch, ids.Count);
                var missing = Enumerable.Range(FirstBatch, SecondBatch)
                    .Select(i => (long)i).Where(i => !ids.Contains(i)).Take(10).ToList();
                Assert.True(
                    missing.Count == 0,
                    $"ids missing from the table after the restart: {string.Join(",", missing)}");
            }
            finally
            {
                rows?.Dispose();
            }
        }
    }

    // ---- the in-test HTTP server ----

    private async Task ServeAsync()
    {
        while (_listener is { IsListening: true })
        {
            HttpListenerContext ctx;
            try
            {
                ctx = await _listener.GetContextAsync();
            }
            catch
            {
                return; // listener stopped
            }
            try
            {
                var bytes = Encoding.UTF8.GetBytes(_body);
                ctx.Response.ContentType = "application/json";
                ctx.Response.ContentLength64 = bytes.Length;
                await ctx.Response.OutputStream.WriteAsync(bytes);
                ctx.Response.Close();
            }
            catch
            {
                // a dropped client is not this test's problem
            }
        }
    }

    private static string RowsJson(int from, int toExclusive)
    {
        var sb = new StringBuilder("[");
        for (var i = from; i < toExclusive; i++)
        {
            if (i > from)
            {
                sb.Append(',');
            }
            sb.Append(JsonSerializer.Serialize(new { id = i, seq = i, value = $"v{i}" }));
        }
        return sb.Append(']').ToString();
    }

    /// <summary>Same shape as the identically-named private helper in <c>HttpSinkClientTests</c>: bind
    /// port 0, read what the OS assigned, release it. There is a TOCTOU here by construction — nothing
    /// stops another process taking the port between the probe and the listener's bind — which is why
    /// a failure to start the listener becomes a skip reason rather than a hard failure.</summary>
    private static int GetFreePort()
    {
        var probe = new TcpListener(IPAddress.Loopback, 0);
        probe.Start();
        var port = ((IPEndPoint)probe.LocalEndpoint).Port;
        probe.Stop();
        return port;
    }

    // ---- shared shapes and REST helpers ----

    private static object[] FieldsJson() =>
    [
        new { name = "id", type = "Long" },
        new { name = "seq", type = "Long" },
        new { name = "value", type = "String" },
    ];

    private static object[] MappingFieldsJson() =>
        FieldsJson().Select(f => (object)new { field = f }).ToArray();

    private static async Task<int> TotalAsync(HttpClient client, string tableId)
    {
        using var doc = await GetJsonAsync(client, $"api/tables/{tableId}/rows?limit=1");
        return doc.RootElement.GetProperty("totalRows").GetInt32();
    }

    private static async Task<JsonDocument> PostOkAsync(HttpClient client, string url, object body)
    {
        var resp = await client.PostAsJsonAsync(url, body);
        var text = await resp.Content.ReadAsStringAsync();
        Assert.True(resp.IsSuccessStatusCode, $"POST {url} -> {(int)resp.StatusCode}: {text}");
        return JsonDocument.Parse(text);
    }

    private static async Task<JsonDocument> GetJsonAsync(HttpClient client, string url)
    {
        var resp = await client.GetAsync(url);
        var text = await resp.Content.ReadAsStringAsync();
        Assert.True(resp.IsSuccessStatusCode, $"GET {url} -> {(int)resp.StatusCode}: {text}");
        return JsonDocument.Parse(text);
    }

    private static async Task PollAsync(TimeSpan timeout, Func<Task<bool>> condition, string message, HostProcess host)
    {
        var sw = Stopwatch.StartNew();
        while (sw.Elapsed < timeout)
        {
            if (await condition())
            {
                return;
            }
            await Task.Delay(250);
        }
        Assert.Fail($"{message} within {timeout.TotalSeconds:0}s.\n--- host log tail ---\n{host.LogTailText()}");
    }
}
