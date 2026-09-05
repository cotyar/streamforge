using System.Net;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using Xunit;

namespace StreamsForge.Dapr.Live.Tests;

/// <summary>
/// Plan 025 D3.2-4: mirrors
/// <c>orleans/tests/StreamsForge.Host.Tests/SourceExactCountClusterTests.cs</c>'s file/folder/url
/// scenarios over REST against a real Dapr-flavor instance, instead of an in-process
/// <c>TestCluster</c> — the same three connector kinds, the same exact-count discipline (no loss, no
/// duplicates), driven the only way this flavor's actors are reachable from a test process at all.
///
/// <para>One host instance serves all three <see cref="Fact"/>s in this class (each uses its own
/// uniquely-named source/table, so they cannot collide with each other or with the seeded demo
/// catalog's continuously-ticking generators) — booting once rather than three times keeps this
/// project's total runtime down without weakening any single scenario's own exact-count assertions.</para>
///
/// <para><b>Setup order avoids the replay window</b> — identical reasoning to the Orleans test this
/// mirrors, restated for this flavor's actors: the data location is made ready FIRST, then the source is
/// upserted with <c>enabled:false</c> (so it exists in the catalog for the table's SQL to compile
/// against, but <c>ConnectorActor</c> never starts polling), then the table is created and started (its
/// stream subscription exists by the time <c>/start</c> returns), and ONLY THEN is the source enabled via
/// <c>PUT</c> — which is what actually starts the connector's poll loop. Skipping this order would let
/// the connector's first poll land before the table has subscribed, and Dapr's pub/sub has no replay any
/// more than Orleans' memory streams do (see <c>HostRestartTests</c>' identical point on the Orleans
/// side) — rows published into a topic nobody is listening to yet are gone permanently.</para>
/// </summary>
[Collection(DaprLiveTestCollection.Name)]
public sealed class SourceExactCountTests : IAsyncLifetime
{
    private DaprHostProcess? _host;
    private string? _skipReason;
    private string _scratchDir = null!;

    public async Task InitializeAsync()
    {
        _skipReason = DaprHostProcess.Preflight();
        if (_skipReason is not null)
        {
            return;
        }

        await DaprHostProcess.ResetAsync();
        _host = new DaprHostProcess("exact-count");
        _host.Start();
        await _host.WaitHealthyAsync();

        _scratchDir = Directory.CreateTempSubdirectory("sf-dapr-live-exact-count-").FullName;
    }

    public async Task DisposeAsync()
    {
        if (_host is not null)
        {
            await _host.DisposeAsync();
        }
        try
        {
            Directory.Delete(_scratchDir, recursive: true);
        }
        catch
        {
            // best-effort cleanup
        }
    }

    // ================================================================================================
    // 1. file — 500 rows, then an appended 200
    // ================================================================================================

    [Fact]
    public async Task File_source_500_rows_land_exactly_then_an_appended_200()
    {
        Assert.True(_skipReason is null, $"skipped: {_skipReason}");
        var host = _host!;
        var name = "exact_file_" + Guid.NewGuid().ToString("n")[..8];
        var filePath = Path.Combine(_scratchDir, name + ".ndjson");
        await File.WriteAllTextAsync(filePath, BuildNdjson(Enumerable.Range(0, 500)));

        using var client = await host.LoginAsync();
        var tableId = await CreateSubscribedTableThenEnableSourceAsync(
            client, host.BaseUrl, name, "tbl_" + name,
            () => new
            {
                name,
                description = "exact-count file source",
                kind = "file",
                enabled = false,
                fields = FieldsJson(),
                connector = new
                {
                    schedule = new { intervalMs = 1000 },
                    file = new { path = filePath, format = "ndjson" },
                    mapping = MappingJson(),
                },
            });

        var rows = await PollRowsUntilAsync(client, host.BaseUrl, tableId, 500, TimeSpan.FromSeconds(45), host);
        await Task.Delay(1500);
        rows = await GetRowsAsync(client, host.BaseUrl, tableId);
        Assert.Equal(500, rows.TotalRows);
        AssertIdSet(rows, Enumerable.Range(0, 500));

        // Wait >=1s so the file ledger's mtime comparison actually sees a change before appending —
        // same requirement the Orleans version of this test documents for its own file ledger.
        await Task.Delay(1200);
        await File.AppendAllTextAsync(filePath, BuildNdjson(Enumerable.Range(500, 200)));

        rows = await PollRowsUntilAsync(client, host.BaseUrl, tableId, 700, TimeSpan.FromSeconds(45), host);
        await Task.Delay(1500);
        rows = await GetRowsAsync(client, host.BaseUrl, tableId);
        Assert.Equal(700, rows.TotalRows);
        AssertIdSet(rows, Enumerable.Range(0, 700));
    }

    // ================================================================================================
    // 2. folder — 15 files x 20 rows, written ~100ms apart while the source is already polling
    // ================================================================================================

    [Fact]
    public async Task Folder_files_slipping_in_while_the_source_polls_all_land_exactly()
    {
        Assert.True(_skipReason is null, $"skipped: {_skipReason}");
        var host = _host!;
        var name = "exact_folder_" + Guid.NewGuid().ToString("n")[..8];
        var dir = Path.Combine(_scratchDir, name);
        Directory.CreateDirectory(dir);

        using var client = await host.LoginAsync();
        var tableId = await CreateSubscribedTableThenEnableSourceAsync(
            client, host.BaseUrl, name, "tbl_" + name,
            () => new
            {
                name,
                description = "exact-count folder source",
                kind = "folder",
                enabled = false,
                fields = FieldsJson(),
                connector = new
                {
                    schedule = new { intervalMs = 1000 },
                    folder = new { path = dir, format = "ndjson" },
                    mapping = MappingJson(),
                },
            });

        const int fileCount = 15;
        const int rowsPerFile = 20;
        for (var i = 0; i < fileCount; i++)
        {
            var ids = Enumerable.Range(i * rowsPerFile, rowsPerFile);
            await File.WriteAllTextAsync(Path.Combine(dir, $"f{i:D3}.ndjson"), BuildNdjson(ids));
            await Task.Delay(100);
        }

        const int total = fileCount * rowsPerFile;
        var rows = await PollRowsUntilAsync(client, host.BaseUrl, tableId, total, TimeSpan.FromSeconds(60), host);
        await Task.Delay(1500);
        rows = await GetRowsAsync(client, host.BaseUrl, tableId);
        Assert.Equal(total, rows.TotalRows);
        AssertIdSet(rows, Enumerable.Range(0, total));
    }

    // ================================================================================================
    // 3. url — a JSON array dataset, then grown between polls
    // ================================================================================================

    [Fact]
    public async Task Url_json_array_rows_land_exactly_then_a_grown_dataset()
    {
        Assert.True(_skipReason is null, $"skipped: {_skipReason}");
        var host = _host!;
        var name = "exact_url_" + Guid.NewGuid().ToString("n")[..8];
        using var server = new VolatileJsonArrayServer(BuildJsonArray(Enumerable.Range(0, 300)));

        using var client = await host.LoginAsync();
        var tableId = await CreateSubscribedTableThenEnableSourceAsync(
            client, host.BaseUrl, name, "tbl_" + name,
            () => new
            {
                name,
                description = "exact-count url source",
                kind = "url",
                enabled = false,
                fields = FieldsJson(),
                connector = new
                {
                    schedule = new { intervalMs = 1000 },
                    url = new { url = $"http://127.0.0.1:{server.Port}/rows", format = "json" },
                    mapping = MappingJson(),
                },
            });

        var rows = await PollRowsUntilAsync(client, host.BaseUrl, tableId, 300, TimeSpan.FromSeconds(45), host);
        await Task.Delay(1500);
        rows = await GetRowsAsync(client, host.BaseUrl, tableId);
        Assert.Equal(300, rows.TotalRows);
        AssertIdSet(rows, Enumerable.Range(0, 300));

        server.SetBody(BuildJsonArray(Enumerable.Range(0, 450)));

        rows = await PollRowsUntilAsync(client, host.BaseUrl, tableId, 450, TimeSpan.FromSeconds(45), host);
        await Task.Delay(1500);
        rows = await GetRowsAsync(client, host.BaseUrl, tableId);
        Assert.Equal(450, rows.TotalRows);
        AssertIdSet(rows, Enumerable.Range(0, 450));
    }

    // ---- shared setup: disabled source -> subscribed+started table -> enabled source ----

    private static async Task<string> CreateSubscribedTableThenEnableSourceAsync(
        HttpClient client, string baseUrl, string sourceName, string tableName, Func<object> disabledSourceBody)
    {
        await PostOkAsync(client, $"{baseUrl}/api/sources", disabledSourceBody());

        using var created = await PostOkAsync(client, $"{baseUrl}/api/tables", new
        {
            name = tableName,
            description = "exact-count test table",
            sql = $"SELECT id, seq, value FROM {sourceName} LATEST BY (id)",
        });
        var tableId = created.RootElement.GetProperty("id").GetString()!;
        (await client.PostAsync($"{baseUrl}/api/tables/{tableId}/start", content: null)).EnsureSuccessStatusCode();

        // PUT with the same body but enabled:true — /api/sources/{name} is the whole-document upsert,
        // matching the Orleans test's "CloneEnabled" step (a second UpsertSourceAsync with Enabled=true).
        var enabledBody = disabledSourceBody();
        var enabledJson = JsonSerializer.SerializeToNode(enabledBody)!.AsObject();
        enabledJson["enabled"] = true;
        var putResp = await client.PutAsJsonAsync($"{baseUrl}/api/sources/{sourceName}", enabledJson);
        var putText = await putResp.Content.ReadAsStringAsync();
        Assert.True(putResp.IsSuccessStatusCode, $"PUT api/sources/{sourceName} -> {(int)putResp.StatusCode}: {putText}");

        return tableId;
    }

    private static async Task<RowsResponse> PollRowsUntilAsync(
        HttpClient client, string baseUrl, string tableId, int expected, TimeSpan timeout, DaprHostProcess host)
    {
        var deadline = DateTime.UtcNow + timeout;
        RowsResponse last = await GetRowsAsync(client, baseUrl, tableId);
        while (DateTime.UtcNow < deadline)
        {
            last = await GetRowsAsync(client, baseUrl, tableId);
            if (last.TotalRows == expected)
            {
                return last;
            }
            await Task.Delay(250);
        }
        Assert.Fail(
            $"table {tableId} reached {last.TotalRows} rows, expected exactly {expected}, within "
          + $"{timeout.TotalSeconds:0}s.\n--- host log tail ---\n{host.LogTailText()}");
        return last;
    }

    private static async Task<RowsResponse> GetRowsAsync(HttpClient client, string baseUrl, string tableId)
    {
        var resp = await client.GetAsync($"{baseUrl}/api/tables/{tableId}/rows?limit=5000");
        var text = await resp.Content.ReadAsStringAsync();
        Assert.True(resp.IsSuccessStatusCode, $"GET rows -> {(int)resp.StatusCode}: {text}");
        using var doc = JsonDocument.Parse(text);
        var total = doc.RootElement.GetProperty("totalRows").GetInt32();
        var ids = doc.RootElement.GetProperty("rows").EnumerateArray()
            .Select(r => r.GetProperty("row").GetProperty("id").GetInt64())
            .ToHashSet();
        return new RowsResponse(total, ids);
    }

    private static void AssertIdSet(RowsResponse rows, IEnumerable<int> expectedIds)
    {
        var expected = expectedIds.Select(i => (long)i).ToHashSet();
        Assert.Equal(expected.Count, rows.Ids.Count);
        var missing = expected.Where(i => !rows.Ids.Contains(i)).Take(10).ToList();
        Assert.True(missing.Count == 0, $"ids missing from the table: {string.Join(",", missing)}");
        var extra = rows.Ids.Where(i => !expected.Contains(i)).Take(10).ToList();
        Assert.True(extra.Count == 0, $"unexpected ids in the table: {string.Join(",", extra)}");
    }

    private static async Task<JsonDocument> PostOkAsync(HttpClient client, string url, object body)
    {
        var resp = await client.PostAsJsonAsync(url, body);
        var text = await resp.Content.ReadAsStringAsync();
        Assert.True(resp.IsSuccessStatusCode, $"POST {url} -> {(int)resp.StatusCode}: {text}");
        return JsonDocument.Parse(text);
    }

    private static object[] FieldsJson() =>
    [
        new { name = "id", type = "Long" },
        new { name = "seq", type = "Long" },
        new { name = "value", type = "String" },
    ];

    private static object MappingJson() => new
    {
        itemsPath = "$",
        dedupKeyField = "id",
        fields = FieldsJson().Select(f => (object)new { field = f }).ToArray(),
    };

    private static string NdjsonLine(int id) => $"{{\"id\":{id},\"seq\":{id},\"value\":\"v\"}}";

    private static string BuildNdjson(IEnumerable<int> ids) =>
        string.Concat(ids.Select(id => NdjsonLine(id) + "\n"));

    private static string BuildJsonArray(IEnumerable<int> ids) =>
        "[" + string.Join(",", ids.Select(id => $"{{\"id\":{id},\"seq\":{id},\"value\":\"v\"}}")) + "]";

    private sealed record RowsResponse(int TotalRows, HashSet<long> Ids);

    /// <summary>Minimal single-endpoint HTTP server whose response body can be swapped at runtime — a
    /// copy (not an import) of the identically-purposed private type in the Orleans version of this
    /// test, scoped to exactly what this class needs.</summary>
    private sealed class VolatileJsonArrayServer : IDisposable
    {
        private readonly HttpListener _listener;
        private readonly Thread _thread;
        private volatile string _body;

        public int Port { get; }

        public VolatileJsonArrayServer(string initialBody)
        {
            _body = initialBody;
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
                    // best-effort — a race with Dispose() closing the response stream is not this test's concern
                }
            }
        }

        private static int GetFreePort()
        {
            var probe = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0);
            probe.Start();
            var port = ((IPEndPoint)probe.LocalEndpoint).Port;
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
