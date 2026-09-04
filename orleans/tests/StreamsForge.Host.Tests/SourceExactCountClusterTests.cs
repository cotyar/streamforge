using System.Net;
using System.Text;
using Microsoft.Extensions.Configuration;
using Orleans;
using Orleans.Hosting;
using Orleans.TestingHost;
using StreamsForge.Abstractions;
using StreamsForge.Engine;
using StreamsForge.Host.Grains;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>
/// Plan "glittery-brewing-feather" Track A / C: proves the file, folder and url connector kinds land
/// EXACT row counts in a downstream table — no loss, no duplicates — including files "slipping in"
/// mid-poll, a URL dataset that grows between polls, and a file appended to after its first poll.
///
/// <para>Own single-silo <see cref="TestCluster"/> reusing the `internal`
/// <see cref="ConnectorTestSiloConfigurator"/>/<see cref="ConnectorTestClientConfigurator"/> from
/// <see cref="ConnectorGrainClusterTests"/> (same assembly, cross-file `internal` reuse — the same
/// pattern <see cref="TableJournalClusterTests"/> already uses against a different sibling file).
/// <see cref="PollUntilAsync{T}"/> and <see cref="GetFreePort"/> below are copies (not imports) of the
/// identically-named private helpers in <see cref="ConnectorGrainClusterTests"/> /
/// <c>HttpSinkClientTests.cs:315</c> — this class owns its own file per this plan's file-ownership rule.</para>
///
/// <para><b>Setup order avoids the replay window</b> (memory streams have no replay — a table only
/// receives rows published after it subscribed): the data location is made ready FIRST, then the source
/// is upserted DISABLED (so it exists in the catalog for the table's SQL to compile against, but the
/// connector grain never starts), then the table is created and set Running (its subscription exists by
/// the time that call returns), and ONLY THEN is the source upserted ENABLED — which is what actually
/// starts <see cref="IConnectorGrain"/> polling. <see cref="RegistryGrain.EnsureInitializedAsync"/> is
/// deliberately never called here — there is nothing to resume, and calling it would re-run the (still
/// two-pass, pre section-E-fix) boot order this class has nothing to do with.</para>
///
/// <para><b>One test is RED in this worktree</b>: <c>Folder_a_malformed_file_does_not_lose_the_good_
/// files_and_lands_once_fixed</c> encodes Track A's per-file isolation fix to
/// <c>ConnectorPollCycle.ExecuteFolder</c>/<c>ConnectorGrain.RunCycleAsync</c> (today ANY failing file in
/// a folder cycle drops the WHOLE cycle's rows — including files that parsed fine — see
/// <c>ConnectorGrain.cs</c>'s emit-policy comment above its `if (result.Error is null)` branch), which is
/// NOT in this worktree, and its 3 good files + 1 bad file in one cycle is exactly the shape that loses
/// rows pre-fix. Written to the plan's spec verbatim, not weakened; the actual observed failure (count
/// never reaches 60) is reported rather than assumed.
///
/// <c>Folder_a_partially_written_file_lands_exactly_once_after_its_second_chunk</c> was also anticipated
/// to be red for the same reason, but is observed to PASS here (verified twice): its folder holds only
/// the ONE partial file, never a good file alongside it, so there is nothing for the aggregate-Error
/// cycle to drop — the pre-fix bug only destroys rows that parsed fine in the SAME cycle as a failing
/// file. The bad file is skipped-and-retried (not ledgered) either way, so once its second chunk lands the
/// next poll parses it cleanly; the 30s-and-growing backoff this test's own comment worried about fits
/// inside its 45s budget. Kept verbatim (not weakened) since it still exercises real behavior, just not
/// the isolation fix specifically.</para>
/// </summary>
public sealed class SourceExactCountClusterTests : IAsyncLifetime
{
    private TestCluster _cluster = null!;
    private string _scratchDir = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<ConnectorTestSiloConfigurator>();
        builder.AddClientBuilderConfigurator<ConnectorTestClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();

        _scratchDir = Directory.CreateTempSubdirectory("sf-exact-count-test-").FullName;
    }

    public async Task DisposeAsync()
    {
        await _cluster.DisposeAsync();
        try
        {
            Directory.Delete(_scratchDir, recursive: true);
        }
        catch
        {
            // best-effort cleanup
        }
    }

    // ---- shared field/mapping shape: id Long, seq Long, value String; dedup key "id" ----

    private static List<FieldDef> Fields() =>
    [
        new FieldDef("id", FieldType.Long),
        new FieldDef("seq", FieldType.Long),
        new FieldDef("value", FieldType.String),
    ];

    private static MappingSpec Mapping() => new()
    {
        ItemsPath = "$",
        DedupKeyField = "id",
        Fields =
        [
            new FieldMapEntry { Field = new FieldDef("id", FieldType.Long) },
            new FieldMapEntry { Field = new FieldDef("seq", FieldType.Long) },
            new FieldMapEntry { Field = new FieldDef("value", FieldType.String) },
        ],
    };

    private static SourceDefinition MakeFileSource(string name, string path, bool enabled) => new()
    {
        Name = name,
        Kind = SourceKinds.File,
        Enabled = enabled,
        Fields = Fields(),
        Connector = new ConnectorConfig
        {
            Schedule = new ScheduleSpec { IntervalMs = 1000 },
            File = new FilePollConfig { Path = path, Format = FileFormats.Ndjson },
            Mapping = Mapping(),
        },
    };

    private static SourceDefinition MakeFolderSource(string name, string path, bool enabled) => new()
    {
        Name = name,
        Kind = SourceKinds.Folder,
        Enabled = enabled,
        Fields = Fields(),
        Connector = new ConnectorConfig
        {
            Schedule = new ScheduleSpec { IntervalMs = 1000 },
            Folder = new FolderPollConfig { Path = path, Format = FileFormats.Ndjson },
            Mapping = Mapping(),
        },
    };

    private static SourceDefinition MakeUrlSource(string name, string url, bool enabled) => new()
    {
        Name = name,
        Kind = SourceKinds.Url,
        Enabled = enabled,
        Fields = Fields(),
        Connector = new ConnectorConfig
        {
            Schedule = new ScheduleSpec { IntervalMs = 1000 },
            Url = new UrlPollConfig { Url = url, Format = FileFormats.JsonArray },
            Mapping = Mapping(),
        },
    };

    private static string NdjsonLine(long id) => $"{{\"id\":{id},\"seq\":{id},\"value\":\"v\"}}";

    private static string BuildNdjson(IEnumerable<long> ids) =>
        string.Concat(ids.Select(id => NdjsonLine(id) + "\n"));

    private static string BuildJsonArray(IEnumerable<long> ids) =>
        "[" + string.Join(",", ids.Select(id => $"{{\"id\":{id},\"seq\":{id},\"value\":\"v\"}}")) + "]";

    /// <summary>Setup order per this class's own doc comment: upsert the source DISABLED, create+start
    /// the table (its subscription exists on return), then upsert the source ENABLED — the last call is
    /// what actually starts <see cref="IConnectorGrain"/> polling.</summary>
    private async Task<(IRegistryGrain Registry, TableDefinition Table)> CreateSubscribedTableThenEnableAsync(
        SourceDefinition disabledDef, string tableName)
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        await registry.UpsertSourceAsync(disabledDef);

        var created = await registry.CreateTableAsync(new TableDefinition
        {
            Name = tableName,
            Sql = $"SELECT id, seq, value FROM {disabledDef.Name} LATEST BY (id)",
        });
        await registry.SetTableStatusAsync(created.Id, PipelineStatus.Running);

        var enabledDef = CloneEnabled(disabledDef);
        await registry.UpsertSourceAsync(enabledDef);

        return (registry, created);
    }

    private static SourceDefinition CloneEnabled(SourceDefinition def) => new()
    {
        Name = def.Name,
        Kind = def.Kind,
        Enabled = true,
        Fields = def.Fields,
        Connector = def.Connector,
    };

    private static void AssertIdSet(List<TableRowDto> rows, IEnumerable<long> expectedIds)
    {
        var actual = rows.Select(r => Convert.ToInt64(r.Row["id"])).OrderBy(x => x).ToList();
        var expected = expectedIds.OrderBy(x => x).ToList();
        Assert.Equal(expected, actual);
    }

    private static async Task<T> PollUntilAsync<T>(Func<Task<T>> poll, Func<T, bool> until, int deadlineSeconds)
    {
        var deadline = DateTime.UtcNow.AddSeconds(deadlineSeconds);
        T last = await poll();
        while (DateTime.UtcNow < deadline)
        {
            last = await poll();
            if (until(last)) return last;
            await Task.Delay(200);
        }
        return last;
    }

    private static int GetFreePort()
    {
        var listener = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    // ================================================================================================
    // 1. file
    // ================================================================================================

    [Fact]
    public async Task File_500_rows_land_exactly_then_an_appended_200_land_exactly()
    {
        var name = "exact_file_" + Guid.NewGuid().ToString("n")[..8];
        var filePath = Path.Combine(_scratchDir, name + ".ndjson");
        await File.WriteAllTextAsync(filePath, BuildNdjson(Enumerable.Range(0, 500).Select(i => (long)i)));

        var (_, table) = await CreateSubscribedTableThenEnableAsync(MakeFileSource(name, filePath, enabled: false), "tbl_" + name);
        var grain = _cluster.GrainFactory.GetGrain<ITableGrain>(table.Name);

        await PollUntilAsync(() => grain.GetRowCountAsync(), c => c == 500, deadlineSeconds: 30);
        await Task.Delay(1500);

        Assert.Equal(500, await grain.GetRowCountAsync());
        AssertIdSet(await grain.GetRowsAsync(2000, 0), Enumerable.Range(0, 500).Select(i => (long)i));

        // Wait >=1s so the file ledger's mtime comparison actually sees a change before appending.
        await Task.Delay(1200);
        await File.AppendAllTextAsync(filePath, BuildNdjson(Enumerable.Range(500, 200).Select(i => (long)i)));

        await PollUntilAsync(() => grain.GetRowCountAsync(), c => c == 700, deadlineSeconds: 30);
        await Task.Delay(1500);

        Assert.Equal(700, await grain.GetRowCountAsync());
        AssertIdSet(await grain.GetRowsAsync(2000, 0), Enumerable.Range(0, 700).Select(i => (long)i));
    }

    // ================================================================================================
    // 2. folder — files slipping in while the source polls
    // ================================================================================================

    [Fact]
    public async Task Folder_files_slipping_in_while_the_source_polls_all_land_exactly()
    {
        var name = "exact_folder_slip_" + Guid.NewGuid().ToString("n")[..8];
        var dir = Path.Combine(_scratchDir, name);
        Directory.CreateDirectory(dir);

        var (_, table) = await CreateSubscribedTableThenEnableAsync(MakeFolderSource(name, dir, enabled: false), "tbl_" + name);
        var grain = _cluster.GrainFactory.GetGrain<ITableGrain>(table.Name);

        const int fileCount = 15;
        const int rowsPerFile = 20;
        for (var i = 0; i < fileCount; i++)
        {
            var ids = Enumerable.Range(i * rowsPerFile, rowsPerFile).Select(x => (long)x);
            await File.WriteAllTextAsync(Path.Combine(dir, $"f{i:D3}.ndjson"), BuildNdjson(ids));
            await Task.Delay(100);
        }

        var total = fileCount * rowsPerFile;
        await PollUntilAsync(() => grain.GetRowCountAsync(), c => c == total, deadlineSeconds: 40);
        await Task.Delay(1500);

        Assert.Equal(total, await grain.GetRowCountAsync());
        AssertIdSet(await grain.GetRowsAsync(2000, 0), Enumerable.Range(0, total).Select(i => (long)i));
    }

    // ================================================================================================
    // 3. folder — a malformed file must not lose the good files' rows (Track A's fix — expected RED here)
    // ================================================================================================

    /// <summary>
    /// Encodes Track A's per-file isolation fix. TODAY (this worktree, pre-fix):
    /// <c>ConnectorPollCycle.ExecuteFolder</c> ledgers every good file it parses but still returns an
    /// aggregate <see cref="PollCycleResult.Error"/> when ANY file in the same cycle fails, and
    /// <c>ConnectorGrain.RunCycleAsync</c> emits NOTHING for a cycle whose result carries an Error — so
    /// the good files' rows are ledgered as "done" (never re-read) yet never published. Expect this test
    /// to fail at (or before) the first `count == 60` poll, because the count never reaches 60 at all —
    /// this is the exact data loss the plan calls out, written to spec and reported honestly rather than
    /// weakened to pass.
    /// </summary>
    [Fact]
    public async Task Folder_a_malformed_file_does_not_lose_the_good_files_and_lands_once_fixed()
    {
        var name = "exact_folder_bad_" + Guid.NewGuid().ToString("n")[..8];
        var dir = Path.Combine(_scratchDir, name);
        Directory.CreateDirectory(dir);

        // Three good files (20 rows each, disjoint id ranges) plus one malformed file, all present
        // before the source is enabled so the very first poll cycle sees all four at once.
        for (var i = 0; i < 3; i++)
        {
            var ids = Enumerable.Range(i * 20, 20).Select(x => (long)x);
            await File.WriteAllTextAsync(Path.Combine(dir, $"good{i}.ndjson"), BuildNdjson(ids));
        }
        var badPath = Path.Combine(dir, "bad.ndjson");
        await File.WriteAllTextAsync(badPath, "{not json");

        var (_, table) = await CreateSubscribedTableThenEnableAsync(MakeFolderSource(name, dir, enabled: false), "tbl_" + name);
        var grain = _cluster.GrainFactory.GetGrain<ITableGrain>(table.Name);
        var connector = _cluster.GrainFactory.GetGrain<IConnectorGrain>(name);

        await PollUntilAsync(() => grain.GetRowCountAsync(), c => c == 60, deadlineSeconds: 30);
        Assert.Equal(60, await grain.GetRowCountAsync());
        AssertIdSet(await grain.GetRowsAsync(2000, 0), Enumerable.Range(0, 60).Select(i => (long)i));

        var status = await connector.GetStatusAsync();
        Assert.Equal("ok", status.LastStatus);
        Assert.NotNull(status.LastError);
        Assert.Contains("bad.ndjson", status.LastError);

        // Fix the bad file — 20 more valid rows, disjoint from the good files' ids.
        await File.WriteAllTextAsync(badPath, BuildNdjson(Enumerable.Range(60, 20).Select(x => (long)x)));

        await PollUntilAsync(() => grain.GetRowCountAsync(), c => c == 80, deadlineSeconds: 30);
        Assert.Equal(80, await grain.GetRowCountAsync());
        AssertIdSet(await grain.GetRowsAsync(2000, 0), Enumerable.Range(0, 80).Select(i => (long)i));

        var recovered = await connector.GetStatusAsync();
        Assert.Null(recovered.LastError);
    }

    // ================================================================================================
    // 4. folder — a partially-written file lands exactly once its second chunk arrives
    //    (folder, not file, because a bad file on the `file` kind is a whole-cycle Error+backoff — see
    //    ConnectorPollCycle.ExecuteFile — same as folder's single-file case here). Observed PASSING in
    //    this worktree (verified twice, ~90-100s each): the folder holds only this one file, so the
    //    pre-fix aggregate-Error-drops-good-rows bug never triggers (nothing else in the cycle to drop),
    //    and the 30s-and-growing backoff a parse failure schedules pre-fix fits inside the 45s budget below.
    // ================================================================================================

    [Fact]
    public async Task Folder_a_partially_written_file_lands_exactly_once_after_its_second_chunk()
    {
        var name = "exact_folder_partial_" + Guid.NewGuid().ToString("n")[..8];
        var dir = Path.Combine(_scratchDir, name);
        Directory.CreateDirectory(dir);
        var filePath = Path.Combine(dir, "partial.ndjson");

        const int total = 20;
        // Chunk 1: ids 0..6 complete, then id 7's line cut off mid-value (no closing brace, no newline).
        var chunk1 = new StringBuilder();
        for (var i = 0; i < 7; i++) chunk1.Append(NdjsonLine(i)).Append('\n');
        chunk1.Append("{\"id\":7,\"se"); // deliberately truncated
        await File.WriteAllTextAsync(filePath, chunk1.ToString());

        var (_, table) = await CreateSubscribedTableThenEnableAsync(MakeFolderSource(name, dir, enabled: false), "tbl_" + name);
        var grain = _cluster.GrainFactory.GetGrain<ITableGrain>(table.Name);
        var connector = _cluster.GrainFactory.GetGrain<IConnectorGrain>(name);

        // Wait until a poll actually saw the partial file (proves it was read, not just that we wrote it).
        var afterFirstLook = await PollUntilAsync(
            () => connector.GetStatusAsync(),
            s => s.LastError is not null && s.LastError.Contains("partial.ndjson"),
            deadlineSeconds: 15);
        Assert.NotNull(afterFirstLook.LastError);
        Assert.Contains("partial.ndjson", afterFirstLook.LastError);

        // Complete id 7's line, then the remaining ids 8..19.
        var chunk2 = new StringBuilder();
        chunk2.Append("q\":7,\"value\":\"v\"}\n");
        for (var i = 8; i < total; i++) chunk2.Append(NdjsonLine(i)).Append('\n');
        await File.AppendAllTextAsync(filePath, chunk2.ToString());

        // A pre-fix parse failure schedules a growing (>=30s) backoff before the next cycle even looks
        // again — budget accordingly (this is the "likely slow" half of this test's documented risk).
        await PollUntilAsync(() => grain.GetRowCountAsync(), c => c == total, deadlineSeconds: 45);
        await Task.Delay(1500);

        Assert.Equal(total, await grain.GetRowCountAsync());
        AssertIdSet(await grain.GetRowsAsync(2000, 0), Enumerable.Range(0, total).Select(i => (long)i));
    }

    // ================================================================================================
    // 5. url — a JSON array dataset, then grown between polls
    // ================================================================================================

    [Fact]
    public async Task Url_json_array_N_rows_land_exactly_then_a_grown_dataset_lands_exactly()
    {
        var name = "exact_url_" + Guid.NewGuid().ToString("n")[..8];
        var port = GetFreePort();
        using var server = new VolatileJsonArrayServer(port, BuildJsonArray(Enumerable.Range(0, 300).Select(i => (long)i)));

        var (_, table) = await CreateSubscribedTableThenEnableAsync(
            MakeUrlSource(name, $"http://127.0.0.1:{port}/rows", enabled: false), "tbl_" + name);
        var grain = _cluster.GrainFactory.GetGrain<ITableGrain>(table.Name);

        await PollUntilAsync(() => grain.GetRowCountAsync(), c => c == 300, deadlineSeconds: 30);
        await Task.Delay(1500);

        Assert.Equal(300, await grain.GetRowCountAsync());
        AssertIdSet(await grain.GetRowsAsync(2000, 0), Enumerable.Range(0, 300).Select(i => (long)i));

        server.SetBody(BuildJsonArray(Enumerable.Range(0, 450).Select(i => (long)i)));

        await PollUntilAsync(() => grain.GetRowCountAsync(), c => c == 450, deadlineSeconds: 30);
        await Task.Delay(1500);

        Assert.Equal(450, await grain.GetRowCountAsync());
        AssertIdSet(await grain.GetRowsAsync(2000, 0), Enumerable.Range(0, 450).Select(i => (long)i));
    }

    /// <summary>Minimal single-endpoint HTTP server whose response body can be swapped at runtime — the
    /// "volatile JSON-array body" this test's url source polls. Deliberately not
    /// <c>HttpSinkClientTests.cs</c>'s richer capture-and-respond fixture (this test needs none of its
    /// request recording): a self-contained copy scoped to exactly what this class needs, per this
    /// class's own file-ownership rule.</summary>
    private sealed class VolatileJsonArrayServer : IDisposable
    {
        private readonly HttpListener _listener;
        private readonly Thread _thread;
        private volatile string _body;

        public VolatileJsonArrayServer(int port, string initialBody)
        {
            _body = initialBody;
            _listener = new HttpListener();
            _listener.Prefixes.Add($"http://127.0.0.1:{port}/");
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
