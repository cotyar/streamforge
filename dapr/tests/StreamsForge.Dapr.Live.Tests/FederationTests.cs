using System.Text;
using Xunit;

namespace StreamsForge.Dapr.Live.Tests;

/// <summary>
/// Plan 025 D1, live: <b>a Dapr instance and an Orleans instance federate over gRPC in BOTH
/// directions</b> — 150 rows each way, exactly, with no gaps.
///
/// <para><b>One of these two directions was impossible before plan 025, and that is the whole point of
/// this class.</b> Dapr → Orleans (a folder source on the Dapr host, consumed by a <c>grpc</c> source on
/// the Orleans host) could not exist: the Dapr host served no gRPC at all, so there was nothing for an
/// Orleans subscriber to dial. <c>dapr/PARITY.md</c> D1 carried it as the flavor's largest owed item and
/// AGENTS.md's port table said so out loud ("gRPC reserved 5499, not yet served — phase 2"). D1 moved
/// the seven services into <c>shared/StreamsForge.Api/Grpc/</c> behind one new runtime primitive
/// (<c>IEntityStreamFacade</c>), and this fact is what makes "the Dapr host is a federation PEER, not
/// only a federation client" true rather than asserted. The other direction (Orleans → Dapr) was already
/// possible in principle — the <c>grpc</c> SOURCE kind is shared code — but had never been run against a
/// real Orleans host from this flavor, which is the same "unverified" category the whole plan
/// exists to empty.</para>
///
/// <para><b>Both ends declare the same three fields, and that is load-bearing.</b> The gRPC egress
/// encodes only the fields the producing entity DECLARES — an undeclared column is not on the wire at
/// all — so a mismatch shows up as a silently narrower row rather than an error. Each producer declares
/// <c>id/seq/value</c>; each federated consumer declares the same three and needs no <c>schemaSource</c>
/// or <c>protoText</c>, because the default <c>reflection</c> fetches the descriptor from the producer.
/// The Dapr → Orleans direction is therefore also, incidentally, a live test of the Dapr host's
/// reflection service being good enough to drive a real client's codegen — a stronger statement than
/// <c>GrpcServingTests</c>' <c>grpcurl list</c>.</para>
///
/// <para><b>By literal address, not by peer name — deliberately.</b>
/// <c>orleans/tests/StreamsForge.Chain.Tests/GrpcChainTests</c> federates by <c>peer</c> + the
/// <c>Discovery:Peers</c> directory, which is the recommended production shape and is already proven
/// there. Using it here would require configuring BOTH hosts' peer directories at spawn time and would
/// couple this test to the directory's own resolution rules; what is under test in this class is the
/// TRANSPORT crossing a flavor boundary, so each source carries <c>address</c> (gRPC) and
/// <c>restAddress</c> (for the login). <c>GrpcSubConfig.Peer</c> wins over both when set, so the two
/// shapes cannot be mixed by accident.</para>
///
/// <para><b>The empty-folder-then-write dance is not a fudge.</b> <c>GrpcSubscriberCore</c> sets its
/// status to "ok" just BEFORE it issues the subscribe RPC, not after the server has accepted it — so
/// "ok" means "about to subscribe". And a federated consumer gets NO replay: the producing side of a
/// gRPC egress is a live stream, with no ring behind it on either flavor (the connector replay ring plan
/// 025 D3 added is per-connector-source and local to the producing host — see
/// <see cref="LateConsumerTests"/>, which is where that IS tested). So each producer's folder is EMPTY
/// at create time, the consumer is stood up and allowed to report "ok" plus a fixed cushion, and only
/// then are the rows written. A test that wrote first would be asserting a promise nobody has made. See
/// <see cref="WaitForSubscriberOkAsync"/> for how long that cushion is and what a shorter one cost.</para>
///
/// <para><b>What this class does NOT prove.</b> Nothing about federation over TLS between the two
/// flavors (each flavor's own TLS gRPC listener is proven in its own TLS suite; a cross-flavor TLS chain
/// would need a third fixture and proves no additional seam). Nothing about reconnect after a peer
/// restarts — <c>GrpcSubscriberCore</c>'s backoff is a private 30 s constant, so such a test would either
/// take 30+ seconds or need a public knob, which is a contract change and belongs in a plan. And nothing
/// about ordering: <c>LATEST BY (id)</c> is order-insensitive on both sides.</para>
/// </summary>
/// <summary>Both hosts. The Dapr half is <see cref="LiveHostFixture"/>'s; the Orleans peer is started
/// alongside it — after the Dapr host is healthy rather than in parallel with it, because the two boots
/// competing for this machine's CPU is exactly the load that makes a Dapr sidecar's actor-placement
/// round time out.</summary>
public sealed class FederationFixture : LiveHostFixture
{
    public OrleansPeerProcess? Orleans { get; private set; }

    protected override string Label => "federation";

    protected override string? ExtraPreflight() => OrleansPeerProcess.Preflight();

    protected override async Task OnStartedAsync()
    {
        Orleans = new OrleansPeerProcess();
        Orleans.Start();
        await Orleans.WaitHealthyAsync();
    }

    protected override async Task OnDisposingAsync()
    {
        if (Orleans is not null)
        {
            await Orleans.DisposeAsync();
            Orleans = null;
        }
    }
}

[Collection(DaprLiveTestCollection.Name)]
public sealed class FederationTests(FederationFixture fixture) : IClassFixture<FederationFixture>
{
    private const int TotalRows = 150;
    private const int FileCount = 5;

    private DaprHostProcess? _dapr => fixture.Host;
    private OrleansPeerProcess? _orleans => fixture.Orleans;
    private string? _skipReason => fixture.SkipReason;
    private string _scratchDir => fixture.ScratchDir;

    // ================================================================================================
    // Orleans -> Dapr
    // ================================================================================================

    [Fact]
    public async Task A_folder_source_on_the_Orleans_peer_lands_every_row_in_a_table_on_the_Dapr_host()
    {
        Assert.True(_skipReason is null, $"skipped: {_skipReason}");
        var dapr = _dapr!;
        var orleans = _orleans!;

        var suffix = Guid.NewGuid().ToString("n")[..8];
        var producer = "fed_o2d_src_" + suffix;
        var federated = "fed_o2d_fed_" + suffix;
        var table = "fed_o2d_tbl_" + suffix;
        var folder = Path.Combine(_scratchDir, producer);
        Directory.CreateDirectory(folder);

        using var producerClient = await orleans.LoginAsync();
        using var consumerClient = await dapr.LoginAsync();

        await LiveRest.PostOkAsync(producerClient, $"{orleans.BaseUrl}/api/sources", FolderSource(producer, folder));

        await LiveRest.PostOkAsync(consumerClient, $"{dapr.BaseUrl}/api/sources",
            FederatedSource(federated, orleans.GrpcUrl, orleans.BaseUrl, $"source:{producer}"));

        var tableId = await CreateAndStartTableAsync(consumerClient, dapr.BaseUrl, table, federated);

        await WaitForSubscriberOkAsync(consumerClient, dapr.BaseUrl, federated, dapr.LogTailText);
        await WriteRowsAsync(folder);

        var rows = await LiveRest.SettledRowsAsync(
            consumerClient, dapr.BaseUrl, tableId, TotalRows, TimeSpan.FromSeconds(90), CombinedLogs);
        Assert.Equal(TotalRows, rows.TotalRows);
        LiveRest.AssertIdSet(rows, Enumerable.Range(0, TotalRows));

        // The consumer's own counter must agree with the table — a source that emitted more than the
        // table holds would mean duplicates that LATEST BY silently absorbed.
        using var status = await LiveRest.GetJsonAsync(consumerClient, $"{dapr.BaseUrl}/api/sources/{federated}/status");
        Assert.Equal(TotalRows, status.RootElement.GetProperty("eventsEmittedTotal").GetInt64());
    }

    // ================================================================================================
    // Dapr -> Orleans  (impossible before plan 025 — see the class doc)
    // ================================================================================================

    [Fact]
    public async Task A_folder_source_on_the_Dapr_host_lands_every_row_in_a_table_on_the_Orleans_peer()
    {
        Assert.True(_skipReason is null, $"skipped: {_skipReason}");
        var dapr = _dapr!;
        var orleans = _orleans!;

        var suffix = Guid.NewGuid().ToString("n")[..8];
        var producer = "fed_d2o_src_" + suffix;
        var federated = "fed_d2o_fed_" + suffix;
        var table = "fed_d2o_tbl_" + suffix;
        var folder = Path.Combine(_scratchDir, producer);
        Directory.CreateDirectory(folder);

        using var producerClient = await dapr.LoginAsync();
        using var consumerClient = await orleans.LoginAsync();

        await LiveRest.PostOkAsync(producerClient, $"{dapr.BaseUrl}/api/sources", FolderSource(producer, folder));

        await LiveRest.PostOkAsync(consumerClient, $"{orleans.BaseUrl}/api/sources",
            FederatedSource(federated, dapr.GrpcUrl, dapr.BaseUrl, $"source:{producer}"));

        var tableId = await CreateAndStartTableAsync(consumerClient, orleans.BaseUrl, table, federated);

        await WaitForSubscriberOkAsync(consumerClient, orleans.BaseUrl, federated, orleans.LogTailText);
        await WriteRowsAsync(folder);

        var rows = await LiveRest.SettledRowsAsync(
            consumerClient, orleans.BaseUrl, tableId, TotalRows, TimeSpan.FromSeconds(90), CombinedLogs);
        Assert.Equal(TotalRows, rows.TotalRows);
        LiveRest.AssertIdSet(rows, Enumerable.Range(0, TotalRows));

        using var status = await LiveRest.GetJsonAsync(consumerClient, $"{orleans.BaseUrl}/api/sources/{federated}/status");
        Assert.Equal(TotalRows, status.RootElement.GetProperty("eventsEmittedTotal").GetInt64());
    }

    // ---- shared shapes ----

    private static object FolderSource(string name, string folder) => new
    {
        name,
        description = "federation producer (folder, empty at create time)",
        kind = "folder",
        enabled = true,
        fields = LiveRest.Fields(),
        connector = new
        {
            schedule = new { intervalMs = 1000 },
            folder = new { path = folder, format = "ndjson" },
            mapping = LiveRest.Mapping(),
        },
    };

    /// <summary>A <c>grpc</c> source addressed by LITERAL address — see the class doc for why not by
    /// peer name. <c>restAddress</c> is not optional here: <c>address</c> is the gRPC port and the
    /// subscriber logs in over REST, which lives on a different one.</summary>
    private static object FederatedSource(string name, string grpcAddress, string restAddress, string entityKey) => new
    {
        name,
        description = $"federated from {grpcAddress} ({entityKey})",
        kind = "grpc",
        enabled = true,
        fields = LiveRest.Fields(),
        connector = new
        {
            grpc = new
            {
                address = grpcAddress,
                restAddress,
                entityKey,
                username = DaprHostProcess.AdminUser,
                password = DaprHostProcess.AdminPassword,
            },
        },
    };

    private static async Task<string> CreateAndStartTableAsync(
        HttpClient client, string baseUrl, string tableName, string sourceName)
    {
        using var created = await LiveRest.PostOkAsync(client, $"{baseUrl}/api/tables", new
        {
            name = tableName,
            description = "federation consumer table",
            sql = $"SELECT id, seq, value FROM {sourceName} LATEST BY (id)",
        });
        var tableId = created.RootElement.GetProperty("id").GetString()!;
        (await client.PostAsync($"{baseUrl}/api/tables/{tableId}/start", content: null)).EnsureSuccessStatusCode();
        return tableId;
    }

    /// <summary>Waits for the federated source to report <c>lastStatus == "ok"</c>, then a fixed three
    /// seconds more. The cushion is not slop: "ok" is set just BEFORE the subscribe RPC is issued, so it
    /// means "about to subscribe", and writing the producer's rows the instant it appears would race the
    /// subscription — which, with no replay across a gRPC hop, loses rows permanently.
    ///
    /// <para><b>Three seconds, not the one the Orleans chain test uses.</b> With one second, the
    /// Dapr → Orleans direction was observed landing <b>149 of 150</b> rows in a whole-suite run (and
    /// 150/150 in the run before it) — a single row lost off the front, the signature of the producer's
    /// first published batch beating the subscriber's registration into the Dapr host's own
    /// <c>EntityStreamFanout</c>. Widening the cushion is a test-side fix for a test-side race, and it
    /// says nothing about whether the fanout's registration ought to be synchronous with the RPC; if a
    /// short-count ever reappears at three seconds, the cause is on the product side and should be
    /// investigated there rather than by widening this again.</para></summary>
    private static async Task WaitForSubscriberOkAsync(
        HttpClient client, string baseUrl, string sourceName, Func<string> logTail)
    {
        string? last = null;
        await LiveRest.PollAsync(
            TimeSpan.FromSeconds(90),
            async () =>
            {
                using var doc = await LiveRest.GetJsonAsync(client, $"{baseUrl}/api/sources/{sourceName}/status");
                last = doc.RootElement.TryGetProperty("lastStatus", out var s) ? s.GetString() : null;
                return last == "ok";
            },
            () => $"federated source {sourceName} never reported lastStatus 'ok' (last: '{last}')",
            logTail);
        await Task.Delay(TimeSpan.FromSeconds(3));
    }

    /// <summary>150 rows as 5 files of 30, ~100 ms apart, so files keep slipping in while the connector
    /// is mid-cycle. Each file is written to a temp name and MOVED into place, so the folder never holds
    /// a half-written <c>.ndjson</c> for a poll to trip over.</summary>
    private static async Task WriteRowsAsync(string folder)
    {
        const int perFile = TotalRows / FileCount;
        for (var file = 0; file < FileCount; file++)
        {
            var sb = new StringBuilder();
            for (var i = 0; i < perFile; i++)
            {
                sb.Append(LiveRest.NdjsonLine((file * perFile) + i)).Append('\n');
            }
            var staging = Path.Combine(folder, $"part{file:D2}.ndjson.tmp");
            await File.WriteAllTextAsync(staging, sb.ToString());
            File.Move(staging, Path.Combine(folder, $"part{file:D2}.ndjson"));
            await Task.Delay(100);
        }
    }

    /// <summary>Both hosts' log tails. A federation failure is otherwise indistinguishable between "the
    /// producer never emitted" (its log), "the consumer never subscribed" (the other one) and "the
    /// table never ran" — and which host is which flips between the two facts in this class.</summary>
    private string CombinedLogs() =>
        $"=== dapr ({DaprHostProcess.AppId}) ===\n{_dapr?.LogTailText()}\n"
      + $"=== orleans peer ({OrleansPeerProcess.HttpPort}) ===\n{_orleans?.LogTailText()}";
}
