using System.Diagnostics;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using Xunit;

namespace StreamsForge.Chain.Tests;

/// <summary>
/// Plan "glittery-brewing-feather" Track A / D: the two-process chain test. A folder source on host A
/// is federated into host B BY PEER NAME over real gRPC, feeds a table on B, and every one of 1000
/// rows written on A is asserted to arrive on B — exactly once, with no gaps.
///
/// <para>This is the only test in the repo that crosses a process boundary on the data path. A
/// <c>TestCluster</c> cannot express it: one silo, one catalog, one in-memory stream provider, and no
/// gRPC listener at all. The chain here is
/// <c>files on disk → A's folder connector → A's gRPC egress → B's grpc source
/// (<c>GrpcSubscriberCore</c>) → B's table executor → B's REST rows endpoint</c>, every hop real.</para>
///
/// <para><b>Both ends declare the same three fields, and that is load-bearing.</b> The gRPC egress
/// encodes only the fields the producing entity DECLARES — an undeclared column is not on the wire at
/// all, so a mismatch would show up as a silently narrower row rather than an error. A declares
/// <c>id/seq/value</c>; B's federated source declares the same three and needs no <c>schemaSource</c>
/// or <c>protoText</c>, because the default <c>reflection</c> fetches the descriptor from A.</para>
///
/// <para><b>The 1s cushion after "ok" is not a fudge.</b> <c>GrpcSubscriberCore</c> sets
/// <c>lastStatus = "ok"</c> just BEFORE it issues the subscribe RPC, not after the server has
/// accepted it — so "ok" means "about to subscribe", not "subscribed". Writing data on A the instant
/// B reports ok would race the subscription; memory streams have no replay, and rows published before
/// a subscriber attaches are gone. One fixed second past "ok" is the cheapest correct wait, and the
/// test writes its rows over ~500ms after that.</para>
///
/// <para><b>Two obvious tests are deliberately absent</b>, and their absence is the point:</para>
/// <list type="bullet">
/// <item><b>Burst-before-subscribe.</b> Writing A's rows BEFORE B's source subscribes would assert
/// that a federated consumer gets rows produced before it existed. Nothing promises that across a gRPC
/// hop — the replay ring added in this plan's section F is per-connector-source and in-memory on the
/// PRODUCING host, and A's egress is a live stream. Writing such a test would enshrine the very replay
/// window the rest of this plan narrows, and pin behavior no one has agreed to.</item>
/// <item><b>A-restart reconnect.</b> B's subscriber does reconnect, but its backoff is a private 30s
/// constant inside <c>GrpcSubscriberCore</c>. A test would either take 30+ seconds or need that
/// constant turned into a knob — a public contract change, which belongs in a plan of its own rather
/// than smuggled in behind a test.</item>
/// </list>
/// </summary>
[Collection(ChainHostCollection.Name)]
public sealed class GrpcChainTests : IClassFixture<TwoHostFixture>
{
    private const string SourceOnA = "chain_src";
    private const string FederatedSourceOnB = "chain_fed";
    private const string TableOnB = "chain_tbl";
    private const int TotalRows = 1000;
    private const int FileCount = 5;

    private readonly TwoHostFixture _hosts;

    public GrpcChainTests(TwoHostFixture hosts) => _hosts = hosts;

    [Fact]
    public async Task Folder_source_on_A_federated_by_peer_name_lands_every_row_in_a_table_on_B()
    {
        Assert.True(_hosts.SkipReason is null, $"skipped: {_hosts.SkipReason}");
        var a = _hosts.A!;
        var b = _hosts.B!;

        var folder = Directory.CreateTempSubdirectory("sf-chain-folder-").FullName;
        using var clientA = await a.LoginAsync();
        using var clientB = await b.LoginAsync();

        try
        {
            // ---- A: the producing folder source. Its directory is EMPTY at create time, so the first
            // polls find nothing and there is no burst waiting for B before B has subscribed.
            await PostOkAsync(clientA, "api/sources", new
            {
                name = SourceOnA,
                description = "chain test producer",
                kind = "folder",
                enabled = true,
                fields = FieldsJson(),
                connector = new
                {
                    schedule = new { intervalMs = 1000 },
                    folder = new { path = folder, format = "ndjson" },
                    mapping = new
                    {
                        itemsPath = "$",
                        dedupKeyField = "id",
                        fields = MappingFieldsJson(),
                    },
                },
            });

            // ---- B: the federated source. NO address anywhere in it — only the peer name B was
            // started with, which resolves A's gRPC and REST endpoints at every (re)connect.
            await PostOkAsync(clientB, "api/sources", new
            {
                name = FederatedSourceOnB,
                description = "chain test consumer (federated from peer 'a')",
                kind = "grpc",
                enabled = true,
                fields = FieldsJson(),
                connector = new
                {
                    grpc = new
                    {
                        peer = TwoHostFixture.PeerName,
                        entityKey = $"source:{SourceOnA}",
                        username = HostProcess.AdminUser,
                        password = HostProcess.AdminPassword,
                    },
                },
            });

            // ---- B: the table over the federated source.
            var created = await PostOkAsync(clientB, "api/tables", new
            {
                name = TableOnB,
                description = "chain test sink table",
                sql = $"SELECT id, seq, value FROM {FederatedSourceOnB} LATEST BY (id)",
            });
            var tableId = created.RootElement.GetProperty("id").GetString()!;

            var start = await clientB.PostAsync($"api/tables/{tableId}/start", content: null);
            start.EnsureSuccessStatusCode();

            await PollAsync(
                TimeSpan.FromSeconds(30),
                async () =>
                {
                    using var doc = await GetJsonAsync(clientB, $"api/tables/{tableId}");
                    return doc.RootElement.GetProperty("status").GetString() == "Running";
                },
                "table chain_tbl never reached Running on B",
                b);

            // "ok" is reported just BEFORE the subscribe RPC is issued (see the class doc); the fixed
            // cushion is what makes "the subscription exists" true rather than merely imminent.
            await PollAsync(
                TimeSpan.FromSeconds(45),
                async () =>
                {
                    using var doc = await GetJsonAsync(clientB, $"api/sources/{FederatedSourceOnB}/status");
                    return doc.RootElement.TryGetProperty("lastStatus", out var s) && s.GetString() == "ok";
                },
                "federated source chain_fed never reported lastStatus 'ok' on B",
                b);
            await Task.Delay(TimeSpan.FromSeconds(1));

            // ---- A: 1000 rows as 5 files of 200, ~100ms apart, so files keep "slipping in" while the
            // connector is mid-cycle. Each file is written to a temp name and MOVED into place, so the
            // folder never contains a half-written .ndjson for a poll to trip over.
            for (var file = 0; file < FileCount; file++)
            {
                var sb = new StringBuilder();
                for (var i = 0; i < TotalRows / FileCount; i++)
                {
                    var seq = (file * (TotalRows / FileCount)) + i;
                    sb.Append(
                        JsonSerializer.Serialize(new { id = seq, seq, value = $"v{seq}" }))
                      .Append('\n');
                }
                var staging = Path.Combine(folder, $"part{file:D2}.ndjson.tmp");
                await File.WriteAllTextAsync(staging, sb.ToString());
                File.Move(staging, Path.Combine(folder, $"part{file:D2}.ndjson"));
                await Task.Delay(100);
            }

            // ---- B: every row, exactly once.
            JsonDocument? rows = null;
            try
            {
                await PollAsync(
                    TimeSpan.FromSeconds(45),
                    async () =>
                    {
                        rows?.Dispose();
                        rows = await GetJsonAsync(clientB, $"api/tables/{tableId}/rows?limit=5000");
                        return rows.RootElement.GetProperty("totalRows").GetInt32() == TotalRows;
                    },
                    $"table chain_tbl on B never reached {TotalRows} rows",
                    b);

                var seqs = rows!.RootElement.GetProperty("rows").EnumerateArray()
                    .Select(r => r.GetProperty("row").GetProperty("seq").GetInt64())
                    .ToHashSet();
                Assert.Equal(TotalRows, seqs.Count);
                var missing = Enumerable.Range(0, TotalRows).Select(i => (long)i).Where(i => !seqs.Contains(i)).Take(10).ToList();
                Assert.True(missing.Count == 0, $"seq values missing from the table on B: {string.Join(",", missing)}");
            }
            finally
            {
                rows?.Dispose();
            }

            using var status = await GetJsonAsync(clientB, $"api/sources/{FederatedSourceOnB}/status");
            Assert.Equal(TotalRows, status.RootElement.GetProperty("eventsEmittedTotal").GetInt64());
        }
        finally
        {
            try
            {
                Directory.Delete(folder, recursive: true);
            }
            catch
            {
                // best-effort cleanup
            }
        }
    }

    // ---- shared field/mapping shape: id Long, seq Long, value String; dedup key "id" ----

    private static object[] FieldsJson() =>
    [
        new { name = "id", type = "Long" },
        new { name = "seq", type = "Long" },
        new { name = "value", type = "String" },
    ];

    private static object[] MappingFieldsJson() =>
        FieldsJson().Select(f => (object)new { field = f }).ToArray();

    // ---- REST helpers. These tests speak the wire, never an in-process type: the whole point of a
    // two-process test is that everything crosses a serialization boundary. ----

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

    /// <summary>Polls <paramref name="condition"/> to a deadline and, on timeout, fails with the
    /// offending host's own log tail attached — a chain failure is otherwise indistinguishable between
    /// "A never emitted", "B never subscribed" and "the table never ran".</summary>
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
