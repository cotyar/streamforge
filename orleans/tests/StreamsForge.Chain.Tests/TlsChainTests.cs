using System.Diagnostics;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using Xunit;

namespace StreamsForge.Chain.Tests;

/// <summary>
/// <see cref="GrpcChainTests"/>, but host A serves TLS with a certificate no public root vouches for
/// and host B is told to trust it via <c>--Tls:TrustedCaPath</c>. Three things that can only be proven
/// together, across a process boundary, are proven here in one pass:
///
/// <list type="number">
/// <item><b>The server's TLS gRPC listener works.</b> Under <c>Tls:Enabled</c> the gRPC port is no
/// longer h2c but ALPN-negotiated h2. Nothing in the unit suites exercises that — a
/// <c>TestCluster</c> has no listener at all.</item>
/// <item><b>The peer directory works over https.</b> B's <c>Discovery:Peers:0</c> entry names A with
/// <c>https://</c> endpoints, so <c>PeerProbe</c>'s REST call and the subscriber's login BOTH cross
/// TLS before a single row moves.</item>
/// <item><b><c>OutboundTls</c> is actually wired into every client.</b> The chain touches the peer
/// probe, the subscriber's REST login, the reflection descriptor fetch and the gRPC channel itself. If
/// any one of them had been left on a bare <c>new HttpClient()</c>, this test fails at that hop and the
/// error names it.</item>
/// </list>
///
/// <para><b>Ports</b>: A = 8799 REST / 8899 gRPC / 18799 silo / 38799 gateway; B = 8999 / 9099 /
/// 18999 / 38999. Disjoint from the plain chain fixture's 9399–9699 so the two can never collide even
/// if the collection's serialization were ever relaxed.</para>
///
/// <para><b>Why A's own cert.pem is the "CA".</b> <c>tools/tls/dev-cert.sh</c> self-signs with
/// <c>CA:TRUE</c>, so the single file is both the leaf A serves and the trust anchor B pins. That is a
/// development shape, not a recommendation — a real deployment has a separate root — but it is the
/// shape the script documents and therefore the one worth testing.</para>
///
/// <para>The row count is 200, not <see cref="GrpcChainTests"/>' 1000: this test is about the TLS hops,
/// and the "every row, exactly once" property is already asserted at volume by that test. The 1 s
/// cushion after <c>lastStatus == "ok"</c> is load-bearing for the same reason it is there — "ok" is
/// set just BEFORE the subscribe RPC is issued, so writing rows on "ok" races the subscription.</para>
/// </summary>
public sealed class TlsTwoHostFixture : IAsyncLifetime
{
    public const int AHttpPort = 8799;
    public const int AGrpcPort = 8899;
    public const int ASiloPort = 18799;
    public const int AGatewayPort = 38799;

    public const int BHttpPort = 8999;
    public const int BGrpcPort = 9099;
    public const int BSiloPort = 18999;
    public const int BGatewayPort = 38999;

    public const string PeerName = "a";

    public HostProcess? A { get; private set; }

    public HostProcess? B { get; private set; }

    /// <summary>A second B, booted with NO <c>Tls:TrustedCaPath</c> at all, on its own ports. Proves
    /// the negative: without the trust configuration the federated source cannot connect, so a green
    /// chain above really is the trust doing the work rather than validation being off somewhere.</summary>
    public HostProcess? BUntrusting { get; private set; }

    public const int UntrustingHttpPort = 8699;
    public const int UntrustingGrpcPort = 8599;
    public const int UntrustingSiloPort = 18699;
    public const int UntrustingGatewayPort = 38699;

    public DevCert? Cert { get; private set; }

    public string? SkipReason { get; private set; }

    public async Task InitializeAsync()
    {
        SkipReason = DevCert.Preflight()
            ?? HostProcess.Preflight(
                AHttpPort, AGrpcPort, ASiloPort, AGatewayPort,
                BHttpPort, BGrpcPort, BSiloPort, BGatewayPort,
                UntrustingHttpPort, UntrustingGrpcPort, UntrustingSiloPort, UntrustingGatewayPort);
        if (SkipReason is not null)
        {
            return;
        }

        try
        {
            Cert = DevCert.Create();

            A = new HostProcess("tls-a", AHttpPort, AGrpcPort, ASiloPort, AGatewayPort, Cert.HostArgs)
            {
                TlsCertPath = Cert.CertPath,
            };

            // B is CLEARTEXT itself — this test is about what B trusts on the way OUT, which is an
            // entirely separate axis from whether B serves TLS on the way in.
            B = new HostProcess(
                "tls-b", BHttpPort, BGrpcPort, BSiloPort, BGatewayPort,
                "--Tls:TrustedCaPath", Cert.CertPath,
                "--Discovery:Peers:0:Name", PeerName,
                "--Discovery:Peers:0:RestEndpoint", $"https://127.0.0.1:{AHttpPort}",
                "--Discovery:Peers:0:GrpcEndpoint", $"https://127.0.0.1:{AGrpcPort}");

            BUntrusting = new HostProcess(
                "tls-b-untrusting",
                UntrustingHttpPort, UntrustingGrpcPort, UntrustingSiloPort, UntrustingGatewayPort,
                "--Discovery:Peers:0:Name", PeerName,
                "--Discovery:Peers:0:RestEndpoint", $"https://127.0.0.1:{AHttpPort}",
                "--Discovery:Peers:0:GrpcEndpoint", $"https://127.0.0.1:{AGrpcPort}");

            A.Start();
            B.Start();
            BUntrusting.Start();
            await Task.WhenAll(A.WaitHealthyAsync(), B.WaitHealthyAsync(), BUntrusting.WaitHealthyAsync());
        }
        catch (Exception ex)
        {
            SkipReason = $"the TLS two-host fixture did not come up cleanly: {ex.Message}";
            await DisposeAsync();
        }
    }

    public async Task DisposeAsync()
    {
        foreach (var host in new[] { A, B, BUntrusting })
        {
            if (host is not null)
            {
                await host.DisposeAsync();
            }
        }
        A = null;
        B = null;
        BUntrusting = null;
        Cert?.Dispose();
        Cert = null;
    }
}

[Collection(ChainHostCollection.Name)]
public sealed class TlsChainTests : IClassFixture<TlsTwoHostFixture>
{
    private const string SourceOnA = "tls_src";
    private const string FederatedSourceOnB = "tls_fed";
    private const string TableOnB = "tls_tbl";
    private const int TotalRows = 200;
    private const int FileCount = 2;

    private readonly TlsTwoHostFixture _hosts;

    public TlsChainTests(TlsTwoHostFixture hosts) => _hosts = hosts;

    [Fact]
    public async Task Every_row_crosses_a_TLS_gRPC_hop_from_a_peer_trusted_only_by_TrustedCaPath()
    {
        Assert.True(_hosts.SkipReason is null, $"skipped: {_hosts.SkipReason}");
        var a = _hosts.A!;
        var b = _hosts.B!;

        var folder = Directory.CreateTempSubdirectory("sf-tls-chain-").FullName;
        using var clientA = await a.LoginAsync();
        using var clientB = await b.LoginAsync();

        try
        {
            await PostOkAsync(clientA, "api/sources", new
            {
                name = SourceOnA,
                description = "tls chain test producer",
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

            // No address anywhere — only the peer name, whose directory entry is https on both halves.
            await PostOkAsync(clientB, "api/sources", new
            {
                name = FederatedSourceOnB,
                description = "tls chain consumer (federated from peer 'a' over https)",
                kind = "grpc",
                enabled = true,
                fields = FieldsJson(),
                connector = new
                {
                    grpc = new
                    {
                        peer = TlsTwoHostFixture.PeerName,
                        entityKey = $"source:{SourceOnA}",
                        username = HostProcess.AdminUser,
                        password = HostProcess.AdminPassword,
                    },
                },
            });

            var created = await PostOkAsync(clientB, "api/tables", new
            {
                name = TableOnB,
                description = "tls chain sink table",
                sql = $"SELECT id, seq, value FROM {FederatedSourceOnB} LATEST BY (id)",
            });
            var tableId = created.RootElement.GetProperty("id").GetString()!;

            var start = await clientB.PostAsync($"api/tables/{tableId}/start", content: null);
            start.EnsureSuccessStatusCode();

            await PollAsync(
                TimeSpan.FromSeconds(45),
                async () =>
                {
                    using var doc = await GetJsonAsync(clientB, $"api/sources/{FederatedSourceOnB}/status");
                    return doc.RootElement.TryGetProperty("lastStatus", out var s) && s.GetString() == "ok";
                },
                $"federated source {FederatedSourceOnB} never reported lastStatus 'ok' on B — the TLS "
              + "gRPC dial or the https peer probe/login did not succeed",
                b);
            await Task.Delay(TimeSpan.FromSeconds(1));

            for (var file = 0; file < FileCount; file++)
            {
                var sb = new StringBuilder();
                for (var i = 0; i < TotalRows / FileCount; i++)
                {
                    var seq = (file * (TotalRows / FileCount)) + i;
                    sb.Append(JsonSerializer.Serialize(new { id = seq, seq, value = $"v{seq}" })).Append('\n');
                }
                var staging = Path.Combine(folder, $"part{file:D2}.ndjson.tmp");
                await File.WriteAllTextAsync(staging, sb.ToString());
                File.Move(staging, Path.Combine(folder, $"part{file:D2}.ndjson"));
                await Task.Delay(100);
            }

            JsonDocument? rows = null;
            try
            {
                await PollAsync(
                    TimeSpan.FromSeconds(45),
                    async () =>
                    {
                        rows?.Dispose();
                        rows = await GetJsonAsync(clientB, $"api/tables/{tableId}/rows?limit=1000");
                        return rows.RootElement.GetProperty("totalRows").GetInt32() == TotalRows;
                    },
                    $"table {TableOnB} on B never reached {TotalRows} rows over the TLS hop",
                    b);

                var seqs = rows!.RootElement.GetProperty("rows").EnumerateArray()
                    .Select(r => r.GetProperty("row").GetProperty("seq").GetInt64())
                    .ToHashSet();
                Assert.Equal(TotalRows, seqs.Count);
                var missing = Enumerable.Range(0, TotalRows).Select(i => (long)i)
                    .Where(i => !seqs.Contains(i)).Take(10).ToList();
                Assert.True(missing.Count == 0, $"seq values missing from the table on B: {string.Join(",", missing)}");
            }
            finally
            {
                rows?.Dispose();
            }
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

    [Fact]
    public async Task Without_TrustedCaPath_the_same_federated_source_fails_with_a_certificate_error()
    {
        Assert.True(_hosts.SkipReason is null, $"skipped: {_hosts.SkipReason}");
        var b = _hosts.BUntrusting!;

        // Same peer, same private certificate, no Tls:TrustedCaPath — the ONLY difference from the fact
        // above. Without this, a green chain test would be equally consistent with "outbound validation
        // is off entirely", which is the failure mode most worth ruling out in a TLS change.
        using var clientB = await b.LoginAsync();
        await PostOkAsync(clientB, "api/sources", new
        {
            name = FederatedSourceOnB,
            description = "tls chain consumer with no trust configured",
            kind = "grpc",
            enabled = true,
            fields = FieldsJson(),
            connector = new
            {
                grpc = new
                {
                    peer = TlsTwoHostFixture.PeerName,
                    entityKey = "source:tls_src",
                    username = HostProcess.AdminUser,
                    password = HostProcess.AdminPassword,
                },
            },
        });

        string? lastError = null;
        await PollAsync(
            TimeSpan.FromSeconds(30),
            async () =>
            {
                using var doc = await GetJsonAsync(clientB, $"api/sources/{FederatedSourceOnB}/status");
                if (doc.RootElement.TryGetProperty("lastError", out var e) && e.ValueKind == JsonValueKind.String)
                {
                    lastError = e.GetString();
                }
                var status = doc.RootElement.TryGetProperty("lastStatus", out var s) ? s.GetString() : null;
                return !string.IsNullOrEmpty(lastError) || status == "error";
            },
            "a federated source pointed at a privately-signed peer with NO Tls:TrustedCaPath never "
          + "reported an error — outbound certificate validation is not happening",
            b);

        Assert.False(string.IsNullOrEmpty(lastError), $"expected a lastError, got '{lastError}'");
        var hay = lastError!.ToLowerInvariant();
        Assert.True(
            hay.Contains("cert") || hay.Contains("ssl") || hay.Contains("tls")
                || hay.Contains("trust") || hay.Contains("secure channel") || hay.Contains("authentication"),
            $"lastError does not read like a TLS trust failure: {lastError}");

        using var status = await GetJsonAsync(clientB, $"api/sources/{FederatedSourceOnB}/status");
        Assert.NotEqual("ok", status.RootElement.TryGetProperty("lastStatus", out var st) ? st.GetString() : null);
    }

    private static object[] FieldsJson() =>
    [
        new { name = "id", type = "Long" },
        new { name = "seq", type = "Long" },
        new { name = "value", type = "String" },
    ];

    private static object[] MappingFieldsJson() =>
        FieldsJson().Select(f => (object)new { field = f }).ToArray();

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
