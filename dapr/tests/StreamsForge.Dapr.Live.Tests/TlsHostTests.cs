using System.Text.Json;
using Xunit;

namespace StreamsForge.Dapr.Live.Tests;

/// <summary>
/// Plan 025 D9, live: one real Dapr-flavor instance booted with <c>--Tls:Enabled true</c> and a
/// certificate minted by the repo's own <c>tools/tls/dev-cert.sh</c>, on this project's usual 5799
/// (REST) / 5899 (gRPC) — the Dapr twin of
/// <c>orleans/tests/StreamsForge.Chain.Tests/TlsHostTests</c>.
///
/// <para>Everything here needs a REAL Kestrel with a REAL certificate. An in-process
/// <c>WebApplicationFactory</c> never binds a socket, so it cannot say whether
/// <c>listenOptions.UseHttps()</c> found the certificate, whether the scheme reaches
/// <c>/api/meta/instance</c>, or whether a plaintext request to the TLS port is refused.</para>
///
/// <para><b>The Dapr-only fact this fixture exists to prove, and the reason it is not just a copy of the
/// Orleans one.</b> The sidecar calls the APP port — for actor activation, actor method invocation and
/// every pub/sub topic delivery — and it speaks cleartext <c>http://</c> unless <c>dapr run</c> is given
/// <c>--app-protocol https</c> (see <see cref="DevCert.DaprRunArgs"/> and
/// <see cref="DaprHostProcess.DaprRunExtraArgs"/>). Get that wrong and the failure is not a TLS error:
/// the app answers <c>curl --cacert</c> perfectly while every actor call and every topic delivery
/// silently fails, i.e. an instance that looks healthy and holds no data. That is exactly why
/// <see cref="Seeded_tables_keep_filling_which_proves_the_sidecar_reaches_the_app_over_https"/> exists
/// and is the most load-bearing fact in this class — the three "does https work" facts around it would
/// all pass against a broken instance.</para>
///
/// <para><b>gRPC over TLS is asserted HERE rather than in <see cref="GrpcServingTests"/></b>, which owns
/// the cleartext gRPC surface. Not for tidiness: every class in this project shares one app-id and one
/// set of ports (see <see cref="DaprLiveTestCollection"/>), so two classes cannot hold a host each, and a
/// TLS gRPC fact living in the other class would mean a second full boot — ~25 s to re-assert something
/// this fixture's host can answer directly.</para>
///
/// <para><b>What is NOT covered here.</b> The fail-fast case (<c>Tls:Enabled</c> with no certificate must
/// exit non-zero rather than serve cleartext) is a Program.cs branch shared with Orleans, already pinned
/// live by <c>TlsStartupFailureTests</c> there and by the Dapr host's own unit coverage; asserting it
/// again would cost a whole boot of a host whose entire job is to not boot. Outbound TLS
/// (<c>Tls:TrustedCaPath</c>, <c>Tls:AcceptAnyCertificate</c>) is likewise a shared
/// <c>OutboundTls</c> concern with its own tests — what is Dapr-specific is only that
/// <c>Program.cs</c> now CALLS it, which a live TLS test cannot distinguish from not needing to.</para>
/// </summary>
public sealed class TlsHostFixture : LiveHostFixture
{
    /// <summary>The certificate this instance serves, and the trust anchor its clients (and grpcurl) use.
    /// Minted in <see cref="CreateHost"/> because the host arguments are built from it.</summary>
    public DevCert? Cert { get; private set; }

    protected override string Label => "tls";

    protected override string? ExtraPreflight() => DevCert.Preflight();

    protected override DaprHostProcess CreateHost()
    {
        Cert = DevCert.Create();
        return new DaprHostProcess(Label, Cert.HostArgs)
        {
            TlsCertPath = Cert.CertPath,
            // Without this the sidecar probes the app port in cleartext and never gets an answer — see
            // DaprHostProcess's TLS paragraph. It is the one half of TLS with no Orleans equivalent.
            DaprRunExtraArgs = DevCert.DaprRunArgs,
        };
    }

    protected override Task OnDisposingAsync()
    {
        Cert?.Dispose();
        Cert = null;
        return Task.CompletedTask;
    }
}

[Collection(DaprLiveTestCollection.Name)]
public sealed class TlsHostTests(TlsHostFixture fixture) : IClassFixture<TlsHostFixture>
{
    private DaprHostProcess? _host => fixture.Host;
    private DevCert? _cert => fixture.Cert;
    private string? _skipReason => fixture.SkipReason;

    [Fact]
    public async Task Healthz_and_login_both_work_over_https_and_plaintext_does_not()
    {
        Assert.True(_skipReason is null, $"skipped: {_skipReason}");
        var host = _host!;

        Assert.StartsWith("https://", host.BaseUrl, StringComparison.Ordinal);

        using var anon = host.NewClient();
        var health = await anon.GetAsync($"{host.BaseUrl}/api/healthz");
        Assert.True(
            health.IsSuccessStatusCode,
            $"GET {host.BaseUrl}/api/healthz -> {(int)health.StatusCode}\n{host.LogTailText()}");

        // Login is the first request that carries a credential, so it is the assertion that matters: a
        // TLS listener answering /healthz but breaking the auth POST is a half-working server nobody
        // notices until they try to use it.
        using var authed = await host.LoginAsync();
        var sources = await authed.GetAsync($"{host.BaseUrl}/api/sources");
        Assert.True(sources.IsSuccessStatusCode, $"GET api/sources over https -> {(int)sources.StatusCode}");

        // The other half of Tls:Enabled: the cleartext door is CLOSED. What is asserted is "no usable
        // 2xx", not a specific exception type — the failure mode of a plaintext HTTP/1.1 request to a TLS
        // listener differs across platforms and runtime versions, and pinning one is a flake waiting to
        // happen.
        using var plain = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };
        try
        {
            var resp = await plain.GetAsync($"http://127.0.0.1:{DaprHostProcess.HttpPort}/api/healthz");
            Assert.False(
                resp.IsSuccessStatusCode,
                $"a plaintext GET to the TLS port answered {(int)resp.StatusCode} — the listener is not "
              + "actually requiring TLS");
        }
        catch (HttpRequestException)
        {
            // Expected: connection reset / invalid response on a TLS listener.
        }
        catch (TaskCanceledException)
        {
            // Also acceptable: the handshake hangs until the client's own timeout.
        }
    }

    [Fact]
    public async Task Meta_instance_reports_https_for_both_endpoints()
    {
        Assert.True(_skipReason is null, $"skipped: {_skipReason}");
        var host = _host!;

        using var anon = host.NewClient();
        var body = await anon.GetStringAsync($"{host.BaseUrl}/api/meta/instance");
        using var doc = JsonDocument.Parse(body);
        var endpoints = doc.RootElement.GetProperty("endpoints");

        // MetaEndpoints derives both strings from request.Scheme, so this is the end-to-end proof that
        // Kestrel really is serving TLS: a cleartext listener would report http:// here whatever
        // Tls:Enabled said. A federated peer reads exactly these two strings out of the directory, which
        // is what makes this more than cosmetic.
        Assert.StartsWith("https://", endpoints.GetProperty("rest").GetString(), StringComparison.Ordinal);
        Assert.Equal($"https://127.0.0.1:{DaprHostProcess.GrpcPort}", endpoints.GetProperty("grpc").GetString());
        Assert.Contains(
            "grpc",
            doc.RootElement.GetProperty("capabilities").EnumerateArray().Select(c => c.GetString()));
    }

    [Fact]
    public async Task Strict_Transport_Security_is_sent_for_a_non_loopback_host_and_not_for_loopback()
    {
        Assert.True(_skipReason is null, $"skipped: {_skipReason}");
        var host = _host!;

        using var client = host.NewClient();

        // ASP.NET's HstsOptions.ExcludedHosts defaults to localhost/127.0.0.1/[::1] and this platform
        // does NOT override it (see StreamsForgeApiExtensions.MapStreamsForgeApi: HSTS is scoped to a
        // HOST and ignores the port, so emitting it for `localhost` would poison every other project on
        // the developer's machine). The header is therefore proven the way a deployment sees it — with a
        // real hostname — and its ABSENCE on loopback is asserted too, because that asymmetry is the
        // thing a future reader would otherwise file as a bug.
        using var named = new HttpRequestMessage(HttpMethod.Get, $"{host.BaseUrl}/api/healthz");
        named.Headers.Host = "streamsforge.test";
        var withName = await client.SendAsync(named);
        Assert.True(withName.IsSuccessStatusCode, $"GET api/healthz (Host: streamsforge.test) -> {(int)withName.StatusCode}");
        Assert.True(
            withName.Headers.Contains("Strict-Transport-Security"),
            "no Strict-Transport-Security on a TLS host addressed by a non-loopback Host header\n"
          + host.LogTailText());

        var loopback = await client.GetAsync($"{host.BaseUrl}/api/healthz");
        Assert.False(
            loopback.Headers.Contains("Strict-Transport-Security"),
            "Strict-Transport-Security was sent for a loopback host — HstsOptions.ExcludedHosts is "
          + "supposed to keep it off localhost/127.0.0.1; if that default changed deliberately, this "
          + "assertion and the comment in MapStreamsForgeApi both need updating.");
    }

    /// <summary>
    /// The fact this whole fixture is really for — see the class doc. If <c>--app-protocol https</c>
    /// were missing (or the host served TLS on a port the sidecar dialled in cleartext), every actor
    /// call and every pub/sub delivery would fail while the three facts above still passed. A seeded
    /// table whose row count is climbing is proof that the sidecar → app channel works, because that
    /// channel is the only way a topic delivery reaches <c>TableActor</c>.
    ///
    /// <para><c>deltasIn</c>, not <c>rowCount</c> — the same measurement <c>RestartResumeTests</c> had to
    /// switch to and documents at length: <c>positions</c> is <c>… GROUP BY symbol</c>, so its row count
    /// is bounded by the fixed number of demo symbols and legitimately stops growing, while
    /// <c>deltasIn</c> keeps climbing for as long as deliveries keep arriving.</para>
    /// </summary>
    [Fact]
    public async Task Seeded_tables_keep_filling_which_proves_the_sidecar_reaches_the_app_over_https()
    {
        Assert.True(_skipReason is null, $"skipped: {_skipReason}");
        var host = _host!;

        using var client = await host.LoginAsync();

        using var tables = await LiveRest.GetJsonAsync(client, $"{host.BaseUrl}/api/tables");
        var positionsId = tables.RootElement.EnumerateArray()
            .First(t => t.GetProperty("name").GetString() == "positions")
            .GetProperty("id").GetString()!;

        long first = 0;
        await LiveRest.PollAsync(
            TimeSpan.FromSeconds(60),
            async () =>
            {
                using var doc = await LiveRest.GetJsonAsync(client, $"{host.BaseUrl}/api/tables/{positionsId}/metrics");
                first = doc.RootElement.GetProperty("deltasIn").GetInt64();
                return first > 0;
            },
            () => $"seeded table 'positions' never received a delta over a TLS app port (deltasIn: {first}) "
                + "— this is what a missing `dapr run --app-protocol https` looks like",
            host.LogTailText);

        long second = first;
        await LiveRest.PollAsync(
            TimeSpan.FromSeconds(60),
            async () =>
            {
                using var doc = await LiveRest.GetJsonAsync(client, $"{host.BaseUrl}/api/tables/{positionsId}/metrics");
                second = doc.RootElement.GetProperty("deltasIn").GetInt64();
                return second > first;
            },
            () => $"seeded table 'positions' received deltas once ({first}) and then stopped (still {second})",
            host.LogTailText);
    }

    /// <summary>
    /// gRPC over TLS, probed from outside the process with grpcurl and the dev certificate as the trust
    /// anchor (<c>-cacert</c>, NOT <c>-insecure</c>): the script self-signs with <c>CA:TRUE</c> and the
    /// SAN list includes <c>127.0.0.1</c>, so a real chain-and-name validation is what this asserts.
    /// <c>-insecure</c> would pass against a host serving any certificate at all, which is the one thing
    /// worth ruling out here.
    /// </summary>
    [Fact]
    public async Task Grpcurl_lists_and_streams_over_TLS_with_the_dev_certificate_as_trust_anchor()
    {
        var skip = _skipReason ?? Grpcurl.Preflight();
        Assert.True(skip is null, $"skipped: {skip}");
        var host = _host!;
        var cert = _cert!;
        var target = $"127.0.0.1:{DaprHostProcess.GrpcPort}";

        var list = await Grpcurl.RunAsync(TimeSpan.FromSeconds(30), "-cacert", cert.CertPath, target, "list");
        Assert.True(list.ExitCode == 0, $"grpcurl -cacert … list failed\n{list.Combined}\n{host.LogTailText()}");
        foreach (var service in GrpcServingTests.ExpectedServices)
        {
            Assert.Contains(service, list.Stdout, StringComparison.Ordinal);
        }

        var token = await GrpcServingTests.BearerTokenAsync(host);
        var stream = await Grpcurl.RunAsync(
            TimeSpan.FromSeconds(45),
            "-cacert", cert.CertPath,
            "-H", $"authorization: Bearer {token}",
            "-max-time", "30",
            "-d", "{\"name\":\"positions\"}",
            target,
            "streamsforge.v1.StreamService/SubscribeTable");

        // Exit code is NOT the verdict on a streaming call — see Grpcurl's class doc.
        Assert.Contains("\"table_name\": \"positions\"", stream.Stdout, StringComparison.Ordinal);
    }
}
