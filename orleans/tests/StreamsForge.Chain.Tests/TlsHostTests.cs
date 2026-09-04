using System.Net.Http.Json;
using System.Text.Json;
using Xunit;

namespace StreamsForge.Chain.Tests;

/// <summary>
/// One real host booted with <c>--Tls:Enabled true</c> and a certificate minted by the repo's own
/// <c>tools/tls/dev-cert.sh</c>, on 6599 (REST) / 6699 (gRPC), silo 16599 / gateway 36599.
///
/// <para>Everything here needs a REAL Kestrel with a REAL certificate — an in-process
/// <c>WebApplicationFactory</c> never binds a socket, so it cannot tell you whether
/// <c>listenOptions.UseHttps()</c> found the certificate, whether the scheme reaches
/// <c>GET /api/meta/instance</c>, or whether a plaintext request to the TLS port is refused. Those are
/// exactly the three things this fixture exists to answer.</para>
/// </summary>
public sealed class TlsHostFixture : IAsyncLifetime
{
    public const int HttpPort = 6599;
    public const int GrpcPort = 6699;
    public const int SiloPort = 16599;
    public const int GatewayPort = 36599;

    public HostProcess? Host { get; private set; }

    public DevCert? Cert { get; private set; }

    public string? SkipReason { get; private set; }

    public async Task InitializeAsync()
    {
        SkipReason = DevCert.Preflight()
            ?? HostProcess.Preflight(HttpPort, GrpcPort, SiloPort, GatewayPort);
        if (SkipReason is not null)
        {
            return;
        }

        try
        {
            Cert = DevCert.Create();
            Host = new HostProcess("tls", HttpPort, GrpcPort, SiloPort, GatewayPort, Cert.HostArgs)
            {
                TlsCertPath = Cert.CertPath,
            };
            Host.Start();
            await Host.WaitHealthyAsync();
        }
        catch (Exception ex)
        {
            SkipReason = $"the TLS host fixture did not come up cleanly: {ex.Message}";
            await DisposeAsync();
        }
    }

    public async Task DisposeAsync()
    {
        if (Host is not null)
        {
            await Host.DisposeAsync();
            Host = null;
        }
        Cert?.Dispose();
        Cert = null;
    }
}

[Collection(ChainHostCollection.Name)]
public sealed class TlsHostTests : IClassFixture<TlsHostFixture>
{
    private readonly TlsHostFixture _fixture;

    public TlsHostTests(TlsHostFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task Healthz_and_login_both_work_over_https()
    {
        Assert.True(_fixture.SkipReason is null, $"skipped: {_fixture.SkipReason}");
        var host = _fixture.Host!;

        Assert.StartsWith("https://", host.BaseUrl, StringComparison.Ordinal);

        using var anon = host.NewClient();
        var health = await anon.GetAsync("api/healthz");
        Assert.True(
            health.IsSuccessStatusCode,
            $"GET {host.BaseUrl}/api/healthz -> {(int)health.StatusCode}\n{host.LogTailText()}");

        // Login is the first thing that carries a credential over the wire, so it is the assertion that
        // actually matters: a TLS listener that answers /healthz but breaks the auth POST would be a
        // half-working server nobody notices until they try to use it.
        using var authed = await host.LoginAsync();
        var me = await authed.GetAsync("api/sources");
        Assert.True(me.IsSuccessStatusCode, $"GET api/sources over https -> {(int)me.StatusCode}");
    }

    [Fact]
    public async Task Meta_instance_reports_https_for_both_endpoints()
    {
        Assert.True(_fixture.SkipReason is null, $"skipped: {_fixture.SkipReason}");
        var host = _fixture.Host!;

        using var anon = host.NewClient();
        var body = await anon.GetStringAsync("api/meta/instance");
        using var doc = JsonDocument.Parse(body);
        var endpoints = doc.RootElement.GetProperty("endpoints");

        // MetaEndpoints derives both from request.Scheme, so this is the end-to-end proof that Kestrel
        // really is serving TLS: a cleartext listener would report http:// here no matter what
        // Tls:Enabled said. A federated peer reads exactly these two strings out of the directory.
        var rest = endpoints.GetProperty("rest").GetString();
        Assert.StartsWith("https://", rest, StringComparison.Ordinal);
        Assert.Equal($"https://127.0.0.1:{TlsHostFixture.GrpcPort}", endpoints.GetProperty("grpc").GetString());
    }

    [Fact]
    public async Task Strict_Transport_Security_is_sent_for_a_non_loopback_host_and_not_for_loopback()
    {
        Assert.True(_fixture.SkipReason is null, $"skipped: {_fixture.SkipReason}");
        var host = _fixture.Host!;

        using var client = host.NewClient();

        // ASP.NET's HstsOptions.ExcludedHosts defaults to localhost/127.0.0.1/[::1] and this platform
        // does NOT override it (see StreamsForgeApiExtensions.MapStreamsForgeApi for why: HSTS is scoped
        // to a host and ignores the port, so emitting it for `localhost` would poison every other
        // project on the developer's machine). So the header is proven the way a deployment sees it —
        // with a real hostname — and its ABSENCE on loopback is asserted too, because that asymmetry is
        // the thing a future reader will otherwise file as a bug.
        using var named = new HttpRequestMessage(HttpMethod.Get, "api/healthz");
        named.Headers.Host = "streamsforge.test";
        var withName = await client.SendAsync(named);
        Assert.True(withName.IsSuccessStatusCode, $"GET api/healthz (Host: streamsforge.test) -> {(int)withName.StatusCode}");
        Assert.True(
            withName.Headers.Contains("Strict-Transport-Security"),
            "no Strict-Transport-Security on a TLS host addressed by a non-loopback Host header\n"
          + host.LogTailText());

        var loopback = await client.GetAsync("api/healthz");
        Assert.False(
            loopback.Headers.Contains("Strict-Transport-Security"),
            "Strict-Transport-Security was sent for a loopback host — HstsOptions.ExcludedHosts is "
          + "supposed to keep it off localhost/127.0.0.1; if this default changed deliberately, this "
          + "assertion and the comment in MapStreamsForgeApi both need updating.");
    }

    [Fact]
    public async Task A_plaintext_request_to_the_TLS_port_does_not_get_a_working_response()
    {
        Assert.True(_fixture.SkipReason is null, $"skipped: {_fixture.SkipReason}");

        // The whole point of Tls:Enabled is that the cleartext door is CLOSED. Kestrel fails the
        // handshake on a plaintext HTTP/1.1 request to an https endpoint and resets the connection, so
        // what is asserted is "no usable 2xx", not a specific exception type — the failure mode differs
        // between platforms and runtime versions and pinning one would be a flake waiting to happen.
        using var plain = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };
        try
        {
            var resp = await plain.GetAsync($"http://127.0.0.1:{TlsHostFixture.HttpPort}/api/healthz");
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
}

/// <summary>
/// The other half of <c>Tls:Enabled</c>: turning it on with no certificate configured must STOP the
/// host, not boot it in cleartext. Its own class (and its own ports, 6999/7099, silo 16999/36999)
/// because the host under test never becomes healthy — folding it into <see cref="TlsHostFixture"/>
/// would make a fixture whose whole job is a working host also own one that must not work, and would
/// depend on xunit running the facts in a particular order.
/// </summary>
[Collection(ChainHostCollection.Name)]
public sealed class TlsStartupFailureTests
{
    private const int HttpPort = 6999;
    private const int GrpcPort = 7099;
    private const int SiloPort = 16999;
    private const int GatewayPort = 36999;

    [Fact]
    public async Task Tls_enabled_without_a_certificate_exits_non_zero_naming_the_configuration_key()
    {
        var skip = HostProcess.Preflight(HttpPort, GrpcPort, SiloPort, GatewayPort);
        Assert.True(skip is null, $"skipped: {skip}");

        await using var host = new HostProcess(
            "tls-nocert", HttpPort, GrpcPort, SiloPort, GatewayPort, "--Tls:Enabled", "true");
        host.Start();

        var exitCode = await host.WaitForExitAsync(TimeSpan.FromSeconds(30));
        Assert.True(
            exitCode is not null,
            "a host started with Tls:Enabled=true and no certificate was STILL RUNNING after 30s — it "
          + "must fail fast rather than serve cleartext under a config that says TLS is on.\n"
          + host.LogTailText());
        Assert.True(exitCode != 0, $"expected a non-zero exit code, got {exitCode}\n{host.LogTailText()}");

        var log = host.LogTailText();
        Assert.Contains("Kestrel:Certificates:Default", log, StringComparison.Ordinal);
        // The message must also say where to get one — an operator hitting this needs the next step,
        // not just the name of a key they already know they did not set.
        Assert.Contains("tools/tls/dev-cert.sh", log, StringComparison.Ordinal);
    }
}
