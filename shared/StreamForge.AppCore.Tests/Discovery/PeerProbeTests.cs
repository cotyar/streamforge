using System.Net;
using System.Text;
using StreamForge.Abstractions;
using StreamForge.AppCore.Discovery;
using Xunit;

namespace StreamForge.AppCore.Tests.Discovery;

/// <summary>Plan 016 wave 5 — <see cref="PeerProbe"/>. Ownership: track A.
///
/// <para><b>Known hazard, written down rather than silently worked around.</b> <see cref="PeerDirectory"/>
/// is a process-wide static registry (its own class doc says so) and this test class calls
/// <see cref="PeerDirectory.Configure"/>/<see cref="PeerDirectory.Clear"/>, which REPLACE the whole set.
/// xunit runs different test CLASSES in this assembly in parallel by default; if another test class in
/// this same test binary also mutates <see cref="PeerDirectory"/> concurrently, the two can race. Every
/// test below is marked <see cref="CollectionAttribute"/> into one xunit collection so it cannot race
/// against ITSELF, but that does not protect against a different file also touching the same static
/// registry — this repo has no assembly-wide <c>DisableTestParallelization</c>, and adding one is outside
/// this track's file ownership. Filtered alone (<c>--filter</c>, which is how this track's own
/// verification runs) this is a non-issue; it is a real, currently-unmitigated risk on a full parallel
/// suite run if another wave-5 track's tests do the same thing.</para>
/// </summary>
[Collection("StreamForge.Discovery.PeerDirectory")]
public sealed class PeerProbeTests : IDisposable
{
    public PeerProbeTests() => PeerDirectory.Clear();

    public void Dispose() => PeerDirectory.Clear();

    [Fact]
    public async Task ProbeAsync_records_an_error_and_returns_null_when_the_peer_has_no_rest_endpoint()
    {
        var peer = new PeerRecord { Name = "no-rest", RestEndpoint = "" };
        PeerDirectory.Configure([peer]);

        var result = await PeerProbe.ProbeAsync(peer);

        Assert.Null(result);
        var recorded = PeerDirectory.Find("no-rest");
        Assert.NotNull(recorded);
        Assert.NotNull(recorded!.LastError);
        Assert.Equal(0, recorded.LastSeenAtMs);
    }

    [Fact]
    public async Task ProbeAsync_records_an_error_and_returns_null_when_the_peer_is_unreachable()
    {
        // Port 1 on loopback: a privileged/unassigned port nothing listens on, so the connection is
        // refused immediately rather than timing out — keeps the test fast without touching PeerProbe's
        // own 5s timeout.
        var peer = new PeerRecord { Name = "unreachable", RestEndpoint = "http://127.0.0.1:1" };
        PeerDirectory.Configure([peer]);

        var result = await PeerProbe.ProbeAsync(peer);

        Assert.Null(result);
        var recorded = PeerDirectory.Find("unreachable");
        Assert.NotNull(recorded);
        Assert.NotNull(recorded!.LastError);
        Assert.Equal(0, recorded.LastSeenAtMs);
    }

    [Fact]
    public async Task ProbeAsync_records_success_and_returns_the_peers_answer()
    {
        using var server = new FakeInstanceServer(new
        {
            instanceId = "peer-abc-123",
            name = "peer-two",
            flavor = "orleans",
            version = "1.0.0",
            endpoints = new Dictionary<string, string> { ["rest"] = "http://localhost:9" },
            capabilities = Array.Empty<string>(),
            plugins = Array.Empty<string>(),
            catalogCounts = new Dictionary<string, int>(),
            catalogWarnings = Array.Empty<string>(),
            startedAtMs = 12345L,
        });

        var peer = new PeerRecord { Name = "reachable", RestEndpoint = server.BaseUrl };
        PeerDirectory.Configure([peer]);

        var result = await PeerProbe.ProbeAsync(peer);

        Assert.NotNull(result);
        Assert.Equal("peer-abc-123", result!.InstanceId);
        Assert.Equal("peer-two", result.Name);

        var recorded = PeerDirectory.Find("reachable");
        Assert.NotNull(recorded);
        Assert.Null(recorded!.LastError);
        Assert.True(recorded.LastSeenAtMs > 0);
        Assert.Equal("peer-abc-123", recorded.InstanceId);
        Assert.NotNull(recorded.Info);
    }

    [Fact]
    public async Task ProbeAsync_records_an_error_when_the_response_has_no_instance_id()
    {
        using var server = new FakeInstanceServer(new { instanceId = "" });
        var peer = new PeerRecord { Name = "empty-id", RestEndpoint = server.BaseUrl };
        PeerDirectory.Configure([peer]);

        var result = await PeerProbe.ProbeAsync(peer);

        Assert.Null(result);
        var recorded = PeerDirectory.Find("empty-id");
        Assert.NotNull(recorded!.LastError);
        Assert.Equal(0, recorded.LastSeenAtMs);
    }

    /// <summary>Minimal loopback HTTP server answering every request with one fixed JSON body — enough
    /// to stand in for a peer's <c>GET /api/meta/instance</c> without spinning up a whole ASP.NET Core
    /// host for a unit test. <see cref="HttpListener"/> rather than a raw socket because it is the
    /// smallest thing in the BCL that speaks real HTTP/1.1, which is what <see cref="PeerProbe"/>'s
    /// <see cref="HttpClient"/> actually needs to talk to.</summary>
    private sealed class FakeInstanceServer : IDisposable
    {
        private readonly HttpListener _listener;
        private readonly Task _loop;
        private readonly CancellationTokenSource _cts = new();

        public string BaseUrl { get; }

        public FakeInstanceServer(object body)
        {
            var port = GetFreeLoopbackPort();
            BaseUrl = $"http://127.0.0.1:{port}/";
            _listener = new HttpListener();
            _listener.Prefixes.Add(BaseUrl);
            _listener.Start();

            var json = System.Text.Json.JsonSerializer.Serialize(body);
            _loop = Task.Run(async () =>
            {
                try
                {
                    while (!_cts.IsCancellationRequested)
                    {
                        var ctx = await _listener.GetContextAsync().ConfigureAwait(false);
                        var bytes = Encoding.UTF8.GetBytes(json);
                        ctx.Response.ContentType = "application/json";
                        ctx.Response.ContentLength64 = bytes.Length;
                        await ctx.Response.OutputStream.WriteAsync(bytes).ConfigureAwait(false);
                        ctx.Response.Close();
                    }
                }
                catch (Exception) when (_cts.IsCancellationRequested)
                {
                    // Expected: Stop() below aborts the pending GetContextAsync call.
                }
            });
        }

        private static int GetFreeLoopbackPort()
        {
            var listener = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            var port = ((IPEndPoint)listener.LocalEndpoint).Port;
            listener.Stop();
            return port;
        }

        public void Dispose()
        {
            _cts.Cancel();
            _listener.Stop();
            _listener.Close();
            try
            {
                _loop.Wait(TimeSpan.FromSeconds(2));
            }
            catch
            {
                // Best-effort join; the process-level test timeout is the backstop.
            }
        }
    }
}
