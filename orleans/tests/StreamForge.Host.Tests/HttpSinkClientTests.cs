using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using StreamForge.Abstractions;
using StreamForge.AppCore.Sinks;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// Wishlist item 9(a): unit tests for <see cref="HttpSinkClient"/>'s fire-and-forget failure contract
/// (mirroring <see cref="NatsSinkClientTests"/>'s shape, since the two classes share the same contract —
/// see <see cref="HttpSinkClient"/>'s own class doc) plus the wishlist's own named requirement: the
/// maxDepth "bounded feedback loop" guard drops rows whose step counter reached the bound, counted rather
/// than silently. There is no real HTTP receiver in this environment — <see cref="StubIngestServer"/>
/// (an <see cref="HttpListener"/>-based stand-in, the same convention <c>GeminiChatServiceTests</c>'s
/// <c>StubGeminiServer</c> already uses in this project) plays the loop's own
/// <c>/api/sources/{name}/events</c> endpoint, and an unreachable port-1 address plays a down receiver —
/// same "exercise the real failure path" approach <see cref="NatsSinkClientTests"/> takes for NATS.
/// </summary>
public class HttpSinkClientTests
{
    private static HttpSinkConfig UnreachableConfig() => new() { Url = "http://127.0.0.1:1/events" };

    // ------------------------------------------------------------------
    // Fire-and-forget contract — same three properties NatsSinkClientTests pins for NATS.
    // ------------------------------------------------------------------

    [Fact]
    public async Task PublishAsync_AgainstAnUnreachableEndpoint_NeverThrows()
    {
        await using var client = new HttpSinkClient(UnreachableConfig(), "pipeline", "p1");

        await client.PublishAsync(new NatsPipelineRowMessage { PipelineId = "p1", Row = new() { ["x"] = 1 } }, CancellationToken.None);
    }

    [Fact]
    public async Task PublishAsync_AgainstAnUnreachableEndpoint_CountsTheFailure()
    {
        await using var client = new HttpSinkClient(UnreachableConfig(), "pipeline", "p1");

        await client.PublishAsync(new NatsPipelineRowMessage { PipelineId = "p1", Row = new() { ["x"] = 1 } }, CancellationToken.None);

        var counters = client.Counters;
        Assert.Equal(0, counters.Published);
        Assert.Equal(1, counters.Failed);
        Assert.NotNull(counters.LastError);
        Assert.True(counters.LastFailureAtMs > 0);
    }

    [Fact]
    public async Task PublishAsync_AgainstAnUnreachableEndpoint_ReportsTheFailureThroughTheCallback()
    {
        var reported = new List<(string Url, Exception Ex)>();
        await using var client = new HttpSinkClient(
            UnreachableConfig(), "table", "t1", (url, ex) => reported.Add((url, ex)));

        await client.PublishAsync(new NatsTableDeltaMessage { Table = "t1", Row = new() { ["x"] = 1 }, Weight = 1 }, CancellationToken.None);

        var call = Assert.Single(reported);
        Assert.Equal("http://127.0.0.1:1/events", call.Url);
    }

    [Fact]
    public async Task PublishAsync_RepeatedFailuresWithinTheThrottleWindow_ReportOnlyOnce()
    {
        var reportCount = 0;
        await using var client = new HttpSinkClient(
            UnreachableConfig(), "pipeline", "p1", (_, _) => Interlocked.Increment(ref reportCount));

        await Task.WhenAll(
            client.PublishAsync(new NatsPipelineRowMessage { Row = new() { ["n"] = 1 } }, CancellationToken.None),
            client.PublishAsync(new NatsPipelineRowMessage { Row = new() { ["n"] = 2 } }, CancellationToken.None),
            client.PublishAsync(new NatsPipelineRowMessage { Row = new() { ["n"] = 3 } }, CancellationToken.None));

        Assert.Equal(3, client.Counters.Failed);
        Assert.Equal(1, reportCount);
    }

    // ------------------------------------------------------------------
    // Wire shape — must round-trip through the real IngestEventsRequest contract the loop posts to.
    // ------------------------------------------------------------------

    [Fact]
    public async Task PublishAsync_PostsOneEventWrappingTheRow()
    {
        using var server = new StubIngestServer();
        await using var client = new HttpSinkClient(new HttpSinkConfig { Url = server.BaseUrl + "/events" }, "table", "t1");

        await client.PublishAsync(
            new NatsTableDeltaMessage { Table = "t1", Row = new() { ["symbol"] = "AAPL", ["step"] = 1L }, Weight = 1 },
            CancellationToken.None);

        var received = await server.NextRequestAsync();
        using var doc = JsonDocument.Parse(received.Body);
        var events = doc.RootElement.GetProperty("events");
        Assert.Equal(1, events.GetArrayLength());
        var row = events[0];
        Assert.Equal("AAPL", row.GetProperty("symbol").GetString());
        Assert.Equal(1, row.GetProperty("step").GetInt64());
        // The table delta's weight rides along, same convention FileSinkClient's RowOf uses — see
        // HttpSinkClient.RowOf's doc comment for why a retraction must not look like a bare insert.
        Assert.Equal(1, row.GetProperty("_weight").GetInt64());
        Assert.Equal(1, client.Counters.Published);
    }

    [Fact]
    public async Task PublishAsync_ExpandsNameTemplateInTheUrl()
    {
        using var server = new StubIngestServer();
        await using var client = new HttpSinkClient(
            new HttpSinkConfig { Url = server.BaseUrl + "/sources/{name}/events" }, "table", "orders");

        await client.PublishAsync(new NatsTableDeltaMessage { Table = "orders", Row = new() { ["x"] = 1 } }, CancellationToken.None);

        var received = await server.NextRequestAsync();
        Assert.Equal("/sources/orders/events", received.Path);
    }

    [Fact]
    public async Task PublishAsync_SendsTheConfiguredHeader_OnlyWhenBothNameAndValueAreSet()
    {
        using var server = new StubIngestServer();
        await using var client = new HttpSinkClient(
            new HttpSinkConfig { Url = server.BaseUrl + "/events", HeaderName = "X-SF-Ingest-Key", HeaderValue = "sfk_test" },
            "table", "t1");

        await client.PublishAsync(new NatsTableDeltaMessage { Table = "t1", Row = new() { ["x"] = 1 } }, CancellationToken.None);

        var received = await server.NextRequestAsync();
        Assert.Equal("sfk_test", received.Headers["X-SF-Ingest-Key"]);
    }

    [Fact]
    public async Task PublishAsync_SendsNoExtraHeader_WhenNotConfigured()
    {
        using var server = new StubIngestServer();
        await using var client = new HttpSinkClient(new HttpSinkConfig { Url = server.BaseUrl + "/events" }, "table", "t1");

        await client.PublishAsync(new NatsTableDeltaMessage { Table = "t1", Row = new() { ["x"] = 1 } }, CancellationToken.None);

        var received = await server.NextRequestAsync();
        Assert.Null(received.Headers["X-SF-Ingest-Key"]);
    }

    [Fact]
    public async Task PublishAsync_ANonSuccessResponse_CountsAsAFailure()
    {
        using var server = new StubIngestServer(HttpStatusCode.BadRequest);
        await using var client = new HttpSinkClient(new HttpSinkConfig { Url = server.BaseUrl + "/events" }, "table", "t1");

        await client.PublishAsync(new NatsTableDeltaMessage { Table = "t1", Row = new() { ["x"] = 1 } }, CancellationToken.None);
        await server.NextRequestAsync(); // wait for the request to actually land before asserting

        Assert.Equal(0, client.Counters.Published);
        Assert.Equal(1, client.Counters.Failed);
    }

    // ------------------------------------------------------------------
    // Wishlist #9: the maxDepth "scenario clock" cycle-breaker.
    // ------------------------------------------------------------------

    [Fact]
    public async Task MaxDepth_DropsARowWhoseStepHasReachedTheBound_WithoutPosting()
    {
        using var server = new StubIngestServer();
        await using var client = new HttpSinkClient(
            new HttpSinkConfig { Url = server.BaseUrl + "/events", MaxDepth = 3 }, "table", "loop");

        await client.PublishAsync(new NatsTableDeltaMessage { Table = "loop", Row = new() { ["step"] = 3L } }, CancellationToken.None);

        Assert.Equal(0, server.RequestCount);
        Assert.Equal(0, client.Counters.Published);
        Assert.Equal(1, client.Counters.Failed);
        Assert.Contains("maxDepth", client.Counters.LastError, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task MaxDepth_DropsARowPastTheBoundToo()
    {
        using var server = new StubIngestServer();
        await using var client = new HttpSinkClient(
            new HttpSinkConfig { Url = server.BaseUrl + "/events", MaxDepth = 3 }, "table", "loop");

        await client.PublishAsync(new NatsTableDeltaMessage { Table = "loop", Row = new() { ["step"] = 99L } }, CancellationToken.None);

        Assert.Equal(0, server.RequestCount);
        Assert.Equal(1, client.Counters.Failed);
    }

    [Fact]
    public async Task MaxDepth_AllowsARowBelowTheBound()
    {
        using var server = new StubIngestServer();
        await using var client = new HttpSinkClient(
            new HttpSinkConfig { Url = server.BaseUrl + "/events", MaxDepth = 3 }, "table", "loop");

        await client.PublishAsync(new NatsTableDeltaMessage { Table = "loop", Row = new() { ["step"] = 2L } }, CancellationToken.None);
        await server.NextRequestAsync();

        Assert.Equal(1, client.Counters.Published);
        Assert.Equal(0, client.Counters.Failed);
    }

    [Fact]
    public async Task MaxDepth_ZeroMeansTheGuardIsOff_EvenForAHugeStep()
    {
        using var server = new StubIngestServer();
        // Default MaxDepth (0) — see HttpSinkConfig's doc comment for why this is the default.
        await using var client = new HttpSinkClient(new HttpSinkConfig { Url = server.BaseUrl + "/events" }, "table", "loop");

        await client.PublishAsync(new NatsTableDeltaMessage { Table = "loop", Row = new() { ["step"] = 1_000_000L } }, CancellationToken.None);
        await server.NextRequestAsync();

        Assert.Equal(1, client.Counters.Published);
    }

    [Fact]
    public async Task MaxDepth_ARowWithNoStepField_IsNotDropped()
    {
        using var server = new StubIngestServer();
        await using var client = new HttpSinkClient(
            new HttpSinkConfig { Url = server.BaseUrl + "/events", MaxDepth = 1 }, "table", "loop");

        await client.PublishAsync(new NatsTableDeltaMessage { Table = "loop", Row = new() { ["symbol"] = "AAPL" } }, CancellationToken.None);
        await server.NextRequestAsync();

        Assert.Equal(1, client.Counters.Published);
    }

    [Fact]
    public async Task MaxDepth_HonorsACustomStepFieldName()
    {
        using var server = new StubIngestServer();
        await using var client = new HttpSinkClient(
            new HttpSinkConfig { Url = server.BaseUrl + "/events", MaxDepth = 2, StepField = "iteration" },
            "table", "loop");

        await client.PublishAsync(new NatsTableDeltaMessage { Table = "loop", Row = new() { ["step"] = 99L, ["iteration"] = 2L } }, CancellationToken.None);

        // "step" is way past 2 but irrelevant — only "iteration" is the configured counter, and it's AT
        // the bound, so this row is dropped.
        Assert.Equal(0, server.RequestCount);
        Assert.Equal(1, client.Counters.Failed);
    }
}

/// <summary>Minimal <see cref="HttpListener"/>-based stand-in for the loop's own
/// <c>/api/sources/{name}/events</c> endpoint — records every request's method/path/headers/body and
/// answers with a configurable status code. Same convention <c>GeminiChatServiceTests.StubGeminiServer</c>
/// already uses in this project.</summary>
internal sealed class StubIngestServer : IDisposable
{
    private readonly HttpListener _listener;
    private readonly HttpStatusCode _status;
    private readonly System.Collections.Concurrent.ConcurrentQueue<Received> _received = new();
    private readonly SemaphoreSlim _signal = new(0);

    public string BaseUrl { get; }

    public int RequestCount => _received.Count;

    public StubIngestServer(HttpStatusCode status = HttpStatusCode.OK)
    {
        _status = status;
        var port = GetFreePort();
        BaseUrl = $"http://127.0.0.1:{port}";
        _listener = new HttpListener();
        _listener.Prefixes.Add($"{BaseUrl}/");
        _listener.Start();
        _ = Task.Run(ServeLoopAsync);
    }

    public readonly record struct Received(string Path, System.Collections.Specialized.NameValueCollection Headers, string Body);

    /// <summary>Waits for (and returns) the next request the server has not yet handed out — the
    /// coordination point every test that asserts on a received request awaits before reading it, since
    /// the sink's own publish is fire-and-forget and returns before the server necessarily finished
    /// reading the request.</summary>
    public async Task<Received> NextRequestAsync()
    {
        await _signal.WaitAsync(TimeSpan.FromSeconds(5));
        return _received.TryDequeue(out var r) ? r : throw new TimeoutException("no request received within 5s");
    }

    private async Task ServeLoopAsync()
    {
        while (_listener.IsListening)
        {
            HttpListenerContext ctx;
            try
            {
                ctx = await _listener.GetContextAsync().ConfigureAwait(false);
            }
            catch (Exception) when (!_listener.IsListening)
            {
                return;
            }
            catch (ObjectDisposedException)
            {
                return;
            }

            using var reader = new StreamReader(ctx.Request.InputStream, Encoding.UTF8);
            var body = await reader.ReadToEndAsync().ConfigureAwait(false);
            _received.Enqueue(new Received(ctx.Request.Url!.AbsolutePath, ctx.Request.Headers, body));
            _signal.Release();

            ctx.Response.StatusCode = (int)_status;
            ctx.Response.OutputStream.Close();
        }
    }

    private static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    public void Dispose()
    {
        _listener.Stop();
        _listener.Close();
        _signal.Dispose();
    }
}
