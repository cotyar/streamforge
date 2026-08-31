using System.Net;
using System.Net.Sockets;
using System.Text;
using StreamsForge.Abstractions;
using StreamsForge.Api;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>
/// Plan 007 (Cloud Run prep + admin app + AI control chat) W1C, decision D-D: unit/integration tests
/// for the AI control chat (POST /api/chat, <see cref="GeminiChatService"/>). There is no HTTP-level
/// test harness in this repo (see SourcesEndpointsLogicTests.cs's class doc) — these tests drive
/// GeminiChatService directly, against a tiny in-process HttpListener stub standing in for Gemini's
/// generateContent REST endpoint (scripted canned responses, in order) and hand-rolled in-memory
/// fakes for ICatalogFacade/ITableReadFacade/ITableHistoryFacade (kept local to this file rather than
/// extending FakeRegistryGrain.cs, which this wave must not modify — several of its members
/// deliberately throw NotImplementedException for calls this feature needs, e.g. UpsertSourceAsync).
/// </summary>
public class GeminiChatServiceTests
{
    // ------------------------------------------------------------------
    // (1) Unconfigured -> IsConfigured false (ChatEndpoints.MapChatEndpoints maps this to 503 with
    // ChatEndpoints.NotConfiguredMessage; that mapping is a one-line lambda with no independently
    // testable logic beyond this flag, consistent with this repo's no-HTTP-harness convention).
    // ------------------------------------------------------------------

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void IsConfigured_is_false_without_an_api_key(string? apiKey)
    {
        var service = new GeminiChatService(new HttpClient(), "http://unused", "gemini-test", apiKey,
            new FakeChatCatalogFacade(), new FakeChatTableReadFacade(), new FakeChatTableHistoryFacade());

        Assert.False(service.IsConfigured);
    }

    [Fact]
    public void IsConfigured_is_true_with_an_api_key()
    {
        var service = new GeminiChatService(new HttpClient(), "http://unused", "gemini-test", "a-real-key",
            new FakeChatCatalogFacade(), new FakeChatTableReadFacade(), new FakeChatTableHistoryFacade());

        Assert.True(service.IsConfigured);
    }

    [Fact]
    public void NotConfiguredMessage_names_the_env_var_and_config_key()
    {
        Assert.Contains("GEMINI_API_KEY", ChatEndpoints.NotConfiguredMessage);
        Assert.Contains("Gemini:ApiKey", ChatEndpoints.NotConfiguredMessage);
    }

    // ------------------------------------------------------------------
    // (2) Full multi-turn loop: functionCall(list_sources) -> functionCall(create_source) -> final
    // text, scripted against a local stub Gemini server. Proves >= 2 tool round-trips, that both
    // facade fakes actually got exercised, and that the response DTO carries both toolCalls.
    // ------------------------------------------------------------------

    [Fact]
    public async Task Multi_turn_tool_loop_creates_a_source_and_returns_both_tool_calls()
    {
        using var stub = new StubGeminiServer(
        [
            """{"candidates":[{"content":{"role":"model","parts":[{"functionCall":{"name":"list_sources","args":{}}}]},"finishReason":"STOP"}]}""",
            """{"candidates":[{"content":{"role":"model","parts":[{"functionCall":{"name":"create_source","args":{"name":"chat_demo","fields":[{"name":"price","type":"Double"}],"eventsPerSecond":5}}}]},"finishReason":"STOP"}]}""",
            """{"candidates":[{"content":{"role":"model","parts":[{"text":"Created chat_demo."}]},"finishReason":"STOP"}]}""",
        ]);

        var catalog = new FakeChatCatalogFacade();
        var service = new GeminiChatService(new HttpClient(), stub.BaseUrl, "gemini-test", "test-key",
            catalog, new FakeChatTableReadFacade(), new FakeChatTableHistoryFacade());

        var request = new ChatRequest([new ChatMessage("user", "create a demo source called chat_demo at 5 events/sec")]);
        var response = await service.HandleAsync(request, AnonymousPrincipal(), CancellationToken.None);

        Assert.Equal("Created chat_demo.", response.Reply);
        Assert.Equal(2, response.ToolCalls.Count);
        Assert.Equal("list_sources", response.ToolCalls[0].Name);
        Assert.Equal("create_source", response.ToolCalls[1].Name);
        Assert.Equal(3, stub.RequestCount);

        Assert.Contains(catalog.Sources, s => s.Name == "chat_demo" && s.Enabled && s.EventsPerSecond == 5);
    }

    // ------------------------------------------------------------------
    // (3) confirmed=false delete guard: the model calls delete_source without confirmation; the tool
    // must refuse and the underlying facade delete must never fire.
    // ------------------------------------------------------------------

    [Fact]
    public async Task Delete_source_without_confirmation_is_refused_and_never_reaches_the_facade()
    {
        using var stub = new StubGeminiServer(
        [
            """{"candidates":[{"content":{"role":"model","parts":[{"functionCall":{"name":"delete_source","args":{"name":"chat_demo","confirmed":false}}}]},"finishReason":"STOP"}]}""",
            """{"candidates":[{"content":{"role":"model","parts":[{"text":"I will not delete it without your confirmation."}]},"finishReason":"STOP"}]}""",
        ]);

        var catalog = new FakeChatCatalogFacade();
        catalog.Sources.Add(new SourceDefinition { Name = "chat_demo", Fields = [new FieldDef("price", FieldType.Double)] });

        var service = new GeminiChatService(new HttpClient(), stub.BaseUrl, "gemini-test", "test-key",
            catalog, new FakeChatTableReadFacade(), new FakeChatTableHistoryFacade());

        var request = new ChatRequest([new ChatMessage("user", "delete chat_demo")]);
        var response = await service.HandleAsync(request, AnonymousPrincipal(), CancellationToken.None);

        Assert.False(catalog.DeleteSourceCalled);
        Assert.Contains(catalog.Sources, s => s.Name == "chat_demo");
        Assert.Single(response.ToolCalls);
        Assert.Contains("confirm", response.ToolCalls[0].Result.GetRawText(), StringComparison.OrdinalIgnoreCase);
    }

    private static System.Security.Claims.ClaimsPrincipal AnonymousPrincipal() =>
        new(new System.Security.Claims.ClaimsIdentity([new System.Security.Claims.Claim(System.Security.Claims.ClaimTypes.Name, "editor")], "test"));
}

/// <summary>Minimal HttpListener-based stand-in for Gemini's generateContent REST endpoint: serves
/// the supplied canned JSON response bodies in order, one per request.</summary>
internal sealed class StubGeminiServer : IDisposable
{
    private readonly HttpListener _listener;
    private readonly Queue<string> _responses;
    private int _requestCount;

    public string BaseUrl { get; }
    public int RequestCount => _requestCount;

    public StubGeminiServer(IEnumerable<string> responses)
    {
        _responses = new Queue<string>(responses);
        var port = GetFreePort();
        BaseUrl = $"http://127.0.0.1:{port}";
        _listener = new HttpListener();
        _listener.Prefixes.Add($"{BaseUrl}/");
        _listener.Start();
        _ = Task.Run(ServeLoopAsync);
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

            Interlocked.Increment(ref _requestCount);
            using var reader = new StreamReader(ctx.Request.InputStream);
            await reader.ReadToEndAsync().ConfigureAwait(false);

            string responseJson;
            lock (_responses)
            {
                responseJson = _responses.Count > 0 ? _responses.Dequeue() : """{"candidates":[]}""";
            }

            var bytes = Encoding.UTF8.GetBytes(responseJson);
            ctx.Response.ContentType = "application/json";
            ctx.Response.ContentLength64 = bytes.Length;
            await ctx.Response.OutputStream.WriteAsync(bytes).ConfigureAwait(false);
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
    }
}

// ============================================================================
// Minimal in-memory fakes local to this test file (see class doc on GeminiChatServiceTests for why
// FakeRegistryGrain.cs isn't reused). Only the members ChatToolExecutor actually calls need to be
// fully functional; everything else throws NotImplementedException so an accidental new dependency
// fails loudly rather than silently no-op'ing (same convention as FakeRegistryGrain.cs).
// ============================================================================

internal sealed class FakeChatCatalogFacade : ICatalogFacade
{
    /// <summary>Interface conformance only — wishlist #8's run-on-demand needs a real runtime to
    /// publish, so a fake correctly reports that there is nothing to run.</summary>
    public Task<ScenarioRunResult> RunSourceAsync(string name, ScenarioRunRequest request) =>
        Task.FromResult(new ScenarioRunResult { Outcome = ScenarioRunOutcome.NotFound });

    public List<SourceDefinition> Sources { get; } = [];
    public List<PipelineDefinition> Pipelines { get; } = [];
    public List<TableDefinition> Tables { get; } = [];
    public bool DeleteSourceCalled { get; private set; }

    public Task<List<SourceDefinition>> GetSourcesAsync() => Task.FromResult(Sources);
    public Task<SourceDefinition?> GetSourceAsync(string name) => Task.FromResult(Sources.FirstOrDefault(s => s.Name == name));

    public Task UpsertSourceAsync(SourceDefinition def)
    {
        Sources.RemoveAll(s => s.Name == def.Name);
        Sources.Add(def);
        return Task.CompletedTask;
    }

    public Task<bool> DeleteSourceAsync(string name)
    {
        DeleteSourceCalled = true;
        return Task.FromResult(Sources.RemoveAll(s => s.Name == name) > 0);
    }

    public Task<List<PipelineDefinition>> GetPipelinesAsync() => Task.FromResult(Pipelines);
    public Task<PipelineDefinition?> GetPipelineAsync(string id) => Task.FromResult(Pipelines.FirstOrDefault(p => p.Id == id));

    public Task<PipelineDefinition> CreatePipelineAsync(PipelineDefinition def)
    {
        def.Id = Guid.NewGuid().ToString("N");
        Pipelines.Add(def);
        return Task.FromResult(def);
    }

    public Task<PipelineDefinition?> UpdatePipelineAsync(PipelineDefinition def) => throw new NotImplementedException();
    public Task<bool> DeletePipelineAsync(string id) => throw new NotImplementedException();
    public Task<PipelineDefinition?> SetPipelineStatusAsync(string id, PipelineStatus status) => throw new NotImplementedException();

    public Task<List<TableDefinition>> GetTablesAsync() => Task.FromResult(Tables);
    public Task<TableDefinition?> GetTableAsync(string id) => Task.FromResult(Tables.FirstOrDefault(t => t.Id == id));
    public Task<TableDefinition> CreateTableAsync(TableDefinition def) => throw new NotImplementedException();
    public Task<TableDefinition?> UpdateTableAsync(TableDefinition def) => throw new NotImplementedException();
    public Task<bool> DeleteTableAsync(string id) => throw new NotImplementedException();
    public Task<TableDefinition?> SetTableStatusAsync(string id, PipelineStatus status) => throw new NotImplementedException();

    public Task<string> EnsureFieldNumbersAsync(string entityKey, List<FieldDef> fields) => throw new NotImplementedException();
}

internal sealed class FakeChatTableReadFacade : ITableReadFacade
{
    public Task<List<TableRowDto>> GetRowsAsync(string tableName, int limit, int offset) => Task.FromResult(new List<TableRowDto>());
    public Task<int> GetRowCountAsync(string tableName) => Task.FromResult(0);
    public Task<long> GetSeqAsync(string tableName) => Task.FromResult(0L);
    public Task<long?> GetSnapshotFrontierEpochAsync(string tableName) => Task.FromResult((long?)null);
    public Task<TableMetrics> GetMetricsAsync(string tableName) => Task.FromResult(new TableMetrics { TableId = tableName });
    public Task<List<TableRowDto>> SearchAsync(string tableName, string query, int limit) => Task.FromResult(new List<TableRowDto>());
}

internal sealed class FakeChatTableHistoryFacade : ITableHistoryFacade
{
    public Task<TableHistoryQueryResult> GetHistoryAsync(string tableName, string key, int limit) =>
        Task.FromResult(new TableHistoryQueryResult { KeyFound = false });

    public Task<TableHistoryStats> GetStatsAsync(string tableName) => Task.FromResult(new TableHistoryStats());
}
