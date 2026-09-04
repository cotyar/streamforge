using System.Diagnostics;
using System.Net.Http.Json;
using System.Runtime.CompilerServices;
using Xunit;

namespace StreamsForge.Client.Tests.Fixtures;

/// <summary>
/// Contract-test fixture: boots an ISOLATED StreamsForge instance on 9199/9299 (never 5199/5299 --
/// the live dev server -- and never 6199 -- the demo container), imports a tiny fixed config (one
/// ingest source, one LATEST BY table, one aggregate over that derived LATEST BY -- the same shape
/// clients/python/tests/conftest.py uses), and tears it down after the whole test run. Ported from
/// that fixture; see its docstring for the two traps this avoids: NO <c>--urls</c> (Program.cs's
/// guard silently skips binding a gRPC port at all when it is set), and the process MUST run with
/// its working directory set to the publish directory (WebApplication.CreateBuilder takes its
/// content root from the current directory, not the assembly's -- run from anywhere else and
/// appsettings.json is never found, so Jwt:Key is null and every request 500s inside auth
/// middleware, including /api/healthz).
///
/// Process lifecycle (publish, spawn, drain logs, wait healthy, tear down) is shared with
/// <see cref="TlsEngineFixture"/> via <see cref="EngineProcess"/>; this class owns only what is
/// specific to the plaintext contract shape -- ports, and the fixture config import.
/// </summary>
public sealed class EngineFixture : IAsyncLifetime
{
    public const int HttpPort = 9199;
    public const int GrpcPort = 9299;
    private static readonly int[] ForbiddenPorts = [5199, 5299, 6199];

    public const string AdminUser = "admin";
    public const string AdminPassword = "admin123!";

    public const string SourceName = "sf_dotnet_client_trades";
    public const string LatestTable = "sf_dotnet_client_latest_trade";
    public const string AggTable = "sf_dotnet_client_desk_totals";
    public const string GlobalAggTable = "sf_dotnet_client_all_totals";

    public string BaseUrl { get; } = $"http://localhost:{HttpPort}";
    public string GrpcTarget { get; } = $"localhost:{GrpcPort}";

    /// <summary>Non-null when the fixture could not be set up (port collision, dotnet missing,
    /// project not found, publish failure). Plain xunit v2 [Fact]/[Theory] cannot dynamically skip
    /// at runtime, so tests that depend on this fixture check it themselves and report a clear,
    /// explicit "skipped: ..." assertion failure rather than colliding with whatever is already
    /// running on these ports.</summary>
    public string? SkipReason { get; private set; }

    private Process? _process;
    private string? _dataDir;
    private string? _publishDir;
    private bool _ownsPublishDir;
    private readonly ProcessLogTail _log = new();

    public async Task InitializeAsync()
    {
        if (ForbiddenPorts.Contains(HttpPort) || ForbiddenPorts.Contains(GrpcPort))
        {
            SkipReason = "refusing to configure the contract-test fixture onto a forbidden port";
            return;
        }
        if (!EngineProcess.PortFree(HttpPort) || !EngineProcess.PortFree(GrpcPort))
        {
            SkipReason = $"port {HttpPort} or {GrpcPort} is already in use -- refusing to collide with a running instance";
            return;
        }
        if (!File.Exists(EngineProcess.Dotnet))
        {
            SkipReason = $"dotnet not found at {EngineProcess.Dotnet} -- cannot boot the contract-test engine";
            return;
        }

        var projectDir = ProjectDir();
        if (!Directory.Exists(projectDir))
        {
            SkipReason = $"StreamsForge.Host project not found at {projectDir}";
            return;
        }

        // SF_TEST_PUBLISH_DIR reuses an existing publish across runs while iterating -- a publish
        // takes ~2 minutes. Mirrors clients/python/tests/conftest.py's identical env var.
        _publishDir = Environment.GetEnvironmentVariable("SF_TEST_PUBLISH_DIR");
        if (string.IsNullOrEmpty(_publishDir))
        {
            _publishDir = Directory.CreateTempSubdirectory("sf-dotnet-client-publish-").FullName;
            _ownsPublishDir = true;
            try
            {
                await EngineProcess.PublishAsync(projectDir, _publishDir);
            }
            catch (Exception ex)
            {
                SkipReason = $"could not publish an isolated engine build: {ex.Message}";
                return;
            }
        }
        else if (!File.Exists(Path.Combine(_publishDir, "StreamsForge.Host.dll")))
        {
            SkipReason = $"SF_TEST_PUBLISH_DIR={_publishDir} has no StreamsForge.Host.dll";
            return;
        }

        _dataDir = Directory.CreateTempSubdirectory("sf-dotnet-client-data-").FullName;
        _process = EngineProcess.StartHost(_publishDir, [
            "--Http:Port", HttpPort.ToString(),
            "--Grpc:Port", GrpcPort.ToString(),
            "--Streams:Transport", "push",
            "--DataDir", _dataDir,
        ], _log);
        try
        {
            using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(2) };
            await EngineProcess.WaitHealthyAsync(http, BaseUrl, _process, _log, TimeSpan.FromSeconds(90));
            await ImportFixtureConfigAsync();
        }
        catch (Exception ex)
        {
            SkipReason = $"engine did not come up cleanly: {ex.Message}";
            await DisposeAsync();
        }
    }

    private static string ProjectDir([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "..", "..", "..", "orleans", "src", "StreamsForge.Host"));

    private async Task ImportFixtureConfigAsync()
    {
        using var http = new HttpClient { BaseAddress = new Uri(BaseUrl) };
        var loginResp = await http.PostAsJsonAsync("api/auth/login", new { username = AdminUser, password = AdminPassword });
        loginResp.EnsureSuccessStatusCode();
        var login = await loginResp.Content.ReadFromJsonAsync<LoginResponseDto>();
        http.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", login!.Token);

        var doc = new
        {
            version = 1,
            sources = new[]
            {
                new
                {
                    name = SourceName,
                    description = ".NET client contract test fixture",
                    kind = "ingest",
                    fields = new object[]
                    {
                        new { name = "trade_id", type = "String" },
                        new { name = "desk", type = "String" },
                        new { name = "notional", type = "Double" },
                    },
                    ingest = new { },
                    enabled = true,
                },
            },
            pipelines = Array.Empty<object>(),
            tables = new[]
            {
                new
                {
                    name = LatestTable,
                    description = "latest row per trade_id",
                    sql = $"SELECT trade_id, desk, notional FROM {SourceName} LATEST BY (trade_id)",
                    running = true,
                },
                new
                {
                    name = AggTable,
                    description = "aggregate over the derived LATEST BY",
                    sql = $"SELECT desk, SUM(notional) AS total FROM {LatestTable} GROUP BY desk",
                    running = true,
                },
                new
                {
                    name = GlobalAggTable,
                    description = "unkeyed global aggregate (no GROUP BY) -- exercises KeyFields=[] over the wire",
                    sql = $"SELECT COUNT(*) AS trade_count, SUM(notional) AS total_notional FROM {LatestTable}",
                    running = true,
                },
            },
        };
        var resp = await http.PostAsJsonAsync("api/config/import?mode=merge", doc);
        var body = await resp.Content.ReadAsStringAsync();
        if (!resp.IsSuccessStatusCode || body.Contains("\"action\":\"error\""))
            throw new InvalidOperationException($"fixture config import failed: {resp.StatusCode} {body}");
    }

    private sealed record LoginResponseDto(string Token);

    public async Task DisposeAsync()
    {
        await EngineProcess.KillAsync(_process);
        _process = null;
        EngineProcess.TryDeleteDirectory(_dataDir);
        if (_ownsPublishDir) EngineProcess.TryDeleteDirectory(_publishDir);
    }
}

[CollectionDefinition(nameof(EngineCollection))]
public sealed class EngineCollection : ICollectionFixture<EngineFixture>;
