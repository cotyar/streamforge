using System.Diagnostics;
using System.Net.Http.Json;
using System.Net.Sockets;
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

    private static readonly string Dotnet =
        Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), ".dotnet", "dotnet");

    private Process? _process;
    private string? _dataDir;
    private string? _publishDir;
    private bool _ownsPublishDir;

    // Drained continuously via OutputDataReceived/ErrorDataReceived rather than read lazily: a
    // RedirectStandardOutput=true process whose pipe nobody reads fills the OS pipe buffer once it
    // has logged enough, and the engine then blocks (or dies) trying to write to a full pipe --
    // this was observed taking the engine down mid-suite before this fixture drained it.
    private readonly object _logLock = new();
    private readonly List<string> _logTail = [];
    private const int LogTailMaxLines = 400;

    public async Task InitializeAsync()
    {
        if (ForbiddenPorts.Contains(HttpPort) || ForbiddenPorts.Contains(GrpcPort))
        {
            SkipReason = "refusing to configure the contract-test fixture onto a forbidden port";
            return;
        }
        if (!PortFree(HttpPort) || !PortFree(GrpcPort))
        {
            SkipReason = $"port {HttpPort} or {GrpcPort} is already in use -- refusing to collide with a running instance";
            return;
        }
        if (!File.Exists(Dotnet))
        {
            SkipReason = $"dotnet not found at {Dotnet} -- cannot boot the contract-test engine";
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
                await PublishAsync(projectDir, _publishDir);
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
        StartProcess(_publishDir, _dataDir);
        try
        {
            await WaitHealthyAsync();
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

    private static bool PortFree(int port)
    {
        try
        {
            using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            socket.Connect("127.0.0.1", port);
            return false; // connected -> something is listening
        }
        catch (SocketException)
        {
            return true;
        }
    }

    private static async Task PublishAsync(string projectDir, string publishDir)
    {
        var psi = new ProcessStartInfo(Dotnet, $"publish \"{projectDir}\" -c Debug -o \"{publishDir}\"")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        using var proc = Process.Start(psi) ?? throw new InvalidOperationException("failed to start dotnet publish");
        var stdoutTask = proc.StandardOutput.ReadToEndAsync();
        var stderrTask = proc.StandardError.ReadToEndAsync();
        try
        {
            await proc.WaitForExitAsync().WaitAsync(TimeSpan.FromMinutes(5));
        }
        catch (TimeoutException)
        {
            try { proc.Kill(entireProcessTree: true); } catch { /* best-effort */ }
            throw new InvalidOperationException("dotnet publish timed out after 5 minutes");
        }
        var stdout = await stdoutTask;
        var stderr = await stderrTask;
        if (proc.ExitCode != 0)
            throw new InvalidOperationException($"dotnet publish failed (code {proc.ExitCode}):\n{Tail(stdout + stderr, 6000)}");
    }

    private void StartProcess(string publishDir, string dataDir)
    {
        var dll = Path.Combine(publishDir, "StreamsForge.Host.dll");
        var psi = new ProcessStartInfo(Dotnet)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            // WebApplication.CreateBuilder takes its content root from the CURRENT DIRECTORY --
            // run the DLL from anywhere else and appsettings.json is never found (see class doc).
            WorkingDirectory = publishDir,
        };
        psi.ArgumentList.Add(dll);
        psi.ArgumentList.Add("--Http:Port");
        psi.ArgumentList.Add(HttpPort.ToString());
        psi.ArgumentList.Add("--Grpc:Port");
        psi.ArgumentList.Add(GrpcPort.ToString());
        psi.ArgumentList.Add("--Streams:Transport");
        psi.ArgumentList.Add("push");
        psi.ArgumentList.Add("--DataDir");
        psi.ArgumentList.Add(dataDir);

        _process = Process.Start(psi) ?? throw new InvalidOperationException("failed to start StreamsForge.Host");
        _process.OutputDataReceived += (_, e) => AppendLog(e.Data);
        _process.ErrorDataReceived += (_, e) => AppendLog(e.Data);
        _process.BeginOutputReadLine();
        _process.BeginErrorReadLine();
    }

    private void AppendLog(string? line)
    {
        if (line is null) return;
        lock (_logLock)
        {
            _logTail.Add(line);
            if (_logTail.Count > LogTailMaxLines) _logTail.RemoveAt(0);
        }
    }

    private string LogTailText()
    {
        lock (_logLock) return string.Join('\n', _logTail);
    }

    private async Task WaitHealthyAsync()
    {
        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(2) };
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(90);
        Exception? lastError = null;
        while (DateTime.UtcNow < deadline)
        {
            if (_process!.HasExited)
            {
                throw new InvalidOperationException(
                    $"engine process exited early (code {_process.ExitCode}):\n{Tail(LogTailText(), 6000)}");
            }
            try
            {
                var resp = await http.GetAsync($"{BaseUrl}/api/healthz");
                if (resp.IsSuccessStatusCode) return;
            }
            catch (Exception ex)
            {
                lastError = ex;
            }
            await Task.Delay(500);
        }
        throw new InvalidOperationException($"engine did not become healthy within 90s (last error: {lastError})");
    }

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

    private static string Tail(string s, int max) => s.Length <= max ? s : s[^max..];

    public async Task DisposeAsync()
    {
        if (_process is { HasExited: false })
        {
            try
            {
                _process.Kill(entireProcessTree: true);
                await _process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(15));
            }
            catch { /* best-effort teardown */ }
        }
        _process?.Dispose();

        if (_dataDir is not null) TryDeleteDirectory(_dataDir);
        if (_ownsPublishDir && _publishDir is not null) TryDeleteDirectory(_publishDir);
    }

    private static void TryDeleteDirectory(string path)
    {
        try { Directory.Delete(path, recursive: true); } catch { /* best-effort cleanup */ }
    }
}

[CollectionDefinition(nameof(EngineCollection))]
public sealed class EngineCollection : ICollectionFixture<EngineFixture>;
