using System.Diagnostics;
using System.Net.Http.Json;
using System.Runtime.CompilerServices;
using Xunit;

namespace StreamsForge.Client.Tests.Fixtures;

/// <summary>
/// TLS counterpart to <see cref="EngineFixture"/>: boots an isolated StreamsForge instance on
/// **7399** (HTTPS)/**7499** (TLS gRPC, ALPN h2) -- reserved ports, never 9199/9299 (the plaintext
/// contract fixture, which may be running concurrently in the same test process) or 5199/5299/6199
/// (see <see cref="EngineFixture"/>'s own doc). Its own silo ports (17399/37399) are set explicitly
/// because a second host in the same test run needs them (see CLAUDE.md's ports table).
///
/// Certificates come from <c>tools/tls/dev-cert.sh</c> (self-signed, its own trust anchor -- see
/// that script's own doc), generated fresh into a temp directory per test run so nothing here
/// depends on -- or pollutes -- a checked-in certificate. Process lifecycle is shared with
/// <see cref="EngineFixture"/> via <see cref="EngineProcess"/>.
/// </summary>
public sealed class TlsEngineFixture : IAsyncLifetime
{
    public const int HttpPort = 7399;
    public const int GrpcPort = 7499;
    public const int SiloPort = 17399;
    public const int SiloGatewayPort = 37399;
    private static readonly int[] ForbiddenPorts = [5199, 5299, 6199, 9199, 9299];

    public const string AdminUser = "admin";
    public const string AdminPassword = "admin123!";

    public string BaseUrl { get; } = $"https://127.0.0.1:{HttpPort}";
    public string GrpcTarget { get; } = $"https://127.0.0.1:{GrpcPort}";

    /// <summary>See <see cref="EngineFixture.SkipReason"/>'s doc -- same contract, same reasons
    /// (port collision, missing dotnet/openssl, publish/cert-generation failure), plus this
    /// fixture's own: a missing <c>tools/tls/dev-cert.sh</c>.</summary>
    public string? SkipReason { get; private set; }

    /// <summary>Path to the CA (= server) certificate <c>dev-cert.sh</c> wrote -- pass this as
    /// <see cref="ConnectOptions.CaCertificatePath"/>.</summary>
    public string? CaCertificatePath { get; private set; }

    private Process? _process;
    private string? _dataDir;
    private string? _certDir;
    private string? _publishDir;
    private bool _ownsPublishDir;
    private readonly ProcessLogTail _log = new();

    public async Task InitializeAsync()
    {
        if (ForbiddenPorts.Contains(HttpPort) || ForbiddenPorts.Contains(GrpcPort))
        {
            SkipReason = "refusing to configure the TLS fixture onto a forbidden port";
            return;
        }
        if (!EngineProcess.PortFree(HttpPort) || !EngineProcess.PortFree(GrpcPort))
        {
            SkipReason = $"port {HttpPort} or {GrpcPort} is already in use -- refusing to collide with a running instance";
            return;
        }
        if (!File.Exists(EngineProcess.Dotnet))
        {
            SkipReason = $"dotnet not found at {EngineProcess.Dotnet} -- cannot boot the TLS test engine";
            return;
        }

        var devCertScript = DevCertScriptPath();
        if (!File.Exists(devCertScript))
        {
            SkipReason = $"tools/tls/dev-cert.sh not found at {devCertScript}";
            return;
        }
        if (FindOpenSsl() is null)
        {
            SkipReason = "openssl not found on PATH -- cannot generate a dev TLS certificate";
            return;
        }

        var projectDir = ProjectDir();
        if (!Directory.Exists(projectDir))
        {
            SkipReason = $"StreamsForge.Host project not found at {projectDir}";
            return;
        }

        _certDir = Directory.CreateTempSubdirectory("sf-dotnet-client-tls-cert-").FullName;
        try
        {
            await RunDevCertScriptAsync(devCertScript, _certDir);
        }
        catch (Exception ex)
        {
            SkipReason = $"could not generate a dev TLS certificate: {ex.Message}";
            return;
        }
        CaCertificatePath = Path.Combine(_certDir, "cert.pem");
        var keyPath = Path.Combine(_certDir, "key.pem");
        if (!File.Exists(CaCertificatePath) || !File.Exists(keyPath))
        {
            SkipReason = $"dev-cert.sh did not produce cert.pem/key.pem under {_certDir}";
            return;
        }

        // Same SF_TEST_PUBLISH_DIR reuse convention as EngineFixture -- a publish takes ~2 minutes,
        // and a caller iterating on this fixture (or running it alongside EngineFixture in the same
        // test session) should not pay that twice.
        _publishDir = Environment.GetEnvironmentVariable("SF_TEST_PUBLISH_DIR");
        if (string.IsNullOrEmpty(_publishDir))
        {
            _publishDir = Directory.CreateTempSubdirectory("sf-dotnet-client-tls-publish-").FullName;
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

        _dataDir = Directory.CreateTempSubdirectory("sf-dotnet-client-tls-data-").FullName;
        _process = EngineProcess.StartHost(_publishDir, [
            "--Http:Port", HttpPort.ToString(),
            "--Grpc:Port", GrpcPort.ToString(),
            "--Streams:Transport", "push",
            "--DataDir", _dataDir,
            "--Silo:Port", SiloPort.ToString(),
            "--Silo:GatewayPort", SiloGatewayPort.ToString(),
            "--Tls:Enabled", "true",
            "--Kestrel:Certificates:Default:Path", CaCertificatePath,
            "--Kestrel:Certificates:Default:KeyPath", keyPath,
        ], _log);
        try
        {
            // Trusts the very CA it just generated -- the same validator real callers configure via
            // ConnectOptions.CaCertificatePath, exercised here for the health probe too.
            var validator = TlsSupport.BuildValidator(new ConnectOptions { CaCertificatePath = CaCertificatePath });
            var handler = new SocketsHttpHandler();
            if (validator is not null) handler.SslOptions.RemoteCertificateValidationCallback = validator;
            using var http = new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(2) };
            await EngineProcess.WaitHealthyAsync(http, BaseUrl, _process, _log, TimeSpan.FromSeconds(90));
            await ImportFixtureConfigAsync();
        }
        catch (Exception ex)
        {
            SkipReason = $"engine did not come up cleanly: {ex.Message}";
            await DisposeAsync();
        }
    }

    public const string SourceName = "sf_dotnet_client_tls_trades";
    public const string LatestTable = "sf_dotnet_client_tls_latest_trade";

    private async Task ImportFixtureConfigAsync()
    {
        var validator = TlsSupport.BuildValidator(new ConnectOptions { CaCertificatePath = CaCertificatePath });
        var handler = new SocketsHttpHandler();
        if (validator is not null) handler.SslOptions.RemoteCertificateValidationCallback = validator;
        using var http = new HttpClient(handler) { BaseAddress = new Uri(BaseUrl) };
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
                    description = ".NET client TLS test fixture",
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
            },
        };
        var resp = await http.PostAsJsonAsync("api/config/import?mode=merge", doc);
        var body = await resp.Content.ReadAsStringAsync();
        if (!resp.IsSuccessStatusCode || body.Contains("\"action\":\"error\""))
            throw new InvalidOperationException($"TLS fixture config import failed: {resp.StatusCode} {body}");
    }

    private sealed record LoginResponseDto(string Token);

    private static async Task RunDevCertScriptAsync(string scriptPath, string outDir)
    {
        var psi = new ProcessStartInfo("bash", $"\"{scriptPath}\" \"{outDir}\"")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        using var proc = Process.Start(psi) ?? throw new InvalidOperationException("failed to start dev-cert.sh");
        var stdoutTask = proc.StandardOutput.ReadToEndAsync();
        var stderrTask = proc.StandardError.ReadToEndAsync();
        await proc.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(30));
        var stdout = await stdoutTask;
        var stderr = await stderrTask;
        if (proc.ExitCode != 0)
            throw new InvalidOperationException($"dev-cert.sh failed (code {proc.ExitCode}):\n{EngineProcess.Tail(stdout + stderr, 4000)}");
    }

    private static string? FindOpenSsl()
    {
        var pathVar = Environment.GetEnvironmentVariable("PATH") ?? "";
        foreach (var dir in pathVar.Split(Path.PathSeparator))
        {
            if (string.IsNullOrEmpty(dir)) continue;
            var candidate = Path.Combine(dir, "openssl");
            if (File.Exists(candidate)) return candidate;
        }
        return null;
    }

    private static string ProjectDir([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "..", "..", "..", "orleans", "src", "StreamsForge.Host"));

    private static string DevCertScriptPath([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "..", "..", "..", "tools", "tls", "dev-cert.sh"));

    public async Task DisposeAsync()
    {
        await EngineProcess.KillAsync(_process);
        _process = null;
        EngineProcess.TryDeleteDirectory(_dataDir);
        EngineProcess.TryDeleteDirectory(_certDir);
        if (_ownsPublishDir) EngineProcess.TryDeleteDirectory(_publishDir);
    }
}

[CollectionDefinition(nameof(TlsEngineCollection))]
public sealed class TlsEngineCollection : ICollectionFixture<TlsEngineFixture>;
