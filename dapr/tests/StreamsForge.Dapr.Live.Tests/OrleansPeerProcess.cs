using System.Diagnostics;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Net.Sockets;
using System.Runtime.CompilerServices;

namespace StreamsForge.Dapr.Live.Tests;

/// <summary>
/// One ORLEANS-flavor StreamsForge host, spawned as a real OS process out of
/// <c>orleans/src/StreamsForge.Host/bin/{Debug|Release}/net10.0</c> — the peer
/// <see cref="FederationTests"/> federates with in both directions.
///
/// <para><b>Why this project spawns the OTHER flavor's binary.</b> Federation is the one property that
/// cannot be tested inside a flavor: "a Dapr instance and an Orleans instance can consume each other's
/// entities over gRPC" needs one of each, and neither flavor's test project can be the only owner of
/// that. It lands here rather than in <c>orleans/tests/StreamsForge.Chain.Tests</c> because the Dapr
/// side is the half that is NEW (its gRPC surface arrived with plan 025) and because this project
/// already owns the harder fixture — a Dapr instance needs a sidecar, a scoped component set and a
/// dedicated placement container, none of which the Orleans project knows anything about, whereas an
/// Orleans host needs four port numbers and a temp directory.</para>
///
/// <para><b>A deliberately minimal near-copy of
/// <c>orleans/tests/StreamsForge.Chain.Tests/HostProcess.cs</c>, not a reference to it.</b> The two test
/// projects live in different solutions and share no assembly; adding a cross-solution ProjectReference
/// to reach one fixture type would drag the Orleans test project (and its Orleans TestingHost
/// dependencies) into the Dapr solution's build for no other reason. What is copied is only what
/// federation needs — start, health, login, stop — and NOT the TLS thumbprint pinning, the restart
/// support or the forbidden-port list, none of which this class's single caller uses. Its four traps are
/// inherited verbatim and are the reason the copy is faithful where it matters:</para>
/// <list type="number">
/// <item><b>Working directory.</b> <c>WebApplication.CreateBuilder</c> takes its content root from the
/// CURRENT directory, so the process is spawned with <c>WorkingDirectory</c> set to the host bin dir —
/// otherwise <c>appsettings.json</c> is never found, <c>Jwt:Key</c> is null, and every request including
/// <c>/api/healthz</c> 500s inside auth middleware.</item>
/// <item><b>No <c>--urls</c>.</b> The Orleans <c>Program.cs</c> only configures its explicit HTTP + gRPC
/// listener split when <c>urls</c> is unset. Set it and no gRPC listener is opened at all — which in a
/// federation test looks exactly like a broken subscriber.</item>
/// <item><b>Drained pipes.</b> A redirected child whose pipe nobody reads blocks once it has logged
/// enough; both streams are drained continuously into a bounded tail that doubles as the diagnostic on a
/// failed boot.</item>
/// <item><b><c>127.0.0.1</c>, never <c>localhost</c>.</b> Both hosts bind IPv4 only, on purpose (see
/// AGENTS.md) — a client that resolves <c>localhost</c> to <c>::1</c> first gets a connection refused
/// that reads like a dead server.</item>
/// </list>
///
/// <para><b>Ports 4999 / 5099 / 14999 / 34999</b> (REST / gRPC / silo / silo gateway), unshared with
/// anything else in the repo — see <see cref="DaprHostProcess"/>'s port list and AGENTS.md's table. The
/// silo pair matters as much as the other two: the Orleans defaults are 11111/30000 and a second silo on
/// this machine would collide there long before anyone noticed the HTTP ports were fine.</para>
/// </summary>
public sealed class OrleansPeerProcess : IAsyncDisposable
{
    public const string AdminUser = "admin";
    public const string AdminPassword = "admin123!";

    public const int HttpPort = 4999;
    public const int GrpcPort = 5099;
    public const int SiloPort = 14999;
    public const int GatewayPort = 34999;

    private static readonly string Dotnet =
        Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), ".dotnet", "dotnet");

    private const int LogTailMaxLines = 400;

    private readonly object _logLock = new();
    private readonly List<string> _logTail = [];

    private Process? _process;

    public OrleansPeerProcess()
    {
        DataDir = Directory.CreateTempSubdirectory("sf-dapr-live-orleans-peer-").FullName;
    }

    public string DataDir { get; }

    public string BaseUrl => $"http://127.0.0.1:{HttpPort}";

    /// <summary>The gRPC endpoint a Dapr-side <c>grpc</c> source is pointed at (h2c, prior
    /// knowledge).</summary>
    public string GrpcUrl => $"http://127.0.0.1:{GrpcPort}";

    /// <summary>The built Orleans host directory, run in place. Located from THIS source file's
    /// compile-time path — this project sits at <c>dapr/tests/StreamsForge.Dapr.Live.Tests/</c>, three
    /// levels below the repo root — with the configuration segment taken from the test assembly's own
    /// output path, so a Release run does not spawn a stale Debug host.</summary>
    public static string HostBinDir([CallerFilePath] string thisFile = "")
    {
        var config = AppContext.BaseDirectory.Contains($"{Path.DirectorySeparatorChar}Release{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
            ? "Release"
            : "Debug";
        return Path.GetFullPath(Path.Combine(
            Path.GetDirectoryName(thisFile)!, "..", "..", "..", "orleans", "src", "StreamsForge.Host", "bin", config, "net10.0"));
    }

    /// <summary>Null when an Orleans peer can be spawned; otherwise the one-line reason the caller
    /// should report as its skip. The interesting case is the FIRST one: this project's own solution
    /// (<c>dapr/StreamsForge.Dapr.sln</c>) does not build the Orleans host, so a developer who ran only
    /// <c>dotnet test dapr/StreamsForge.Dapr.sln</c> on a clean tree has no binary here — and must be
    /// told which command produces one, not left with a connection refused.</summary>
    public static string? Preflight()
    {
        if (!File.Exists(Dotnet))
        {
            return $"dotnet not found at {Dotnet} — cannot spawn an Orleans peer host";
        }

        var binDir = HostBinDir();
        var dll = Path.Combine(binDir, "StreamsForge.Host.dll");
        if (!File.Exists(dll))
        {
            return $"{dll} not found — the Orleans host is not built by dapr/StreamsForge.Dapr.sln; run "
                 + "`dotnet build orleans/src/StreamsForge.Host` (or the whole orleans solution) before "
                 + "the federation test can spawn a peer";
        }
        if (!File.Exists(Path.Combine(binDir, "appsettings.json")))
        {
            return $"{binDir} has no appsettings.json — an incomplete host build cannot serve /api/healthz "
                 + "(Jwt:Key would be null and every request 500s inside auth middleware)";
        }

        foreach (var port in new[] { HttpPort, GrpcPort, SiloPort, GatewayPort })
        {
            if (!DaprHostProcess.PortFree(port))
            {
                return $"port {port} is already in use — refusing to collide with whatever owns it "
                     + "(these four are the Orleans federation peer's; a stray host from a previous run "
                     + "is the usual cause)";
            }
        }

        return null;
    }

    public void Start()
    {
        var binDir = HostBinDir();
        var psi = new ProcessStartInfo(Dotnet)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            WorkingDirectory = binDir, // trap 1 — see the class doc
        };
        psi.ArgumentList.Add(Path.Combine(binDir, "StreamsForge.Host.dll"));
        psi.ArgumentList.Add("--Http:Port");
        psi.ArgumentList.Add(HttpPort.ToString());
        psi.ArgumentList.Add("--Grpc:Port");
        psi.ArgumentList.Add(GrpcPort.ToString());
        psi.ArgumentList.Add("--Silo:Port");
        psi.ArgumentList.Add(SiloPort.ToString());
        psi.ArgumentList.Add("--Silo:GatewayPort");
        psi.ArgumentList.Add(GatewayPort.ToString());
        psi.ArgumentList.Add("--DataDir");
        psi.ArgumentList.Add(DataDir);

        _process = Process.Start(psi)
            ?? throw new InvalidOperationException("failed to start the Orleans peer host");
        _process.OutputDataReceived += (_, e) => AppendLog(e.Data);
        _process.ErrorDataReceived += (_, e) => AppendLog(e.Data);
        _process.BeginOutputReadLine();
        _process.BeginErrorReadLine();
    }

    public HttpClient NewClient(TimeSpan? timeout = null) => new()
    {
        BaseAddress = new Uri(BaseUrl),
        Timeout = timeout ?? TimeSpan.FromSeconds(30),
    };

    public async Task WaitHealthyAsync(TimeSpan? timeout = null)
    {
        using var http = NewClient(TimeSpan.FromSeconds(3));
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(120));
        Exception? lastError = null;
        while (DateTime.UtcNow < deadline)
        {
            if (_process!.HasExited)
            {
                throw new InvalidOperationException(
                    $"the Orleans peer exited early (code {_process.ExitCode}):\n{Tail(LogTailText(), 6000)}");
            }
            try
            {
                var resp = await http.GetAsync($"{BaseUrl}/api/healthz");
                if (resp.IsSuccessStatusCode)
                {
                    return;
                }
            }
            catch (Exception ex)
            {
                lastError = ex;
            }
            await Task.Delay(500);
        }
        throw new InvalidOperationException(
            $"the Orleans peer did not become healthy within "
          + $"{(timeout ?? TimeSpan.FromSeconds(120)).TotalSeconds:0}s (last error: {lastError})\n"
          + Tail(LogTailText(), 6000));
    }

    public async Task<HttpClient> LoginAsync()
    {
        var http = NewClient();
        try
        {
            var resp = await http.PostAsJsonAsync("api/auth/login", new { username = AdminUser, password = AdminPassword });
            resp.EnsureSuccessStatusCode();
            var login = await resp.Content.ReadFromJsonAsync<LoginResponseDto>()
                ?? throw new InvalidOperationException("the Orleans peer returned an empty login response");
            http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", login.Token);
            return http;
        }
        catch
        {
            http.Dispose();
            throw;
        }
    }

    public string LogTailText()
    {
        lock (_logLock)
        {
            return string.Join('\n', _logTail);
        }
    }

    private void AppendLog(string? line)
    {
        if (line is null)
        {
            return;
        }
        lock (_logLock)
        {
            _logTail.Add(line);
            if (_logTail.Count > LogTailMaxLines)
            {
                _logTail.RemoveAt(0);
            }
        }
    }

    private static string Tail(string s, int max) => s.Length <= max ? s : s[^max..];

    public async ValueTask DisposeAsync()
    {
        if (_process is { HasExited: false })
        {
            try
            {
                _process.Kill(entireProcessTree: true);
                await _process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(20));
            }
            catch
            {
                // best-effort teardown
            }
        }
        _process?.Dispose();
        _process = null;

        // The OS holds the listening sockets briefly after the process dies; poll rather than sleep so a
        // following test that checks these ports does not lose the race.
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(20);
        var ports = new[] { HttpPort, GrpcPort, SiloPort, GatewayPort };
        while (DateTime.UtcNow < deadline && ports.Any(p => !DaprHostProcess.PortFree(p)))
        {
            await Task.Delay(200);
        }

        try
        {
            Directory.Delete(DataDir, recursive: true);
        }
        catch
        {
            // best-effort cleanup
        }
    }

    private sealed record LoginResponseDto(string Token);
}
