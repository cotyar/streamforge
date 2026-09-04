using System.Diagnostics;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Net.Sockets;
using System.Runtime.CompilerServices;

namespace StreamsForge.Chain.Tests;

/// <summary>
/// One StreamsForge host, spawned as a real OS process out of the repo's own
/// <c>orleans/src/StreamsForge.Host/bin/{Debug|Release}/net10.0</c> directory and driven over REST.
/// Ported from <c>clients/dotnet/StreamsForge.Client.Tests/Fixtures/EngineFixture.cs</c> MINUS the
/// publish step: that fixture publishes the host (~2 minutes) before running it, which is pure cost
/// here — the built bin directory is already runnable in place (it has <c>StreamsForge.Host.dll</c>,
/// <c>appsettings.json</c> and <c>plugins/</c>, the last two put there by the host csproj's own
/// <c>CopyBuiltInPlugins</c> target), and a solution build is the gate for these tests anyway.
///
/// <para>The four traps this type exists to not fall into, all inherited from EngineFixture's own
/// hard-won class doc:</para>
/// <list type="number">
/// <item><b>Working directory.</b> <c>WebApplication.CreateBuilder</c> takes its content root from the
/// CURRENT DIRECTORY, not the assembly's. Run the DLL from anywhere but the host bin directory and
/// <c>appsettings.json</c> is never found, so <c>Jwt:Key</c> is null and every request — including
/// <c>/api/healthz</c> — 500s inside auth middleware.</item>
/// <item><b>No <c>--urls</c>.</b> <c>Program.cs</c> only configures its explicit HTTP + gRPC listener
/// split when <c>urls</c> is unset; set it and the host silently never opens a gRPC listener at all,
/// which for a federation test would look exactly like a broken subscriber.</item>
/// <item><b>Drained pipes.</b> A <c>RedirectStandardOutput=true</c> child whose pipe nobody reads
/// blocks (or dies) writing into a full OS pipe buffer once it has logged enough. Both streams are
/// drained continuously into a bounded tail, which doubles as the diagnostic attached to a failed
/// boot.</item>
/// <item><b><c>127.0.0.1</c>, never <c>localhost</c>.</b> This host's Kestrel binds IPv4 only, on
/// purpose (see CLAUDE.md) — a client that resolves <c>localhost</c> to <c>::1</c> first gets a
/// connection refused that reads like a dead server.</item>
/// </list>
///
/// <para>The same instance can be stopped and started again against the SAME <see cref="DataDir"/> and
/// the same ports — that is what <c>HostRestartTests</c> does. Data-directory ownership is therefore
/// deliberately separate from process lifetime: <see cref="StopAsync"/> kills the process tree and
/// leaves the state on disk; only <see cref="DisposeAsync"/> deletes it.</para>
/// </summary>
public sealed class HostProcess : IAsyncDisposable
{
    public const string AdminUser = "admin";
    public const string AdminPassword = "admin123!";

    /// <summary>Ports these tests must never touch: the Orleans dev server (5199/5299), the Dapr dev
    /// server (5399/5499), the two containerized stacks (6199/6399), the admin app (5599) and the .NET
    /// client contract fixture (9199/9299). A configured port landing on one of these is a bug in the
    /// test, not a busy machine, and is reported as such rather than as a collision.</summary>
    private static readonly int[] ForbiddenPorts = [5199, 5299, 5399, 5499, 5599, 6199, 6399, 9199, 9299];

    private static readonly string Dotnet =
        Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), ".dotnet", "dotnet");

    private const int LogTailMaxLines = 600;

    private readonly string _label;
    private readonly int _httpPort;
    private readonly int _grpcPort;
    private readonly int _siloPort;
    private readonly int _gatewayPort;
    private readonly string[] _extraArgs;

    private readonly object _logLock = new();
    private readonly List<string> _logTail = [];

    private Process? _process;

    public HostProcess(
        string label,
        int httpPort,
        int grpcPort,
        int siloPort,
        int gatewayPort,
        params string[] extraArgs)
    {
        _label = label;
        _httpPort = httpPort;
        _grpcPort = grpcPort;
        _siloPort = siloPort;
        _gatewayPort = gatewayPort;
        _extraArgs = extraArgs;
        DataDir = Directory.CreateTempSubdirectory($"sf-chain-{label}-").FullName;
    }

    /// <summary>Temp state directory, kept across a stop/start pair and deleted only on dispose.</summary>
    public string DataDir { get; }

    public string BaseUrl => $"http://127.0.0.1:{_httpPort}";

    public int HttpPort => _httpPort;

    public int GrpcPort => _grpcPort;

    /// <summary>Every port this host binds — the set <see cref="Preflight"/> checks and the set a
    /// caller can re-check for cleanliness after teardown.</summary>
    public IReadOnlyList<int> Ports => [_httpPort, _grpcPort, _siloPort, _gatewayPort];

    /// <summary>The built host directory, run in place. Located from THIS source file's compile-time
    /// path (so it does not depend on the test runner's working directory), with the configuration
    /// segment taken from the test assembly's own output path — a Release test run must run a Release
    /// host, not a stale Debug one.</summary>
    public static string HostBinDir([CallerFilePath] string thisFile = "")
    {
        var config = AppContext.BaseDirectory.Contains($"{Path.DirectorySeparatorChar}Release{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
            ? "Release"
            : "Debug";
        return Path.GetFullPath(Path.Combine(
            Path.GetDirectoryName(thisFile)!, "..", "..", "src", "StreamsForge.Host", "bin", config, "net10.0"));
    }

    /// <summary>Null when everything these tests need is present and free; otherwise the one-line
    /// reason the caller should report. Plain xunit v2 <c>[Fact]</c> cannot dynamically skip at
    /// runtime, so — exactly as <c>EngineFixture</c> does — the fixture records this and each test
    /// turns it into an explicit "skipped: ..." assertion failure rather than colliding with whatever
    /// is already listening.</summary>
    public static string? Preflight(params int[] ports)
    {
        if (!File.Exists(Dotnet))
        {
            return $"dotnet not found at {Dotnet} — cannot spawn a StreamsForge host";
        }

        var binDir = HostBinDir();
        var dll = Path.Combine(binDir, "StreamsForge.Host.dll");
        if (!File.Exists(dll))
        {
            return $"{dll} not found — build orleans/StreamsForge.sln before running the chain tests";
        }
        if (!File.Exists(Path.Combine(binDir, "appsettings.json")))
        {
            return $"{binDir} has no appsettings.json — an incomplete host build cannot serve /api/healthz "
                 + "(Jwt:Key would be null and every request 500s inside auth middleware)";
        }

        foreach (var port in ports)
        {
            if (ForbiddenPorts.Contains(port))
            {
                return $"refusing to configure a chain-test host onto reserved port {port}";
            }
            if (!PortFree(port))
            {
                return $"port {port} is already in use — refusing to collide with whatever owns it";
            }
        }

        return null;
    }

    public static bool PortFree(int port)
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

    public void Start()
    {
        var binDir = HostBinDir();
        var psi = new ProcessStartInfo(Dotnet)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            // Trap 1 in the class doc: the content root is the CURRENT directory.
            WorkingDirectory = binDir,
        };
        psi.ArgumentList.Add(Path.Combine(binDir, "StreamsForge.Host.dll"));
        psi.ArgumentList.Add("--Http:Port");
        psi.ArgumentList.Add(_httpPort.ToString());
        psi.ArgumentList.Add("--Grpc:Port");
        psi.ArgumentList.Add(_grpcPort.ToString());
        // Two silos on one machine need distinct cluster ports; the defaults (11111/30000) are shared.
        psi.ArgumentList.Add("--Silo:Port");
        psi.ArgumentList.Add(_siloPort.ToString());
        psi.ArgumentList.Add("--Silo:GatewayPort");
        psi.ArgumentList.Add(_gatewayPort.ToString());
        psi.ArgumentList.Add("--DataDir");
        psi.ArgumentList.Add(DataDir);
        foreach (var arg in _extraArgs)
        {
            psi.ArgumentList.Add(arg);
        }

        _process = Process.Start(psi)
            ?? throw new InvalidOperationException($"failed to start StreamsForge host '{_label}'");
        _process.OutputDataReceived += (_, e) => AppendLog(e.Data);
        _process.ErrorDataReceived += (_, e) => AppendLog(e.Data);
        _process.BeginOutputReadLine();
        _process.BeginErrorReadLine();
    }

    public async Task WaitHealthyAsync(TimeSpan? timeout = null)
    {
        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(2) };
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(90));
        Exception? lastError = null;
        while (DateTime.UtcNow < deadline)
        {
            if (_process!.HasExited)
            {
                throw new InvalidOperationException(
                    $"host '{_label}' exited early (code {_process.ExitCode}):\n{Tail(LogTailText(), 6000)}");
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
            $"host '{_label}' did not become healthy within {(timeout ?? TimeSpan.FromSeconds(90)).TotalSeconds:0}s "
          + $"(last error: {lastError})\n{Tail(LogTailText(), 6000)}");
    }

    /// <summary>An <see cref="HttpClient"/> already carrying an admin bearer token for this host. The
    /// caller owns (and disposes) it; a restart mints a new one, since the old token's issuer instance
    /// is gone.</summary>
    public async Task<HttpClient> LoginAsync()
    {
        var http = new HttpClient { BaseAddress = new Uri(BaseUrl), Timeout = TimeSpan.FromSeconds(30) };
        try
        {
            var resp = await http.PostAsJsonAsync("api/auth/login", new { username = AdminUser, password = AdminPassword });
            resp.EnsureSuccessStatusCode();
            var login = await resp.Content.ReadFromJsonAsync<LoginResponseDto>()
                ?? throw new InvalidOperationException($"host '{_label}' returned an empty login response");
            http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", login.Token);
            return http;
        }
        catch
        {
            http.Dispose();
            throw;
        }
    }

    /// <summary>Kills the whole process tree (a `dotnet <dll>` launch can leave the real server as a
    /// child) and leaves <see cref="DataDir"/> on disk so the same state can be started again.</summary>
    public async Task StopAsync()
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

        // The OS holds the listening sockets briefly after the process dies; a restart onto the same
        // ports races that. Poll rather than sleep a fixed amount.
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(20);
        while (DateTime.UtcNow < deadline && Ports.Any(p => !PortFree(p)))
        {
            await Task.Delay(200);
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
        await StopAsync();
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
