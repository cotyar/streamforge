using System.Diagnostics;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Security.Cryptography.X509Certificates;
using Xunit;

namespace StreamsForge.Dapr.Live.Tests;

/// <summary>
/// One isolated Dapr-flavor StreamsForge instance, spawned as a real <c>dapr run</c>-wrapped OS process
/// against the built <c>dapr/src/StreamsForge.Dapr.Host/bin/{Debug|Release}/net10.0</c> host, driven over
/// REST — the process-level pattern <c>orleans/tests/StreamsForge.Chain.Tests/HostProcess.cs</c>
/// established, ported to a runtime whose isolation story is materially harder. See this class's own
/// "Isolation design" section below for what plan 025 D1 actually needed to make that safe, and
/// <c>dapr/PARITY.md</c> §3 ("Why this was not simply fixed by running it") for why nobody had done this
/// before.
///
/// <para><b>Fixed identity, not configurable ports.</b> Unlike <c>HostProcess</c> (which takes its ports
/// as constructor arguments because the Chain tests run TWO Orleans silos side by side), every instance
/// this type creates uses the SAME app-id (<see cref="AppId"/>) and the SAME four ports — app
/// <c>5799</c>, gRPC <c>5899</c> (reserved, matching the dev host's own 5399/5499 shape — nothing
/// listens on it yet, see <c>dapr/PARITY.md</c> D1), sidecar HTTP <c>3799</c>, sidecar gRPC
/// <c>4799</c> — because a Dapr app-id is baked into every actor id and every statestore key at the
/// component level (<c>dapr/components-test/statestore.yaml</c>'s <c>scopes</c>), not passed as a
/// per-instance parameter the way an Orleans silo's cluster id is. Running two instances of this type
/// concurrently would collide on both the ports AND the actor namespace, which is exactly why every test
/// class in this project joins <see cref="DaprLiveTestCollection"/> — one class's instance at a time.</para>
///
/// <para><b>Isolation design (plan 025 D1).</b> Three problems, three fixes:</para>
/// <list type="number">
/// <item><b>Statestore scope.</b> The dev components (<c>dapr/components/statestore.yaml</c>/
/// <c>pubsub.yaml</c>) pin <c>scopes: [streamsforge-dapr]</c> — an app-id outside that list has NO
/// statestore component in scope for actors at all, which panics the 1.18 actor runtime outright rather
/// than degrading gracefully (verified in plan 021, recorded in <c>dapr/ARCHITECTURE.md</c>'s
/// "Statestore scoping caveat" and in <c>PARITY.md</c> §3). <c>dapr/components-test/*.yaml</c> scopes
/// both components to <c>streamsforge-dapr-test</c> instead — a different app-id, a different scope
/// list, no shared statestore.</item>
/// <item><b>Redis key/stream isolation.</b> <c>keyPrefix: appid</c> alone would already put every key
/// under a distinct string (<c>streamsforge-dapr-test||...</c> vs <c>streamsforge-dapr||...</c>) inside
/// the SAME Redis logical database, which is enough for correctness but not for a clean, single-command
/// wipe — <c>redis-cli FLUSHDB</c> has no "matching this prefix" mode. Both test components additionally
/// set <c>redisDB: "1"</c>, moving every key AND every pub/sub stream this instance ever touches into
/// Redis logical database 1, fully apart from database 0 where the dev instance (app-id
/// <c>streamsforge-dapr</c>, see AGENTS.md's port table) and the polyglot processors live.
/// <see cref="ResetAsync"/> is
/// therefore a plain <c>-n 1 FLUSHDB</c> — the Dapr-flavor equivalent of
/// <c>dapr/tools/reset.sh</c>'s scoped SCAN, except here isolation is a WHOLE DATABASE rather than a key
/// prefix, so there is no scan pattern to get wrong. Verified live (see the class's own commit message
/// and report): <c>redis-cli -n 1 --scan</c> after a boot lists this app's actor keys and its five
/// fixed pub/sub topics; <c>redis-cli -n 0 --scan --pattern 'streamsforge-dapr-test*'</c> is empty both
/// before and after, and the dev instance's own 17 <c>streamsforge-dapr||...</c> keys in database 0 are
/// untouched.</item>
/// <item><b>Actor placement.</b> This is the one dev-container port this class's instances do NOT
/// share: <see cref="PlacementHostAddress"/> points at a SEPARATE, dedicated <c>dapr_placement_test</c>
/// container on host port <c>6150</c> (cloned from <c>dapr_placement</c>'s own image/entrypoint —
/// <c>docker run -d --name dapr_placement_test -p 6150:50005 docker.io/daprio/dapr:1.18.3 ./placement</c>
/// — left running exactly like <c>dapr init</c>'s own containers), never the shared
/// <c>dapr_placement</c> the dev instance and `dapr init` itself created. <b>This was tested empirically
/// before deciding it, not assumed</b>: two throwaway app-ids running the SAME
/// <c>StreamsForge.Dapr.Host.dll</c> binary (so both register the identical actor type set —
/// <c>RegistryActor</c>, <c>TableActor</c>, etc. — Dapr actor TYPE NAMES are placement-global, not
/// app-id-scoped) were booted side by side against the ONE shared default placement service
/// (<c>localhost:50005</c>). The result was not a clean failure to isolate — it was outright breakage on
/// BOTH sides: placement logged a single shared dissemination round covering both apps' actor types
/// ("Dissemination complete for version 2 (changed types [...])" — the exact same version number and
/// type list in both apps' logs), app A logged
/// "Timed out waiting for actor type 'UserStoreActor' in-flight lock claims to be released for
/// rebalanced types, force cancelling remaining claims" and its login attempts started failing (401,
/// then 500) as PLACEMENT REBALANCING churned mid-request, and BOTH apps logged
/// <c>Dapr.DaprApiException: error invoke actor method: failed to invoke target &lt;LAN-IP&gt;:&lt;port&gt;
/// after 5 retries ... i/o timeout</c> — each app's sidecar was told by placement to route an actor
/// invocation to a HOST ADDRESS THAT WASN'T ITS OWN APP, and that cross-app route timed out rather than
/// silently succeeding. App B's own <c>/api/sources</c> read back <c>[]</c> immediately after its own
/// log had printed "catalog seeded (6 sources, 7 pipelines, 5 tables)" — the seed write and the
/// subsequent read did not even agree with each other inside the SAME app-id, consistent with the
/// `RegistryActor "catalog"` activation bouncing between hosts mid-sequence. Two app-ids sharing one
/// placement service while hosting identically-named actor types is therefore not a hypothetical risk
/// here, it is reproducible breakage — hence the dedicated container rather than a documented caveat.</item>
/// </list>
///
/// <para><b>The same four traps <c>HostProcess</c>'s own class doc names, inherited unchanged</b>:
/// working directory (content root comes from the CURRENT directory, not the assembly's — this is why
/// <see cref="Start"/> sets <c>WorkingDirectory</c> to the host bin dir before spawning), drained pipes
/// (stdout/stderr are continuously read into a bounded tail), <c>127.0.0.1</c> never <c>localhost</c>,
/// and (new to this type) the `dapr run` CLI itself sits IN FRONT of the actual host process — killing
/// only the child the .NET <see cref="Process"/> handle refers to would leave the sidecar and the
/// `daprd`-launched app orphaned, which is why <see cref="StopAsync"/> asks the CLI to stop the app-id
/// FIRST (<c>dapr stop --app-id streamsforge-dapr-test</c>, which tears down the sidecar cleanly) and
/// only then kills whatever OS process tree is left.</para>
///
/// <para><b>TLS.</b> <see cref="TlsCertPath"/> exists for shape parity with <c>HostProcess</c> and a
/// later wave only — the Dapr host has NO TLS support today (CLAUDE.md's TLS paragraph: "plan 024,
/// Orleans only — the Dapr host is untouched and still loopback-only"). Setting it currently only flips
/// this fixture's own idea of the scheme; it does not make the spawned host serve HTTPS, so a test that
/// sets it today would simply fail to connect. <see cref="EnvVars"/> and <see cref="ExtraArgs"/> exist
/// for the same forward-compatibility reason.</para>
///
/// <para>The same instance can be stopped and started again against the SAME <see cref="DataDir"/> and
/// the SAME Redis database 1 state — that is what <c>RestartResumeTests</c> does via
/// <see cref="RestartAsync"/>. Data ownership is therefore deliberately separate from process lifetime:
/// <see cref="StopAsync"/> leaves both the temp directory AND database 1 exactly as they were; only
/// <see cref="DisposeAsync"/> deletes <see cref="DataDir"/>, and only an explicit <see cref="ResetAsync"/>
/// (always called BEFORE the first <see cref="Start"/> of a fresh scenario, never after) flushes
/// database 1.</para>
/// </summary>
public sealed class DaprHostProcess : IAsyncDisposable
{
    public const string AdminUser = "admin";
    public const string AdminPassword = "admin123!";

    /// <summary>The one app-id every instance this type creates uses. Fixed, not a constructor
    /// parameter — see the class doc's "Fixed identity" paragraph.</summary>
    public const string AppId = "streamsforge-dapr-test";

    public const int HttpPort = 5799;
    public const int GrpcPort = 5899;
    public const int SidecarHttpPort = 3799;
    public const int SidecarGrpcPort = 4799;

    /// <summary>The dedicated placement container's host port — see the class doc's "Actor placement"
    /// paragraph for why this instance never uses the shared <c>dapr_placement</c> the dev instance and
    /// `dapr init` itself created.</summary>
    public const string PlacementHostAddress = "127.0.0.1:6150";

    /// <summary>The Redis logical database every component in <c>dapr/components-test/*.yaml</c> is
    /// pinned to (<c>redisDB: "1"</c>). Database 0 is the dev instance's (and the polyglot processors').
    /// Never hard-code the literal <c>1</c> at a second call site — go through this constant so a future
    /// change to the isolation database only has one place to edit.</summary>
    public const int TestRedisDb = 1;

    private const string RedisContainer = "dapr_redis";

    private static readonly string Dotnet =
        Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), ".dotnet", "dotnet");

    private const int LogTailMaxLines = 600;

    private readonly string _label;
    private readonly string[] _extraArgs;

    private readonly object _logLock = new();
    private readonly List<string> _logTail = [];

    private Process? _process;
    private string? _certThumbprint;

    public DaprHostProcess(string label = "test", params string[] extraArgs)
    {
        _label = label;
        _extraArgs = extraArgs;
        DataDir = Directory.CreateTempSubdirectory($"sf-dapr-live-{label}-").FullName;
    }

    /// <summary>Temp state directory, kept across a stop/start pair (see <see cref="RestartAsync"/>) and
    /// deleted only on <see cref="DisposeAsync"/>.</summary>
    public string DataDir { get; }

    /// <summary>See the class doc's TLS paragraph — inert today, shaped for a later wave.</summary>
    public string? TlsCertPath { get; init; }

    public bool IsTls => TlsCertPath is not null;

    /// <summary>Extra environment variables for the spawned `dapr run` process (NOT the inner app —
    /// `dapr run` forwards its own environment to the child it launches, so this reaches the app too),
    /// filled before <see cref="Start"/>.</summary>
    public IDictionary<string, string> EnvVars { get; } = new Dictionary<string, string>(StringComparer.Ordinal);

    public string BaseUrl => $"{(IsTls ? "https" : "http")}://127.0.0.1:{HttpPort}";

    /// <summary>The built host directory, run in place. Located from THIS source file's compile-time
    /// path (so it does not depend on the test runner's working directory), with the configuration
    /// segment taken from the test assembly's own output path — a Release test run must run a Release
    /// host, not a stale Debug one. Identical technique to
    /// <c>orleans/tests/StreamsForge.Chain.Tests/HostProcess.cs</c>'s <c>HostBinDir</c>.</summary>
    public static string HostBinDir([CallerFilePath] string thisFile = "")
    {
        var config = AppContext.BaseDirectory.Contains($"{Path.DirectorySeparatorChar}Release{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
            ? "Release"
            : "Debug";
        return Path.GetFullPath(Path.Combine(
            Path.GetDirectoryName(thisFile)!, "..", "..", "src", "StreamsForge.Dapr.Host", "bin", config, "net10.0"));
    }

    /// <summary>The repo's <c>dapr/</c> directory, located the same way as <see cref="HostBinDir"/> —
    /// this project lives at <c>dapr/tests/StreamsForge.Dapr.Live.Tests/</c>, two levels below it.</summary>
    public static string DaprRootDir([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", ".."));

    public static string ComponentsTestDir([CallerFilePath] string thisFile = "") =>
        Path.Combine(DaprRootDir(thisFile), "components-test");

    /// <summary>Resolves the `dapr` CLI: PATH first, then the fixed Homebrew location AGENTS.md records
    /// for this machine (<c>/opt/homebrew/bin/dapr</c>) as a fallback, since a spawned test process's
    /// PATH does not always match an interactive shell's.</summary>
    private static string? ResolveDaprCli()
    {
        var fromPath = Environment.GetEnvironmentVariable("PATH")?
            .Split(Path.PathSeparator)
            .Select(dir => Path.Combine(dir, "dapr"))
            .FirstOrDefault(File.Exists);
        if (fromPath is not null)
        {
            return fromPath;
        }
        const string homebrew = "/opt/homebrew/bin/dapr";
        return File.Exists(homebrew) ? homebrew : null;
    }

    /// <summary>Null when everything a live test needs is present and free; otherwise the one-line
    /// reason the caller should report. Same "record, don't throw" convention as
    /// <c>HostProcess.Preflight</c> — plain xunit v2 <c>[Fact]</c> cannot dynamically skip at runtime.</summary>
    public static string? Preflight()
    {
        if (!File.Exists(Dotnet))
        {
            return $"dotnet not found at {Dotnet} — cannot spawn a StreamsForge Dapr host";
        }

        var daprCli = ResolveDaprCli();
        if (daprCli is null)
        {
            return "dapr CLI not found on PATH or at /opt/homebrew/bin/dapr — cannot spawn a sidecar";
        }

        var binDir = HostBinDir();
        var dll = Path.Combine(binDir, "StreamsForge.Dapr.Host.dll");
        if (!File.Exists(dll))
        {
            return $"{dll} not found — build dapr/StreamsForge.Dapr.sln before running the live tests";
        }
        if (!File.Exists(Path.Combine(binDir, "appsettings.json")))
        {
            return $"{binDir} has no appsettings.json — an incomplete host build cannot serve /api/healthz";
        }

        if (!DockerContainerRunning(RedisContainer))
        {
            return $"docker container '{RedisContainer}' is not running — `dapr init` was expected to have created it";
        }
        if (!DockerContainerRunning("dapr_placement_test"))
        {
            return "docker container 'dapr_placement_test' is not running — create it once with: "
                 + "docker run -d --name dapr_placement_test -p 6150:50005 docker.io/daprio/dapr:1.18.3 ./placement "
                 + "(see this class's doc comment for why a dedicated placement service is required, not optional)";
        }

        foreach (var port in new[] { HttpPort, GrpcPort, SidecarHttpPort, SidecarGrpcPort })
        {
            if (!PortFree(port))
            {
                return $"port {port} is already in use — refusing to collide with whatever owns it "
                     + "(a previous run's process may not have been cleaned up; check for a stray "
                     + $"`dapr run --app-id {AppId}` before assuming the machine is just busy)";
            }
        }

        return null;
    }

    private static bool DockerContainerRunning(string name)
    {
        try
        {
            var psi = new ProcessStartInfo("docker")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            };
            psi.ArgumentList.Add("ps");
            psi.ArgumentList.Add("--filter");
            psi.ArgumentList.Add($"name={name}");
            psi.ArgumentList.Add("--format");
            psi.ArgumentList.Add("{{.Names}}");
            using var proc = Process.Start(psi);
            if (proc is null)
            {
                return false;
            }
            var output = proc.StandardOutput.ReadToEnd();
            proc.WaitForExit(5000);
            return output.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .Any(line => line == name);
        }
        catch
        {
            return false;
        }
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

    /// <summary>The Dapr-flavor equivalent of <c>dapr/tools/reset.sh</c> — except this instance's
    /// isolation is a WHOLE Redis logical database (see the class doc's "Redis key/stream isolation"
    /// paragraph), so there is no key-prefix SCAN to get subtly wrong: a plain, unconditional
    /// <c>-n 1 FLUSHDB</c> against <see cref="TestRedisDb"/> is both correct and impossible to point at
    /// the wrong app's data by accident. MUST be called before <see cref="Start"/>, never after — this
    /// is a fresh-slate operation, not a live reset of a running instance.</summary>
    public static async Task ResetAsync()
    {
        var psi = new ProcessStartInfo("docker")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        psi.ArgumentList.Add("exec");
        psi.ArgumentList.Add(RedisContainer);
        psi.ArgumentList.Add("redis-cli");
        psi.ArgumentList.Add("-n");
        psi.ArgumentList.Add(TestRedisDb.ToString());
        psi.ArgumentList.Add("FLUSHDB");

        using var proc = Process.Start(psi)
            ?? throw new InvalidOperationException("failed to start docker exec for redis-cli FLUSHDB");
        var stdout = await proc.StandardOutput.ReadToEndAsync();
        var stderr = await proc.StandardError.ReadToEndAsync();
        await proc.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10));
        if (proc.ExitCode != 0)
        {
            throw new InvalidOperationException(
                $"redis-cli -n {TestRedisDb} FLUSHDB failed (exit {proc.ExitCode}): {stdout} {stderr}");
        }
    }

    /// <summary>Lists Redis keys in database <see cref="TestRedisDb"/> matching a glob
    /// (<c>redis-cli --scan --pattern</c> semantics), used only by
    /// <c>EnvironmentIsolationTests</c> to check actor-key composition live rather than trusting
    /// <c>dapr/ARCHITECTURE.md</c>'s description of it on faith. Always against database 1 — see
    /// <see cref="ResetAsync"/>'s identical care about the <c>-n</c> flag.</summary>
    public static async Task<List<string>> ScanTestRedisKeysAsync(string pattern)
    {
        var psi = new ProcessStartInfo("docker")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        psi.ArgumentList.Add("exec");
        psi.ArgumentList.Add(RedisContainer);
        psi.ArgumentList.Add("redis-cli");
        psi.ArgumentList.Add("-n");
        psi.ArgumentList.Add(TestRedisDb.ToString());
        psi.ArgumentList.Add("--scan");
        psi.ArgumentList.Add("--pattern");
        psi.ArgumentList.Add(pattern);

        using var proc = Process.Start(psi)
            ?? throw new InvalidOperationException("failed to start docker exec for redis-cli --scan");
        var stdout = await proc.StandardOutput.ReadToEndAsync();
        await proc.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10));
        return stdout.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).ToList();
    }

    public void Start()
    {
        var binDir = HostBinDir();
        var daprCli = ResolveDaprCli()
            ?? throw new InvalidOperationException("dapr CLI not found — call Preflight() before Start()");

        var psi = new ProcessStartInfo(daprCli)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            // Trap 1 (inherited from HostProcess's class doc): the app's content root is the CURRENT
            // directory, and `dapr run` passes its own working directory through to the app it spawns.
            WorkingDirectory = binDir,
        };
        psi.ArgumentList.Add("run");
        psi.ArgumentList.Add("--app-id");
        psi.ArgumentList.Add(AppId);
        psi.ArgumentList.Add("--app-port");
        psi.ArgumentList.Add(HttpPort.ToString());
        psi.ArgumentList.Add("--dapr-http-port");
        psi.ArgumentList.Add(SidecarHttpPort.ToString());
        psi.ArgumentList.Add("--dapr-grpc-port");
        psi.ArgumentList.Add(SidecarGrpcPort.ToString());
        psi.ArgumentList.Add("--placement-host-address");
        psi.ArgumentList.Add(PlacementHostAddress);
        psi.ArgumentList.Add("--resources-path");
        psi.ArgumentList.Add(ComponentsTestDir());
        psi.ArgumentList.Add("--config");
        psi.ArgumentList.Add(Path.Combine(ComponentsTestDir(), "config.yaml"));
        psi.ArgumentList.Add("--");
        psi.ArgumentList.Add(Dotnet);
        psi.ArgumentList.Add(Path.Combine(binDir, "StreamsForge.Dapr.Host.dll"));
        psi.ArgumentList.Add("--Http:Port");
        psi.ArgumentList.Add(HttpPort.ToString());
        psi.ArgumentList.Add("--Grpc:Port");
        psi.ArgumentList.Add(GrpcPort.ToString());
        psi.ArgumentList.Add("--DataDir");
        psi.ArgumentList.Add(DataDir);
        foreach (var arg in _extraArgs)
        {
            psi.ArgumentList.Add(arg);
        }
        foreach (var (name, value) in EnvVars)
        {
            psi.Environment[name] = value;
        }

        _process = Process.Start(psi)
            ?? throw new InvalidOperationException($"failed to start Dapr-wrapped StreamsForge host '{_label}'");
        _process.OutputDataReceived += (_, e) => AppendLog(e.Data);
        _process.ErrorDataReceived += (_, e) => AppendLog(e.Data);
        _process.BeginOutputReadLine();
        _process.BeginErrorReadLine();
    }

    /// <summary>A fresh, unauthenticated <see cref="HttpClient"/> pointed at this instance. Caller
    /// disposes. Identical shape to <c>HostProcess.NewClient</c>, including the TLS thumbprint-pinning
    /// path — see the class doc's TLS paragraph for why that path is currently unreachable in practice.</summary>
    public HttpClient NewClient(TimeSpan? timeout = null)
    {
        HttpClient http;
        if (TlsCertPath is null)
        {
            http = new HttpClient();
        }
        else
        {
            var expected = _certThumbprint ??= LoadThumbprint(TlsCertPath);
            var handler = new HttpClientHandler
            {
                ServerCertificateCustomValidationCallback = (_, cert, _, _) =>
                    cert is not null
                    && string.Equals(cert.Thumbprint, expected, StringComparison.OrdinalIgnoreCase),
            };
            http = new HttpClient(handler, disposeHandler: true);
        }
        http.BaseAddress = new Uri(BaseUrl);
        http.Timeout = timeout ?? TimeSpan.FromSeconds(30);
        return http;
    }

    private static string LoadThumbprint(string pemPath)
    {
        using var cert = X509CertificateLoader.LoadCertificateFromFile(pemPath);
        return cert.Thumbprint;
    }

    public bool HasExited => _process is null || _process.HasExited;

    /// <summary>Polls <c>/api/healthz</c> until it answers, THEN polls <c>/api/meta/instance</c> until
    /// its <c>catalogCounts.sources</c> is non-zero — the sidecar coming up and the app port opening is
    /// only half of "ready": <c>CatalogInitializationService</c>'s seed write happens after that, through
    /// an actor call that (per the class doc's "Actor placement" paragraph) genuinely can take a few
    /// seconds while the dedicated placement service disseminates this app's actor types for the first
    /// time. 120s default — generous because sidecar + actor placement dissemination is the slowest
    /// startup step this fixture has, not because it is usually needed (observed live: healthy within a
    /// few seconds on this machine).</summary>
    public async Task WaitHealthyAsync(TimeSpan? timeout = null)
    {
        using var http = NewClient(TimeSpan.FromSeconds(3));
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(120));
        Exception? lastError = null;
        var healthy = false;
        while (DateTime.UtcNow < deadline)
        {
            if (_process!.HasExited)
            {
                throw new InvalidOperationException(
                    $"host '{_label}' exited early (code {_process.ExitCode}):\n{Tail(LogTailText(), 6000)}");
            }
            try
            {
                if (!healthy)
                {
                    var resp = await http.GetAsync($"{BaseUrl}/api/healthz");
                    if (resp.IsSuccessStatusCode)
                    {
                        healthy = true;
                    }
                }
                else
                {
                    var meta = await http.GetFromJsonAsync<MetaInstanceDto>($"{BaseUrl}/api/meta/instance");
                    if (meta?.CatalogCounts?.Sources > 0)
                    {
                        return;
                    }
                }
            }
            catch (Exception ex)
            {
                lastError = ex;
            }
            await Task.Delay(500);
        }
        throw new InvalidOperationException(
            $"host '{_label}' did not become healthy-and-seeded within "
          + $"{(timeout ?? TimeSpan.FromSeconds(120)).TotalSeconds:0}s (healthz reached: {healthy}, last error: "
          + $"{lastError})\n{Tail(LogTailText(), 6000)}");
    }

    public async Task<HttpClient> LoginAsync(string username = AdminUser, string password = AdminPassword)
    {
        var http = NewClient();
        try
        {
            var resp = await http.PostAsJsonAsync("api/auth/login", new { username, password });
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

    /// <summary>Kills this instance's process tree. Asks the `dapr` CLI to stop the app-id FIRST — see
    /// the class doc's paragraph on why: `dapr run` is a supervisor process in front of the sidecar and
    /// the actual app, and killing only the .NET <see cref="Process"/> handle would orphan the sidecar.
    /// Leaves <see cref="DataDir"/> AND Redis database <see cref="TestRedisDb"/> exactly as they were —
    /// this is what makes <see cref="RestartAsync"/> a real restart rather than a fresh instance.</summary>
    public async Task StopAsync()
    {
        var daprCli = ResolveDaprCli();
        if (daprCli is not null)
        {
            try
            {
                var stopPsi = new ProcessStartInfo(daprCli)
                {
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    UseShellExecute = false,
                };
                stopPsi.ArgumentList.Add("stop");
                stopPsi.ArgumentList.Add("--app-id");
                stopPsi.ArgumentList.Add(AppId);
                using var stop = Process.Start(stopPsi);
                if (stop is not null)
                {
                    await stop.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(15));
                }
            }
            catch
            {
                // best-effort — the process-tree kill below is the fallback
            }
        }

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
        // ports races that. Poll rather than sleep a fixed amount — same technique as HostProcess.
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(20);
        var ports = new[] { HttpPort, GrpcPort, SidecarHttpPort, SidecarGrpcPort };
        while (DateTime.UtcNow < deadline && ports.Any(p => !PortFree(p)))
        {
            await Task.Delay(200);
        }
    }

    /// <summary>Stops and restarts this SAME instance (same app-id, same <see cref="DataDir"/>, Redis
    /// database <see cref="TestRedisDb"/> untouched) and waits for it to be healthy-and-seeded again.
    /// The convenience `RestartResumeTests`/`EnvironmentIsolationTests` use — equivalent to
    /// <c>await StopAsync(); Start(); await WaitHealthyAsync();</c> spelled out, which is exactly what it
    /// does.</summary>
    public async Task RestartAsync(TimeSpan? healthyTimeout = null)
    {
        await StopAsync();
        Start();
        await WaitHealthyAsync(healthyTimeout);
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

    /// <summary>Stops the process and deletes <see cref="DataDir"/>. Deliberately does NOT flush Redis
    /// database <see cref="TestRedisDb"/> — the next scenario's own <see cref="ResetAsync"/> (called
    /// before ITS <see cref="Start"/>) does that; leaving a failed run's data in place until the next
    /// reset is a feature, not an oversight, mirroring `HostProcess`'s identical choice to leave
    /// `DataDir` behind on `StopAsync` and delete it only here.</summary>
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

    private sealed record MetaInstanceDto(CatalogCountsDto? CatalogCounts);

    private sealed record CatalogCountsDto(int Sources, int Pipelines, int Tables);
}

/// <summary>
/// One xunit collection for every live test class in this project, so instances never overlap — every
/// class in this project uses the SAME fixed app-id and ports (see <see cref="DaprHostProcess"/>'s
/// "Fixed identity" paragraph), so two classes' instances running at once would collide on both the OS
/// ports and the Redis-database-1 actor namespace, not just slow each other down. Identical role to
/// <c>orleans/tests/StreamsForge.Chain.Tests</c>'s <c>ChainHostCollection</c>.
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class DaprLiveTestCollection
{
    public const string Name = "StreamsForge Dapr live host";
}
