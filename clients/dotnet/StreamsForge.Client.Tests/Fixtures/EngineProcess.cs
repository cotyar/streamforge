using System.Diagnostics;
using System.Net.Sockets;

namespace StreamsForge.Client.Tests.Fixtures;

/// <summary>
/// "Publish an isolated StreamsForge.Host build, spawn it, wait for it to come up healthy, tear it
/// down" -- the machinery shared by <see cref="EngineFixture"/> (plaintext, 9199/9299) and
/// <see cref="TlsEngineFixture"/> (HTTPS + TLS gRPC, 7399/7499). Extracted here rather than
/// duplicated so the two fixtures' process lifecycle, log draining and publish-dir reuse
/// (<c>SF_TEST_PUBLISH_DIR</c>) can never drift apart.
/// </summary>
internal static class EngineProcess
{
    public static readonly string Dotnet =
        Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), ".dotnet", "dotnet");

    // xunit v2 runs different [Collection]s (EngineCollection, TlsEngineCollection, ...) in
    // parallel by default, and each fixture's InitializeAsync calls PublishAsync independently --
    // both target the SAME orleans/src/StreamsForge.Host source tree (only the -o output dir
    // differs, never -p:BaseIntermediateOutputPath), so two concurrent `dotnet publish`
    // invocations fight over the same obj/ directory's MSBuild locks. Observed cost: a run that
    // takes ~2.5 minutes serialized blew past the 5-minute per-publish timeout when two collided,
    // failing every test behind either fixture. Serializing here (rather than disabling xunit
    // parallelization, which would also serialize unrelated fast tests) fixes the collision at its
    // actual source with no effect on tests that never touch a fixture.
    private static readonly SemaphoreSlim PublishGate = new(1, 1);

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

    public static async Task PublishAsync(string projectDir, string publishDir)
    {
        await PublishGate.WaitAsync();
        try
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

            // A zero exit code is not sufficient on its own: observed live, under enough concurrent
            // build load on this machine, `dotnet publish` can report success while the output
            // directory ends up without StreamsForge.Host.dll (a corrupted/incomplete incremental
            // state under contention, most likely). Left unchecked, that surfaces two hops away and
            // confusingly -- StartHost's `dotnet <missing dll>` fails immediately, and
            // WaitHealthyAsync reports it as "engine process exited early" with the OS's generic
            // "could not execute" text, nothing pointing at the actual publish. Fail here instead,
            // at the one place that actually knows a publish just happened.
            var expectedDll = Path.Combine(publishDir, "StreamsForge.Host.dll");
            if (!File.Exists(expectedDll))
            {
                throw new InvalidOperationException(
                    $"dotnet publish exited 0 but {expectedDll} does not exist -- likely build-cache " +
                    $"corruption from concurrent publishes of the same source tree under heavy load:\n{Tail(stdout + stderr, 6000)}");
            }
        }
        finally
        {
            PublishGate.Release();
        }
    }

    /// <summary>Starts <c>StreamsForge.Host.dll</c> from <paramref name="publishDir"/> with
    /// <paramref name="args"/> appended after the DLL path, working directory set to
    /// <paramref name="publishDir"/> (content root: see <see cref="EngineFixture"/>'s original
    /// class doc for why that is not optional), and its stdout/stderr continuously drained into
    /// <paramref name="log"/> (an unread pipe fills and can kill the engine mid-suite).</summary>
    public static Process StartHost(string publishDir, IEnumerable<string> args, ProcessLogTail log)
    {
        var dll = Path.Combine(publishDir, "StreamsForge.Host.dll");
        var psi = new ProcessStartInfo(Dotnet)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            WorkingDirectory = publishDir,
        };
        psi.ArgumentList.Add(dll);
        foreach (var a in args) psi.ArgumentList.Add(a);

        var process = Process.Start(psi) ?? throw new InvalidOperationException("failed to start StreamsForge.Host");
        log.Attach(process);
        return process;
    }

    public static async Task WaitHealthyAsync(HttpClient http, string baseUrl, Process process, ProcessLogTail log, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        Exception? lastError = null;
        while (DateTime.UtcNow < deadline)
        {
            if (process.HasExited)
            {
                throw new InvalidOperationException(
                    $"engine process exited early (code {process.ExitCode}):\n{Tail(log.Text(), 6000)}");
            }
            try
            {
                var resp = await http.GetAsync($"{baseUrl}/api/healthz");
                if (resp.IsSuccessStatusCode) return;
            }
            catch (Exception ex)
            {
                lastError = ex;
            }
            await Task.Delay(500);
        }
        throw new InvalidOperationException(
            $"engine did not become healthy within {timeout} (last error: {lastError}):\n{Tail(log.Text(), 6000)}");
    }

    public static async Task KillAsync(Process? process)
    {
        if (process is { HasExited: false })
        {
            try
            {
                process.Kill(entireProcessTree: true);
                await process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(15));
            }
            catch { /* best-effort teardown */ }
        }
        process?.Dispose();
    }

    public static void TryDeleteDirectory(string? path)
    {
        if (path is null) return;
        try { Directory.Delete(path, recursive: true); } catch { /* best-effort cleanup */ }
    }

    public static string Tail(string s, int max) => s.Length <= max ? s : s[^max..];
}

/// <summary>Continuously drains a spawned process's stdout/stderr (see <see cref="EngineProcess.StartHost"/>'s
/// doc for why that is mandatory, not optional) and keeps a bounded tail for failure diagnostics.</summary>
internal sealed class ProcessLogTail
{
    private const int MaxLines = 400;
    private readonly object _lock = new();
    private readonly List<string> _lines = [];

    public void Attach(Process process)
    {
        process.OutputDataReceived += (_, e) => Append(e.Data);
        process.ErrorDataReceived += (_, e) => Append(e.Data);
        process.BeginOutputReadLine();
        process.BeginErrorReadLine();
    }

    private void Append(string? line)
    {
        if (line is null) return;
        lock (_lock)
        {
            _lines.Add(line);
            if (_lines.Count > MaxLines) _lines.RemoveAt(0);
        }
    }

    public string Text()
    {
        lock (_lock) return string.Join('\n', _lines);
    }
}
