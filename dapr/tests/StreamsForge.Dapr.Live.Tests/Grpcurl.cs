using System.Diagnostics;

namespace StreamsForge.Dapr.Live.Tests;

/// <summary>
/// A thin shell-out to the <c>grpcurl</c> binary, used by <see cref="GrpcServingTests"/> and by
/// <see cref="TlsHostTests"/>'s gRPC-over-TLS fact.
///
/// <para><b>Why an external binary rather than a generated C# client.</b> What these tests are about is
/// the SERVER's reflection surface — "what does an outside tool, holding nothing but the address,
/// discover and successfully call here?". A generated client compiled from this repo's own
/// <c>Protos/streamsforge.proto</c> would answer a different, weaker question (it already knows the
/// service names and message shapes, so it would still pass against a host whose reflection service was
/// broken or absent). grpcurl learns everything over the wire, which is exactly the property
/// <c>DynamicReflectionService</c> exists to provide. It is also the tool the docs tell an operator to
/// use, so this keeps the documented command honest.</para>
///
/// <para>The cost is a machine dependency, handled the way every other optional dependency in this repo
/// is: <see cref="Preflight"/> returns a one-line reason and the caller turns it into an explicit
/// "skipped: …" — never a silent pass.</para>
///
/// <para><b>A streaming call's exit code is not a verdict.</b> <c>grpcurl -max-time N</c> on a
/// server-streaming RPC that legitimately never ends (<c>StreamService/SubscribeTable</c> is a live
/// subscription) exits NON-ZERO with <c>DeadlineExceeded</c> after printing every message it received.
/// So a caller asserting "at least one delta arrived" must read <see cref="Result.Stdout"/>, not
/// <see cref="Result.ExitCode"/> — and a caller asserting a UNARY call succeeded (e.g. <c>list</c>)
/// checks the exit code as usual.</para>
/// </summary>
public static class Grpcurl
{
    /// <summary>Resolves the binary: PATH first, then the fixed Homebrew location AGENTS.md records for
    /// this machine, since a spawned test process's PATH does not always match an interactive shell's.
    /// Same two-step as <c>DaprHostProcess</c>'s own `dapr` CLI lookup.</summary>
    public static string? ResolveBinary()
    {
        var fromPath = Environment.GetEnvironmentVariable("PATH")?
            .Split(Path.PathSeparator)
            .Select(dir => Path.Combine(dir, "grpcurl"))
            .FirstOrDefault(File.Exists);
        if (fromPath is not null)
        {
            return fromPath;
        }
        const string homebrew = "/opt/homebrew/bin/grpcurl";
        return File.Exists(homebrew) ? homebrew : null;
    }

    /// <summary>Null when grpcurl is available; otherwise the one-line skip reason.</summary>
    public static string? Preflight() =>
        ResolveBinary() is null
            ? "grpcurl not found on PATH or at /opt/homebrew/bin/grpcurl — cannot probe the gRPC surface "
            + "from outside the process (brew install grpcurl)"
            : null;

    public sealed record Result(int ExitCode, string Stdout, string Stderr)
    {
        /// <summary>Both streams, for a failure message — grpcurl puts protocol errors on stderr and
        /// message payloads on stdout, and a reader needs both to tell "refused" from "empty".</summary>
        public string Combined => $"--- exit {ExitCode} ---\n--- stdout ---\n{Stdout}\n--- stderr ---\n{Stderr}";
    }

    public static async Task<Result> RunAsync(TimeSpan timeout, params string[] args)
    {
        var binary = ResolveBinary()
            ?? throw new InvalidOperationException("grpcurl not found — call Preflight() before RunAsync()");

        var psi = new ProcessStartInfo(binary)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        foreach (var arg in args)
        {
            psi.ArgumentList.Add(arg);
        }

        using var proc = Process.Start(psi)
            ?? throw new InvalidOperationException($"failed to start {binary}");
        // Read both pipes concurrently BEFORE waiting: a streaming subscription can outrun a 64 KB OS
        // pipe buffer, and a child blocked writing into a full pipe would never reach its own -max-time.
        var stdoutTask = proc.StandardOutput.ReadToEndAsync();
        var stderrTask = proc.StandardError.ReadToEndAsync();
        try
        {
            await proc.WaitForExitAsync().WaitAsync(timeout);
        }
        catch (TimeoutException)
        {
            // grpcurl's own -max-time should have ended it; this is the belt-and-braces path so a wedged
            // child can never hang the whole test run.
            try
            {
                proc.Kill(entireProcessTree: true);
            }
            catch
            {
                // best-effort
            }
        }

        return new Result(
            proc.HasExited ? proc.ExitCode : -1,
            await stdoutTask,
            await stderrTask);
    }
}
