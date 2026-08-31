using System.Diagnostics;
using Xunit;

namespace StreamsForge.Connectors.Database.Tests.Integration;

/// <summary>
/// Plan 014 wave M: the one thing that decides whether the live database tests RUN or SKIP, and the
/// reason they must never do a third thing.
///
/// <para><b>The hard requirement this class exists to satisfy:</b> <c>dotnet test
/// orleans/StreamsForge.sln</c> has to come back all-green on a machine with no Docker daemon, no images
/// and no network. An integration test that cannot reach a server must therefore skip — not fail, and
/// not hang waiting for a connection that will never open.</para>
///
/// <para><b>Why an overridden <c>Skip</c> on a <see cref="FactAttribute"/> subclass and not a package.</b>
/// xunit 2.x has no dynamic skip (<c>Assert.Skip</c> arrived in v3), and the usual answer —
/// <c>Xunit.SkippableFact</c> — is a new PackageReference in a csproj this wave does not own. xunit's own
/// <c>FactAttribute.Skip</c> is <c>virtual</c> and is read through <c>ReflectionAttributeInfo</c>, i.e. by
/// invoking the property getter on a real attribute instance at DISCOVERY time. Overriding the getter is
/// therefore a supported, dependency-free dynamic skip, and it is what <see cref="PostgresFactAttribute"/>
/// and <see cref="MsSqlFactAttribute"/> do.</para>
///
/// <para><b>A skip always says why.</b> Every reason below names the missing prerequisite and, where one
/// exists, the command that supplies it — so "0 integration tests ran" can never be mistaken for
/// "integration passed". Run <c>dotnet test --logger "console;verbosity=detailed"</c> to see them; the
/// reasons are the skip messages xunit prints next to each skipped test.</para>
///
/// <para><b>What is probed, in order, and why each one is cheap.</b> An opt-out environment variable, then
/// <c>docker version</c> (does a daemon answer at all), then <c>docker image inspect</c> PER BACKEND (is
/// THIS image already local). The per-image check is what keeps a machine that pulled only
/// <c>postgres:17</c> from losing its Postgres coverage to a missing SQL Server image, and it is also what
/// makes "Docker is installed but there is no network to pull with" a clean skip rather than a five-minute
/// pull attempt inside a test. Nothing here starts a container — that is <see cref="DbServers"/>'s job,
/// and it happens after discovery.</para>
///
/// <para><b>Every probe is bounded.</b> A hung <c>docker</c> CLI is killed at
/// <see cref="ProbeTimeout"/> and reported as unavailable, because a test run that never returns is worse
/// than one that skips.</para>
/// </summary>
public static class DockerGate
{
    /// <summary>Set to <c>0</c>, <c>off</c>, <c>false</c> or <c>no</c> to skip every live database test
    /// even where Docker is perfectly healthy — for a laptop on battery, or a CI leg that has already run
    /// them. Anything else (including unset) leaves the decision to the probes below.</summary>
    public const string OptOutVariable = "SF_DB_INTEGRATION";

    /// <summary>Ceiling on ANY <c>docker</c> CLI invocation made from this class. Generous for a local
    /// daemon, and short enough that a wedged one costs a few seconds rather than the run.</summary>
    public static readonly TimeSpan ProbeTimeout = TimeSpan.FromSeconds(20);

    private static readonly Lazy<string?> DaemonProbe = new(ProbeDaemon, LazyThreadSafetyMode.ExecutionAndPublication);
    private static readonly Lock ImageGate = new();
    private static readonly Dictionary<string, string?> ImageProbes = new(StringComparer.Ordinal);

    /// <summary>Null when the tests for <paramref name="backend"/> can run; otherwise the reason they are
    /// being skipped, phrased for someone reading a test log who did not write this file.</summary>
    public static string? SkipReason(DbBackend backend)
    {
        ArgumentNullException.ThrowIfNull(backend);

        var optOut = Environment.GetEnvironmentVariable(OptOutVariable);
        if (optOut is not null && optOut.Trim() is "0" or "off" or "false" or "no")
        {
            return $"live {backend.Name} tests disabled by {OptOutVariable}={optOut}";
        }

        if (DaemonProbe.Value is { } daemon)
        {
            return $"live {backend.Name} tests need Docker: {daemon}";
        }

        return ImageProbe(backend.Image) is { } image ? $"live {backend.Name} tests need Docker: {image}" : null;
    }

    /// <summary>Runs <c>docker</c> with <paramref name="arguments"/> and returns its exit code plus its
    /// combined output. Shared with <see cref="DbServers"/> so container start-up and teardown are bounded
    /// by the same rule the probes are: nothing waits forever on the CLI.</summary>
    public static (int ExitCode, string Output) Docker(TimeSpan timeout, params string[] arguments)
    {
        ArgumentNullException.ThrowIfNull(arguments);

        ProcessStartInfo info = new()
        {
            FileName = "docker",
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        foreach (var argument in arguments)
        {
            info.ArgumentList.Add(argument);
        }

        try
        {
            using var process = Process.Start(info)
                ?? throw new InvalidOperationException("docker did not start");

            // Read both streams to completion BEFORE waiting: a child that fills a redirected pipe blocks
            // forever, which is the classic way a "bounded" probe stops being bounded.
            var stdout = process.StandardOutput.ReadToEndAsync();
            var stderr = process.StandardError.ReadToEndAsync();
            if (!process.WaitForExit((int)timeout.TotalMilliseconds))
            {
                try
                {
                    process.Kill(entireProcessTree: true);
                }
                catch (InvalidOperationException)
                {
                    // Exited between the check and the kill. Nothing to do.
                }

                return (-1, $"'docker {string.Join(' ', arguments)}' did not return within {timeout.TotalSeconds:0}s");
            }

            return (process.ExitCode, string.Join('\n', stdout.Result.Trim(), stderr.Result.Trim()).Trim());
        }
        catch (Exception ex) when (ex is System.ComponentModel.Win32Exception or InvalidOperationException)
        {
            // No docker on PATH at all — the "no Docker daemon, no images, no network" machine.
            return (-1, $"docker could not be launched: {ex.Message}");
        }
    }

    /// <summary>Asked twice before giving up. Not superstition: a Docker Desktop daemon that has just
    /// finished a large pull answers <c>docker version</c> with an error for a second or two, and because
    /// this probe's answer is cached for the whole process, one unlucky moment would silently skip every
    /// live test in the run — the exact "0 tests ran, therefore green" outcome this gate exists to make
    /// impossible.</summary>
    private static string? ProbeDaemon()
    {
        for (var attempt = 0; ; attempt++)
        {
            var (exit, output) = Docker(ProbeTimeout, "version", "--format", "{{.Server.Version}}");
            if (exit == 0)
            {
                return null;
            }

            if (attempt == 1)
            {
                return $"docker is unavailable ({Head(output)})";
            }

            Thread.Sleep(TimeSpan.FromSeconds(2));
        }
    }

    private static string? ImageProbe(string image)
    {
        lock (ImageGate)
        {
            if (ImageProbes.TryGetValue(image, out var cached))
            {
                return cached;
            }

            var (exit, output) = Docker(ProbeTimeout, "image", "inspect", image, "--format", "{{.Id}}");
            var reason = exit == 0
                ? null
                : $"image '{image}' is not present locally, and this is not the place to pull one — run 'docker pull {image}' ({Head(output)})";
            ImageProbes[image] = reason;
            return reason;
        }
    }

    private static string Head(string output)
    {
        var line = output.Split('\n', StringSplitOptions.RemoveEmptyEntries).FirstOrDefault()?.Trim() ?? "no output";
        return line.Length <= 160 ? line : line[..160];
    }
}

/// <summary>A <c>[Fact]</c> that skips itself, with a reason, unless a live PostgreSQL container can be
/// run — see <see cref="DockerGate"/>.</summary>
[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1813:Avoid unsealed attributes", Justification = "sealed")]
public sealed class PostgresFactAttribute : FactAttribute
{
    public override string? Skip
    {
        get => DockerGate.SkipReason(DbBackends.Postgres) ?? base.Skip;
        set => base.Skip = value;
    }
}

/// <summary>A <c>[Fact]</c> that skips itself, with a reason, unless a live SQL Server container can be
/// run — see <see cref="DockerGate"/>.</summary>
public sealed class MsSqlFactAttribute : FactAttribute
{
    public override string? Skip
    {
        get => DockerGate.SkipReason(DbBackends.SqlServer) ?? base.Skip;
        set => base.Skip = value;
    }
}
