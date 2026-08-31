using System.Diagnostics;
using Xunit;

namespace StreamsForge.Connectors.Database.Tests.Integration;

/// <summary>
/// Starts one PostgreSQL and one SQL Server container for the whole assembly and — this is the part that
/// matters — removes whatever it started again, on the way out of a green run and of a red one alike.
///
/// <para><b>Plain <c>docker run</c>, no Testcontainers.</b> Same discipline as
/// <c>tools/soak/run-soak.sh</c>: the harness that already exists in this repo starts what it needs with
/// the CLI, waits for a real health signal rather than for a sleep, and tears down unconditionally. A
/// container-orchestration NuGet package would be a dependency in a test project whose whole point is that
/// it proves the connector talks to a SERVER, not that it can talk to a library that talks to a server.</para>
///
/// <para><b>Readiness is a successful query, never <c>pg_isready</c>.</b> <c>pg_isready</c> reports success
/// while the PostgreSQL image is still running its own initialisation scripts and rejecting connections —
/// so gating on it produces a flaky suite whose first test fails roughly whenever the machine is busy. The
/// only signal that means "this server will answer the next test" is a query that answered.</para>
///
/// <para><b>Containers this fixture STARTED are removed on the way out, always</b> — green run, red run,
/// or a test that threw. A container it merely FOUND already running is adopted and left alone, which is
/// the difference between two concurrent runs coexisting and one of them yanking the server out from under
/// the other. That case is not hypothetical: this project is referenced by both solutions, and this repo is
/// worked on by several agents in one checkout, so "another <c>dotnet test</c> is already up" is an ordinary
/// Tuesday. A container that exists but is NOT running is a leftover from a killed process and is removed
/// and replaced, because a stopped container has nothing to adopt.</para>
///
/// <para><b>What concurrent runs still cost, stated rather than implied.</b> Adoption plus per-process
/// table names (<see cref="DbBackend.NewTable"/>) make two simultaneous runs correct while both are alive.
/// The one race left is the end: whichever run CREATED the container removes it when it finishes, and if
/// the other is still going its next query fails. Fixed container names and ports are still the right trade
/// — a per-process container would leak whenever a run is killed, costing a developer disk rather than a
/// retry — so the honest advice is the repo's own verification order, which is sequential (build+test
/// Orleans, then build+test Dapr). Set <c>SF_DB_INTEGRATION=0</c> on one leg if you must overlap them.</para>
///
/// <para><b>If a backend is gated out</b> — no daemon, no image, <c>SF_DB_INTEGRATION</c> off — its
/// container is never started, and every test that needed it has already skipped itself with a reason. On
/// a machine with no Docker at all this fixture does nothing whatsoever, which is what keeps
/// <c>dotnet test orleans/StreamsForge.sln</c> green there.</para>
/// </summary>
public sealed class DbServers : IAsyncLifetime
{
    /// <summary>Every live database test class joins this collection, which serialises them: two classes
    /// creating tables in one server in parallel is a race nobody needs, and the containers are the
    /// expensive part, not the tests.</summary>
    public const string CollectionName = "live database servers";

    private static readonly TimeSpan DockerTimeout = TimeSpan.FromMinutes(3);

    private readonly List<DbBackend> _started = [];
    private readonly List<DbBackend> _created = [];

    /// <summary>Backends whose container is up and answering. A test is only ever handed one it asked for
    /// by attribute, so this is a diagnostic, not a gate.</summary>
    public IReadOnlyList<DbBackend> Running => _started;

    public async Task InitializeAsync()
    {
        foreach (var backend in DbBackends.All.Where(b => DockerGate.SkipReason(b) is null))
        {
            if (Start(backend))
            {
                _created.Add(backend);
            }

            _started.Add(backend);
        }

        // Started first, waited on second: SQL Server's ~30s boot under emulation overlaps PostgreSQL's
        // ~2s instead of following it.
        foreach (var backend in _started)
        {
            await WaitAsync(backend).ConfigureAwait(false);
            await backend.PrepareAsync(CancellationToken.None).ConfigureAwait(false);
        }
    }

    /// <summary>Removes only what this fixture created — see the class doc on adoption.</summary>
    public Task DisposeAsync()
    {
        foreach (var backend in _created)
        {
            DockerGate.Docker(DockerTimeout, "rm", "-f", backend.ContainerName);
        }

        _created.Clear();
        _started.Clear();
        return Task.CompletedTask;
    }

    /// <summary>True when this call STARTED the container (and therefore owns removing it), false when it
    /// adopted one that was already running.</summary>
    private static bool Start(DbBackend backend)
    {
        if (IsRunning(backend))
        {
            return false;
        }

        // Not running, but it may exist stopped — a leftover from a process that was killed rather than
        // disposed. Failure here is expected and ignored.
        DockerGate.Docker(DockerTimeout, "rm", "-f", backend.ContainerName);

        List<string> arguments = ["run", "-d", "--rm", "--name", backend.ContainerName, .. backend.RunArguments];
        var (exit, output) = DockerGate.Docker(DockerTimeout, [.. arguments]);
        if (exit == 0)
        {
            return true;
        }

        // Lost a start race with another test process between the check above and here: its container is
        // up, so adopt that instead of failing on "name already in use" / "port is already allocated".
        if (IsRunning(backend))
        {
            return false;
        }

        throw new InvalidOperationException(
            $"could not start the {backend.Name} container: {output}\n" +
            $"(set {DockerGate.OptOutVariable}=0 to skip the live database tests entirely)");
    }

    private static bool IsRunning(DbBackend backend)
    {
        var (exit, output) = DockerGate.Docker(DockerTimeout, "inspect", "-f", "{{.State.Running}}", backend.ContainerName);
        return exit == 0 && output.Trim().StartsWith("true", StringComparison.Ordinal);
    }

    private static async Task WaitAsync(DbBackend backend)
    {
        var clock = Stopwatch.StartNew();
        Exception? last = null;
        while (clock.Elapsed < backend.StartupBudget)
        {
            try
            {
                await using var connection = await backend.OpenAsync(backend.AdminDatabase).ConfigureAwait(false);
                await using var command = connection.CreateCommand();
                command.CommandText = "SELECT 1";
                await command.ExecuteScalarAsync().ConfigureAwait(false);
                return;
            }
            catch (Exception ex)
            {
                // Every shape of "not yet": connection refused, "the database system is starting up",
                // SQL Server's login timeout. None of them is worth distinguishing while the budget holds.
                last = ex;
                await Task.Delay(TimeSpan.FromMilliseconds(500)).ConfigureAwait(false);
            }
        }

        var (_, log) = DockerGate.Docker(DockerTimeout, "logs", "--tail", "40", backend.ContainerName);
        throw new TimeoutException(
            $"the {backend.Name} container did not accept a query within {backend.StartupBudget.TotalSeconds:0}s. " +
            $"Last error: {last?.Message}\n--- docker logs ---\n{log}");
    }
}

/// <summary>Binds <see cref="DbServers"/> to the collection every live test class declares.</summary>
[CollectionDefinition(DbServers.CollectionName)]
public sealed class DbServersCollection : ICollectionFixture<DbServers>;
