using System.Diagnostics;
using Xunit;

namespace StreamForge.Connectors.Database.Tests.Integration;

/// <summary>
/// Plan 017 wave G: <see cref="DbServers"/>'s CDC-specific twin — starts (or adopts) the two
/// <see cref="CdcDbBackends"/> containers for the whole assembly and removes only what IT started, on both
/// the pass path and the fail path. The discipline is <see cref="DbServers"/>'s, copied rather than shared:
/// <see cref="DbServers"/> is sealed and hard-wired to <see cref="DbBackends.All"/> — plan 014 wave M's
/// file, out of this wave's ownership — and generalizing it to take a caller-supplied backend list would be
/// a bigger, riskier edit to someone else's file than one more small fixture is.
///
/// <para>A separate xunit collection (<see cref="CollectionName"/>), so the CDC containers and the
/// wave-M polled-kind containers start, wait and tear down independently — a CDC test is never blocked on,
/// or blocking, a polled-kind test's container lifecycle.</para>
///
/// <para>Same adoption rule as <see cref="DbServers"/>: a container this fixture finds already running is
/// left alone (two concurrent <c>dotnet test</c> runs in this repo are an ordinary Tuesday — see
/// <see cref="DbServers"/>'s own class doc for why); a container it STARTS is the one it removes.</para>
/// </summary>
public sealed class CdcServers : IAsyncLifetime
{
    /// <summary>Every live CDC test class joins this collection, which serialises them against the same two
    /// containers — the containers are the expensive part, not the tests.</summary>
    public const string CollectionName = "live CDC database servers";

    private static readonly TimeSpan DockerTimeout = TimeSpan.FromMinutes(3);

    private readonly List<DbBackend> _started = [];
    private readonly List<DbBackend> _created = [];

    /// <summary>Backends whose CDC container is up and answering. A test is only ever handed one it asked
    /// for by attribute, so this is a diagnostic, not a gate.</summary>
    public IReadOnlyList<DbBackend> Running => _started;

    public async Task InitializeAsync()
    {
        foreach (var backend in CdcDbBackends.All.Where(b => DockerGate.SkipReason(b) is null))
        {
            if (Start(backend))
            {
                _created.Add(backend);
            }

            _started.Add(backend);
        }

        // Started first, waited on second — SQL Server's emulated boot overlaps PostgreSQL's fast one
        // instead of following it, same reasoning as DbServers.InitializeAsync.
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
            $"could not start the {backend.Name} CDC container: {output}\n" +
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
                // Every shape of "not yet": connection refused, "the database system is starting up", SQL
                // Server's login timeout. None of them is worth distinguishing while the budget holds.
                last = ex;
                await Task.Delay(TimeSpan.FromMilliseconds(500)).ConfigureAwait(false);
            }
        }

        var (_, log) = DockerGate.Docker(DockerTimeout, "logs", "--tail", "40", backend.ContainerName);
        throw new TimeoutException(
            $"the {backend.Name} CDC container did not accept a query within {backend.StartupBudget.TotalSeconds:0}s. " +
            $"Last error: {last?.Message}\n--- docker logs ---\n{log}");
    }
}

/// <summary>Binds <see cref="CdcServers"/> to the collection every live CDC test class declares.</summary>
[CollectionDefinition(CdcServers.CollectionName)]
public sealed class CdcServersCollection : ICollectionFixture<CdcServers>;
