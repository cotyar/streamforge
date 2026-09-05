using Xunit;

namespace StreamsForge.Dapr.Live.Tests;

/// <summary>
/// One booted, seeded <see cref="DaprHostProcess"/> shared by every <see cref="Fact"/> in ONE test class,
/// plus a scratch directory for that class's data files.
///
/// <para><b>Why this type exists at all: xunit constructs a new test-class instance per fact.</b> A class
/// that implements <see cref="IAsyncLifetime"/> DIRECTLY therefore runs <c>InitializeAsync</c> once per
/// fact, not once per class — so a five-fact class booted five Dapr instances, each with a sidecar, an
/// actor-placement round and a catalog seed. That is roughly 25 s of setup per fact, and it is also five
/// stop/start cycles onto the same four fixed ports (see <see cref="DaprHostProcess"/>'s "Fixed identity"
/// paragraph), each one a chance for the previous instance's teardown to lose its race with the next
/// one's bind. It cost this project a whole run: a TLS boot failed mid-sequence, its `dapr run` CLI
/// exited leaving the app orphaned on 5799, and the following NINETEEN tests failed with
/// "port 5799 is already in use" — a skip reason naming the wrong cause. (<c>StopAsync</c> now reaps such
/// an orphan, but not booting fifteen times is the real fix.)</para>
///
/// <para>An <see cref="IClassFixture{T}"/> is constructed once per class and disposed after its last
/// fact, which is the lifetime the facts' doc comments always claimed. Same role as
/// <c>orleans/tests/StreamsForge.Chain.Tests</c>'s <c>TlsHostFixture</c>/<c>TwoHostFixture</c>.</para>
///
/// <para><b>Wave 1's classes are deliberately not converted.</b> <c>SourceExactCountTests</c>,
/// <c>BootSmokeTests</c>, <c>RestartResumeTests</c>, <c>AccessAuditTests</c> and
/// <c>EnvironmentIsolationTests</c> still take the per-fact lifetime. They pass, and rewriting a passing
/// live test with no gate behind the rewrite is how a working test quietly stops testing what it did.
/// (<c>SourceExactCountTests</c>' class doc says "one host instance serves all three Facts", which is not
/// what xunit does with the lifetime it uses — worth correcting when someone touches that file for a
/// reason of its own.)</para>
///
/// <para><b>Failure convention.</b> A fixture that cannot come up records WHY in
/// <see cref="SkipReason"/> and every fact turns that into an explicit "skipped: …" assertion — plain
/// xunit v2 <c>[Fact]</c> cannot skip dynamically, and a silent pass is the one outcome a live test must
/// never produce. <see cref="Preflight"/>'s reason (a busy port, a missing container) is recorded the same
/// way, before anything is started.</para>
/// </summary>
public abstract class LiveHostFixture : IAsyncLifetime
{
    /// <summary>Short label for the temp directories and log lines this fixture's instance produces.</summary>
    protected abstract string Label { get; }

    public DaprHostProcess? Host { get; private set; }

    /// <summary>Null when the fixture is usable; otherwise the one-line reason every fact must report.</summary>
    public string? SkipReason { get; protected set; }

    /// <summary>A per-class temp directory for source files. Deleted on dispose.</summary>
    public string ScratchDir { get; private set; } = "";

    /// <summary>Extra preflight beyond <see cref="DaprHostProcess.Preflight"/> (a dev certificate, an
    /// Orleans peer binary, grpcurl). Returns null when satisfied.</summary>
    protected virtual string? ExtraPreflight() => null;

    /// <summary>Builds the instance. Overridden where the host needs arguments (TLS).</summary>
    protected virtual DaprHostProcess CreateHost() => new(Label);

    /// <summary>Runs after the Dapr host is healthy, for a fixture that needs more (the Orleans peer).</summary>
    protected virtual Task OnStartedAsync() => Task.CompletedTask;

    /// <summary>Runs before the Dapr host is disposed, for whatever <see cref="OnStartedAsync"/> started.</summary>
    protected virtual Task OnDisposingAsync() => Task.CompletedTask;

    public async Task InitializeAsync()
    {
        SkipReason = ExtraPreflight() ?? DaprHostProcess.Preflight();
        if (SkipReason is not null)
        {
            return;
        }

        ScratchDir = Directory.CreateTempSubdirectory($"sf-dapr-live-{Label}-").FullName;
        try
        {
            // Always BEFORE the first Start of a fresh scenario, never after — see
            // DaprHostProcess.ResetAsync.
            await DaprHostProcess.ResetAsync();
            Host = CreateHost();
            Host.Start();
            await Host.WaitHealthyAsync();
            await OnStartedAsync();
        }
        catch (Exception ex)
        {
            SkipReason = $"the '{Label}' live fixture did not come up cleanly: {ex.Message}";
            await DisposeAsync();
        }
    }

    public async Task DisposeAsync()
    {
        try
        {
            await OnDisposingAsync();
        }
        catch
        {
            // best-effort teardown
        }

        if (Host is not null)
        {
            await Host.DisposeAsync();
            Host = null;
        }

        if (ScratchDir.Length > 0)
        {
            try
            {
                Directory.Delete(ScratchDir, recursive: true);
            }
            catch
            {
                // best-effort cleanup
            }
        }
    }
}
