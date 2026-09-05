using Dapr.Actors;
using Dapr.Actors.Client;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Environments;
using StreamsForge.Dapr.Host.Actors;
using StreamsForge.Dapr.Host.Facades;
using StreamsForge.Dapr.Host.Lifecycle;

namespace StreamsForge.Dapr.Host.Services;

/// <summary>
/// Plan 005 (Dapr sibling runtime) W5-A: Dapr counterpart of the Orleans host's
/// <c>GeneratorSupervisorService</c> (orleans/src/StreamsForge.Host/Services/GeneratorSupervisorService.cs)
/// — every ~15s, lists sources via <see cref="ICatalogFacade"/> and ensures every enabled one's
/// <see cref="GeneratorActor"/> has a live timer, by calling <c>StartAsync</c> with its current
/// definition (idempotent — see that method's own doc comment).
///
/// <para><b>Why this is still needed even though GeneratorActor self-heals</b> (see that class's own
/// class doc): self-healing only fires when an actor that was PREVIOUSLY started gets reactivated. It
/// does nothing for a source that has never been started at all — e.g. one enabled directly in a fresh
/// seed, or one whose very first <c>StartAsync</c> call (via
/// <see cref="Lifecycle.DaprLifecycleOrchestrator"/> from <c>CatalogStore.UpsertSourceAsync</c>) happened
/// to fail transiently (sidecar hiccup). This sweep is that safety net, exactly mirroring the Orleans
/// side's own doc comment ("keep them activated (or reactivate them if evicted)").</para>
///
/// <para><b>Sidecar readiness:</b> actor proxy calls (both <see cref="ICatalogFacade"/>, which is itself
/// backed by an actor proxy on this flavor, and the per-source <see cref="IGeneratorActor"/> proxies
/// created below) fail until the Dapr sidecar is up. Rather than a bespoke retry-with-backoff loop, this
/// mirrors <c>CatalogInitializationService</c>'s own best-effort philosophy (try, log on failure, don't
/// crash the host) — the natural ~15s sweep period already IS the backoff: a sweep that fails because the
/// sidecar isn't ready yet simply tries again next tick.</para>
///
/// <para><b>Plan 025 (PARITY.md D6 bullet 2) — this sweep is no longer part of the boot resume, and it
/// waits for the one that is.</b> Its first pass used to be a boot resume in its own right, racing the
/// other three sweeps: nothing coordinated "start the consumers before the producers", so this sweep could
/// start a `url` source whose table had not re-registered its router yet, and with a dedup key configured
/// those rows never came round again. <see cref="CatalogInitializationService"/> now owns one ordered pass
/// and this loop awaits <see cref="BootGate"/> before its FIRST tick — the Dapr shape of Orleans'
/// <c>GeneratorSupervisorService</c> awaiting <c>RegistryGrain.EnsureInitializedAsync</c>. The wait is
/// bounded (see <see cref="BootGate.DefaultWaitTimeout"/>): a boot pass that wedges must degrade this back
/// to its old uncoordinated behaviour, never disable self-healing outright. Everything from the second
/// tick on is unchanged — this is still the safety net for a source enabled after boot, or one whose actor
/// was evicted, which is what its "Why this is still needed" paragraph above describes.</para>
///
/// <para><b>Plan 006 (ingestion connectors) W3-B addendum — connector-kind sources join the same sweep,
/// but with a not-running GUARD the generator branch doesn't need.</b> <see cref="IGeneratorActor.StartAsync"/>
/// is safe to call unconditionally every ~15s (it just re-arms a fixed 200ms cadence timer — see that
/// class's own doc comment), but <see cref="IConnectorActor.StartAsync"/> is a genuine "fresh start" that
/// resets the failure streak and reschedules from "now" (see <see cref="ConnectorActor.StartAsync"/>'s doc
/// comment) — calling it unconditionally on a source whose sweep-15s period is SHORTER than its own poll
/// schedule would perpetually reset/restart the connector before its own timer ever gets to fire,
/// defeating both its schedule and any in-progress backoff. So this sweep first checks
/// <see cref="IConnectorActor.IsRunningAsync"/> and only calls <c>StartAsync</c> when it answers false —
/// exactly the "Start enabled connector-kind sources whose actor isn't running" the plan calls for. Which
/// sources are eligible for this connector branch at all (enabled AND non-generator-kind) is the pure,
/// separately unit-tested <see cref="ConnectorSourceSweep.SelectConnectorSources"/> — see
/// dapr/tests/StreamsForge.Dapr.Tests/ConnectorSupervisorSweepTests.cs.</para>
/// </summary>
public sealed class GeneratorSupervisorService(
    ICatalogFacadeFactory catalogFactory,
    IEnvironmentFacade environments,
    IHostApplicationLifetime lifetime,
    ILogger<GeneratorSupervisorService> logger) : BackgroundService
{
    // Plan 021 D5: this is a background sweep, not a request — EnvironmentAmbient.Current is empty here
    // (D4/D5), and empty would silently mean "only the default environment ever gets resumed". So it
    // iterates EVERY environment's catalog via ICatalogFacadeFactory, keyed off IEnvironmentFacade.
    // ListAsync — the ONLY correct choice for a boot-resume safety net whose whole job is "every enabled
    // source everywhere keeps a live timer", not just the default environment's.
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await WaitForApplicationStartedAsync(stoppingToken);
        await BootGateWait.AwaitBootPassAsync(logger, nameof(GeneratorSupervisorService), stoppingToken);

        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(15));
        do
        {
            try
            {
                foreach (var env in await environments.ListAsync())
                {
                    var environment = EnvKeys.Normalize(env.Name);
                    var catalog = catalogFactory.For(environment);
                    var sources = await catalog.GetSourcesAsync();

                    // Plan 025: both branches go through EntityResume.EnsureSourceRunningAsync — the SAME
                    // call CatalogInitializationService's boot pass makes — rather than two copies of the
                    // generator/connector dispatch. It carries the "generators unconditionally, connectors
                    // only when not already running" rule this class's own doc paragraph describes.
                    foreach (var src in sources.Where(s => s.Enabled && SourceKindDispatch.Classify(s.Kind) == SourceKindDispatch.ActorKind.Generator)
                                 .Concat(ConnectorSourceSweep.SelectConnectorSources(sources)))
                    {
                        try
                        {
                            await EntityResume.EnsureSourceRunningAsync(src);
                        }
                        catch (Exception ex)
                        {
                            // Best-effort per source, mirroring the Orleans supervisor's per-grain try/catch —
                            // one misbehaving source must never stop the sweep from reaching the rest.
                            logger.LogDebug(ex,
                                "GeneratorSupervisorService: failed to (re)start source '{Source}' in environment '{Environment}' — will retry next sweep.",
                                src.Name, environment);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                logger.LogDebug(ex,
                    "GeneratorSupervisorService: sweep failed (Dapr sidecar likely not ready yet) — will retry next tick.");
            }
        } while (await timer.WaitForNextTickAsync(stoppingToken));
    }

    /// <summary>Small inline equivalent of the Orleans host's internal <c>StartupSignal</c> helper
    /// (orleans/src/StreamsForge.Host/Services/StartupSignal.cs) — that type is internal to a different
    /// assembly (StreamsForge.Host), so it isn't reachable from here; this is the same handful of lines
    /// rather than a shared abstraction for one four-line helper.</summary>
    private async Task WaitForApplicationStartedAsync(CancellationToken ct)
    {
        if (lifetime.ApplicationStarted.IsCancellationRequested)
        {
            return;
        }

        var tcs = new TaskCompletionSource();
        await using var registration = lifetime.ApplicationStarted.Register(() => tcs.TrySetResult());
        await tcs.Task.WaitAsync(ct);
    }
}

/// <summary>
/// Plan 006 (ingestion connectors) W3-B: the pure "which sources does the connector sweep branch even
/// consider" filter behind <see cref="GeneratorSupervisorService"/>'s connector loop — extracted so it is
/// unit-testable without any actor/Dapr-sidecar machinery (the actual <c>IsRunningAsync</c>/
/// <c>StartAsync</c> calls per eligible source are inherently I/O and stay in the service itself, exactly
/// as <see cref="ConnectorBookkeeping"/> and <see cref="SourceKindDispatch"/> keep their own pure slices
/// separate from their actor-bound callers). See
/// dapr/tests/StreamsForge.Dapr.Tests/ConnectorSupervisorSweepTests.cs.
/// </summary>
public static class ConnectorSourceSweep
{
    /// <summary>Enabled, non-generator-kind sources — the set <see cref="GeneratorSupervisorService"/>'s
    /// connector branch checks <see cref="IConnectorActor.IsRunningAsync"/> for and conditionally
    /// <see cref="IConnectorActor.StartAsync"/>s.</summary>
    public static IEnumerable<SourceDefinition> SelectConnectorSources(IEnumerable<SourceDefinition> sources) =>
        sources.Where(s => s.Enabled && SourceKindDispatch.Classify(s.Kind) == SourceKindDispatch.ActorKind.Connector);
}
