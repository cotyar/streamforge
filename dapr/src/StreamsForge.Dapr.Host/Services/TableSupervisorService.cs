using Dapr.Actors;
using Dapr.Actors.Client;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Environments;
using StreamsForge.Dapr.Host.Actors;
using StreamsForge.Dapr.Host.Facades;
using StreamsForge.Dapr.Host.Streaming;

namespace StreamsForge.Dapr.Host.Services;

/// <summary>
/// Plan 005 (Dapr sibling runtime) W7-A: Dapr counterpart of <see cref="PipelineSupervisorService"/>'s own
/// "boot resume" role — every ~15s, lists tables via <see cref="ICatalogFacade"/> and ensures every
/// catalog-<c>Running</c> one's <see cref="TableActor"/> is actually processing, mirroring Orleans' own
/// one-shot <c>RegistryGrain.EnsureInitializedAsync</c> boot-resume loop but as a periodic sweep — same
/// rationale as <see cref="PipelineSupervisorService"/>'s own doc comment (actor-proxy calls fail until the
/// Dapr sidecar is up; the sweep period is the backoff).
///
/// <para><b>Not a blind "always restart" sweep — same discipline as <see cref="PipelineSupervisorService"/>.</b>
/// Restarting an already-running <see cref="TableActor"/> would discard its in-flight join/aggregate state
/// (see that class's restart-resume doc) for no reason, every ~15s, for a table that's simply been running
/// fine. So this sweep checks <see cref="ITableActor.IsRunningAsync"/> first, exactly like
/// <see cref="PipelineSupervisorService.EnsureRunningAsync"/>:
/// <list type="bullet">
/// <item>NOT running (first-ever start, a transient earlier failure, or — see the topo-order paragraph
/// below — a table-over-table dependency not yet satisfied) → go through
/// <see cref="ICatalogFacade.SetTableStatusAsync"/> (id, Running) — the exact same code path
/// <c>POST /api/tables/{id}/start</c> uses, so compiling, starting the actor, registering
/// <see cref="TableEventRouter"/>, and persisting Failed/Running/Error bookkeeping on failure all happen
/// identically whether triggered by a user or by this sweep.</item>
/// <item>Already running (self-healed via <see cref="TableActor.OnActivateAsync"/> on some earlier
/// reactivation this process never orchestrated) → only <see cref="TableEventRouter"/>'s in-memory routing
/// table needs repair (it does NOT survive a host restart the way the actor's persisted Dapr state does),
/// read cheaply via <see cref="ITableActor.GetInputNamesAsync"/> with no recompile.</item>
/// </list></para>
///
/// <para><b>Topo-order for table-over-table chains — moved to the boot pass in plan 025, not computed
/// here.</b> This paragraph used to say the flavor computed NO topological order anywhere, and leaned on
/// <c>Catalog.CatalogStore.SetTableStatusAsync</c>'s dependency guard
/// (<c>"table input(s) not running: ..."</c> → <c>Status=Failed</c>) plus a retry on the next ~15 s sweep to
/// converge. That is still exactly what happens HERE, and it is still the right trade for a periodic
/// self-healing sweep whose job description already includes "retry next tick" as the normal case (see the
/// catch block below). What changed is that cold start no longer relies on it:
/// <see cref="CatalogInitializationService"/>'s one-shot boot pass resumes tables in dependency order via
/// <see cref="BootResumePlan.TopoSortByTableInputs"/> — a straight port of the Orleans original — and this
/// sweep waits for that pass (<see cref="BootGate"/>) before its first tick, so a deep chain comes up in
/// one pass instead of over several sweep periods. A cycle is tolerated rather than diagnosed there; see
/// that method's own doc comment.</para>
///
/// <para><b>Plan 025 (PARITY.md D6 bullet 2), the rest of it:</b> both branches of this sweep's
/// per-table decision moved verbatim into <see cref="EntityResume.EnsureTableRunningAsync"/>, shared with
/// the boot pass so the "repair, never restart" discipline cannot drift between the two callers. The seed
/// catalog observation still holds: <c>SeedCatalog.Tables</c> has no Running table that itself depends on
/// another Running table — "hot_symbols" (the one table-over-table demo) is seeded Stopped precisely so
/// starting it after "positions" is a deliberate user action — so the chain path is exercised by
/// USER-created chains, not by the shipped seed.</para>
/// </summary>
public sealed class TableSupervisorService(
    ICatalogFacadeFactory catalogFactory,
    IEnvironmentFacade environments,
    TableEventRouter router,
    IHostApplicationLifetime lifetime,
    ILogger<TableSupervisorService> logger) : BackgroundService
{
    // Plan 021 D5: same reasoning as GeneratorSupervisorService/PipelineSupervisorService — a boot-resume
    // sweep must cover every environment, never just the (empty, here) ambient one.
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await WaitForApplicationStartedAsync(stoppingToken);
        await BootGateWait.AwaitBootPassAsync(logger, nameof(TableSupervisorService), stoppingToken);

        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(15));
        do
        {
            try
            {
                foreach (var env in await environments.ListAsync())
                {
                    var catalog = catalogFactory.For(EnvKeys.Normalize(env.Name));
                    var tables = await catalog.GetTablesAsync();
                    foreach (var table in tables.Where(t => t.Status == PipelineStatus.Running))
                    {
                        try
                        {
                            await EntityResume.EnsureTableRunningAsync(catalog, router, table);
                        }
                        catch (Exception ex)
                        {
                            // Best-effort per table, mirroring PipelineSupervisorService's own per-pipeline
                            // try/catch — one misbehaving/not-yet-startable table must never stop the sweep
                            // from reaching the rest, and simply retries next sweep (see class doc's
                            // topo-order paragraph).
                            logger.LogDebug(ex,
                                "TableSupervisorService: failed to (re)start/repair table '{TableName}' — will retry next sweep.",
                                table.Name);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                logger.LogDebug(ex,
                    "TableSupervisorService: sweep failed (Dapr sidecar likely not ready yet) — will retry next tick.");
            }
        } while (await timer.WaitForNextTickAsync(stoppingToken));
    }

    /// <summary>Small inline equivalent of the Orleans host's internal <c>StartupSignal</c> helper — see
    /// <see cref="PipelineSupervisorService"/>'s identical private method for the same rationale.</summary>
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
