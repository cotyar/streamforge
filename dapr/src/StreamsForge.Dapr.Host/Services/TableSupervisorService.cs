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
/// <para><b>Topo-order for table-over-table chains — the simpler choice, documented:</b> Orleans'
/// <c>RegistryGrain</c>/<c>CatalogStore</c> resume tables in dependency order on boot so a table that reads
/// another table never starts before its upstream does. This sweep does NOT compute a topological order —
/// it simply iterates every catalog-Running table each pass and lets <c>Catalog.CatalogStore.
/// SetTableStatusAsync</c>'s own existing dependency guard (<c>"table input(s) not running: ..."</c> →
/// <c>Status=Failed</c>) do its job: a table whose upstream hasn't started yet fails THIS sweep's attempt,
/// then simply gets retried on the NEXT ~15s sweep — by which point an earlier iteration of the same (or a
/// prior) sweep has very likely already started its upstream. This converges to "every startable table is
/// Running" over a few sweep periods without any explicit graph analysis here, at the cost of a few extra
/// ~15s retries for a deep chain on cold start — an acceptable tradeoff for a periodic self-healing sweep
/// (not a one-shot boot gate) whose job description already includes "retry next tick" as the normal case
/// (see the catch block below). The current seed catalog (<c>SeedCatalog.Tables</c>) has no Running table
/// that itself depends on another Running table — "hot_symbols" (the one table-over-table demo) is seeded
/// Stopped precisely so starting it after "positions" is a deliberate user action, not an implicit boot
/// race — so this codepath is exercised by USER-created chains, not by the shipped seed.</para>
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
                            await EnsureRunningAsync(catalog, table);
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

    private async Task EnsureRunningAsync(ICatalogFacade catalog, TableDefinition table)
    {
        var qualifiedName = EnvKeys.Qualify(table.Environment, table.Name);
        var actor = ActorProxy.Create<ITableActor>(new ActorId(qualifiedName), nameof(TableActor), ActorProxyDefaults.Options);

        if (await actor.IsRunningAsync())
        {
            // Already self-healed (or continuously running) — repair the router only, never restart.
            // Plan 021 D6: GetInputNamesAsync returns BARE names — qualify with this table's own
            // environment before they go in the process-wide router index (same reasoning as
            // DaprLifecycleOrchestrator.StartTableAsync).
            var inputs = await actor.GetInputNamesAsync();
            router.Register(
                qualifiedName,
                inputs.StreamInputs.Select(s => EnvKeys.Qualify(table.Environment, s)).ToList(),
                inputs.TableInputs.Select(t => EnvKeys.Qualify(table.Environment, t)).ToList());
            return;
        }

        // Never started (fresh Redis state, a transient earlier failure, or an unmet table-over-table
        // dependency — see class doc's topo-order paragraph) — go through the full, user-equivalent start
        // path (compiles, starts the actor, registers the router, persists Failed/Running/Error on the
        // outcome).
        await catalog.SetTableStatusAsync(table.Id, PipelineStatus.Running);
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
