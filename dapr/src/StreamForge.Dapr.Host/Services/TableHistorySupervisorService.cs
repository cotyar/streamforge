using Dapr.Actors;
using Dapr.Actors.Client;
using StreamForge.Abstractions;
using StreamForge.AppCore.Environments;
using StreamForge.Dapr.Host.Actors;
using StreamForge.Dapr.Host.Facades;
using StreamForge.Dapr.Host.Streaming;

namespace StreamForge.Dapr.Host.Services;

/// <summary>
/// Plan 005 (Dapr sibling runtime) W7-B: fixes a live-verification gap — <see cref="TableHistoryActor"/>
/// configuration previously only ever happened inline, via <see cref="Lifecycle.ILifecycleOrchestrator.ResetTableHistoryAsync"/>/
/// <see cref="Lifecycle.ILifecycleOrchestrator.DisableTableHistoryAsync"/>, called from
/// <c>Catalog/CatalogStore.cs</c>'s <c>CreateTableAsync</c>/<c>UpdateTableAsync</c>/<c>DeleteTableAsync</c>
/// (W7-A's file). Two paths bypass those call sites entirely:
/// <list type="number">
/// <item>The SEED path — <c>CatalogStore.EnsureInitialized</c> seeds tables directly into state, never
/// through <c>CreateTableAsync</c>, so a seeded table with <c>HistoryEnabled=true</c> never gets its
/// <see cref="TableHistoryActor"/> configured or its <see cref="TableHistoryEnabledMap"/> entry set.</item>
/// <item>Every host RESTART — <see cref="TableHistoryEnabledMap"/> is in-memory (see that class's own
/// doc comment) and starts empty on every boot; nothing re-populates it for a table whose
/// <see cref="TableHistoryActor"/> may already hold correctly-configured, correctly-persisted Dapr state
/// from before the restart.</item>
/// </list>
///
/// <para><b>Mirrors <see cref="PipelineSupervisorService"/>'s own "boot resume" role and Orleans'
/// <c>RegistryGrain.EnsureInitializedAsync</c> table-history resume loop</b> ("Re-subscribe history grains
/// for every table with HistoryEnabled — independent of the table's own Running status ... uses
/// ResumeAsync (not ResetAsync) so previously accumulated history survives a silo restart") — the Dapr
/// counterpart of that Orleans <c>ResumeAsync</c> call is <see cref="ITableHistoryActor.EnsureConfiguredAsync"/>,
/// idempotent by comparison rather than by construction (see that method's own doc comment): a no-op that
/// preserves previously accumulated history when the actor's current config already matches the table's
/// current history settings, and a real (history-clearing) configure only on a genuine mismatch (first-
/// ever configuration, or a config/SQL change that raced this sweep instead of
/// <see cref="Lifecycle.DaprLifecycleOrchestrator.ResetTableHistoryAsync"/>).</para>
///
/// <para><b>Every table's <see cref="TableHistoryEnabledMap"/> entry is refreshed every sweep tick, not
/// just history-enabled ones</b> — a table whose history is off still needs an explicit <c>false</c> entry
/// (today indistinguishable from "missing" in <see cref="TableHistoryEnabledMap.IsEnabled"/>, but explicit
/// is cheap and keeps this sweep's contract simple: "every table this process knows about has a correct
/// map entry", not "every table this process has ever cared to update".</para>
///
/// <para><b>Deliberately its own file/service, not <c>Services.TableSupervisorService</c></b> (W7-A's) —
/// disjoint file ownership for this wave's parallel agents; registered from
/// <c>Actors/TableHistoryRuntimeSetup.cs</c>'s <c>AddServices</c> (this class's own owner).</para>
/// </summary>
public sealed class TableHistorySupervisorService(
    ICatalogFacadeFactory catalogFactory,
    IEnvironmentFacade environments,
    TableHistoryEnabledMap enabledMap,
    IHostApplicationLifetime lifetime,
    ILogger<TableHistorySupervisorService> logger) : BackgroundService
{
    // Plan 021 D5: same reasoning as the other three supervisors — every environment's tables need their
    // TableHistoryEnabledMap entry refreshed, not just the (empty, here) ambient one's.
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
                    foreach (var table in tables)
                    {
                        try
                        {
                            await EnsureConfiguredAsync(table);
                        }
                        catch (Exception ex)
                        {
                            // Best-effort per table, mirroring PipelineSupervisorService's own per-pipeline
                            // try/catch — one misbehaving table must never stop the sweep from reaching the
                            // rest.
                            logger.LogDebug(ex,
                                "TableHistorySupervisorService: failed to (re)configure history for table '{TableName}' — will retry next sweep.",
                                table.Name);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                logger.LogDebug(ex,
                    "TableHistorySupervisorService: sweep failed (Dapr sidecar likely not ready yet) — will retry next tick.");
            }
        } while (await timer.WaitForNextTickAsync(stoppingToken));
    }

    private async Task EnsureConfiguredAsync(TableDefinition table)
    {
        // Plan 021 D6: qualified so this matches the key TableHistoryDeltaSink looks entries up by
        // (envelope.Table — see DaprLifecycleOrchestrator.History.cs's identical qualification).
        var qualifiedName = EnvKeys.Qualify(table.Environment, table.Name);

        // Refresh the enable-map for EVERY table, enabled or not — see this class's own doc comment.
        enabledMap.SetEnabled(qualifiedName, table.HistoryEnabled);

        if (!table.HistoryEnabled)
        {
            return;
        }

        var actor = ActorProxy.Create<ITableHistoryActor>(new ActorId(qualifiedName), nameof(TableHistoryActor), ActorProxyDefaults.Options);
        await actor.EnsureConfiguredAsync(table);
    }

    /// <summary>Small inline equivalent of the Orleans host's internal <c>StartupSignal</c> helper — same
    /// rationale as <see cref="PipelineSupervisorService"/>'s identical private method (that type is
    /// internal to a different assembly).</summary>
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
