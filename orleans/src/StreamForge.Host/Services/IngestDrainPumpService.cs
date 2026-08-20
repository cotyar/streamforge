using Orleans;
using StreamForge.Abstractions;
using StreamForge.AppCore.Environments;
using StreamForge.AppCore.Ingest;
using StreamForge.Host.Facades;

namespace StreamForge.Host.Services;

/// <summary>
/// Plan 008 W4: the driver behind <c>OrleansIngressFacade</c>'s drain delegate — the only thing in this
/// flavor that periodically calls <see cref="SourceIngressBuffer.DrainAsync"/>. Without it a buffered
/// (non-Inline) push is admitted, counted, and then never published: <see cref="SourceIngressBuffer"/> is
/// pure admission bookkeeping with nothing moving its queue forward on its own, and an ingest-kind source
/// — unlike a generator or a connector — has no grain and no timer of its own to do it. The Dapr flavor's
/// <c>IngestDrainPumpService</c> is the same service against that flavor's publish path.
///
/// <para>Default period 100ms (<c>Ingest:DrainMs</c>), matching this repo's <c>Streams:PullPeriodMs</c>
/// default: this sweep IS the hot path for buffered ingress, not a reconciliation safety net, so it does
/// not share <see cref="GeneratorSupervisorService"/>'s 15s cadence.</para>
///
/// <para>It also reconciles the registry against the catalog, which IS a safety net: a buffer whose source
/// is gone would otherwise hold its rows and counters until the process restarts, and deletion reaches us
/// by several routes (the DELETE endpoint, a kind change, a replace-mode config import) rather than one
/// interceptable one. A source with no buffer yet is skipped, never created — this pump only drains;
/// originating a buffer stays the facade's job, on first push.</para>
/// </summary>
public sealed class IngestDrainPumpService(
    IClusterClient client,
    SourceIngressRegistry ingress,
    IngressStatsReportTracker statsTracker,
    IConfiguration configuration,
    IHostApplicationLifetime lifetime,
    ILogger<IngestDrainPumpService> logger) : BackgroundService
{
    /// <summary>Plan 021 environment strategy: ITERATES EVERY ENVIRONMENT, same reasoning as
    /// GeneratorSupervisorService's identical choice (this is a timer-driven sweep, outside any request, so
    /// the ambient is always empty). <see cref="IEnvironmentFacade.ListAsync"/> is NOT cheap — it counts
    /// every environment's catalog — and this pump's own default cadence is 100ms, the hot path for
    /// buffered ingress (see the class doc above); calling it every tick would make environment discovery
    /// itself a meaningful fraction of the sweep's own cost. So the environment list is cached and
    /// refreshed on this TTL rather than every tick — a newly created environment's ingest sources start
    /// draining within one TTL window, not necessarily the very next 100ms tick.</summary>
    private static readonly TimeSpan EnvironmentListTtl = TimeSpan.FromSeconds(5);
    private List<string> _cachedEnvironments = [EnvKeys.Default];
    private DateTime _environmentsCachedAtUtc = DateTime.MinValue;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await StartupSignal.WaitForApplicationStartedAsync(lifetime, stoppingToken);

        var periodMs = Math.Max(1, configuration.GetValue("Ingest:DrainMs", 100));
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(periodMs));
        do
        {
            try
            {
                // Plan 021 — ALL environments' sources, gathered into ONE list before either static helper
                // runs: SweepAsync's own RetainOnly call purges any buffer whose name is not in the list it
                // is given, so calling it once PER environment with only that environment's names would
                // have each call purge every OTHER environment's buffers out from under it mid-tick.
                var sources = new List<SourceDefinition>();
                foreach (var env in await GetEnvironmentsAsync())
                {
                    sources.AddRange(await client.RegistryFor(env).GetSourcesAsync());
                }

                await SweepAsync(sources, ingress, logger, stoppingToken);
                await ReportStatsAsync(sources, ingress, client, statsTracker, logger, stoppingToken);
            }
            catch (Exception ex)
            {
                logger.LogDebug(ex, "IngestDrainPumpService: sweep failed — will retry next tick.");
            }
        } while (await timer.WaitForNextTickAsync(stoppingToken));
    }

    private async Task<List<string>> GetEnvironmentsAsync()
    {
        if (DateTime.UtcNow - _environmentsCachedAtUtc < EnvironmentListTtl)
        {
            return _cachedEnvironments;
        }

        var environments = await client.GetGrain<IEnvironmentRegistryGrain>(StreamConstants.EnvironmentsKey).ListAsync();
        _cachedEnvironments = environments.Select(e => EnvKeys.Normalize(e.Name)).ToList();
        _environmentsCachedAtUtc = DateTime.UtcNow;
        return _cachedEnvironments;
    }

    /// <summary>One tick's work, split out so it is testable without a cluster: reconcile the registry
    /// against <paramref name="sources"/>, then drain every ingest source that already has a buffer.</summary>
    public static async Task SweepAsync(
        IReadOnlyList<SourceDefinition> sources,
        SourceIngressRegistry ingress,
        ILogger logger,
        CancellationToken ct)
    {
        // Plan 021 D3/item 3 — the buffer key is EnvKeys.Qualify(s.Environment, s.Name), not the bare
        // name: sources was gathered across EVERY environment (see ExecuteAsync's own comment), so two
        // environments' same-named ingest source would otherwise share one buffer — the exact leak this
        // wave exists to close. A SourceDefinition's own Environment field is the source of truth here
        // (D5 — this is background work, never the ambient).
        var ingestNames = sources.Where(s => SourceKindDispatch.Classify(s.Kind) == SourceKindDispatch.ActorKind.Ingest)
            .Select(s => EnvKeys.Qualify(s.Environment, s.Name)).ToList();
        ingress.RetainOnly(ingestNames.ToHashSet(StringComparer.Ordinal));

        foreach (var name in ingestNames)
        {
            var buffer = ingress.TryGet(name);
            if (buffer is null)
            {
                continue; // never pushed to yet — nothing queued
            }

            try
            {
                await buffer.DrainAsync(ct: ct);
            }
            catch (Exception ex)
            {
                // Per-source, mirroring GeneratorSupervisorService: one buffer's publish failure must
                // never stop the sweep from reaching the rest.
                logger.LogDebug(ex, "IngestDrainPumpService: drain failed for source '{Source}' — will retry next tick.", name);
            }
        }
    }

    /// <summary>Plan 009 A1.3: reports every ingest source's LOCAL counter delta (since this replica's
    /// own last report — <see cref="IngressStatsReportTracker"/>) into its per-source
    /// <see cref="IIngressStatsGrain"/>. Split out from <see cref="SweepAsync"/> — that method's
    /// signature is pinned by existing tests (<c>IngestDrainPumpTests</c>), so this is additive rather
    /// than folded into it. A source with no local buffer (never pushed to on this replica) has
    /// nothing to report and is skipped; an empty delta (nothing changed since the last tick) is
    /// skipped too, to avoid paying a grain call for a no-op.</summary>
    public static async Task ReportStatsAsync(
        IReadOnlyList<SourceDefinition> sources,
        SourceIngressRegistry ingress,
        IClusterClient client,
        IngressStatsReportTracker statsTracker,
        ILogger logger,
        CancellationToken ct)
    {
        // Plan 021 D3/item 3 — same qualified key as SweepAsync (s.Environment, not the ambient — this is
        // background work), so the buffer lookup, the IIngressStatsGrain address, and the baseline tracker
        // all agree on identity across environments.
        foreach (var s in sources.Where(s => SourceKindDispatch.Classify(s.Kind) == SourceKindDispatch.ActorKind.Ingest))
        {
            var name = EnvKeys.Qualify(s.Environment, s.Name);
            var buffer = ingress.TryGet(name);
            if (buffer is null)
            {
                continue;
            }

            var current = buffer.GetStatus();
            var baseline = statsTracker.GetBaseline(name);
            var delta = IngressStatsReportTracker.ComputeDelta(baseline, current);
            if (delta.IsEmpty)
            {
                continue;
            }

            try
            {
                await client.GetGrain<IIngressStatsGrain>(name).ReportDeltaAsync(delta);
                statsTracker.SetBaseline(name, current);
            }
            catch (Exception ex)
            {
                // Best-effort, same per-source isolation as SweepAsync's own drain try/catch — one
                // source's grain call failing must never stop the tick from reporting the rest, and
                // the baseline is left UNMOVED on failure so the next tick's delta naturally includes
                // whatever this one failed to report (nothing is lost, only delayed).
                logger.LogDebug(ex, "IngestDrainPumpService: stats report failed for source '{Source}' — will retry next tick.", name);
            }
        }
    }
}
