using StreamsForge.Abstractions;
using StreamsForge.AppCore.Environments;
using StreamsForge.AppCore.Ingest;
using StreamsForge.Dapr.Host.Facades;

namespace StreamsForge.Dapr.Host.Ingest;

/// <summary>
/// Plan 008 W4c: the "drain pump" <see cref="DaprIngressFacade"/>'s own doc comment refers to — the
/// only thing in this flavor that periodically calls <see cref="SourceIngressBuffer.DrainAsync"/> for a
/// buffered (non-Inline) ingest source, handing whatever is queued to <see cref="DaprIngressFacade"/>'s
/// drain delegate, which publishes it as one <see cref="SourceEventsEnvelope"/> per batch.
///
/// <para><b>Why a periodic sweep and not a per-push background kick:</b> an ingest-kind source has no
/// actor and no timer of its own (unlike <see cref="Actors.GeneratorActor"/>'s 200ms cadence or
/// <see cref="Actors.ConnectorActor"/>'s re-armed one-shot) — <see cref="SourceIngressBuffer"/> is pure
/// admission bookkeeping with nothing driving its queue forward on its own. This service is that driver,
/// mirroring <see cref="Services.GeneratorSupervisorService"/>'s own "list sources, act per eligible one"
/// shape but at a much shorter period (default 100ms, matching this repo's existing
/// <c>Streams:PullPeriodMs</c> default for the Orleans pull-transport cadence — see root CLAUDE.md) since
/// this sweep IS the hot path for buffered ingress, not a reconciliation safety net.</para>
///
/// <para><b>Discovery goes through <see cref="ICatalogFacade"/>, not a registry enumeration</b> —
/// <see cref="SourceIngressRegistry"/> deliberately exposes no "list every buffer" method (only
/// <c>GetOrCreate</c>/<c>TryGet</c>/<c>Remove</c>, keyed by name), so this service re-lists ingest-kind
/// sources from the catalog every tick and looks each one's buffer up by name — exactly the same
/// per-source-proxy-resolution shape <see cref="Services.ConnectorSourceSweep"/> already uses for
/// connector-kind sources. A source with no buffer yet (never pushed to) is skipped, not created — this
/// pump only drains, it never originates a buffer (that stays <see cref="DaprIngressFacade.PushAsync"/>'s
/// job, on first push).</para>
///
/// <para><b>Sidecar readiness / per-source isolation:</b> same best-effort philosophy as
/// <see cref="Services.GeneratorSupervisorService"/> — an outer try/catch absorbs a sidecar not being
/// ready yet (logged at Debug, retried next tick), and a per-source try/catch means one buffer's
/// publish failure never stops the sweep from draining the rest.</para>
/// </summary>
public sealed class IngestDrainPumpService(
    ICatalogFacadeFactory catalogFactory,
    IEnvironmentFacade environments,
    SourceIngressRegistry registry,
    IConfiguration configuration,
    IHostApplicationLifetime lifetime,
    ILogger<IngestDrainPumpService> logger) : BackgroundService
{
    // Plan 021 D5: same reasoning as the supervisors — this is the hot path for EVERY environment's
    // buffered ingest sources, not just the (empty, here) ambient one's.
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await WaitForApplicationStartedAsync(stoppingToken);

        var periodMs = Math.Max(1, configuration.GetValue("Ingest:DrainMs", 100));
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(periodMs));
        do
        {
            try
            {
                // Plan 021: RetainOnly runs over the QUALIFIED names of every environment's ingest-kind
                // sources in ONE call — SourceIngressRegistry has no environment concept of its own (it is
                // shared/AppCore, frozen), so "everything not in this set is stale" has to mean "across
                // every environment", or a source that still exists in environment B would look stale (and
                // get its buffer dropped) purely because environment A's sweep pass ran first.
                var qualifiedLive = new HashSet<string>(StringComparer.Ordinal);
                var envSources = new List<(string Environment, List<SourceDefinition> Sources)>();
                foreach (var env in await environments.ListAsync())
                {
                    var environment = EnvKeys.Normalize(env.Name);
                    var sources = await catalogFactory.For(environment).GetSourcesAsync();
                    envSources.Add((environment, sources));
                    foreach (var s in sources.Where(s => s.Kind == SourceKinds.Ingest))
                    {
                        qualifiedLive.Add(EnvKeys.Qualify(environment, s.Name));
                    }
                }

                // The lifecycle orchestrator already drops a buffer on delete and on a kind change, but
                // not every deletion route goes through it (a replace-mode config import rewrites the
                // catalog wholesale). Reconciling here — against the catalog we just read anyway — makes
                // the sweep the backstop, exactly as GeneratorSupervisorService does on the Orleans side.
                registry.RetainOnly(qualifiedLive);

                foreach (var (environment, sources) in envSources)
                {
                    foreach (var src in sources)
                    {
                        if (src.Kind != SourceKinds.Ingest)
                        {
                            continue;
                        }

                        var qualifiedName = EnvKeys.Qualify(environment, src.Name);
                        var buffer = registry.TryGet(qualifiedName);
                        if (buffer is null)
                        {
                            continue; // never pushed to yet — nothing queued, nothing to drain
                        }

                        try
                        {
                            await buffer.DrainAsync(ct: stoppingToken);
                        }
                        catch (Exception ex)
                        {
                            // Best-effort per source, mirroring GeneratorSupervisorService's own per-source
                            // try/catch — one misbehaving buffer's drain must never stop the sweep from
                            // reaching the rest.
                            logger.LogDebug(ex,
                                "IngestDrainPumpService: drain failed for source '{Source}' — will retry next tick.",
                                qualifiedName);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                logger.LogDebug(ex,
                    "IngestDrainPumpService: sweep failed (Dapr sidecar likely not ready yet) — will retry next tick.");
            }
        } while (await timer.WaitForNextTickAsync(stoppingToken));
    }

    /// <summary>Same small inline helper as <see cref="Services.GeneratorSupervisorService"/>'s own
    /// private method of the same name/shape — that one is private to a different class, so this is the
    /// same handful of lines rather than a shared abstraction for one four-line helper (same rationale
    /// that class's own doc comment gives for its copy).</summary>
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
