using Dapr.Actors;
using Dapr.Actors.Client;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Environments;
using StreamsForge.Dapr.Host.Actors;
using StreamsForge.Dapr.Host.Facades;
using StreamsForge.Dapr.Host.Streaming;

namespace StreamsForge.Dapr.Host.Services;

/// <summary>
/// Plan 005 W4: Dapr counterpart of the Orleans host's <c>InitializeGrainsAsync</c> (Program.cs,
/// registered on <c>ApplicationStarted</c>) — calls the boot-only <c>EnsureInitializedAsync</c> on both
/// singleton actors once the app (and, in practice, the Dapr sidecar) is up. Seeds the demo
/// catalog/users on an empty Redis actor-state store; a no-op on a populated one (both actors'
/// EnsureInitializedAsync check Count == 0 first). Best-effort, matching the Orleans host's own
/// try/catch-and-log — no retry loop; a failure here just means the demo world isn't seeded yet, visible
/// immediately as empty catalogs on first login.
///
/// <para><b>Plan 021:</b> the registry actor addressed below is <c>StreamConstants.RegistryKey</c>
/// UNQUALIFIED — i.e. always the DEFAULT environment's catalog (<see cref="EnvKeys.Qualify"/> is a no-op
/// for it). Seeding intentionally does not iterate <see cref="IEnvironmentFacade"/>: there is nothing to
/// seed in a named environment (nobody has created one yet at boot, and D7 says creation is deliberate,
/// never implicit), so "seed the default catalog, exactly as before this plan" is not a simplification —
/// it is the whole of what seeding is supposed to do.</para>
///
/// <para><b>Plan 025 (PARITY.md D6 bullet 2) — this service now also OWNS THE BOOT RESUME, and the four
/// supervisor sweeps wait for it.</b> The paragraph that used to stand here said the boot-resume sweep was
/// "a different job, done by the four <c>Services.*SupervisorService</c> classes". That was the debt: those
/// four sweeps each waited for <see cref="IHostApplicationLifetime.ApplicationStarted"/> independently and
/// then swept with no coordination between them, so a `url` source with a dedup key could poll — and
/// ledger — before the table or pipeline reading it had re-registered its router after a restart, and
/// those rows never came round again. Orleans has never had that window:
/// <c>RegistryGrain.EnsureInitializedAsync</c> resumes pipelines, then tables in dependency order, then
/// sources, once, and the supervisor awaits it.
///
/// <para>So: after the two <c>EnsureInitializedAsync</c> calls, this service runs ONE ordered pass over
/// EVERY environment (<see cref="IEnvironmentFacade.ListAsync"/>, the same enumeration the supervisors use
/// — a named environment's already-Running entities need resuming even though nothing seeds them) using
/// <see cref="BootResumePlan"/> for the order and <see cref="EntityResume"/> for the per-entity work, then
/// opens <see cref="BootGate"/> so the sweeps can start. Per-entity failures are logged and skipped: the
/// sweeps still run every 15 s afterwards and remain the self-healing safety net they always were, for a
/// source enabled after boot, an actor evicted later, or an entity this pass could not start yet.</para>
///
/// <para><b>Environments created at runtime need nothing special here.</b> This pass sees whatever
/// <see cref="IEnvironmentFacade.ListAsync"/> returns at boot; an environment created afterwards is empty
/// by construction (D7 — creation is deliberate, never implicit), and the first entity in it is started by
/// the API call that creates it, with the sweeps as the backstop. Same as before this plan.</para>
///
/// <para><b>The residual this does NOT close</b>, stated so silence is not read as absence: an API call
/// that activates a connector actor during the boot window (<c>GET /api/sources/{name}/status</c>, a
/// console page load) makes <c>ConnectorActor.OnActivateAsync</c> self-resume an overdue poll, which can
/// still publish before that source's consumers are routable. Ordering narrows the window; plan 025's
/// source-side replay ring (<see cref="ConnectorAttachState"/>, handed over by
/// <see cref="IConnectorActor.BeginAttachAsync"/>) is what hands those rows to a table or pipeline when it
/// attaches. Identical residual, identical mitigation, as the Orleans twin's.</para>
/// </summary>
public sealed class CatalogInitializationService(
    ICatalogFacadeFactory catalogFactory,
    IEnvironmentFacade environments,
    PipelineEventRouter pipelineRouter,
    TableEventRouter tableRouter,
    IHostApplicationLifetime lifetime,
    ILogger<CatalogInitializationService> logger) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken)
    {
        lifetime.ApplicationStarted.Register(() => _ = InitializeAsync());
        return Task.CompletedTask;
    }

    private async Task InitializeAsync()
    {
        try
        {
            var users = ActorProxy.Create<IUserStoreActor>(new ActorId(StreamConstants.UsersKey), nameof(UserStoreActor), ActorProxyDefaults.Options);
            await users.EnsureInitializedAsync();

            var registry = ActorProxy.Create<IRegistryActor>(new ActorId(StreamConstants.RegistryKey), nameof(RegistryActor), ActorProxyDefaults.Options);
            await registry.EnsureInitializedAsync();

            logger.LogInformation("StreamsForge.Dapr.Host: catalog/users actors initialized.");

            await ResumeEveryEnvironmentAsync();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "StreamsForge.Dapr.Host: actor initialization failed.");
        }
        finally
        {
            // ALWAYS, including on the failure path above — a boot pass that could not run (sidecar not
            // ready, Redis unreachable) must not leave the four sweeps parked forever. They are precisely
            // the retry mechanism for everything this pass did not manage to start.
            BootGate.Shared.Complete();
        }
    }

    /// <summary>The ordered resume, per environment. Environments are independent catalogs (plan 021 D1 —
    /// one <c>RegistryActor</c> each), so they are resumed one after another with no ordering constraint
    /// BETWEEN them; the consumers-before-producers order that matters is WITHIN each one.</summary>
    private async Task ResumeEveryEnvironmentAsync()
    {
        foreach (var env in await environments.ListAsync())
        {
            var environment = EnvKeys.Normalize(env.Name);
            var catalog = catalogFactory.For(environment);

            var phases = BootResumePlan.Build(
                await catalog.GetPipelinesAsync(),
                await catalog.GetTablesAsync(),
                await catalog.GetSourcesAsync());

            foreach (var pipeline in phases.Pipelines)
            {
                await ResumeOneAsync(
                    () => EntityResume.EnsurePipelineRunningAsync(catalog, pipelineRouter, pipeline),
                    "pipeline", pipeline.Id, environment);
            }

            foreach (var table in phases.Tables)
            {
                await ResumeOneAsync(
                    () => EntityResume.EnsureTableRunningAsync(catalog, tableRouter, table),
                    "table", table.Name, environment);
            }

            // PRODUCERS LAST — see BootResumePlan's class doc for why that is the whole point.
            foreach (var src in phases.Sources)
            {
                await ResumeOneAsync(
                    () => EntityResume.EnsureSourceRunningAsync(src),
                    "source", src.Name, environment);
            }

            logger.LogInformation(
                "StreamsForge.Dapr.Host: boot resume for environment '{Environment}' — {Pipelines} pipeline(s), {Tables} table(s), {Sources} source(s).",
                environment, phases.Pipelines.Count, phases.Tables.Count, phases.Sources.Count);
        }
    }

    /// <summary>Per-entity best-effort, mirroring every supervisor sweep's own per-entity try/catch: one
    /// entity that cannot start must never stop the pass from reaching the rest — least of all from
    /// reaching the SOURCE phase, since a boot pass that gave up halfway through the table phase would
    /// leave producers stopped, which is worse than the ordering problem this pass exists to fix.</summary>
    private async Task ResumeOneAsync(Func<Task> resume, string kind, string name, string environment)
    {
        try
        {
            await resume();
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex,
                "CatalogInitializationService: boot resume failed for {Kind} '{Name}' in environment '{Environment}' — the supervisor sweeps will retry.",
                kind, name, environment);
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
