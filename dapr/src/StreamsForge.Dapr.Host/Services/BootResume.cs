using Dapr.Actors;
using Dapr.Actors.Client;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Environments;
using StreamsForge.Dapr.Host.Actors;
using StreamsForge.Dapr.Host.Streaming;

namespace StreamsForge.Dapr.Host.Services;

/// <summary>
/// Plan 025 (PARITY.md D6 bullet 2, "consumers before producers at boot, one resume pass"): the pure
/// ordering half of this flavor's boot resume — what to start, in what order — with no actor, sidecar or
/// catalog dependency at all, so it is unit-testable (dapr/tests/StreamsForge.Dapr.Tests/
/// BootResumePlanTests.cs). The I/O half is <see cref="EntityResume"/>, driven by
/// <see cref="CatalogInitializationService"/>.
///
/// <para><b>CONSUMERS BEFORE PRODUCERS is the whole point of the ordering</b>, and it is the same
/// reasoning <c>RegistryGrain.EnsureInitializedAsync</c> spells out on the Orleans side. Dapr pub/sub, like
/// Orleans memory streams, has no replay: a table or pipeline receives only what was published after its
/// router registration existed. Resuming sources first — which is effectively what four independent,
/// uncoordinated 15 s supervisor sweeps did — opens a window in which a `url`/`file`/`folder` source's
/// first poll lands before any consumer is routable, and with a dedup key configured those rows never come
/// round again, because the source has already ledgered them.</para>
///
/// <para><b>Residual, and deliberately not closed by ordering alone:</b> a connector actor that some
/// unrelated API call activates during the boot window (<c>GET /api/sources/{name}/status</c>, say)
/// self-resumes an overdue poll in <c>ConnectorActor.OnActivateAsync</c> and can still publish early.
/// Plan 025's source-side replay ring (<see cref="ConnectorAttachState"/>) is what hands those rows to a
/// table or pipeline when it attaches — ordering narrows the window, the ring covers what is left.</para>
/// </summary>
public static class BootResumePlan
{
    /// <summary>The three phases, in the order they must be executed. Filtering rules: a pipeline or table
    /// resumes only if the CATALOG says it was Running (a Stopped or Failed entity stays that way — boot is
    /// not a place to promote anything); a source resumes only if it is Enabled AND has a driver on this
    /// flavor. <see cref="SourceKindDispatch.ActorKind.Ingest"/> has no actor by design (rows arrive through
    /// <c>IIngressFacade</c>) and <see cref="SourceKindDispatch.ActorKind.Crdt"/> has none on Dapr at all
    /// (PARITY.md D5) — both are excluded here rather than being handed to a driver that would only log an
    /// error, which is what <c>DaprLifecycleOrchestrator</c> does when it meets a crdt source.</summary>
    public static BootResumePhases Build(
        IReadOnlyList<PipelineDefinition> pipelines,
        IReadOnlyList<TableDefinition> tables,
        IReadOnlyList<SourceDefinition> sources)
    {
        var runningPipelines = pipelines.Where(p => p.Status == PipelineStatus.Running).ToList();
        var runningTables = tables.Where(t => t.Status == PipelineStatus.Running).ToList();
        var startableSources = sources
            .Where(s => s.Enabled && SourceKindDispatch.Classify(s.Kind)
                is SourceKindDispatch.ActorKind.Generator or SourceKindDispatch.ActorKind.Connector)
            .ToList();

        return new BootResumePhases(
            runningPipelines,
            TopoSortByTableInputs(runningTables, tables),
            startableSources);
    }

    /// <summary>Depth-first topological sort of <paramref name="running"/> by <c>TableInputs</c> edges,
    /// upstream first — a straight port of <c>RegistryGrain.TopoSortByTableInputs</c>, so a table-over-table
    /// chain resumes in dependency order instead of relying on <c>CatalogStore.SetTableStatusAsync</c>'s
    /// "table input(s) not running" refusal and a retry on the next 15 s sweep (which is what this flavor
    /// did before, and what <see cref="TableSupervisorService"/>'s class doc describes as the accepted cost
    /// of NOT having this).
    ///
    /// <para><b>A cycle is tolerated, not diagnosed.</b> The <c>stack</c> set makes a back edge return
    /// immediately, so a cyclic group simply comes out in catalog order rather than throwing or looping —
    /// the same behaviour the Orleans original has. Boot is the wrong place to reject a catalog: whatever
    /// order such a group lands in, the entities that cannot start fail individually and are retried by the
    /// supervisor sweeps, exactly as they were before this ordering existed.</para></summary>
    public static List<TableDefinition> TopoSortByTableInputs(
        IReadOnlyList<TableDefinition> running, IReadOnlyList<TableDefinition> all)
    {
        // A duplicate name would be a corrupt catalog (names are unique by construction) — take the first
        // rather than throwing out of a boot path over it.
        var byName = new Dictionary<string, TableDefinition>(StringComparer.Ordinal);
        foreach (var t in all)
        {
            byName.TryAdd(t.Name, t);
        }

        var runningSet = running.ToHashSet();
        var visited = new HashSet<string>(StringComparer.Ordinal);
        var result = new List<TableDefinition>();

        void Visit(TableDefinition t, HashSet<string> stack)
        {
            if (visited.Contains(t.Name) || !stack.Add(t.Name))
            {
                return;
            }

            foreach (var dep in t.TableInputs)
            {
                if (byName.TryGetValue(dep, out var depDef) && runningSet.Contains(depDef))
                {
                    Visit(depDef, stack);
                }
            }

            visited.Add(t.Name);
            result.Add(t);
        }

        foreach (var t in running)
        {
            Visit(t, new HashSet<string>(StringComparer.Ordinal));
        }

        return result;
    }
}

/// <summary>What <see cref="BootResumePlan.Build"/> answers: the three resume phases in execution order.
/// Consumers first (<see cref="Pipelines"/>, then <see cref="Tables"/> in dependency order), producers last
/// (<see cref="Sources"/>) — see <see cref="BootResumePlan"/>'s class doc for why that order is the whole
/// point.</summary>
public sealed record BootResumePhases(
    List<PipelineDefinition> Pipelines,
    List<TableDefinition> Tables,
    List<SourceDefinition> Sources);

/// <summary>
/// Plan 025: the "make this one entity actually run" step, shared verbatim between
/// <see cref="CatalogInitializationService"/>'s one-shot boot pass and the four periodic supervisor sweeps.
/// It lived only in the sweeps before this plan; the boot pass needed the identical behaviour, and a second
/// copy of "check IsRunning, else go through the user-equivalent start path" is exactly the kind of
/// duplication that drifts.
///
/// <para>Every method here is best-effort at the CALLER's discretion: none of them swallows, because the
/// two callers want different logging (a sweep says "will retry next sweep", the boot pass says "the
/// supervisors will retry"). Per-entity try/catch stays with the caller.</para>
/// </summary>
public static class EntityResume
{
    /// <summary>Running already → repair only <see cref="PipelineEventRouter"/>'s in-memory routing table
    /// (it does NOT survive a host restart the way the actor's persisted Dapr state does), read cheaply
    /// with no recompile. Not running → the full, user-equivalent start path
    /// (<see cref="ICatalogFacade.SetPipelineStatusAsync"/>), which compiles, starts the actor and persists
    /// Failed/Running/Error identically whether a user or this code triggered it.
    ///
    /// <para>Restarting an ALREADY-running pipeline is what this avoids: it would discard in-flight
    /// window/join state — visibly disrupting a pipeline that has been fine for hours, every ~15 s.</para></summary>
    public static async Task EnsurePipelineRunningAsync(
        ICatalogFacade catalog, PipelineEventRouter router, PipelineDefinition pipeline)
    {
        var actor = ActorProxy.Create<IPipelineActor>(new ActorId(pipeline.Id), nameof(PipelineActor), ActorProxyDefaults.Options);

        if (await actor.IsRunningAsync())
        {
            // Plan 021 D6: GetSourceNamesAsync returns BARE names (this pipeline's own compile) — qualify
            // with its own environment before they go in the process-wide router index (same reasoning as
            // DaprLifecycleOrchestrator.StartPipelineAsync).
            var sourceNames = await actor.GetSourceNamesAsync();
            router.Register(pipeline.Id, sourceNames.Select(s => EnvKeys.Qualify(pipeline.Environment, s)).ToList());
            return;
        }

        await catalog.SetPipelineStatusAsync(pipeline.Id, PipelineStatus.Running);
    }

    /// <summary>The table twin of <see cref="EnsurePipelineRunningAsync"/>, with the same
    /// repair-don't-restart discipline (a restart would discard the table's join/aggregate state).</summary>
    public static async Task EnsureTableRunningAsync(
        ICatalogFacade catalog, TableEventRouter router, TableDefinition table)
    {
        var qualifiedName = EnvKeys.Qualify(table.Environment, table.Name);
        var actor = ActorProxy.Create<ITableActor>(new ActorId(qualifiedName), nameof(TableActor), ActorProxyDefaults.Options);

        if (await actor.IsRunningAsync())
        {
            var inputs = await actor.GetInputNamesAsync();
            router.Register(
                qualifiedName,
                inputs.StreamInputs.Select(s => EnvKeys.Qualify(table.Environment, s)).ToList(),
                inputs.TableInputs.Select(t => EnvKeys.Qualify(table.Environment, t)).ToList());
            // Pipeline inputs are keyed by BARE pipeline id (globally unique) — see
            // TableEventRouter.RegisterPipelineInputs — so no qualification here.
            router.RegisterPipelineInputs(qualifiedName, inputs.PipelineInputs ?? []);
            return;
        }

        await catalog.SetTableStatusAsync(table.Id, PipelineStatus.Running);
    }

    /// <summary>Starts one source's driver. A generator's <see cref="IGeneratorActor.StartAsync"/> is safe
    /// to call unconditionally (it just re-arms a fixed 200 ms cadence timer); a connector's is a genuine
    /// "fresh start" that resets the failure streak and reschedules from now, so it is gated on
    /// <see cref="IConnectorActor.IsRunningAsync"/> — calling it unconditionally on a source whose poll
    /// schedule is longer than the sweep period would perpetually restart it before its own timer ever
    /// fired. Ingest and Crdt kinds never reach here (<see cref="BootResumePlan.Build"/> and
    /// <see cref="ConnectorSourceSweep.SelectConnectorSources"/> both exclude them).</summary>
    public static async Task EnsureSourceRunningAsync(SourceDefinition src)
    {
        var key = new ActorId(EnvKeys.Qualify(src.Environment, src.Name));

        if (SourceKindDispatch.Classify(src.Kind) == SourceKindDispatch.ActorKind.Generator)
        {
            await ActorProxy.Create<IGeneratorActor>(key, nameof(GeneratorActor), ActorProxyDefaults.Options).StartAsync(src);
            return;
        }

        var connector = ActorProxy.Create<IConnectorActor>(key, nameof(ConnectorActor), ActorProxyDefaults.Options);
        if (!await connector.IsRunningAsync())
        {
            await connector.StartAsync(src);
        }
    }
}

/// <summary>
/// Plan 025: the one-shot latch the four supervisor sweeps wait on before their FIRST pass, so
/// <see cref="CatalogInitializationService"/>'s ordered boot resume is not raced by a sweep that would
/// start a producer before its consumers exist. The Dapr shape of what
/// <c>RegistryGrain.EnsureInitializedAsync</c>'s <c>_resumed</c> latch plus
/// <c>GeneratorSupervisorService</c>'s "await the registry's resume before pinging" do together on Orleans.
///
/// <para><b>Why a static <see cref="Shared"/> instance rather than a DI singleton:</b> the two sides of
/// this gate are registered from files with different owners in this repo's wave discipline (Program.cs
/// registers <see cref="CatalogInitializationService"/>; the <c>*RuntimeSetup</c> classes register the
/// supervisors), and a DI registration would have to be added to one of them. A process-wide static
/// registry is an established pattern on both flavors for exactly this shape of cross-cutting singleton —
/// <c>InboundTransports</c>, <c>PolledTransports</c>, <c>DuplexSessions</c>, <c>NamedEndpoints</c> are all
/// static — and there is exactly one boot per process, so a per-container instance would buy nothing. The
/// gate is nonetheless an ordinary INSTANCE type so a test can exercise it without touching
/// <see cref="Shared"/>.</para>
///
/// <para><b>The wait is bounded, deliberately.</b> A boot pass that hangs (a wedged sidecar call with no
/// timeout, say) must not disable self-healing for the life of the process — that would turn one slow call
/// into "nothing ever gets retried". So the supervisors wait at most
/// <see cref="DefaultWaitTimeout"/> and then proceed anyway, logging that they did. The cost of proceeding
/// early is the pre-plan-025 behaviour (an uncoordinated sweep), which is a degradation, not a
/// breakage.</para>
/// </summary>
public sealed class BootGate
{
    /// <summary>How long a supervisor waits before giving up on the boot pass and sweeping anyway.</summary>
    public static readonly TimeSpan DefaultWaitTimeout = TimeSpan.FromSeconds(60);

    /// <summary>The process-wide gate — see this class's doc for why it is static.</summary>
    public static BootGate Shared { get; } = new();

    // RunContinuationsAsynchronously: Complete() is called from CatalogInitializationService's boot task,
    // and four supervisor loops are parked on this. Without it they would all resume INLINE on that task's
    // thread, serializing four sweeps behind the tail of the boot pass for no reason.
    private readonly TaskCompletionSource _completed = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>True once <see cref="Complete"/> has been called.</summary>
    public bool IsCompleted => _completed.Task.IsCompleted;

    /// <summary>Opens the gate. Idempotent — a second call (a retried boot, a test) is a no-op rather than
    /// an <see cref="InvalidOperationException"/>.</summary>
    public void Complete() => _completed.TrySetResult();

    /// <summary>Waits for the gate, at most <paramref name="timeout"/>. Answers TRUE when the boot pass
    /// actually completed and FALSE when the wait timed out — the caller decides what to log; either way it
    /// proceeds. Never throws for the timeout (that is a normal outcome here, not an error); a cancelled
    /// <paramref name="ct"/> still throws, because that means the host is shutting down and the caller's
    /// loop should unwind.</summary>
    public async Task<bool> WaitAsync(TimeSpan timeout, CancellationToken ct)
    {
        if (_completed.Task.IsCompleted)
        {
            return true;
        }

        try
        {
            await _completed.Task.WaitAsync(timeout, ct);
            return true;
        }
        catch (TimeoutException)
        {
            return false;
        }
    }
}

/// <summary>Plan 025: the four-line "wait for the boot pass, then say what happened" the four supervisor
/// sweeps each need before their FIRST tick — factored out so the timeout, the log level and the wording
/// cannot drift between them, in exactly the spirit of each supervisor's own
/// <c>WaitForApplicationStartedAsync</c> being the same handful of lines four times over.</summary>
public static class BootGateWait
{
    public static async Task AwaitBootPassAsync(ILogger logger, string supervisor, CancellationToken ct)
    {
        if (await BootGate.Shared.WaitAsync(BootGate.DefaultWaitTimeout, ct))
        {
            return;
        }

        // Information, not Debug: proceeding without the boot pass means this sweep may start a producer
        // before its consumers are routable — the exact window plan 025 closed. An operator looking at why
        // a table came up empty after a restart needs to be able to find this line.
        logger.LogInformation(
            "{Supervisor}: the boot resume pass did not complete within {Seconds}s — sweeping anyway (self-healing must not be gated on it). " +
            "Sources may start before their consumers this pass; the connector replay ring covers rows emitted in that window.",
            supervisor, (int)BootGate.DefaultWaitTimeout.TotalSeconds);
    }
}
