using Dapr.Actors;
using Dapr.Actors.Client;
using Dapr.Client;
using StreamForge.Abstractions;
using StreamForge.AppCore.Environments;
using StreamForge.AppCore.Ingest;
using StreamForge.Dapr.Host.Actors;
using StreamForge.Dapr.Host.Streaming;

namespace StreamForge.Dapr.Host.Lifecycle;

/// <summary>
/// Plan 005 (Dapr sibling runtime) W5-A/W6: replaces <see cref="NoopLifecycleOrchestrator"/> as the real
/// <see cref="ILifecycleOrchestrator"/> once <see cref="GeneratorActor"/>/<see cref="PipelineActor"/>
/// exist — registered in <c>Actors/GeneratorRuntimeSetup.cs</c>'s <c>AddServices</c>, which runs AFTER
/// Program.cs's Noop registration, so this implementation wins.
///
/// <para><b>Sources and pipelines are real; tables/history are still W4's no-op behavior.</b>
/// <see cref="StartTableAsync"/>/<see cref="ResetTableHistoryAsync"/>/etc. are copied verbatim from
/// <see cref="NoopLifecycleOrchestrator"/> (log a warning, report success) — W7 replaces them; the
/// <see cref="LifecycleOutcome"/> contract is unchanged.</para>
///
/// <para><b>Acyclic by construction (see dapr/ARCHITECTURE.md's reentrancy decision).</b> This class is
/// invoked synchronously, inline, from <see cref="Catalog.CatalogStore"/>'s methods — which themselves run
/// inside <see cref="Actors.RegistryActor"/>'s own (non-reentrant) actor turn. The hazard that decision
/// guards against is a CYCLE: RegistryActor's turn blocked waiting on a call chain that loops back into
/// that same still-in-flight turn. Calling into <see cref="IGeneratorActor"/>/<see cref="IPipelineActor"/>
/// from here is NOT such a cycle — neither <see cref="GeneratorActor"/> nor <see cref="PipelineActor"/>
/// ever calls RegistryActor, <c>ICatalogFacade</c>, or any other actor (see each class's own doc comment:
/// everything either needs arrives as a method parameter). Both are pure leaves in the call graph, so a
/// synchronous actor-to-actor call to either can never deadlock — there is nothing to make
/// fire-and-forget here. Awaiting inline (exactly like Orleans' <c>RegistryGrain</c> awaiting
/// <c>GeneratorGrain.StartAsync</c>/<c>PipelineGrain.StartAsync</c> directly) is both simpler and
/// sufficient. <b>Rule for whoever builds W7's table orchestration on this same seam:</b> that only holds
/// because GeneratorActor/PipelineActor are leaves — a worker actor that reads back from the registry
/// (directly or via a facade) would reintroduce the exact cycle this design avoids, and MUST go through
/// fire-and-forget pub/sub or an equivalent non-blocking path instead.</para>
///
/// <para><b><see cref="Streaming.PipelineEventRouter"/> bookkeeping (W6):</b> every successful
/// <see cref="StartPipelineAsync"/> registers the router's routing table with the source names
/// <see cref="PipelineActor.StartAsync"/> itself resolved (no second compile here); every
/// <see cref="StopPipelineAsync"/> (and a failed start) unregisters it. This is the ONLY place besides
/// <see cref="Services.PipelineSupervisorService"/>'s boot-resume sweep that mutates the router — see that
/// service's doc comment for why a sweep-side repair is still needed for a self-healed actor.</para>
///
/// <para><b><see cref="Streaming.TableEventRouter"/> bookkeeping (W7-A) — same pattern, one more
/// router:</b> every successful <see cref="StartTableAsync"/> registers
/// <see cref="Streaming.TableEventRouter"/> with the stream/table input names
/// <see cref="TableActor.StartAsync"/> itself resolved; every <see cref="StopTableAsync"/> (and a failed
/// start) unregisters it. <see cref="Services.TableSupervisorService"/>'s boot-resume sweep is the only
/// other place that mutates it, for the same self-healed-actor reason as the pipeline router.</para>
/// </summary>
public sealed partial class DaprLifecycleOrchestrator(
    DaprClient daprClient,
    Streaming.PipelineEventRouter pipelineRouter,
    Streaming.TableEventRouter tableRouter,
    ILogger<DaprLifecycleOrchestrator> logger,
    // Plan 008 W4c: added LAST, with a default, so GeneratorLifecycleOrchestratorTests.NewOrchestrator's
    // pre-existing 4-positional-argument construction (an existing test file — never edited for a
    // production signature change) keeps compiling unmodified. The DI container still injects the real
    // registered SourceIngressRegistry singleton here in the running host regardless of the default —
    // .NET's built-in ServiceProvider only falls back to a parameter's default value when the parameter's
    // type has NO registration at all (see IngestRuntimeSetup.AddServices, called from Program.cs before
    // the container is built); the default only matters for the direct `new(...)` that unit test uses,
    // where no method needing ingressRegistry is ever invoked (see that test's own class doc comment).
    SourceIngressRegistry ingressRegistry = null!) : ILifecycleOrchestrator
{
    /// <summary>Plan 006 (ingestion connectors) W3-B, extended by plan 008 W4c to a THIRD kind:
    /// dispatches on <see cref="SourceDefinition.Kind"/> via <see cref="SourceKindDispatch.Classify"/> —
    /// generator-kind (null/""/"generator") goes to <see cref="IGeneratorActor"/>, connector kinds
    /// (url/file/folder/grpc) to <see cref="IConnectorActor"/>, and <see cref="SourceKinds.Ingest"/> goes
    /// to NEITHER — an ingest-kind source has no actor at all (see <see cref="Ingest.DaprIngressFacade"/>'s
    /// class doc: its <see cref="SourceIngressBuffer"/> lives in a host-process singleton, not a
    /// grain/actor).
    ///
    /// <para><b>Always stops/cleans up the OTHER kinds' state too, idempotently.</b> A source's Kind can
    /// change on an update (<c>CatalogStore.UpsertSourceAsync</c> calls this on every upsert, including
    /// edits to an existing source — see that method's doc comment), and this method has no cheap way to
    /// know in-line whether THIS particular call is such a kind-change (that would mean reading the
    /// previous definition back — the exact kind of registry-turn read-back the reentrancy decision
    /// forbids). <see cref="IGeneratorActor.StopAsync"/>/<see cref="IConnectorActor.StopAsync"/> are
    /// already documented idempotent (safe on an actor that was never started, or already stopped), and
    /// <see cref="SourceIngressRegistry.Remove"/> is documented idempotent the same way ("a no-op if none
    /// exists") — so unconditionally stopping/clearing every kind's state EXCEPT the one this call
    /// actually dispatches to is simply a no-op extra call in the common case (Kind unchanged) and the
    /// correct cleanup in the kind-changed case (including a kind changing AWAY from Ingest, which is
    /// exactly when <see cref="SourceIngressRegistry.Remove"/>'s own doc comment says to call it), with
    /// no branch needed to tell the two apart.</para></summary>
    public async Task NotifySourceChangedAsync(SourceDefinition def)
    {
        var kind = SourceKindDispatch.Classify(def.Kind);

        if (kind != SourceKindDispatch.ActorKind.Generator)
        {
            await GeneratorActorProxy(def.Environment, def.Name).StopAsync();
        }

        if (kind != SourceKindDispatch.ActorKind.Connector)
        {
            await ConnectorActorProxy(def.Environment, def.Name).StopAsync();
        }

        if (kind != SourceKindDispatch.ActorKind.Ingest)
        {
            // Plan 021: qualified so the registry entry a same-named source in a DIFFERENT environment
            // holds is never touched by this one's Remove — see SourceIngressRegistry's own key contract
            // (it is keyed by whatever string this project hands it, with no environment concept of its
            // own).
            ingressRegistry.Remove(EnvKeys.Qualify(def.Environment, def.Name));
        }

        switch (kind)
        {
            case SourceKindDispatch.ActorKind.Generator:
                var generator = GeneratorActorProxy(def.Environment, def.Name);
                if (def.Enabled)
                {
                    await generator.StartAsync(def);
                }
                else
                {
                    await generator.StopAsync();
                }

                break;

            case SourceKindDispatch.ActorKind.Connector:
                var connector = ConnectorActorProxy(def.Environment, def.Name);
                if (def.Enabled)
                {
                    await connector.StartAsync(def);
                }
                else
                {
                    await connector.StopAsync();
                }

                break;

            case SourceKindDispatch.ActorKind.Ingest:
                // Nothing to start: an ingest-kind source's buffer is created lazily on its first
                // PushAsync (or rebuilt automatically if IngestConfig changes — see
                // SourceIngressRegistry.GetOrCreate's fingerprint check), not eagerly here.
                break;

            case SourceKindDispatch.ActorKind.Crdt:
                // Plan 020 D9: Orleans-first. There is no CrdtDocActor on this flavor, so the kind is
                // stored (a catalog exported from an Orleans instance imports here intact and can be
                // promoted back without loss — the same bargain ShardBy strikes, see CatalogStore's
                // "WHERE KEY SHARDING IS REFUSED ON THIS FLAVOR" note) and never runs.
                //
                // It is refused LOUDLY and only here, because "looks armed and never emits" is the one
                // outcome that must not happen. What is still missing relative to the ShardBy precedent
                // is a Failed status carrying this text: a sharded table gets one because TableActor
                // exists to hold it, and a crdt source has no actor at all on this flavor. Tracked in
                // dapr/PARITY.md; the escape hatch meanwhile is plan 006's cross-flavour grpc link,
                // which lets a Dapr instance subscribe a document projected by an Orleans one.
                if (def.Enabled)
                {
                    logger.LogError(
                        "Source '{Source}' has kind '{Kind}', which is Orleans-only (plan 020 D9) — this " +
                        "flavor stores the definition but will never run it, so this source emits nothing. " +
                        "Run it on an Orleans instance and subscribe to it here with a 'grpc' source.",
                        def.Name,
                        def.Kind);
                }

                break;
        }
    }

    public async Task NotifySourceRemovedAsync(string name, string environment)
    {
        await GeneratorActorProxy(environment, name).StopAsync();
        await ConnectorActorProxy(environment, name).StopAsync();
        ingressRegistry.Remove(EnvKeys.Qualify(environment, name));
    }

    /// <summary>Plan 021 D3: every name-keyed actor id in this class goes through
    /// <see cref="EnvKeys.Qualify"/> — <see cref="EnvKeys.Qualify(string?,string)"/> is a no-op for the
    /// default environment (D2), so every one of these is byte-identical to the pre-021 literal
    /// <c>new ActorId(sourceName)</c> when no environment is in play.</summary>
    private static IGeneratorActor GeneratorActorProxy(string environment, string sourceName) =>
        ActorProxy.Create<IGeneratorActor>(new ActorId(EnvKeys.Qualify(environment, sourceName)), nameof(GeneratorActor), ActorProxyDefaults.Options);

    private static IConnectorActor ConnectorActorProxy(string environment, string sourceName) =>
        ActorProxy.Create<IConnectorActor>(new ActorId(EnvKeys.Qualify(environment, sourceName)), nameof(ConnectorActor), ActorProxyDefaults.Options);

    /// <summary>Compiles (inside <see cref="PipelineActor.StartAsync"/>, not here — see the class doc's
    /// "acyclic by construction" note) and starts <paramref name="def"/>'s pipeline actor, then registers
    /// <see cref="Streaming.PipelineEventRouter"/> with the source names the actor's own compile
    /// resolved. On a compile/start failure, unregisters the router (defensive — a pipeline that failed
    /// to start must not be left routable) and turns the actor's error message into a
    /// <see cref="LifecycleOutcome.Failure"/> for <c>CatalogStore</c> to record as
    /// <c>Status=Failed</c>/<c>Error</c>, mirroring <c>PipelineGrain.StartAsync</c>'s thrown-exception
    /// outcome without letting an exception cross the Dapr actor-invocation boundary (see
    /// <see cref="ActorResult{T}"/>'s class doc).</summary>
    public async Task<LifecycleOutcome> StartPipelineAsync(PipelineDefinition def, IReadOnlyList<SourceDefinition> sources)
    {
        var actor = PipelineActorProxy(def.Id);
        var result = await actor.StartAsync(new PipelineStartRequest(def, sources.ToList()));
        if (!result.Ok)
        {
            pipelineRouter.Unregister(def.Id);
            return LifecycleOutcome.Failure(result.Error ?? "pipeline failed to start");
        }

        // Plan 021 D6: the compiled source names PipelineActor.StartAsync resolved are BARE (compiled
        // against this pipeline's own environment's catalog — see CatalogStore.BuildStreamSchemas, which
        // never needs qualification itself because each RegistryActor's state is already one environment's
        // whole catalog, D1). The ROUTER's index is shared process-wide across every environment's
        // generators publishing on the same five fixed topics (D6), so its keys — and the envelope.Source
        // every publisher stamps — have to be qualified here, at the boundary, or two same-named sources
        // in two environments would collide in one dictionary entry and cross-route each other's events.
        pipelineRouter.Register(def.Id, (result.Value ?? []).Select(s => EnvKeys.Qualify(def.Environment, s)).ToList());
        return LifecycleOutcome.Success;
    }

    public async Task StopPipelineAsync(string pipelineId)
    {
        await PipelineActorProxy(pipelineId).StopAsync();
        pipelineRouter.Unregister(pipelineId);
    }

    private static IPipelineActor PipelineActorProxy(string pipelineId) =>
        ActorProxy.Create<IPipelineActor>(new ActorId(pipelineId), nameof(PipelineActor), ActorProxyDefaults.Options);

    // ------------------------------------------------------------------
    // Tables (W7-A): real as of this wave. History (W7-B) is the partial file
    // DaprLifecycleOrchestrator.History.cs — still W4's warn-and-succeed no-op, untouched here.
    // ------------------------------------------------------------------

    /// <summary>Compiles (inside <see cref="TableActor.StartAsync"/>, not here — see the class doc's
    /// "acyclic by construction" note) and starts <paramref name="def"/>'s table actor, then registers
    /// <see cref="Streaming.TableEventRouter"/> with the stream/table input names the actor's own compile
    /// resolved. On a compile/start/Parallelism-rejection failure, unregisters the router (defensive — a
    /// table that failed to start must not be left routable) and turns the actor's error message into a
    /// <see cref="LifecycleOutcome.Failure"/> for <c>CatalogStore</c> to record as
    /// <c>Status=Failed</c>/<c>Error</c>, mirroring <see cref="StartPipelineAsync"/>'s identical
    /// exception-free actor-boundary contract.</summary>
    public async Task<LifecycleOutcome> StartTableAsync(TableDefinition def, IReadOnlyList<SourceDefinition> sources, IReadOnlyList<TableDefinition> tables)
    {
        var actor = TableActorProxy(def.Environment, def.Name);
        var qualifiedName = EnvKeys.Qualify(def.Environment, def.Name);
        var result = await actor.StartAsync(new TableStartRequest(def, sources.ToList(), tables.ToList()));
        if (!result.Ok)
        {
            tableRouter.Unregister(qualifiedName);
            return LifecycleOutcome.Failure(result.Error ?? "table failed to start");
        }

        // Plan 021 D6: same reasoning as StartPipelineAsync above — StreamInputs/TableInputs are BARE
        // (this table's own environment's catalog), the router is shared process-wide, so both the
        // router's own key (this table's qualified name) and every input it fans in from must be
        // qualified with THIS table's environment before they go in the index.
        tableRouter.Register(
            qualifiedName,
            result.Value!.StreamInputs.Select(s => EnvKeys.Qualify(def.Environment, s)).ToList(),
            result.Value.TableInputs.Select(t => EnvKeys.Qualify(def.Environment, t)).ToList());
        return LifecycleOutcome.Success;
    }

    public async Task StopTableAsync(string tableName, string environment)
    {
        await TableActorProxy(environment, tableName).StopAsync();
        tableRouter.Unregister(EnvKeys.Qualify(environment, tableName));
    }

    private static ITableActor TableActorProxy(string environment, string tableName) =>
        ActorProxy.Create<ITableActor>(new ActorId(EnvKeys.Qualify(environment, tableName)), nameof(TableActor), ActorProxyDefaults.Options);

    private void WarnNoRuntime(string action, string id) =>
        logger.LogWarning("{Action}({Id}): no runtime yet (W7-B) — catalog status updated, no process started.", action, id);

    /// <summary>Real publish to the <c>sf-lifecycle</c> topic (decision D-D) — the one W4 left as a log
    /// line only (see <see cref="NoopLifecycleOrchestrator.PublishLifecycleAsync"/>). Publishing here is
    /// an outward call to Dapr pub/sub, not an actor call, so it carries none of the reentrancy
    /// considerations discussed in this class's own doc comment.</summary>
    public async Task PublishLifecycleAsync(string entityId, string kind, PipelineStatus status)
    {
        await daprClient.PublishEventAsync(StreamingRuntimeSetup.PubsubName, StreamingRuntimeSetup.LifecycleTopic, new LifecycleEvent
        {
            PipelineId = entityId,
            Kind = kind,
            Status = status,
            TimestampMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
        });
    }
}

// SourceKindDispatch (the pure "which actor/grain type owns this source" classification) moved to
// shared/StreamForge.Contracts/SourceKindDispatch.cs in plan 009 wave D — both flavors already reference
// that assembly (StreamForge.Abstractions, already `using`d above), so the Orleans flavor's own
// independently hand-written RegistryGrain/GeneratorSupervisorService/IngestDrainPumpService dispatch logic
// could be collapsed onto the exact same tested implementation instead of staying a second copy here. See
// that type's own class doc for the full history (plan 006 W3-B origin, the plan 008 W4c Ingest addition,
// and the "kind != generator" defect that motivated sharing it in the first place).
