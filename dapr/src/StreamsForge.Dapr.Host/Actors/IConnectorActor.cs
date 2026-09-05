using StreamsForge.Engine;
using Dapr.Actors;
using StreamsForge.Abstractions;

namespace StreamsForge.Dapr.Host.Actors;

/// <summary>
/// Plan 006 (ingestion connectors) W3-B: actor-invocation surface for one connector-kind source's
/// runtime — actor type "ConnectorActor", key = the source's <see cref="SourceDefinition.Name"/>. Dapr
/// counterpart of the Orleans flavor's <c>IConnectorGrain</c> (W3-A). Handles the non-generator kinds
/// (<see cref="SourceKinds.Url"/>/<see cref="SourceKinds.File"/>/<see cref="SourceKinds.Folder"/>/
/// <see cref="SourceKinds.Grpc"/>/<see cref="SourceKinds.Nats"/> — plan 009 B1 added the last one) —
/// see <see cref="ConnectorActor"/> for the per-kind driving logic.
/// </summary>
public interface IConnectorActor : IActor
{
    /// <summary>(Re)starts this source's connector with the given definition, replacing any previous
    /// timer/subscriber task — same "definition wholly replaces the previous run" contract as
    /// <see cref="IGeneratorActor.StartAsync"/>. A definition with <c>Enabled == false</c> is accepted
    /// but starts nothing (persists <c>Running = false</c>). For url/file/folder kinds this arms a
    /// one-shot timer at the schedule's first due time (D-E backoff-aware); for the grpc/nats kinds this
    /// launches a background reconnecting subscriber task. Idempotent — safe to call repeatedly with the
    /// current definition (what <see cref="Services.GeneratorSupervisorService"/>'s sweep does for
    /// connector-kind sources whose actor isn't already running — see that class's updated doc comment).
    /// </summary>
    Task StartAsync(SourceDefinition def);

    /// <summary>Stops the connector (unregisters its timer or cancels its subscriber task, whichever
    /// applies). Idempotent.</summary>
    Task StopAsync();

    /// <summary>True if this actor currently has a live timer armed or subscriber task running (backed
    /// by persisted state — see <see cref="ConnectorActor"/>'s class doc).</summary>
    Task<bool> IsRunningAsync();

    /// <summary>Current runtime status (plan 006 D-C) — backs <c>IConnectorStatusFacade</c>/
    /// <c>GET /api/sources/{name}/status</c>.</summary>
    Task<ConnectorRuntimeStatus> GetStatusAsync();

    /// <summary>Marshal point for the grpc/nats kinds' background subscriber task (see
    /// <see cref="ConnectorActor"/>'s class doc — <c>GrpcSubscriberCore</c>/<c>NatsSubscriberCore</c>'s
    /// <c>onRows</c>/<c>onStatus</c> callbacks run OFF this actor's turn, on a background thread; they
    /// call this method via a fresh <c>ActorProxy</c> to marshal the bookkeeping update back onto a
    /// normal actor turn). Not meant to be called by anything else. <paramref name="status"/>:
    /// "connecting" | "ok" | "error" (mirrors the subscriber cores' own <c>onStatus</c> vocabulary);
    /// <paramref name="rowCount"/> is 0 for a pure status update. <paramref name="dedupKeys"/> (plan 009
    /// B1, additive — default null keeps every pre-009/grpc call site byte-identical): the nats kind's
    /// <c>MappingSpec.DedupKeyField</c> tracker snapshot, non-null exactly when it changed this call —
    /// the background subscriber task cannot safely write actor state directly (see this class's own
    /// reentrancy discipline doc on <see cref="ConnectorActor"/>), so persisting it has to go through
    /// this same marshal point rather than a second one.</summary>
    /// <param name="coercionFailures">Plan 009 C2: field-level coercion failures in this batch, added to
    /// the cumulative counter. Additive with a zero default, so every pre-009 call site keeps its exact
    /// meaning.</param>
    /// <para><b>No optional parameters, and that is a hard Dapr constraint, not a style preference.</b>
    /// <c>Dapr.Actors.Description.MethodArgumentDescription.EnsureNotOutRefOptional</c> throws
    /// <see cref="ArgumentException"/> for any <c>out</c>/<c>ref</c>/optional parameter on an actor
    /// interface method, and it throws while <c>MapActorsHandlers</c> is building the dispatcher map — so
    /// a default value here does not degrade one call, it prevents the entire host process from starting.
    /// Both parameters below carried C# defaults from plan 009 until plan 021 wave 1 found the host
    /// unbootable because of them; every call site now passes them explicitly.</para>
    Task RecordSubscriberBatchAsync(
        int rowCount, string status, string? error, List<string>? dedupKeys, int coercionFailures);

    /// <summary>Plan 025 (Dapr parity, PARITY.md D6 "late-consumer replay") — the source-side half of the
    /// subscribe-then-attach protocol for STREAM inputs, the Dapr counterpart of
    /// <c>IConnectorGrain.BeginAttachAsync</c>. A table or pipeline that starts after this source already
    /// emitted calls this FIRST, then registers itself with the router, then feeds the returned
    /// <see cref="SourceAttachSnapshot.Rows"/> through its own live-traffic handler, then calls
    /// <see cref="EndAttachAsync"/> in a <c>finally</c>. While at least one attach hold is open this
    /// actor publishes NOTHING — rows a poll/subscriber produces meanwhile are queued and flushed, in
    /// order, when the last hold is released, so the consumer sees each row exactly once (in the
    /// snapshot OR on the live topic, never both). A hold that is never released is force-released by
    /// a 10 s safety timer, so a consumer that dies mid-attach cannot gate the source forever. Holds
    /// nest: N callers → N <see cref="EndAttachAsync"/> calls.</summary>
    Task<SourceAttachSnapshot> BeginAttachAsync();

    /// <summary>Releases one attach hold taken by <see cref="BeginAttachAsync"/>; at zero holds the rows
    /// deferred meanwhile are published. Idempotent past zero (a stray extra call is a no-op).</summary>
    Task EndAttachAsync();
}

/// <summary>Plan 025: what <see cref="IConnectorActor.BeginAttachAsync"/> hands a late consumer. Dapr
/// counterpart of the Orleans flavor's <c>SourceReplaySnapshot</c>. <see cref="Rows"/> is the most
/// recent rows this source published since activation (oldest first, already <c>_ts</c>/<c>_source</c>
/// stamped, at most <c>SourceReplayBuffer.Capacity</c> — the shared ring in
/// <c>StreamsForge.AppCore.Connectors</c>); <see cref="TotalSeen"/> is everything ever published,
/// including rows the ring has evicted, so <c>TotalSeen &gt; Rows.Count</c> tells the consumer it is
/// missing older rows it cannot recover. Crosses the actor wire as System.Text.Json (see Program.cs's
/// <c>UseJsonSerialization</c> note), so cell values come back as <c>JsonElement</c> — normalize with
/// <c>JsonValueNormalizer.NormalizeInPlace</c> exactly like the sf-sources topic endpoint does.</summary>
public sealed record SourceAttachSnapshot(List<EventRecord> Rows, long TotalSeen);
