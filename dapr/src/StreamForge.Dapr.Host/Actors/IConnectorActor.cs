using Dapr.Actors;
using StreamForge.Abstractions;

namespace StreamForge.Dapr.Host.Actors;

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
    Task RecordSubscriberBatchAsync(
        int rowCount, string status, string? error, List<string>? dedupKeys = null, int coercionFailures = 0);
}
