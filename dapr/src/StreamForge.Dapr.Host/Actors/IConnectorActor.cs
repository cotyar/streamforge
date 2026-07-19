using Dapr.Actors;
using StreamForge.Abstractions;

namespace StreamForge.Dapr.Host.Actors;

/// <summary>
/// Plan 006 (ingestion connectors) W3-B: actor-invocation surface for one connector-kind source's
/// runtime — actor type "ConnectorActor", key = the source's <see cref="SourceDefinition.Name"/>. Dapr
/// counterpart of the Orleans flavor's <c>IConnectorGrain</c> (W3-A). Handles the four non-generator
/// kinds (<see cref="SourceKinds.Url"/>/<see cref="SourceKinds.File"/>/<see cref="SourceKinds.Folder"/>/
/// <see cref="SourceKinds.Grpc"/>) — see <see cref="ConnectorActor"/> for the per-kind driving logic.
/// </summary>
public interface IConnectorActor : IActor
{
    /// <summary>(Re)starts this source's connector with the given definition, replacing any previous
    /// timer/subscriber task — same "definition wholly replaces the previous run" contract as
    /// <see cref="IGeneratorActor.StartAsync"/>. A definition with <c>Enabled == false</c> is accepted
    /// but starts nothing (persists <c>Running = false</c>). For url/file/folder kinds this arms a
    /// one-shot timer at the schedule's first due time (D-E backoff-aware); for the grpc kind this
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

    /// <summary>Marshal point for the grpc kind's background subscriber task (see
    /// <see cref="ConnectorActor"/>'s class doc — <c>GrpcSubscriberCore</c>'s <c>onRows</c>/<c>onStatus</c>
    /// callbacks run OFF this actor's turn, on a background thread; they call this method via a fresh
    /// <c>ActorProxy</c> to marshal the bookkeeping update back onto a normal actor turn). Not meant to be
    /// called by anything else. <paramref name="status"/>: "connecting" | "ok" | "error" (mirrors
    /// <c>GrpcSubscriberCore</c>'s own <c>onStatus</c> vocabulary); <paramref name="rowCount"/> is 0 for a
    /// pure status update.</summary>
    Task RecordSubscriberBatchAsync(int rowCount, string status, string? error);
}
