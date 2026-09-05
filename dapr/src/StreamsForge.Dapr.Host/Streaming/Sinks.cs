using StreamsForge.Abstractions.Streaming;

namespace StreamsForge.Dapr.Host.Streaming;

// ============================================================================
// Plan 005 (Dapr sibling runtime) W5-B: the in-process fan-out seam later waves plug additional
// consumers into. Every fixed pub/sub topic (decision D-D) that carries "raw ingress" data — as
// opposed to data only the SignalR bridge cares about — gets a matching sink interface here, resolved
// via DI as IEnumerable<T> at the topic's subscription endpoint (Streaming/StreamingRuntimeSetup.cs).
// Registering an additional implementation is the entire integration surface for a later wave: no
// change to the endpoint, no change to Program.cs (frozen during W5), no change to this file beyond
// what's already declared.
//
//   sf-sources     -> ISourceEventsSink   (W6's PipelineActor routes matching sources into SQL
//                                          execution; W7's TableActor routes matching sources into
//                                          Z-set ingestion — both register an extra sink, they don't
//                                          replace this one)
//   sf-table-delta -> ITableDeltaSink     (W7-B's TableHistoryActor registers one, fed by table
//                                          deltas to build row history)
//   sf-pipeline-out -> IPipelineResultsSink (plan 025: table-over-pipeline routing and the gRPC
//                                          per-entity fan-out both consume pipeline output, so this
//                                          topic became a generic sink too — it used to call the
//                                          bridge and the NATS publisher directly by concrete type)
//
// DaprStreamBridge (Streaming/DaprStreamBridge.cs) implements all THREE interfaces itself (so it's
// just another entry in the IEnumerable<T> the endpoint iterates) — it is ALSO invoked directly, by
// concrete type, for the two topics that have no other consumer in this project
// (sf-lifecycle/sf-metrics: nothing except "relay to SignalR" ever needs those).
// ============================================================================

/// <summary>Registered consumer of topic <c>sf-sources</c> — a batch of raw events published by one
/// source (a GeneratorActor's timer tick, or any polyglot sidecar publishing into the platform; the
/// router treats both identically, decision D-D's "polyglot door"). The endpoint normalizes every
/// event dictionary's <see cref="System.Text.Json.JsonElement"/> values (StreamsForge.AppCore.Json.
/// JsonValueNormalizer.NormalizeInPlace) BEFORE calling any sink, so implementations always see plain
/// CLR values (string/long/double/bool/null/Dictionary/List) in <see cref="SourceEventsEnvelope.Events"/>
/// — never a raw JsonElement.</summary>
public interface ISourceEventsSink
{
    Task OnSourceEventsAsync(SourceEventsEnvelope envelope);
}

/// <summary>Registered consumer of topic <c>sf-table-delta</c> — a batch of Z-set deltas for one table.
/// Same normalization guarantee as <see cref="ISourceEventsSink"/>: every <c>TableDeltaDto.Row</c>
/// dictionary in <see cref="TableDeltaEnvelope.Deltas"/> is normalized before any sink sees it.</summary>
public interface ITableDeltaSink
{
    Task OnTableDeltaAsync(TableDeltaEnvelope envelope);
}

/// <summary>Registered consumer of topic <c>sf-pipeline-out</c> — one pipeline's batch of result rows
/// (<see cref="PipelineResultsEnvelope.PipelineId"/> is the environment-qualified pipeline id). Same
/// normalization guarantee as the other two sinks: every <c>ResultEnvelope.Row</c> in
/// <see cref="PipelineResultsEnvelope.Results"/> is normalized before any sink sees it.</summary>
public interface IPipelineResultsSink
{
    Task OnPipelineResultsAsync(PipelineResultsEnvelope envelope);
}
