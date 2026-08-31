namespace StreamsForge.Abstractions.Streaming;

// ============================================================================
// Plan 005 (Dapr sibling runtime), wave W1, decision D-D: fixed envelope topics.
//
// These immutable records are the Dapr flavor's internal pub/sub payloads — additive, forward-only
// members added to Contracts now so both the Orleans-side (which doesn't consume them yet) and the
// future Dapr host (wave W5+) compile against the exact same wire shape from day one. They mirror
// Orleans' own stream item shapes one for one:
//
//   topic            Orleans equivalent (StreamConstants)              this envelope
//   sf-sources        (SourcesNamespace, source)      stream of EventRecord   SourceEventsEnvelope
//   sf-pipeline-out   (OutputNamespace, pipelineId)    stream of ResultEnvelope PipelineResultsEnvelope
//   sf-table-delta    (TableDeltaNamespace, table)     stream of TableDeltaDto  TableDeltaEnvelope
//
// Once published, a topic's payload shape is part of the frozen polyglot contract (D-D) — any
// sidecar'd process (Python enricher, bun consumer, etc. — wave W8) depends on it — so field numbers
// here are forever, exactly like every other [GenerateSerializer] DTO in this project.
//
// SourceEventsEnvelope.Events is List<Dictionary<string, object?>> rather than
// StreamsForge.Engine.EventRecord itself: EventRecord is a Dictionary<string, object?> SUBCLASS, which
// needs a hand-written surrogate to cross Orleans' serializer (see
// StreamsForge.Host.Serialization.EventRecordSurrogate) and — more to the point here — Contracts has no
// dependency on StreamsForge.Engine (kept minimal per decision D-A). A plain Dictionary carries the
// identical row shape (including the reserved "_ts"/"_source" keys EventRecord's own accessors read)
// and is directly Orleans/JSON serializable with no surrogate, exactly like ResultEnvelope.Row/
// TableDeltaDto.Row/TableRowDto.Row already are elsewhere in this project.
// ============================================================================

/// <summary>Dapr pub/sub payload for topic <c>sf-sources</c>: a batch of raw events published by one
/// source (a GeneratorActor's timer tick, or any polyglot sidecar publishing into the platform — the
/// router treats both identically). Mirrors the Orleans stream keyed
/// (<see cref="StreamConstants.SourcesNamespace"/>, <see cref="Source"/>).</summary>
[GenerateSerializer]
public sealed class SourceEventsEnvelope
{
    [Id(0)] public string Source { get; set; } = "";
    [Id(1)] public List<Dictionary<string, object?>> Events { get; set; } = [];
}

/// <summary>Dapr pub/sub payload for topic <c>sf-pipeline-out</c>: a batch of emitted result rows from
/// one running pipeline. Mirrors the Orleans stream keyed
/// (<see cref="StreamConstants.OutputNamespace"/>, <see cref="PipelineId"/>).</summary>
[GenerateSerializer]
public sealed class PipelineResultsEnvelope
{
    [Id(0)] public string PipelineId { get; set; } = "";
    [Id(1)] public List<ResultEnvelope> Results { get; set; } = [];
}

/// <summary>Dapr pub/sub payload for topic <c>sf-table-delta</c>: a batch of Z-set deltas (rows
/// entering/leaving) for one table, stamped with the table's own monotonic sequence number. Mirrors
/// the Orleans stream keyed (<see cref="StreamConstants.TableDeltaNamespace"/>, <see cref="Table"/>).</summary>
[GenerateSerializer]
public sealed class TableDeltaEnvelope
{
    [Id(0)] public string Table { get; set; } = "";
    [Id(1)] public long Seq { get; set; }
    [Id(2)] public List<TableDeltaDto> Deltas { get; set; } = [];
}
