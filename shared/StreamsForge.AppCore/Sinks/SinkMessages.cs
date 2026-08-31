namespace StreamsForge.AppCore.Sinks;

// Plan 009 B2: wire shapes published to a NATS sink. Deliberately NOT StreamsForge.Contracts types —
// these never cross a grain/actor boundary and never need [GenerateSerializer]; they exist purely to be
// System.Text.Json-serialized into the bytes handed to NatsSinkClient.PublishAsync. Kept in AppCore
// (not Contracts, which is frozen/off-limits for this wave) since they're an implementation detail of
// the sink publisher, not part of the platform's cross-runtime contract surface.

/// <summary>One pipeline result row as published to a NATS sink. This is deliberately the SAME shape as
/// <see cref="StreamsForge.Abstractions.ResultEnvelope"/> (PipelineId/Seq/TimestampMs/Row) — a pipeline
/// sink message is simply one element of the batch StreamBridgeService/DaprStreamBridge already relay to
/// SignalR, published on its own rather than re-wrapped, since ResultEnvelope already carries everything
/// a downstream NATS consumer needs (which pipeline, which row, when).</summary>
public sealed class NatsPipelineRowMessage
{
    public string PipelineId { get; init; } = "";
    public long Seq { get; init; }
    public long TimestampMs { get; init; }
    public Dictionary<string, object?> Row { get; init; } = [];
}

/// <summary>One table-delta entry as published to a NATS sink. Unlike a pipeline row,
/// <see cref="StreamsForge.Abstractions.TableDeltaDto"/> alone carries neither the table's name nor the
/// delivering batch's sequence number — both are attached here so a single NATS message is
/// self-describing. <see cref="Seq"/> is the SAME value for every delta entry published from one
/// delivered batch (mirrors the SignalR <c>tableDelta</c> event, where every delta in one push shares one
/// seq) — it identifies the batch, not the individual row.</summary>
public sealed class NatsTableDeltaMessage
{
    public string Table { get; init; } = "";
    public long Seq { get; init; }
    public Dictionary<string, object?> Row { get; init; } = [];
    public long Weight { get; init; }
}
