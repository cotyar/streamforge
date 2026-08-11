using StreamForge.Abstractions.Streaming;
using StreamForge.Engine;

namespace StreamForge.AppCore.Ingest;

/// <summary>
/// Pure shaping of accepted rows into what each host actually publishes — no I/O, no stream/pub-sub
/// handles. Orleans publishes one <see cref="EventRecord"/> per row (matching GeneratorGrain's and
/// ConnectorGrain's existing one-OnNextAsync-per-row convention); Dapr publishes one
/// <see cref="SourceEventsEnvelope"/> per drained batch (the frozen "sf-sources" topic shape, D-D in
/// Envelopes.cs). The per-row-vs-per-batch choice stays on the host side — this only builds the
/// shapes from rows <see cref="IngressRowAcceptance"/> already accepted.
/// </summary>
public static class IngressEnvelopeBuilder
{
    public static List<EventRecord> ToEventRecords(IReadOnlyList<Dictionary<string, object?>> rows)
    {
        var result = new List<EventRecord>(rows.Count);
        foreach (var row in rows)
        {
            result.Add(new EventRecord(row));
        }
        return result;
    }

    public static SourceEventsEnvelope ToSourceEventsEnvelope(string sourceName, IReadOnlyList<Dictionary<string, object?>> rows)
        => new() { Source = sourceName, Events = [.. rows] };
}
