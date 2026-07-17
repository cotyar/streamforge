using Orleans;
using StreamForge.Engine;

namespace StreamForge.Host.Serialization;

// StreamForge.Engine.EventRecord (a Dictionary<string, object?> subclass) is a frozen,
// third-party type with no [GenerateSerializer] attribute — it can't cross Orleans'
// serializer even for in-memory streams (MemoryAdapterFactory still serializes queued
// messages). This surrogate/converter pair teaches the Orleans serializer how to handle it
// without touching the frozen Engine contract.

[GenerateSerializer]
public struct EventRecordSurrogate
{
    [Id(0)]
    public Dictionary<string, object?> Data;
}

[RegisterConverter]
public sealed class EventRecordSurrogateConverter : IConverter<EventRecord, EventRecordSurrogate>
{
    public EventRecord ConvertFromSurrogate(in EventRecordSurrogate surrogate) =>
        new(surrogate.Data ?? []);

    public EventRecordSurrogate ConvertToSurrogate(in EventRecord value) =>
        new() { Data = new Dictionary<string, object?>(value) };
}
