using StreamsForge.AppCore.Ingest;
using Xunit;

namespace StreamsForge.Engine.Tests;

/// <summary>Plan 008 W4: IngressEnvelopeBuilder — pure shaping of accepted rows into what each host
/// publishes (one EventRecord per row for Orleans, one SourceEventsEnvelope per batch for Dapr).</summary>
public class IngressEnvelopeBuilderTests
{
    [Fact]
    public void ToEventRecords_produces_one_EventRecord_per_row_in_order()
    {
        var rows = new List<Dictionary<string, object?>>
        {
            new() { ["_source"] = "s", ["_ts"] = 1L, ["symbol"] = "AAPL" },
            new() { ["_source"] = "s", ["_ts"] = 2L, ["symbol"] = "MSFT" },
        };

        var events = IngressEnvelopeBuilder.ToEventRecords(rows);

        Assert.Equal(2, events.Count);
        Assert.Equal("AAPL", events[0]["symbol"]);
        Assert.Equal("MSFT", events[1]["symbol"]);
        Assert.Equal(1L, events[0].Timestamp);
        Assert.Equal("s", events[0].Source);
    }

    [Fact]
    public void ToEventRecords_of_an_empty_batch_is_an_empty_list()
    {
        var events = IngressEnvelopeBuilder.ToEventRecords([]);

        Assert.Empty(events);
    }

    [Fact]
    public void ToSourceEventsEnvelope_carries_the_source_name_and_every_row()
    {
        var rows = new List<Dictionary<string, object?>> { new() { ["a"] = 1L }, new() { ["a"] = 2L } };

        var envelope = IngressEnvelopeBuilder.ToSourceEventsEnvelope("my-source", rows);

        Assert.Equal("my-source", envelope.Source);
        Assert.Equal(2, envelope.Events.Count);
        Assert.Equal(1L, envelope.Events[0]["a"]);
    }
}
