using System.Text.Json;
using StreamsForge.Abstractions.Streaming;
using StreamsForge.Dapr.Host.Streaming;
using Xunit;

namespace StreamsForge.Dapr.Tests;

/// <summary>
/// Plan 005 W5-B: proves the topic-endpoint normalization step (StreamingRuntimeSetup.Normalize*)
/// actually eliminates <see cref="JsonElement"/> leakage from a real deserialized envelope — not just
/// from a hand-built <see cref="Dictionary{TKey,TValue}"/> the way JsonValueNormalizerTests (W4) does.
/// Every envelope here is built by ACTUALLY round-tripping a JSON string through
/// <see cref="JsonSerializer"/> (the same deserialization step <c>HttpRequest.ReadFromJsonAsync</c>
/// performs on the wire), so <c>Dictionary&lt;string, object?&gt;</c> values start life as real
/// <see cref="JsonElement"/> instances — exactly what a Dapr pub/sub subscriber sees for
/// <c>sf-sources</c>/<c>sf-table-delta</c>/<c>sf-pipeline-out</c> payloads.
/// </summary>
public class StreamingNormalizationTests
{
    private static readonly JsonSerializerOptions Options = new() { PropertyNameCaseInsensitive = true };

    [Fact]
    public void NormalizeSourceEvents_RoundTrippedEnvelope_ReplacesJsonElementValuesWithPlainClr()
    {
        const string json = """
            {
              "source": "trades",
              "events": [
                { "symbol": "AAPL", "price": 101.5, "qty": 10, "active": true, "meta": { "tier": "gold" } }
              ]
            }
            """;

        var envelope = JsonSerializer.Deserialize<SourceEventsEnvelope>(json, Options)!;
        var evt = envelope.Events[0];
        Assert.IsType<JsonElement>(evt["price"]); // sanity: still raw JsonElement before normalization

        StreamingRuntimeSetup.NormalizeSourceEvents(envelope);

        Assert.IsType<string>(evt["symbol"]);
        Assert.Equal("AAPL", evt["symbol"]);
        Assert.IsType<double>(evt["price"]);
        Assert.Equal(101.5, evt["price"]);
        Assert.IsType<long>(evt["qty"]);
        Assert.Equal(10L, evt["qty"]);
        Assert.IsType<bool>(evt["active"]);
        Assert.Equal(true, evt["active"]);
        var meta = Assert.IsType<Dictionary<string, object?>>(evt["meta"]);
        Assert.Equal("gold", meta["tier"]);
    }

    [Fact]
    public void NormalizeSourceEvents_MultipleEventsInBatch_NormalizesEveryEvent()
    {
        const string json = """
            {
              "source": "trades",
              "events": [
                { "symbol": "AAPL", "qty": 1 },
                { "symbol": "MSFT", "qty": 2 },
                { "symbol": "GOOG", "qty": 3 }
              ]
            }
            """;

        var envelope = JsonSerializer.Deserialize<SourceEventsEnvelope>(json, Options)!;
        StreamingRuntimeSetup.NormalizeSourceEvents(envelope);

        Assert.All(envelope.Events, evt => Assert.IsType<long>(evt["qty"]));
    }

    [Fact]
    public void NormalizeTableDeltaRows_RoundTrippedEnvelope_NormalizesEachDeltaRow()
    {
        const string json = """
            {
              "table": "positions",
              "seq": 3,
              "deltas": [
                { "row": { "symbol": "AAPL", "qty": 5 }, "weight": 1 },
                { "row": { "symbol": "MSFT", "qty": 2 }, "weight": -1 }
              ]
            }
            """;

        var envelope = JsonSerializer.Deserialize<TableDeltaEnvelope>(json, Options)!;
        Assert.IsType<JsonElement>(envelope.Deltas[0].Row["qty"]);

        StreamingRuntimeSetup.NormalizeTableDeltaRows(envelope);

        Assert.Equal(5L, envelope.Deltas[0].Row["qty"]);
        Assert.IsType<string>(envelope.Deltas[0].Row["symbol"]);
        Assert.Equal(2L, envelope.Deltas[1].Row["qty"]);
        // Non-Dictionary fields (Seq, Weight) are untouched scalars — never JsonElement to begin with.
        Assert.Equal(3L, envelope.Seq);
        Assert.Equal(1L, envelope.Deltas[0].Weight);
    }

    [Fact]
    public void NormalizePipelineResultRows_RoundTrippedEnvelope_NormalizesEachResultRow()
    {
        const string json = """
            {
              "pipelineId": "p1",
              "results": [
                { "pipelineId": "p1", "seq": 1, "timestampMs": 123, "row": { "total": 42.5, "count": 7 } }
              ]
            }
            """;

        var envelope = JsonSerializer.Deserialize<PipelineResultsEnvelope>(json, Options)!;
        Assert.IsType<JsonElement>(envelope.Results[0].Row["total"]);

        StreamingRuntimeSetup.NormalizePipelineResultRows(envelope);

        Assert.Equal(42.5, envelope.Results[0].Row["total"]);
        Assert.Equal(7L, envelope.Results[0].Row["count"]);
    }

    [Fact]
    public void Envelope_PascalCaseJsonPropertyNames_AlsoDeserializeCorrectly()
    {
        // Documents the case-handling half of the frozen polyglot contract (dapr/POLYGLOT.md): the app's
        // Microsoft.AspNetCore.Http.Json.JsonOptions (shared/StreamsForge.Api/StreamsForgeApiExtensions.cs'
        // ConfigureHttpJsonOptions call) leaves PropertyNameCaseInsensitive at its ASP.NET Core default of
        // true, so a publisher sending PascalCase keys works exactly like one sending camelCase — this
        // test's JsonSerializerOptions mirrors that setting explicitly rather than relying on a live host.
        const string pascalJson = """{ "Source": "trades", "Events": [ { "Symbol": "AAPL" } ] }""";

        var envelope = JsonSerializer.Deserialize<SourceEventsEnvelope>(pascalJson, Options)!;

        Assert.Equal("trades", envelope.Source);
        Assert.Single(envelope.Events);
    }
}
