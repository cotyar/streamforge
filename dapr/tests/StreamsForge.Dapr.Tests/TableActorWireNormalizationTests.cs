using System.Text.Json;
using StreamsForge.Abstractions;
using StreamsForge.Abstractions.Streaming;
using StreamsForge.AppCore.Json;
using Xunit;

namespace StreamsForge.Dapr.Tests;

/// <summary>
/// Plan 005 (Dapr sibling runtime) W7-A: proves the same JsonElement-across-the-actor-wire finding
/// <c>PipelineActorWireNormalizationTests</c> proves for <c>PipelineActor</c> — but for BOTH of
/// <see cref="StreamsForge.Dapr.Host.Actors.TableActor"/>'s two ingress methods,
/// <c>ProcessSourceEventsAsync(SourceEventsEnvelope)</c> AND <c>ProcessTableDeltasAsync(TableDeltaEnvelope)</c>.
/// The second is a genuinely NEW wire-crossing scenario <c>PipelineActor</c> never has to deal with (a
/// pipeline never consumes another table's delta batch) — <c>TableDeltaEnvelope.Deltas[].Row</c> is a
/// fresh <c>Dictionary&lt;string, object?&gt;</c> that crosses the identical Dapr actor-invocation wire
/// (<c>ActorProxyOptions.UseJsonSerialization = true</c> — see dapr/ARCHITECTURE.md's serialization note),
/// so it needs the exact same re-normalization treatment, independently verified here.
///
/// <para><b>The finding (see <c>PipelineActorWireNormalizationTests</c> for the full explanation):</b>
/// even though <c>Streaming/StreamingRuntimeSetup.cs</c> already normalizes every event/delta dictionary
/// once at pub/sub ingress (<c>NormalizeSourceEvents</c>/<c>NormalizeTableDeltaRows</c>), that normalization
/// does NOT survive a SECOND JSON round trip — and the Dapr actor-invocation call
/// (<c>ActorProxy.Create&lt;ITableActor&gt;(...).ProcessSourceEventsAsync/ProcessTableDeltasAsync(envelope)</c>)
/// is exactly that second round trip. System.Text.Json has no static type information for a
/// <c>Dictionary&lt;string, object?&gt;</c> value at deserialization time, so every value comes back out as
/// a <see cref="JsonElement"/> again regardless of what it was on the publish side.</para>
/// </summary>
public class TableActorWireNormalizationTests
{
    /// <summary>Default (no options) — mirrors the Dapr .NET SDK's actor-invocation JSON serializer, same
    /// rationale as <c>PipelineActorWireNormalizationTests.ActorWireOptions</c>.</summary>
    private static readonly JsonSerializerOptions ActorWireOptions = new();

    [Fact]
    public void SourceEventsEnvelope_AlreadyNormalizedOnce_StillDeserializesAsJsonElementAfterAnActorWireRoundTrip()
    {
        var alreadyNormalized = new SourceEventsEnvelope
        {
            Source = "trades",
            Events =
            [
                new Dictionary<string, object?> { ["symbol"] = "AAPL", ["qty"] = 10L, ["price"] = 101.5 },
            ],
        };
        Assert.IsType<string>(alreadyNormalized.Events[0]["symbol"]); // sanity: genuinely plain CLR going in

        var wireJson = JsonSerializer.Serialize(alreadyNormalized, ActorWireOptions);
        var afterWireRoundTrip = JsonSerializer.Deserialize<SourceEventsEnvelope>(wireJson, ActorWireOptions)!;

        var evt = afterWireRoundTrip.Events[0];
        Assert.IsType<JsonElement>(evt["symbol"]);
        Assert.IsType<JsonElement>(evt["qty"]);
        Assert.IsType<JsonElement>(evt["price"]);
    }

    [Fact]
    public void SourceEventsEnvelope_AfterActorWireRoundTrip_NormalizeInPlaceRestoresPlainClrValues()
    {
        var envelope = new SourceEventsEnvelope
        {
            Source = "trades",
            Events = [new Dictionary<string, object?> { ["symbol"] = "AAPL", ["qty"] = 10L, ["price"] = 101.5 }],
        };

        var wireJson = JsonSerializer.Serialize(envelope, ActorWireOptions);
        var afterWireRoundTrip = JsonSerializer.Deserialize<SourceEventsEnvelope>(wireJson, ActorWireOptions)!;
        var evt = afterWireRoundTrip.Events[0];

        // This is exactly the line TableActor.ProcessSourceEventsAsync runs on each event dictionary
        // before constructing an EventRecord/handing it to TableExecutor.OnStreamEvent.
        JsonValueNormalizer.NormalizeInPlace(evt);

        Assert.IsType<string>(evt["symbol"]);
        Assert.Equal("AAPL", evt["symbol"]);
        Assert.IsType<long>(evt["qty"]);
        Assert.Equal(10L, evt["qty"]);
        Assert.IsType<double>(evt["price"]);
        Assert.Equal(101.5, evt["price"]);
    }

    [Fact]
    public void TableDeltaEnvelope_AlreadyNormalizedOnce_StillDeserializesAsJsonElementAfterAnActorWireRoundTrip()
    {
        var alreadyNormalized = new TableDeltaEnvelope
        {
            Table = "positions",
            Seq = 7,
            Deltas =
            [
                new TableDeltaDto
                {
                    Row = new Dictionary<string, object?> { ["symbol"] = "AAPL", ["trades"] = 3L, ["avg_price"] = 101.5 },
                    Weight = 1,
                },
            ],
        };
        Assert.IsType<string>(alreadyNormalized.Deltas[0].Row["symbol"]); // sanity: genuinely plain CLR going in

        var wireJson = JsonSerializer.Serialize(alreadyNormalized, ActorWireOptions);
        var afterWireRoundTrip = JsonSerializer.Deserialize<TableDeltaEnvelope>(wireJson, ActorWireOptions)!;

        var row = afterWireRoundTrip.Deltas[0].Row;
        Assert.IsType<JsonElement>(row["symbol"]);
        Assert.IsType<JsonElement>(row["trades"]);
        Assert.IsType<JsonElement>(row["avg_price"]);
    }

    [Fact]
    public void TableDeltaEnvelope_AfterActorWireRoundTrip_NormalizeInPlaceRestoresPlainClrValues()
    {
        var envelope = new TableDeltaEnvelope
        {
            Table = "positions",
            Seq = 7,
            Deltas =
            [
                new TableDeltaDto
                {
                    Row = new Dictionary<string, object?> { ["symbol"] = "AAPL", ["trades"] = 3L },
                    Weight = 1,
                },
            ],
        };

        var wireJson = JsonSerializer.Serialize(envelope, ActorWireOptions);
        var afterWireRoundTrip = JsonSerializer.Deserialize<TableDeltaEnvelope>(wireJson, ActorWireOptions)!;
        var row = afterWireRoundTrip.Deltas[0].Row;

        // This is exactly the line TableActor.ProcessTableDeltasAsync runs on each delta's Row dictionary
        // before constructing an EventRecord/handing it to TableExecutor.OnTableDelta.
        JsonValueNormalizer.NormalizeInPlace(row);

        Assert.IsType<string>(row["symbol"]);
        Assert.Equal("AAPL", row["symbol"]);
        Assert.IsType<long>(row["trades"]);
        Assert.Equal(3L, row["trades"]);
    }
}
