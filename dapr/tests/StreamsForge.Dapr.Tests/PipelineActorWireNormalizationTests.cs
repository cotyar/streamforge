using System.Text.Json;
using StreamsForge.Abstractions.Streaming;
using StreamsForge.AppCore.Json;
using Xunit;

namespace StreamsForge.Dapr.Tests;

/// <summary>
/// Plan 005 (Dapr sibling runtime) W6: proves the JsonElement-across-the-actor-wire finding the wave brief
/// calls out explicitly ("ProcessEventsAsync's envelope crosses the actor wire — re-normalize inside the
/// actor or verify it arrives CLR — TEST this explicitly").
///
/// <para><b>The finding:</b> <c>Streaming/StreamingRuntimeSetup.cs</c>'s <c>sf-sources</c> endpoint already
/// normalizes every event dictionary once, at pub/sub ingress (<c>NormalizeSourceEvents</c>), before
/// <c>PipelineEventRouter.OnSourceEventsAsync</c> ever sees it — so by the time the router calls
/// <c>IPipelineActor.ProcessEventsAsync(envelope)</c>, every <c>Dictionary&lt;string, object?&gt;</c> value
/// in that SAME envelope instance is already a plain CLR value (string/long/double/bool/null/...), not a
/// <see cref="JsonElement"/>. It would be reasonable to assume <c>PipelineActor.ProcessEventsAsync</c>
/// therefore never needs to normalize again.</para>
///
/// <para><b>That assumption is wrong.</b> <c>ActorProxy.Create&lt;IPipelineActor&gt;(...).ProcessEventsAsync(envelope)</c>
/// is NOT an in-process method call — it's a Dapr actor-invocation round trip (client proxy → sidecar →
/// this process's actor-method HTTP handler), serialized/deserialized via
/// <c>ActorProxyOptions.UseJsonSerialization = true</c> (see dapr/ARCHITECTURE.md's serialization note).
/// System.Text.Json has no static type information for a <c>Dictionary&lt;string, object?&gt;</c> value at
/// deserialization time, so every value — even ones that started as plain CLR on the publish side — comes
/// back out as a <see cref="JsonElement"/> AGAIN once it lands inside this actor's method body. This test
/// proves it with the exact same serializer configuration the actor wire uses (default
/// <see cref="JsonSerializerOptions"/> — no <c>PropertyNameCaseInsensitive</c>, no camelCase policy;
/// see <c>ActorProxyDefaults.cs</c>/Program.cs's <c>AddActors</c> call, neither of which configure a
/// custom <see cref="JsonSerializerOptions"/>), NOT the ASP.NET Core <c>Http.Json.JsonOptions</c> a
/// <c>StreamingNormalizationTests</c>-style round trip would use — those are two independently configured
/// serializer surfaces in this project (see dapr/POLYGLOT.md's case-handling note for the same
/// distinction on enums).</para>
///
/// <para><b>Conclusion, verified below:</b> <c>PipelineActor.ProcessEventsAsync</c>'s own
/// <see cref="JsonValueNormalizer.NormalizeInPlace"/> call is NOT redundant with the pub/sub-ingress
/// normalization — it is the only thing standing between the Engine and a <see cref="JsonElement"/> value
/// it doesn't know how to compare/serialize/JSON-path into. Removing it would silently break every
/// pipeline that receives events via <c>PipelineEventRouter</c> (i.e. every pipeline, since that's the
/// only path events reach a running <c>PipelineActor</c>).</para>
/// </summary>
public class PipelineActorWireNormalizationTests
{
    /// <summary>Default (no options) — mirrors the Dapr .NET SDK's actor-invocation JSON serializer when
    /// <c>ActorProxyOptions.UseJsonSerialization</c>/<c>ActorRuntimeOptions.UseJsonSerialization</c> are
    /// both true and neither side configures a custom <see cref="JsonSerializerOptions"/> (this project
    /// doesn't — see <c>Actors/ActorProxyDefaults.cs</c> and Program.cs's <c>AddActors</c> call).</summary>
    private static readonly JsonSerializerOptions ActorWireOptions = new();

    [Fact]
    public void Envelope_AlreadyNormalizedOnce_StillDeserializesAsJsonElementAfterAnActorWireRoundTrip()
    {
        // Start from an envelope whose event dictionary already holds PLAIN CLR values — exactly what
        // StreamingRuntimeSetup.NormalizeSourceEvents leaves behind at the sf-sources pub/sub ingress,
        // and exactly what PipelineEventRouter.OnSourceEventsAsync receives.
        var alreadyNormalized = new SourceEventsEnvelope
        {
            Source = "trades",
            Events =
            [
                new Dictionary<string, object?>
                {
                    ["symbol"] = "AAPL",
                    ["price"] = 101.5,
                    ["qty"] = 10L,
                    ["active"] = true,
                    ["meta"] = new Dictionary<string, object?> { ["tier"] = "gold" },
                },
            ],
        };
        Assert.IsType<string>(alreadyNormalized.Events[0]["symbol"]); // sanity: genuinely plain CLR going in

        // Simulate the actor-invocation wire: serialize exactly like the client-side ActorProxy call
        // does, then deserialize exactly like the server-side actor-method dispatch does.
        var wireJson = JsonSerializer.Serialize(alreadyNormalized, ActorWireOptions);
        var afterWireRoundTrip = JsonSerializer.Deserialize<SourceEventsEnvelope>(wireJson, ActorWireOptions)!;

        // The finding: despite starting fully normalized, every value is JsonElement again on this side
        // of the actor call.
        var evt = afterWireRoundTrip.Events[0];
        Assert.IsType<JsonElement>(evt["symbol"]);
        Assert.IsType<JsonElement>(evt["price"]);
        Assert.IsType<JsonElement>(evt["qty"]);
        Assert.IsType<JsonElement>(evt["active"]);
        Assert.IsType<JsonElement>(evt["meta"]);
    }

    [Fact]
    public void Envelope_AfterActorWireRoundTrip_NormalizeInPlaceRestoresPlainClrValues()
    {
        var alreadyNormalized = new SourceEventsEnvelope
        {
            Source = "trades",
            Events = [new Dictionary<string, object?> { ["symbol"] = "AAPL", ["price"] = 101.5, ["qty"] = 10L }],
        };

        var wireJson = JsonSerializer.Serialize(alreadyNormalized, ActorWireOptions);
        var afterWireRoundTrip = JsonSerializer.Deserialize<SourceEventsEnvelope>(wireJson, ActorWireOptions)!;
        var evt = afterWireRoundTrip.Events[0];

        // This is exactly the line PipelineActor.ProcessEventsAsync runs on each event dictionary before
        // constructing an EventRecord/handing it to PipelineExecutor.OnEvent.
        JsonValueNormalizer.NormalizeInPlace(evt);

        Assert.IsType<string>(evt["symbol"]);
        Assert.Equal("AAPL", evt["symbol"]);
        Assert.IsType<double>(evt["price"]);
        Assert.Equal(101.5, evt["price"]);
        Assert.IsType<long>(evt["qty"]);
        Assert.Equal(10L, evt["qty"]);
    }
}
