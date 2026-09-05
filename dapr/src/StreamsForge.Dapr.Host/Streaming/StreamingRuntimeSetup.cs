using System.Text.Json;
using StreamsForge.Abstractions;
using StreamsForge.Abstractions.Streaming;
using StreamsForge.AppCore.Json;

namespace StreamsForge.Dapr.Host.Streaming;

/// <summary>
/// Plan 005 W5-B: the streaming spine registration, called from Program.cs (frozen during wave W5 so
/// the parallel W5-A agent building GeneratorActor/DaprLifecycleOrchestrator never touches it).
/// <see cref="AddServices"/> registers <see cref="DaprStreamBridge"/> and forwards both fan-out
/// interfaces (Streaming/Sinks.cs) to it; <see cref="MapTopicEndpoints"/> maps one POST endpoint per
/// fixed envelope topic (decision D-D) ahead of <c>MapSubscribeHandler()</c>'s discovery pass in
/// Program.cs. See dapr/POLYGLOT.md for the frozen external-publisher JSON contract these endpoints
/// enforce (exact topic names, route paths, and the case-handling rule for incoming payloads).
/// </summary>
public static class StreamingRuntimeSetup
{
    /// <summary>Dapr pub/sub component name every endpoint below binds to — matches
    /// dapr/components/pubsub.yaml's <c>metadata.name</c>. The only component in play; egress topics
    /// (<c>sf-source-{name}</c>, W5-A's GeneratorActor) use the same component, just publish-only, so
    /// this router never subscribes them (see Sinks.cs's fan-out doc comment).</summary>
    public const string PubsubName = "pubsub";

    public const string SourcesTopic = "sf-sources";
    public const string PipelineOutTopic = "sf-pipeline-out";
    public const string TableDeltaTopic = "sf-table-delta";
    public const string LifecycleTopic = "sf-lifecycle";
    public const string MetricsTopic = "sf-metrics";

    public static void AddServices(IServiceCollection services)
    {
        services.AddSingleton<DaprStreamBridge>();
        // Both fan-out interfaces resolve to the SAME bridge instance registered above, so the bridge
        // is just one more entry in IEnumerable<ISourceEventsSink>/IEnumerable<ITableDeltaSink> at the
        // endpoints below — no special-cased dispatch path for it there.
        services.AddSingleton<ISourceEventsSink>(sp => sp.GetRequiredService<DaprStreamBridge>());
        services.AddSingleton<ITableDeltaSink>(sp => sp.GetRequiredService<DaprStreamBridge>());
        services.AddSingleton<IPipelineResultsSink>(sp => sp.GetRequiredService<DaprStreamBridge>());

        // Plan 005 W6: PipelineEventRouter registers as a SECOND ISourceEventsSink alongside the bridge
        // above — exactly what Sinks.cs's class doc anticipated ("W6's PipelineActor routes matching
        // sources into SQL execution... both register an extra sink, they don't replace this one"). It's
        // also injected directly by Lifecycle/DaprLifecycleOrchestrator.cs and
        // Services/PipelineSupervisorService.cs to maintain its routing table, so it's registered as its
        // own concrete singleton (not just behind the sink interface) here.
        services.AddSingleton<PipelineEventRouter>();
        services.AddSingleton<ISourceEventsSink>(sp => sp.GetRequiredService<PipelineEventRouter>());

        // Plan 009 B2: the NATS sink publisher. Registered as a singleton (so the sf-pipeline-out
        // endpoint below and the ITableDeltaSink fan-out resolve the SAME instance the BackgroundService
        // itself is) and forwarded to ITableDeltaSink, mirroring DaprStreamBridge's own registration
        // shape immediately above. Plan 025 made sf-pipeline-out a generic IPipelineResultsSink fan-out
        // (table-over-pipeline routing and the gRPC per-entity streams consume it too), so it is one more
        // entry there rather than the direct concrete-type call it used to be.
        services.AddSingleton<NatsSinkPublisherService>();
        services.AddSingleton<ITableDeltaSink>(sp => sp.GetRequiredService<NatsSinkPublisherService>());
        services.AddSingleton<IPipelineResultsSink>(sp => sp.GetRequiredService<NatsSinkPublisherService>());
        services.AddHostedService(sp => sp.GetRequiredService<NatsSinkPublisherService>());
    }

    public static void MapTopicEndpoints(WebApplication app)
    {
        var logger = app.Services.GetRequiredService<ILoggerFactory>()
            .CreateLogger("StreamsForge.Dapr.Host.Streaming.TopicEndpoints");

        // sf-sources: fan out to every registered ISourceEventsSink (the bridge today; W6/W7 add their
        // own routing sinks alongside it — see Sinks.cs).
        app.MapPost($"/{SourcesTopic}", async (HttpContext ctx, IEnumerable<ISourceEventsSink> sinks) =>
        {
            var envelope = await TryReadAsync<SourceEventsEnvelope>(ctx, logger, SourcesTopic);
            if (envelope is null)
            {
                return Results.Ok();
            }

            NormalizeSourceEvents(envelope);
            await DispatchSourceEventsAsync(envelope, sinks);
            return Results.Ok();
        }).WithTopic(PubsubName, SourcesTopic);

        // sf-table-delta: fan out to every registered ITableDeltaSink (the bridge today; W7-B's
        // TableHistoryActor adds its own).
        app.MapPost($"/{TableDeltaTopic}", async (HttpContext ctx, IEnumerable<ITableDeltaSink> sinks) =>
        {
            var envelope = await TryReadAsync<TableDeltaEnvelope>(ctx, logger, TableDeltaTopic);
            if (envelope is null)
            {
                return Results.Ok();
            }

            NormalizeTableDeltaRows(envelope);
            await DispatchTableDeltaAsync(envelope, sinks);
            return Results.Ok();
        }).WithTopic(PubsubName, TableDeltaTopic);

        // sf-pipeline-out: fan out to every registered IPipelineResultsSink (the bridge, the NATS sink
        // publisher, and since plan 025 the table-over-pipeline router and the gRPC entity fan-out).
        app.MapPost($"/{PipelineOutTopic}", async (HttpContext ctx, IEnumerable<IPipelineResultsSink> sinks) =>
        {
            var envelope = await TryReadAsync<PipelineResultsEnvelope>(ctx, logger, PipelineOutTopic);
            if (envelope is null)
            {
                return Results.Ok();
            }

            NormalizePipelineResultRows(envelope);
            await DispatchPipelineResultsAsync(envelope, sinks);
            return Results.Ok();
        }).WithTopic(PubsubName, PipelineOutTopic);

        // sf-lifecycle / sf-metrics: nothing in this project ever needs these besides "relay to
        // SignalR" (see DaprStreamBridge's class doc comment), so these two call the bridge directly by
        // concrete type instead of through a dedicated sink interface.
        app.MapPost($"/{LifecycleTopic}", async (HttpContext ctx, DaprStreamBridge bridge) =>
        {
            var evt = await TryReadAsync<LifecycleEvent>(ctx, logger, LifecycleTopic);
            if (evt is null)
            {
                return Results.Ok();
            }

            await bridge.OnLifecycleEventAsync(evt);
            return Results.Ok();
        }).WithTopic(PubsubName, LifecycleTopic);

        app.MapPost($"/{MetricsTopic}", async (HttpContext ctx, DaprStreamBridge bridge) =>
        {
            var metrics = await TryReadAsync<PipelineMetrics>(ctx, logger, MetricsTopic);
            if (metrics is null)
            {
                return Results.Ok();
            }

            await bridge.OnMetricsAsync(metrics);
            return Results.Ok();
        }).WithTopic(PubsubName, MetricsTopic);
    }

    /// <summary>Normalizes every event dictionary in <paramref name="envelope"/> in place (decision D-D:
    /// "JsonElement values are normalized at every topic ingress"). Extracted as its own method — rather
    /// than inlined in the endpoint lambda — so normalization is directly unit-testable against a fake
    /// envelope built by round-tripping JSON through <see cref="JsonSerializer"/> (StreamingNormalizationTests),
    /// without needing an HTTP request/response at all.</summary>
    public static void NormalizeSourceEvents(SourceEventsEnvelope envelope)
    {
        foreach (var evt in envelope.Events)
        {
            JsonValueNormalizer.NormalizeInPlace(evt);
        }
    }

    /// <summary>Normalizes every delta's <c>Row</c> dictionary in <paramref name="envelope"/> in place —
    /// same rationale as <see cref="NormalizeSourceEvents"/>.</summary>
    public static void NormalizeTableDeltaRows(TableDeltaEnvelope envelope)
    {
        foreach (var delta in envelope.Deltas)
        {
            JsonValueNormalizer.NormalizeInPlace(delta.Row);
        }
    }

    /// <summary>Normalizes every result's <c>Row</c> dictionary in <paramref name="envelope"/> in place.
    /// Extends decision D-D's normalization requirement from sf-sources/sf-table-delta (the two topics
    /// the wave brief calls out by name, because those are the ones an external polyglot publisher can
    /// write into) to sf-pipeline-out too: <c>ResultEnvelope.Row</c> is the identical
    /// <c>Dictionary&lt;string, object?&gt;</c> shape and crosses the same JSON pub/sub wire — once this
    /// envelope round-trips through Redis and System.Text.Json deserializes it back into this process,
    /// its dictionary values are <see cref="JsonElement"/> regardless of whether the original publisher
    /// was in-process (W6's PipelineActor) or external. Normalizing here keeps every
    /// <c>Dictionary&lt;string, object?&gt;</c> payload in this project in the same post-ingress
    /// shape.</summary>
    public static void NormalizePipelineResultRows(PipelineResultsEnvelope envelope)
    {
        foreach (var result in envelope.Results)
        {
            JsonValueNormalizer.NormalizeInPlace(result.Row);
        }
    }

    /// <summary>Fans <paramref name="envelope"/> out to every sink in <paramref name="sinks"/>,
    /// sequentially, awaiting each in turn. Extracted for the same testability reason as the
    /// normalization methods above — a fan-out test registers two or more fake sinks and asserts every
    /// one of them observed the envelope, with no endpoint/HTTP/DI-container machinery involved.</summary>
    public static async Task DispatchSourceEventsAsync(SourceEventsEnvelope envelope, IEnumerable<ISourceEventsSink> sinks)
    {
        foreach (var sink in sinks)
        {
            await sink.OnSourceEventsAsync(envelope);
        }
    }

    /// <summary>Fans <paramref name="envelope"/> out to every sink in <paramref name="sinks"/> — table-
    /// delta counterpart of <see cref="DispatchSourceEventsAsync"/>.</summary>
    public static async Task DispatchTableDeltaAsync(TableDeltaEnvelope envelope, IEnumerable<ITableDeltaSink> sinks)
    {
        foreach (var sink in sinks)
        {
            await sink.OnTableDeltaAsync(envelope);
        }
    }

    /// <summary>Fans <paramref name="envelope"/> out to every sink in <paramref name="sinks"/> —
    /// pipeline-results counterpart of <see cref="DispatchSourceEventsAsync"/> (plan 025).</summary>
    public static async Task DispatchPipelineResultsAsync(PipelineResultsEnvelope envelope, IEnumerable<IPipelineResultsSink> sinks)
    {
        foreach (var sink in sinks)
        {
            await sink.OnPipelineResultsAsync(envelope);
        }
    }

    /// <summary>Deserializes the CloudEvents-unwrapped request body (<c>app.UseCloudEvents()</c> runs
    /// ahead of these endpoints in Program.cs, so <c>ctx.Request.Body</c> here is already just the
    /// CloudEvent's "data" field) as <typeparamref name="TEnvelope"/>. Uses the app's normal
    /// <c>Microsoft.AspNetCore.Http.Json.JsonOptions</c> (the same options every other minimal-API
    /// endpoint in StreamsForge.Api binds with — camelCase output, case-INsensitive input, per
    /// StreamsForgeApiExtensions.AddStreamsForgeApi's <c>ConfigureHttpJsonOptions</c> call), so PascalCase
    /// and camelCase property names both deserialize correctly — see dapr/POLYGLOT.md.
    ///
    /// <para><b>Poison-message loop protection:</b> returns null — logging a warning, never throwing —
    /// on any malformed/unparseable/empty payload, instead of letting a <see cref="JsonException"/>
    /// propagate into a 4xx/5xx response. Dapr's pub/sub delivery is at-least-once and a non-2xx
    /// response from the subscriber is exactly the signal that triggers redelivery; a permanently
    /// malformed message (e.g. a schema mismatch from a buggy polyglot publisher) would otherwise retry
    /// forever. Every endpoint above always returns <c>Results.Ok()</c> (200) whether or not the payload
    /// parsed — the warning log line is the only observable signal a malformed message ever produces.
    /// </para></summary>
    private static async Task<TEnvelope?> TryReadAsync<TEnvelope>(HttpContext ctx, ILogger logger, string topic)
        where TEnvelope : class
    {
        try
        {
            var value = await ctx.Request.ReadFromJsonAsync<TEnvelope>(ctx.RequestAborted);
            if (value is null)
            {
                logger.LogWarning("{Topic}: empty/null pub/sub payload — acking (200) anyway", topic);
            }

            return value;
        }
        catch (JsonException ex)
        {
            logger.LogWarning(
                ex,
                "{Topic}: malformed pub/sub payload — acking (200) anyway to avoid a redelivery loop",
                topic);
            return null;
        }
    }
}
