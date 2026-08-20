using Microsoft.AspNetCore.SignalR;
using StreamForge.Abstractions;
using StreamForge.Abstractions.Streaming;
using StreamForge.AppCore.Environments;
using StreamForge.Api.Hubs;

namespace StreamForge.Dapr.Host.Streaming;

/// <summary>
/// Dapr-flavor counterpart of orleans/src/StreamForge.Host/Services/StreamBridgeService.cs — relays the
/// five fixed pub/sub topics (decision D-D) to the SAME SignalR groups/event names/argument shapes, so
/// the console SPA (web/src/realtime/hub.ts) needs no per-runtime branching. Read every method next to
/// its Orleans equivalent; deviations are called out explicitly.
///
/// <para><b>Registration:</b> implements <see cref="ISourceEventsSink"/> and <see cref="ITableDeltaSink"/>
/// so it participates in the same DI fan-out as any other registered sink (Streaming/Sinks.cs);
/// <see cref="StreamingRuntimeSetup.AddServices"/> registers it once as a singleton and forwards both
/// interfaces to that instance. The three topics with no other in-project consumer
/// (sf-pipeline-out/sf-lifecycle/sf-metrics) are wired directly by concrete type in
/// <see cref="StreamingRuntimeSetup.MapTopicEndpoints"/> instead of through a sink interface — nothing
/// else needs those, so a dedicated interface would only add ceremony.</para>
///
/// <para><b>Why no per-pipeline/per-table stream subscribe/unsubscribe here (deliberate simplification
/// vs. Orleans):</b> StreamBridgeService dynamically subscribes/unsubscribes Orleans stream handles per
/// pipeline/table, gated on lifecycle "started"/"stopped" events, because Orleans streams are addressed
/// per-entity (namespace, key). The Dapr flavor's equivalents are FIXED topics (decision D-D) — this
/// bridge's endpoint is subscribed to sf-pipeline-out/sf-table-delta unconditionally for the process's
/// entire lifetime, and simply relays whatever arrives. The "only running entities produce output"
/// behavior is therefore the PUBLISHER's responsibility (W6's PipelineActor / W7's TableActor only
/// publish while actually running) rather than this bridge's — there is no dynamic subscription
/// bookkeeping to replicate on this side because there is nothing to subscribe TO per-entity in the
/// first place.</para>
/// </summary>
public sealed class DaprStreamBridge(IHubContext<StreamHub> hub) : ISourceEventsSink, ITableDeltaSink
{
    private readonly SourceRateSampler _sourceSampler = new();

    /// <summary>Mirrors StreamBridgeService.SubscribeToSourceAsync's per-event handler: group
    /// <c>source:{name}</c>, event <c>sourceEvent</c>, args <c>(name, eventDict)</c> — one SignalR send
    /// per surviving event, not one send per batch (see SourceRateSampler's design note for why sampling
    /// is applied per-event even though the envelope arrives as a batch).</summary>
    public async Task OnSourceEventsAsync(SourceEventsEnvelope envelope)
    {
        foreach (var evt in envelope.Events)
        {
            if (!_sourceSampler.ShouldRelay(envelope.Source))
            {
                continue;
            }

            // Plan 021 wave 2 — GROUP name qualified, PAYLOAD name bare. The group has to match
            // StreamHub's subscribe half, which composes $"source:{EnvKeys.Qualify(env, name)}"; the
            // argument the browser receives has to be the name the browser asked for, because the SPA
            // keys its own state on it and never sees a qualified name anywhere else. envelope.Source is
            // already the qualified key (the actor's own id — see GeneratorActor/ConnectorActor), so the
            // group is right as-is and only the payload needs stripping.
            await hub.Clients.Group($"source:{envelope.Source}")
                .SendAsync("sourceEvent", EnvKeys.Split(envelope.Source).Key, evt);
        }
    }

    /// <summary>Mirrors StreamBridgeService.SubscribeToTableOutputAsync: group <c>table:{name}</c>, event
    /// <c>tableDelta</c>, args <c>(name, deltas, seq)</c>. Unlike the Orleans side (which assigns its own
    /// monotonic <c>_tableSeq</c> counter locally per subscription), the Dapr envelope already carries the
    /// table's own <see cref="TableDeltaEnvelope.Seq"/> — this bridge only relays it, it never invents
    /// one; W7's TableActor is the single source of truth for sequence numbers on this flavor.</summary>
    public async Task OnTableDeltaAsync(TableDeltaEnvelope envelope)
    {
        // Plan 021 wave 2 — group qualified, payload bare; see OnSourceEventsAsync for the rule.
        await hub.Clients.Group($"table:{envelope.Table}")
            .SendAsync("tableDelta", EnvKeys.Split(envelope.Table).Key, envelope.Deltas, envelope.Seq);
    }

    /// <summary>Mirrors StreamBridgeService.SubscribeToPipelineOutputAsync: group
    /// <c>pipeline:{id}</c>, event <c>pipelineResult</c>, args <c>(id, results)</c> — same
    /// <c>List&lt;ResultEnvelope&gt;</c> batch shape as Orleans sends per stream item.</summary>
    public async Task OnPipelineResultsAsync(PipelineResultsEnvelope envelope)
    {
        // Plan 021 wave 2 — group qualified, payload bare; see OnSourceEventsAsync for the rule.
        await hub.Clients.Group($"pipeline:{envelope.PipelineId}")
            .SendAsync("pipelineResult", EnvKeys.Split(envelope.PipelineId).Key, envelope.Results);
    }

    /// <summary>Mirrors StreamBridgeService.OnLifecycleEventAsync/OnTableLifecycleEventAsync: table
    /// lifecycle events reuse the same envelope type — <see cref="LifecycleEvent.Kind"/> prefixed
    /// "table-" disambiguates, and <see cref="LifecycleEvent.PipelineId"/> holds the table's Name (its
    /// actor id) in that case, exactly like the Orleans stream item's doc comment says. Pipeline case:
    /// group <c>pipeline:{id}</c>, event <c>pipelineStatus</c>, args <c>(id, status)</c>. Table case:
    /// group <c>table:{name}</c>, event <c>tableStatus</c>, args <c>(name, status)</c>.
    ///
    /// <para>Deliberately NOT mirrored: Orleans' side-effecting subscribe/unsubscribe-on-"started"/
    /// "stopped" branch inside this same handler — see this class's own doc comment for why there is no
    /// per-entity subscription to start/stop on the Dapr flavor.</para></summary>
    public async Task OnLifecycleEventAsync(LifecycleEvent evt)
    {
        if (evt.Kind.StartsWith("table-", StringComparison.Ordinal))
        {
            // Plan 021 wave 2 — group qualified, payload bare; see OnSourceEventsAsync for the rule.
            var tableName = evt.PipelineId;
            await hub.Clients.Group($"table:{tableName}")
                .SendAsync("tableStatus", EnvKeys.Split(tableName).Key, evt.Status);
            return;
        }

        await hub.Clients.Group($"pipeline:{evt.PipelineId}")
            .SendAsync("pipelineStatus", EnvKeys.Split(evt.PipelineId).Key, evt.Status);
    }

    /// <summary>Mirrors StreamBridgeService.OnMetricsAsync: group <c>metrics</c>, event
    /// <c>pipelineMetrics</c>, args <c>(metrics)</c>.</summary>
    public async Task OnMetricsAsync(PipelineMetrics metrics)
    {
        await hub.Clients.Group("metrics").SendAsync("pipelineMetrics", metrics);
    }
}
