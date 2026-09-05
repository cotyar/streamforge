using Microsoft.AspNetCore.SignalR;
using StreamsForge.Abstractions;
using StreamsForge.Abstractions.Streaming;
using StreamsForge.AppCore.Environments;
using StreamsForge.Api.Hubs;

namespace StreamsForge.Dapr.Host.Streaming;

/// <summary>
/// Dapr-flavor counterpart of orleans/src/StreamsForge.Host/Services/StreamBridgeService.cs — relays the
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
    private readonly SourceRateSampler _sourcePacer = new();

    /// <summary>Mirrors StreamBridgeService.SubscribeToSourceAsync's per-event handler: group
    /// <c>source:{name}</c>, event <c>sourceEvent</c>, args <c>(name, eventDict)</c> — one SignalR send
    /// per surviving event, not one send per batch (see SourceRateSampler's design note for why pacing
    /// is applied per-event even though the envelope arrives as a batch).
    ///
    /// <para><b>PACED, NOT SAMPLED (plan 025 D5/D6, porting Orleans' plan 023 decision).</b> This used to
    /// DROP any event arriving less than <see cref="SourceRateSampler.MinIntervalMs"/> after the last
    /// relayed one for the same source, which meant a burst — the normal shape of a polled source, one
    /// poll cycle emitting every row of a file in a tight loop — reached the console as one or two rows
    /// with the rest simply never appearing. <see cref="SourceRateSampler.Evaluate"/> now WAITS OUT the
    /// remainder of a too-early event's slot instead, so a burst relays in full, in order, merely spread
    /// over time; only a SUSTAINED firehose past <see cref="SourceRateSampler.MaxPacedStreak"/> degrades
    /// to the old drop behavior. Awaiting the delay HERE, inside the per-event loop, is what makes this
    /// safe: this method runs on the one HTTP delivery the Dapr sidecar makes for this pub/sub message
    /// (<c>sf-sources</c>), so a <c>Task.Delay</c> here holds THAT delivery — the same back-pressure
    /// Orleans accepts inside its own stream subscription callback (see StreamBridgeService's
    /// SubscribeToSourceAsync doc comment) — rather than racing ahead and reordering what reaches the
    /// hub.</para></summary>
    public async Task OnSourceEventsAsync(SourceEventsEnvelope envelope)
    {
        foreach (var evt in envelope.Events)
        {
            var plan = _sourcePacer.Evaluate(envelope.Source);
            if (plan.Decision == RelayDecision.Drop)
            {
                continue;
            }

            if (plan.Decision == RelayDecision.SendAfterDelay)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(plan.DelayMs));
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

    /// <summary>Mirrors StreamBridgeService.OnLifecycleEventAsync/OnTableLifecycleEventAsync/
    /// OnSourceLifecycleEventAsync: all three entity kinds share one envelope type —
    /// <see cref="LifecycleEvent.Kind"/>'s PREFIX disambiguates which, per the doc comment on
    /// <see cref="LifecycleEvent.Kind"/> itself (shared/StreamsForge.Contracts/Models.cs): "table-"
    /// prefixed kinds are tables (<see cref="LifecycleEvent.PipelineId"/> holds the table's Name, its
    /// actor id), "source-" prefixed kinds are sources (<see cref="LifecycleEvent.PipelineId"/> holds the
    /// source's Name), and everything else is one of the six bare pipeline kinds ("created" | "updated" |
    /// "deleted" | "started" | "stopped" | "failed" — see every <c>PublishLifecycleAsync</c> call site in
    /// <c>Catalog/CatalogStore.cs</c>: pipeline kinds are the only ones published with NO hyphen at all).
    /// Table case: group <c>table:{name}</c>, event <c>tableStatus</c>, args <c>(name, status)</c>.
    /// Pipeline case: group <c>pipeline:{id}</c>, event <c>pipelineStatus</c>, args <c>(id, status)</c>.
    ///
    /// <para><b>Source case sends NO hub message</b> — mirroring Orleans' <c>OnSourceLifecycleEventAsync</c>,
    /// whose own doc comment explains why: <c>StreamHub</c>'s clients listen for
    /// <c>pipelineStatus</c>/<c>tableStatus</c> and there is no <c>sourceStatus</c> counterpart to invent;
    /// the console learns a source's state from the REST catalog it already re-reads. Orleans' handler
    /// still takes an ACTION on "source-started"/"source-stopped"/"source-deleted" (subscribing/
    /// unsubscribing its per-entity stream handle) — this flavor has no per-entity subscription to
    /// start or stop in the first place (see this class's own doc comment on FIXED topics), so a
    /// "source-*" kind here is inert other than clearing this bridge's OWN per-source pacing state on
    /// a delete, which costs nothing and mirrors Orleans' <c>UnsubscribeFromSourceAsync</c> clearing its
    /// pacing dictionaries so a source deleted and recreated under the same qualified name does not
    /// inherit stale "last sent at"/streak state — see <see cref="SourceRateSampler.Forget"/>.</para>
    ///
    /// <para><b>Any OTHER prefixed kind is ignored too</b> (never falls through to the pipeline branch) —
    /// this is the "subscriber that does not recognise a prefix must ignore the event" rule
    /// <see cref="LifecycleEvent.Kind"/>'s own doc comment states explicitly, so this list can grow
    /// additively without a stale build here silently misrouting a kind it has never heard of into the
    /// pipeline group. Detected the same way the three known categories are told apart from pipelines:
    /// containing a hyphen at all, since every current bare pipeline kind is a single word.</para></summary>
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

        if (evt.Kind.StartsWith("source-", StringComparison.Ordinal))
        {
            if (evt.Kind == "source-deleted")
            {
                _sourcePacer.Forget(evt.PipelineId);
            }

            return;
        }

        if (evt.Kind.Contains('-'))
        {
            // Unrecognised prefix — not a kind this build knows about. Ignore per Models.cs' own rule
            // rather than guess it belongs to the pipeline branch below.
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
