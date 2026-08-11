using System.Runtime.CompilerServices;
using Dapr.Client;
using StreamForge.Abstractions;
using StreamForge.AppCore.Ingest;
using StreamForge.Dapr.Host.Streaming;

// Plan 008 W4c: every other facade in this project (DaprCatalogFacade, DaprConnectorStatusFacade, ...) is
// internal and untested directly — only their pure helper classes (e.g. SourceKindDispatch) get unit
// tests. DaprIngressFacade's admission-path logic (coerce -> whole-batch-reject-vs-partial -> buffer
// admission -> counter reconciliation) is dense enough to be worth testing directly rather than only
// through SourceIngressRegistry/SourceIngressBuffer's own AppCore-level tests, so this grants the test
// assembly access instead of widening the class to `public` (every sibling facade stays internal;
// DaprIngressFacadeTests.cs documents which of its tests avoid needing a live Dapr sidecar and why).
[assembly: InternalsVisibleTo("StreamForge.Dapr.Tests")]

namespace StreamForge.Dapr.Host.Ingest;

/// <summary>
/// Plan 008 W4c: Dapr's <see cref="IIngressFacade"/> — a host-process singleton wrapping
/// <see cref="SourceIngressRegistry"/> (shared/StreamForge.AppCore/Ingest/), exactly as
/// <see cref="IIngressFacade"/>'s own class doc requires ("the buffer lives in a host-process singleton
/// in both flavors... an unbounded, unobservable grain/actor inbox with no admission point would make
/// the policy choice decorative"). There is deliberately no IngestActor and this class never resolves
/// one — <see cref="Ingest.IngestDrainPumpService"/> is the only other moving part, periodically calling
/// <see cref="SourceIngressBuffer.DrainAsync"/> on whatever this facade admitted.
///
/// <para><b>PushAsync order, matching <see cref="IIngressFacade.PushAsync"/>'s doc comment exactly:</b>
/// resolve the source and check its kind (NotFound/WrongKind short-circuit before any row is touched),
/// run every row through <see cref="IngressRowAcceptance.AcceptBatch"/> (coercion BEFORE admission — a
/// whole-batch <see cref="IngestOutcome.Invalid"/> never reaches <see cref="SourceIngressRegistry"/> at
/// all unless <c>partial</c> was requested), then hand only the coerced/accepted rows to the source's
/// <see cref="SourceIngressBuffer"/> for the actual admission decision.</para>
///
/// <para><b>The drain delegate publishes ONE <see cref="SourceEventsEnvelope"/> per drained batch</b> —
/// byte-identical to <see cref="Actors.ConnectorActor.PublishAsync"/> (same two topics, same order, same
/// swallow-and-log-on-failure behavior for a sidecar hiccup): "sf-sources" (the in-host router fan-out)
/// and "sf-source-{name}" (the publish-only polyglot egress copy). Per-row publishing would be one
/// sidecar hop per row and is explicitly the wrong shape for this flavor — see
/// <see cref="IngressEnvelopeBuilder"/>'s class doc ("Dapr publishes one SourceEventsEnvelope per drained
/// batch... the per-row-vs-per-batch choice stays on the host side").</para>
/// </summary>
internal sealed class DaprIngressFacade(
    ICatalogFacade catalog,
    SourceIngressRegistry registry,
    DaprClient daprClient,
    ILogger<DaprIngressFacade> logger) : IIngressFacade
{
    /// <summary>Same literal as <see cref="Actors.ConnectorActor"/>'s private <c>EgressTopicPrefix</c> —
    /// duplicated rather than shared for the same reason that class's own doc comment gives for ITS copy
    /// (which itself duplicates <c>GeneratorActor</c>'s): this facade and <see cref="Actors.ConnectorActor"/>
    /// were built in different, disjointly-owned waves, and both must agree on the "sf-source-{name}"
    /// literal, which is stable enough that duplication is cheaper than threading a shared constant
    /// across wave-owned files.</summary>
    private const string EgressTopicPrefix = "sf-source-";

    public async Task<IngestResult> PushAsync(string sourceName, IReadOnlyList<Dictionary<string, object?>> events, bool partial)
    {
        var def = await catalog.GetSourceAsync(sourceName);
        if (def is null)
        {
            return new IngestResult { Outcome = IngestOutcome.NotFound, Error = $"no such source \"{sourceName}\"" };
        }

        if (def.Kind != SourceKinds.Ingest)
        {
            return new IngestResult
            {
                Outcome = IngestOutcome.WrongKind,
                Error = $"source \"{sourceName}\" is kind \"{def.Kind}\", not \"{SourceKinds.Ingest}\"",
            };
        }

        var config = def.Ingest ?? new IngestConfig();
        var arrivalMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var batch = IngressRowAcceptance.AcceptBatch(def.Fields, sourceName, config.RejectUnknownFields, events, arrivalMs);

        // Whole-batch reject: coercion happens BEFORE admission (IngestModels.cs's header) so a 400
        // never leaves partial state — the buffer never sees a row from this request at all, unless the
        // caller asked for `partial`.
        if (batch.RowErrors.Count > 0 && !partial)
        {
            return new IngestResult
            {
                Outcome = IngestOutcome.Invalid,
                Accepted = 0,
                Dropped = 0,
                Invalid = batch.RowErrors.Count,
                Error = "batch contains invalid rows",
                RowErrors = batch.RowErrors,
            };
        }

        var buffer = registry.GetOrCreate(sourceName, config, (rows, ct) => DrainAsync(sourceName, rows, ct));
        buffer.RecordInvalid(batch.RowErrors.Count);

        var result = await buffer.PushAsync(batch.Accepted);
        // Accepted + Dropped + Invalid must account for every row in the ORIGINAL request
        // (IngestResult's own doc comment) — batch.RowErrors are rows the buffer never saw at all.
        result.Invalid = batch.RowErrors.Count;
        if (batch.RowErrors.Count > 0)
        {
            result.RowErrors = batch.RowErrors;
        }

        return result;
    }

    /// <summary>Null when the source doesn't exist or isn't ingest-kind. A source that IS ingest-kind but
    /// has never been pushed to (no buffer created yet — <see cref="SourceIngressRegistry.TryGet"/>
    /// returns null) still gets an honest zeroed status reflecting its configured policy/capacity, rather
    /// than null, since the source itself is real.</summary>
    public async Task<IngestStatus?> GetStatusAsync(string sourceName)
    {
        var def = await catalog.GetSourceAsync(sourceName);
        if (def is null || def.Kind != SourceKinds.Ingest)
        {
            return null;
        }

        var config = def.Ingest ?? new IngestConfig();
        var buffer = registry.TryGet(sourceName);
        if (buffer is null)
        {
            return new IngestStatus
            {
                Policy = config.Policy,
                CapacityRows = config.CapacityRows,
                MaxBatchRows = config.MaxBatchRows,
            };
        }

        // IngestStatus.DownstreamDropped is always 0 on this flavor, and deliberately so — not a
        // placeholder. Dapr's PublishEventAsync (in DrainAsync below) reports only the sidecar hop, not
        // whether the in-host router or any downstream subscriber kept up; there is no Dapr equivalent of
        // Orleans' PushStreamBus.TotalDropped to sample, so SourceIngressBuffer.RecordDownstreamDropped is
        // simply never called on this flavor. The zero returned here is therefore honest ("we cannot
        // observe this loss point"), not "nothing is being lost" — same convention as
        // IArrangementMetaFacade's "always returns an empty list" note (decision D-F) for other
        // Orleans-only observability this flavor cannot honestly provide.
        return buffer.GetStatus();
    }

    private async Task DrainAsync(string sourceName, IReadOnlyList<Dictionary<string, object?>> rows, CancellationToken ct)
    {
        var envelope = IngressEnvelopeBuilder.ToSourceEventsEnvelope(sourceName, rows);
        try
        {
            // sf-sources: the in-host router fans this out (same as ConnectorActor.PublishAsync).
            // sf-source-{name}: publish-only egress copy for polyglot subscribers.
            await daprClient.PublishEventAsync(StreamingRuntimeSetup.PubsubName, StreamingRuntimeSetup.SourcesTopic, envelope, ct);
            await daprClient.PublishEventAsync(StreamingRuntimeSetup.PubsubName, EgressTopicPrefix + sourceName, envelope, ct);
        }
        catch (Exception ex)
        {
            // A transient sidecar hiccup must not throw out of the drain pump, and must not block a
            // waiting PushAsync(Inline) caller forever either — the batch is already accounted for as
            // accepted/published in the buffer's counters (at-least-once semantics, same rationale as
            // ConnectorActor.PublishAsync's identical catch); only the publish hop failed.
            logger.LogWarning(ex, "DaprIngressFacade[{Source}]: failed to publish a drained batch of {Count} row(s).", sourceName, rows.Count);
        }
    }
}
