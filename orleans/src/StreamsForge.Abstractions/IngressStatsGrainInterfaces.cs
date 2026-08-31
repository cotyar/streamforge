namespace StreamsForge.Abstractions;

// ============================================================================
// Plan 009 A1.3 — "counters that admit what they are". Orleans already has a cluster, so a per-source
// singleton grain is the natural place to fold every replica's local ingress counters into a real
// cluster-wide total: IngestDrainPumpService reports LOCAL deltas here on its existing drain tick
// (deltas, not absolutes — two replicas overwriting each other's absolute totals would silently lose
// counts), and OrleansIngressFacade.GetStatusAsync reads the running total back out. A NEW grain
// interface file (not an addition to GrainInterfaces.cs) — see plan 009 A1's task brief: this wave's
// file ownership is additive/new grain files only, not edits to that shared file.
// ============================================================================

/// <summary>Key = source name, so one activation per ingest-kind source is a cluster-wide singleton —
/// every replica's <c>IngestDrainPumpService</c> reports into the SAME grain for a given source no
/// matter which silo it lands on. Deliberately not persisted: the counters it holds are themselves a
/// best-effort aggregation of process-local buffers that are never persisted either (see
/// SourceIngressBuffer's own doc comment) — losing this grain's state on a silo restart just resets
/// the aggregate to match, it does not introduce a NEW loss the rest of the feature didn't already
/// have.</summary>
public interface IIngressStatsGrain : IGrainWithStringKey
{
    /// <summary>Adds one replica's local counter delta (rows observed on THAT replica since its last
    /// report) into the running cluster-wide total. Deltas, never absolutes — reporting an absolute
    /// would let the last-reporting replica silently clobber every other replica's contribution.</summary>
    Task ReportDeltaAsync(IngressStatsDelta delta);

    /// <summary>The current cluster-wide running total — sum of every delta ever reported for this
    /// source, by every replica.</summary>
    Task<IngressStatsSnapshot> GetSnapshotAsync();
}

/// <summary>One replica's contribution since its last report — field-for-field the subset of
/// <see cref="IngestStatus"/> that is a genuine cumulative COUNTER (DepthRows/Policy/CapacityRows/
/// MaxBatchRows/LastPushMs are per-replica-meaningful, not summable, so they stay out of this and are
/// answered from the local buffer instead — see OrleansIngressFacade.GetStatusAsync).</summary>
[GenerateSerializer]
public sealed class IngressStatsDelta
{
    [Id(0)] public long Accepted { get; set; }
    [Id(1)] public long Rejected { get; set; }
    [Id(2)] public long Dropped { get; set; }
    [Id(3)] public long Invalid { get; set; }
    [Id(4)] public long Published { get; set; }
    [Id(5)] public long DownstreamDropped { get; set; }
    [Id(6)] public long Duplicate { get; set; }

    /// <summary>True when every field above is zero — a report worth skipping entirely (no reason to
    /// pay a grain call for a no-op tick).</summary>
    public bool IsEmpty =>
        Accepted == 0 && Rejected == 0 && Dropped == 0 && Invalid == 0 &&
        Published == 0 && DownstreamDropped == 0 && Duplicate == 0;
}

/// <summary>Running cluster-wide totals, mirrored 1:1 onto the counter fields of <see cref="IngestStatus"/>.</summary>
[GenerateSerializer]
public sealed class IngressStatsSnapshot
{
    [Id(0)] public long TotalAccepted { get; set; }
    [Id(1)] public long TotalRejected { get; set; }
    [Id(2)] public long TotalDropped { get; set; }
    [Id(3)] public long TotalInvalid { get; set; }
    [Id(4)] public long TotalPublished { get; set; }
    [Id(5)] public long DownstreamDropped { get; set; }
    [Id(6)] public long TotalDuplicate { get; set; }
}
