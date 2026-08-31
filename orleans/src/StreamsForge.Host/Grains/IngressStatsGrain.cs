using Orleans;
using StreamsForge.Abstractions;

namespace StreamsForge.Host.Grains;

/// <summary>
/// Plan 009 A1.3: per-source cluster-wide counter aggregator — see
/// <see cref="IIngressStatsGrain"/>'s own doc comment for why this is a plain in-memory grain (no
/// <c>[PersistentState]</c>) rather than a durable one. Trivial by design: every real decision
/// (admission policy, dedup, idempotency) already happened in <c>SourceIngressBuffer</c> before a
/// delta ever reaches here — this only sums.
/// </summary>
public sealed class IngressStatsGrain : Grain, IIngressStatsGrain
{
    private long _totalAccepted;
    private long _totalRejected;
    private long _totalDropped;
    private long _totalInvalid;
    private long _totalPublished;
    private long _downstreamDropped;
    private long _totalDuplicate;

    public Task ReportDeltaAsync(IngressStatsDelta delta)
    {
        _totalAccepted += delta.Accepted;
        _totalRejected += delta.Rejected;
        _totalDropped += delta.Dropped;
        _totalInvalid += delta.Invalid;
        _totalPublished += delta.Published;
        _downstreamDropped += delta.DownstreamDropped;
        _totalDuplicate += delta.Duplicate;
        return Task.CompletedTask;
    }

    public Task<IngressStatsSnapshot> GetSnapshotAsync() => Task.FromResult(new IngressStatsSnapshot
    {
        TotalAccepted = _totalAccepted,
        TotalRejected = _totalRejected,
        TotalDropped = _totalDropped,
        TotalInvalid = _totalInvalid,
        TotalPublished = _totalPublished,
        DownstreamDropped = _downstreamDropped,
        TotalDuplicate = _totalDuplicate,
    });
}
