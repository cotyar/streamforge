using StreamsForge.Abstractions;

namespace StreamsForge.Host.Facades;

/// <summary>
/// Plan 009 A1.3: host-process singleton recording, per ingest-kind source, the LOCAL
/// <c>SourceIngressBuffer.GetStatus()</c> snapshot this replica most recently reported into its
/// <see cref="IIngressStatsGrain"/> (<see cref="StreamsForge.Host.Services.IngestDrainPumpService"/> is the writer, on
/// its existing drain tick).
///
/// <para><b>Why this exists at all:</b> <see cref="OrleansIngressFacade"/>.GetStatusAsync
/// needs to answer "cluster-wide total right now", not "cluster-wide total as of the last tick" — the
/// latter would make a push-then-immediately-GET on the SAME replica look like it lost rows (the
/// existing <c>IngestFacadeClusterTests</c> cluster tests assert exactly this: a push must be visible
/// in <c>GetStatusAsync</c> with no drain pump running at all). So GetStatusAsync computes
/// <c>grainSnapshot + (thisReplica'sLocalTotal - thisReplica'sLastReportedBaseline)</c>: the grain's
/// last known sum from every replica, plus whatever THIS replica has done since it last told the grain
/// about it. Once the drain pump's next tick reports that delta and moves the baseline forward, the
/// "pending" half drops to zero and the grain snapshot alone already reflects it — the two terms never
/// double-count because the baseline update and the grain report happen together (see
/// <see cref="StreamsForge.Host.Services.IngestDrainPumpService.ReportStatsAsync"/>).</para>
/// </summary>
public sealed class IngressStatsReportTracker
{
    private readonly object _gate = new();
    private readonly Dictionary<string, IngestStatus> _lastReported = new(StringComparer.Ordinal);

    /// <summary>Zeroed status (every counter 0) when nothing has been reported for this source yet —
    /// which also correctly handles a buffer that was just rebuilt (config edit): the old buffer's
    /// baseline is stale for the NEW buffer's fresh counters, so <see cref="ComputeDelta"/> would
    /// otherwise see a spurious negative; see <see cref="SetBaseline"/>'s reset-on-rebuild note.</summary>
    public IngestStatus GetBaseline(string sourceName)
    {
        lock (_gate)
        {
            return _lastReported.TryGetValue(sourceName, out var status) ? status : new IngestStatus();
        }
    }

    public void SetBaseline(string sourceName, IngestStatus status)
    {
        lock (_gate) { _lastReported[sourceName] = status; }
    }

    public void Remove(string sourceName)
    {
        lock (_gate) { _lastReported.Remove(sourceName); }
    }

    /// <summary>Per-field <c>current - baseline</c>, clamped at 0 — a buffer rebuild (fresh counters
    /// starting over) can otherwise make a field's "current" lower than its stale baseline; treating
    /// that as a negative delta would UNDER-report the grain's running total, so it is treated as "no
    /// new information yet" instead (the fresh buffer's own growth from here catches up on later
    /// ticks).</summary>
    public static IngressStatsDelta ComputeDelta(IngestStatus baseline, IngestStatus current) => new()
    {
        Accepted = Math.Max(0, current.TotalAccepted - baseline.TotalAccepted),
        Rejected = Math.Max(0, current.TotalRejected - baseline.TotalRejected),
        Dropped = Math.Max(0, current.TotalDropped - baseline.TotalDropped),
        Invalid = Math.Max(0, current.TotalInvalid - baseline.TotalInvalid),
        Published = Math.Max(0, current.TotalPublished - baseline.TotalPublished),
        DownstreamDropped = Math.Max(0, current.DownstreamDropped - baseline.DownstreamDropped),
        Duplicate = Math.Max(0, current.TotalDuplicate - baseline.TotalDuplicate),
    };
}
