namespace StreamsForge.Engine.Dataflow;

/// <summary>
/// Identifies one upstream sender: a specific partition of a specific edge. This is the unit
/// <see cref="FrontierTracker"/> tracks high-water marks for, and the unit
/// <see cref="EpochBuffer"/> orders flushed batches by.
/// </summary>
public readonly record struct UpstreamId(EdgeId EdgeId, int FromPartition) : IComparable<UpstreamId>
{
    public int CompareTo(UpstreamId other)
    {
        var byEdge = EdgeId.CompareTo(other.EdgeId);
        return byEdge != 0 ? byEdge : FromPartition.CompareTo(other.FromPartition);
    }

    public override string ToString() => $"{EdgeId}/p{FromPartition}";
}

/// <summary>
/// ALL cross-grain movement is batched per (edge, epoch) — see plans/003-materialize-territory.md,
/// "Protocol details". A batch may legitimately carry zero deltas: an upstream that produced no
/// output for an epoch still sends the marker so downstream frontier tracking can advance
/// (silence is indistinguishable from "still working" otherwise — see the DBSP progress
/// protocol). <see cref="IsMarker"/> flags that case.
/// </summary>
public sealed record DeltaBatch(EdgeId EdgeId, int FromPartition, Epoch Epoch, IReadOnlyList<TableDelta> Deltas)
{
    /// <summary>The (edge, partition) this batch came from — the key FrontierTracker and
    /// EpochBuffer both key off.</summary>
    public UpstreamId Upstream => new(EdgeId, FromPartition);

    /// <summary>True for an empty epoch marker: no rows changed upstream this epoch, but the
    /// epoch still happened and downstream still needs to know that to advance its own frontier.</summary>
    public bool IsMarker => Deltas.Count == 0;
}
