namespace StreamForge.Engine.Dataflow;

/// <summary>
/// Buffers incoming <see cref="DeltaBatch"/>es until the operator's combined frontier (see
/// <see cref="FrontierTracker"/>) permits flushing them. This is the operator-loop half of the
/// protocol described in plans/003-materialize-territory.md, "Protocol details": "buffer inputs
/// per epoch -> on frontier advance to E: process all batches &lt;= E in deterministic order".
///
/// THE core replay guarantee: the flush sequence for a fixed set of batches depends ONLY on each
/// batch's (Epoch, EdgeId, FromPartition) — never on arrival order. Add the same batches to a
/// fresh buffer in any interleaving/permutation, call OnFrontier with the same frontier
/// progression, and every call returns byte-identical output. Deterministic = replayable =
/// debuggable (plan, same section).
///
/// INVARIANTS this type maintains:
///  - A batch is only ever returned once, by the first OnFrontier call whose frontier is &gt;=
///    that batch's Epoch — batches are never emitted "above" the frontier that admitted them.
///  - Within a flush, batches are ordered by (Epoch, EdgeId, FromPartition) ascending — fully
///    deterministic regardless of Add() call order.
///  - No silent drops: every added batch is either still buffered (see <see cref="BatchCount"/> /
///    <see cref="DeltaCount"/>) or has been returned by OnFrontier. There is no other way for a
///    batch to leave the buffer.
///
/// THIS BUFFER IS UNBOUNDED, and nothing in this repo bounds it (plan 011 wave C — corrected; the
/// comment here previously said BatchCount/DeltaCount were "exposed so a caller can apply
/// backpressure", which promised a guarantee no code provides). No caller outside the unit tests
/// reads either counter, there is no high-water mark, no rejection path, and no status field that
/// surfaces buffer depth. The consequence is real and worth stating rather than discovering: while
/// any ONE upstream partition is stalled, the combined frontier cannot advance, so OnFrontier
/// returns nothing and every batch from every OTHER partition accumulates here for as long as the
/// stall lasts — memory proportional to (stall duration x ingest rate), with no ceiling. The
/// companion structure grows with it: TableStageGrain._originByBatch is only drained inside the
/// `observation.Advanced` branch, i.e. only when the frontier moves. The counters remain public
/// and correct, so wiring a real bound (a cap plus a visible status, and a decision about what to
/// do when it is hit — block ingest, or drop and mark the table degraded) stays a small change;
/// it is deliberately NOT made here, because choosing between those two answers is a semantics
/// decision, not a memory fix. See orleans/DESIGN.md's "Known ceilings".
///  - Epoch markers (<see cref="DeltaBatch.IsMarker"/>, zero deltas) are buffered and flushed
///    exactly like any other batch — an empty epoch still advances and still appears in the
///    flush sequence, which is what lets a downstream consumer learn that upstream reached that
///    epoch with nothing to say.
/// </summary>
public sealed class EpochBuffer
{
    private readonly List<DeltaBatch> _pending = [];

    /// <summary>Number of batches currently buffered (not yet flushed).</summary>
    public int BatchCount => _pending.Count;

    /// <summary>Total delta rows across all currently buffered batches (BatchCount alone hides a few
    /// huge batches). A diagnostic only: nothing reads it, and it bounds nothing — see the class
    /// doc's "THIS BUFFER IS UNBOUNDED" paragraph.</summary>
    public long DeltaCount
    {
        get
        {
            long total = 0;
            foreach (var batch in _pending) total += batch.Deltas.Count;
            return total;
        }
    }

    public void Add(DeltaBatch batch) => _pending.Add(batch);

    /// <summary>
    /// Removes and returns every buffered batch with Epoch &lt;= <paramref name="frontier"/>,
    /// ordered by (Epoch, EdgeId, FromPartition) ascending, grouping batches into ascending
    /// epoch order first. Batches above the frontier are left buffered for a later call.
    /// </summary>
    public IReadOnlyList<DeltaBatch> OnFrontier(Epoch frontier)
    {
        List<DeltaBatch>? ready = null;
        List<DeltaBatch>? remaining = null;

        foreach (var batch in _pending)
        {
            if (batch.Epoch <= frontier)
            {
                (ready ??= []).Add(batch);
            }
            else
            {
                (remaining ??= []).Add(batch);
            }
        }

        _pending.Clear();
        if (remaining is not null) _pending.AddRange(remaining);

        if (ready is null) return [];

        ready.Sort(static (a, b) =>
        {
            var byEpoch = a.Epoch.CompareTo(b.Epoch);
            if (byEpoch != 0) return byEpoch;
            return a.Upstream.CompareTo(b.Upstream);
        });
        return ready;
    }
}
