namespace StreamForge.Engine.Runtime;

/// <summary>
/// Plan 011 wave C2 — the per-table ROW RETENTION machinery, Engine side.
///
/// WHAT A RETENTION SCOPE IS. Wave C1 removed the write AMPLIFIER (the whole-table snapshot rebuild every
/// FlushMs); it did not bound the SIZE of a table. The structures that actually hold a row are the
/// operator's per-key state (<see cref="Ops.TableLatestByOp.Current"/>) and/or the consolidated output
/// ledger (<see cref="ConsolidationLedger"/>) — the persisted snapshot, the search index and the history
/// grain are all DERIVED from those two. A policy that trimmed only the derived copies would report
/// success while every real structure kept growing, so retention is defined here, at the state that
/// actually owns the row, and a scope is exactly "one place a row is retained, that can hand back the
/// oldest ones as ordinary retraction deltas".
///
/// WHY EVICTION PRODUCES DELTAS RATHER THAN JUST DELETING. A row that vanishes from a table without a
/// retraction corrupts every consumer downstream of it — a downstream table's Z-set keeps the stale row
/// forever, the delta stream never mentions the removal, SignalR clients keep showing it, sinks never see
/// it leave. So an eviction emits exactly what a genuine upstream retraction emits (negative weight, same
/// row, same return value of OnStreamEvent/OnTableDelta), and every existing consumer stays consistent
/// with no knowledge of retention at all. The one bit consumers MAY care about is
/// <see cref="TableDelta.Retention"/>, which distinguishes "this row left because its input retracted"
/// from "this row left because the table is a bounded view" — the history grain uses it to reclaim the
/// evicted key's version list instead of recording one more retraction against a key that will never
/// come back.
///
/// ORDERING IS BY EVENT TIME, NOT WALL CLOCK, AND NOT HASH ORDER. Every candidate carries the event
/// timestamp of the row that produced it (<c>WorkingRow.Ts</c> / the output row's <c>_ts</c>), and the
/// ordering key is (Ts, identity-string) — a total order with a deterministic tie-break. That makes
/// eviction REPLAYABLE: feeding the same input in the same order to a fresh executor evicts exactly the
/// same rows, which is the invariant this codebase tests for everywhere else (see EpochBuffer's doc). A
/// wall-clock rule would fail that outright, and a "just drop whatever Dictionary enumeration hands back
/// first" rule would fail it under any change of hash seed.
///
/// TTL IS EVENT-TIME TOO, for the same reason: the cutoff is <see cref="MaxObservedTs"/> - TtlMs, i.e.
/// relative to the highest event timestamp this scope has ever admitted, never to DateTime.UtcNow. The
/// honest consequence, documented rather than hidden: if the input stops, event time stops, and nothing
/// further ages out. A TTL here means "keep the last N ms OF DATA", not "keep the last N ms of clock".
/// </summary>
internal interface IRetentionScope
{
    /// <summary>How many rows/keys this scope currently retains — the quantity a MaxRows bound bounds.</summary>
    int RetainedCount { get; }

    /// <summary>The highest event timestamp admitted so far; the TTL cutoff is measured back from it. A
    /// max over admitted timestamps is itself order-independent, so this does not weaken the determinism
    /// argument in the class doc.</summary>
    long MaxObservedTs { get; }

    /// <summary>Removes the <paramref name="count"/> oldest retained entries and returns their retraction
    /// deltas (<see cref="TableDelta.Retention"/> = true). The scope drops its OWN state here; the caller
    /// is responsible for folding the returned deltas into the consolidated ledger exactly like any other
    /// emitted delta.</summary>
    IReadOnlyList<TableDelta> EvictOldest(int count);

    /// <summary>Removes every retained entry whose event timestamp is strictly older than
    /// <paramref name="cutoffTs"/>, same contract as <see cref="EvictOldest"/>.</summary>
    IReadOnlyList<TableDelta> EvictOlderThan(long cutoffTs);
}

/// <summary>Ordering comparer shared by every scope: event timestamp ascending, then the entry's own
/// identity string (ordinal) as the tie-break. Two rows with the same timestamp therefore always evict in
/// the same order, run after run, regardless of dictionary enumeration order.</summary>
internal static class RetentionOrder
{
    public static readonly IComparer<(long Ts, string Key)> Comparer = Comparer<(long Ts, string Key)>.Create(
        (a, b) =>
        {
            int byTs = a.Ts.CompareTo(b.Ts);
            return byTs != 0 ? byTs : string.CompareOrdinal(a.Key, b.Key);
        });
}

/// <summary>
/// The retention scope for a plan whose terminal state IS the consolidated output ledger — i.e. a plain
/// projection/filter table (no LATEST BY, no GROUP BY). For such a plan the ledger is the only structure
/// that holds a row at all: <c>TableIngestOp</c> and <c>TableFilterProjectOp</c> are both documented as
/// stateless, so evicting a ledger entry genuinely reclaims everything the row occupies rather than
/// trimming a copy of it.
///
/// Kept as an ordered index BESIDE the ledger rather than as an ordered ledger, because the ledger is on
/// the hot read path (<c>Snapshot()</c> hands its dictionary out by reference) and must not pay for an
/// ordering nobody reads when retention is off. The index is only ever built when retention is actually
/// configured — see TableExecutor.ConfigureRetention.
/// </summary>
internal sealed class LedgerRetentionScope(ConsolidationLedger ledger) : IRetentionScope
{
    private readonly SortedSet<(long Ts, string Key)> _order = new(RetentionOrder.Comparer);
    private readonly Dictionary<string, long> _tsByKey = new(StringComparer.Ordinal);

    public long MaxObservedTs { get; private set; }

    public int RetainedCount => _order.Count;

    /// <summary>Called after every <see cref="ConsolidationLedger.Apply"/> — the ledger has already decided
    /// whether this canonical key is visible, so this only mirrors that decision into the ordered index.
    /// Idempotent in both directions, which is what lets an eviction's own consolidation pass through here
    /// again harmlessly.</summary>
    public void Observe(string key)
    {
        if (ledger.Visible.TryGetValue(key, out var current))
        {
            long ts = current.Row.Timestamp;
            if (_tsByKey.TryGetValue(key, out long previous))
            {
                if (previous == ts) return;
                _order.Remove((previous, key));
            }
            _order.Add((ts, key));
            _tsByKey[key] = ts;
            if (ts > MaxObservedTs) MaxObservedTs = ts;
        }
        else if (_tsByKey.Remove(key, out long stale))
        {
            _order.Remove((stale, key));
        }
    }

    public IReadOnlyList<TableDelta> EvictOldest(int count)
    {
        var results = new List<TableDelta>();
        for (int i = 0; i < count && _order.Count > 0; i++)
        {
            EvictOne(_order.Min, results);
        }
        return results;
    }

    public IReadOnlyList<TableDelta> EvictOlderThan(long cutoffTs)
    {
        var results = new List<TableDelta>();
        while (_order.Count > 0 && _order.Min.Ts < cutoffTs)
        {
            EvictOne(_order.Min, results);
        }
        return results;
    }

    private void EvictOne((long Ts, string Key) entry, List<TableDelta> results)
    {
        _order.Remove(entry);
        _tsByKey.Remove(entry.Key);
        if (!ledger.Visible.TryGetValue(entry.Key, out var current)) return;

        // Retract the row's ENTIRE running weight, so consolidation nets it to exactly zero and leaves no
        // residue in either half of the ledger (a plain -1 would leave a fan-out row at weight n-1 still
        // visible, i.e. an eviction that evicted nothing).
        results.Add(new TableDelta(current.Row, -current.Weight) { Retention = true });
    }
}

/// <summary>Plan 011 C2 — the single place that decides whether a compiled plan's shape can honestly carry
/// a retention policy. Backs the public <see cref="TablePlan.SupportsRetention"/>, whose doc comment
/// carries the full rationale for each exclusion.</summary>
internal static class TableRetentionSupport
{
    public static bool IsSupported(Planning.CompiledTablePlan plan) =>
        plan.UnionBranches is null
        && plan.Joins.Count == 0
        && plan.GroupBy is null
        && !plan.HasAggregates
        && plan.Sources.Count > 0
        && plan.Sources[0].DerivedPlan is null;
}
