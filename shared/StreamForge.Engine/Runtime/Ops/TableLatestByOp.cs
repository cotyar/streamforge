using StreamForge.Engine.Dataflow;
using StreamForge.Engine.Planning;
using StreamForge.Engine.Sql;

namespace StreamForge.Engine.Runtime.Ops;

/// <summary>
/// Plan 002 L3 (deferred sugar, landed alongside L2's UNNEST): `LATEST BY (col[, col...])` — table-mode
/// "keep the most recent row per key" running argmax, ordered by event timestamp (<see cref="WorkingRow.Ts"/>,
/// i.e. `_ts`). This is the terminal stage for a LATEST-BY-shaped table plan, playing the exact role
/// <see cref="TableReduceOp"/> plays for a GROUP BY-shaped one (TableExecutorImpl picks whichever of the two
/// applies — the two are mutually exclusive by construction; see Validator's exclusivity diagnostics) —
/// same "hand the WHERE-filtered-but-still-WorkingRow delta to this op, it produces the table's final
/// output TableDeltas directly" shape, just a different per-key policy than aggregation.
///
/// SEMANTICS (see class-level acceptance in plan 002 L3 / the L2+L3 implementation report for the exact
/// wording pinned by tests):
///  - Assertion (weight &gt; 0 — including weight &gt; 1, which this op treats identically to a plain
///    assertion; see the "Weight &gt;1 rows: treat as assert" ceiling note below) for a key with NO
///    currently-retained row: retain it, emit assert(row, +1). No retraction (nothing to retract yet).
///  - Assertion for a key that already has a retained row: replace it ONLY IF the arriving row's Ts is
///    &gt;= the retained row's Ts (ties replace — "last write wins" among same-timestamp arrivals); emit
///    retract(oldProjectedRow, -1) then assert(newProjectedRow, +1). A STRICTLY OLDER arriving row (Ts &lt;
///    retained Ts) is a late/out-of-order arrival: it is IGNORED — no output, no state change, and (this is
///    the honest limitation to document) it is counted NOWHERE — there is no "late row" metric this op
///    exposes.
///  - Retraction (weight &lt;= 0) that matches the row THIS op currently has retained for that key
///    (upstream retraction — e.g. an earlier WHERE-passing row now filtered out, or a genuine upstream
///    delete): drop the key entirely and emit retract(row, -1) — "no surviving row" is the honest answer
///    here; this op has no multiset/history of PAST contributing rows for a key to fall back to (a true
///    "argmax with fallback to the next-most-recent row" would need exactly that — a full per-key row
///    history — which is out of scope for this tier; see plan 002 L3's own scoping note). A retraction that
///    does NOT match the currently-retained row (e.g. retracting some row that was never the latest, or
///    retracting after the key was already dropped) is a no-op.
///  - KEY RETRACTION (wishlist "explicit key retraction through ingest"; weight &lt;= 0 AND the row carries
///    TableIngestOp.RetractField under this op's own alias — see <see cref="IsKeyRetraction"/>): retracts
///    whatever THIS op currently has retained for the key, REGARDLESS of the arriving row's content —
///    the whole reason this exists is that a client-issued retraction only ever carries the key columns
///    it bothered to send, not the full row it means to retract, so it can never satisfy the ordinary
///    content-match rule above. This is also exactly why a retraction only means anything to THIS op:
///    only <see cref="Current"/> knows what "the current row for a key" is at all — a GROUP BY or a plain
///    projection has no equivalent concept, which is why admitting a key retraction to either of those is
///    rejected up front, at ingest-validate time (AppCore's RetractConsumerValidation), rather than being
///    silently accepted here. An unknown key or a second retraction of an already-freed key both land on
///    a <see cref="Current"/> miss and are a no-op — no delta, no state change — which is what makes a
///    double retraction idempotent for free, with no separate "already retracted" bookkeeping needed.
///  - PROJECTION applies to the retained WorkingRow directly (not a synthetic aggregate-state row the way
///    TableReduceOp's BuildRow needs — LATEST BY keeps the real row, so every output expression, key or
///    non-key alike, evaluates normally against it; there is no GroupByIndex substitution mechanism here).
///
/// STATE: <see cref="Current"/> — encoded-key string -> the single retained (WorkingRow, Ts) pair. No
/// per-key history beyond that one row (see the retraction-ceiling note above).
///
/// PLAN 011 C2 — RETENTION. <see cref="Current"/> is THE structure that makes a `LATEST BY (&lt;unbounded
/// key&gt;)` table grow forever: one permanent entry per key ever seen, and nothing ever removed it. It is
/// therefore also the structure a retention policy has to evict FROM — evicting the consolidated output
/// row (let alone the persisted snapshot) would leave this map holding the WorkingRow and its whole field
/// dictionary, so the table would look bounded and remain unbounded. This op implements
/// <see cref="IRetentionScope"/> directly for that reason: eviction removes the key from
/// <see cref="Current"/> AND emits the same retract(projected row, -1) an upstream retraction of that row
/// would have emitted, so the ledger, the search index, downstream tables and history all follow along
/// through the ordinary delta path. The ordering index (<see cref="_order"/>) is built only when
/// <see cref="EnableRetention"/> has been called, so a table without a policy pays nothing.
/// </summary>
internal sealed class TableLatestByOp : ITableOp, IRetentionScope
{
    internal sealed class KeyState
    {
        public required WorkingRow Row;
        public required long Ts;
    }

    private readonly CompiledTablePlan _plan;
    private readonly List<Expr> _keys;

    /// <summary>This op's state: encoded LATEST BY key -> the single row currently retained for it.</summary>
    public Dictionary<string, KeyState> Current { get; } = [];

    // Plan 011 C2 — retention (all three fields inert until EnableRetention is called; see class doc).
    // _order mirrors Current as (retained row's event Ts, encoded key), giving eviction a deterministic
    // oldest-first total order in O(log n) instead of a hash-order scan.
    private bool _retentionEnabled;
    private SortedSet<(long Ts, string Key)>? _order;
    private long _maxObservedTs;

    public TableLatestByOp(CompiledTablePlan plan, List<Expr> keys)
    {
        _plan = plan;
        _keys = keys;
    }

    public IReadOnlyList<TableDelta> OnBatch(Epoch epoch, IReadOnlyList<TableRowDelta> input)
    {
        var results = new List<TableDelta>();
        foreach (var d in input) results.AddRange(OnDelta(d.Row, d.Weight));
        return results;
    }

    private List<TableDelta> OnDelta(WorkingRow row, long weight)
    {
        var results = new List<TableDelta>();
        var key = EncodeKey(row);

        // Plan 011 C2: the event-time high-water mark a TTL cutoff is measured back from. Taken over every
        // ADMITTED assertion, including one this op is about to ignore as a late arrival — a max is
        // order-independent either way, so this stays replay-deterministic (see IRetentionScope's doc).
        if (weight > 0 && row.Ts > _maxObservedTs) _maxObservedTs = row.Ts;

        if (weight > 0)
        {
            // Assertion (weight >1 is deliberately NOT tracked as a multiplicity — see class doc's
            // documented ceiling: this op treats any positive weight as a single assert).
            if (Current.TryGetValue(key, out var existing))
            {
                if (row.Ts < existing.Ts) return results; // strictly older late arrival: ignored, counted nowhere
                results.Add(new TableDelta(ProjectRow(existing.Row), -1));
                _order?.Remove((existing.Ts, key));
            }
            Current[key] = new KeyState { Row = row, Ts = row.Ts };
            _order?.Add((row.Ts, key));
            results.Add(new TableDelta(ProjectRow(row), 1));
            return results;
        }

        // KEY RETRACTION (class doc) short-circuits the content-match rule below: drop whatever is
        // currently retained for the key, if anything, no matter what this arriving row's own fields
        // are — a client-issued retraction never carries the full row it means to remove.
        if (IsKeyRetraction(row))
        {
            if (Current.Remove(key, out var retained))
            {
                _order?.Remove((retained.Ts, key));
                results.Add(new TableDelta(ProjectRow(retained.Row), -1));
            }
            return results;
        }

        // Retraction: only meaningful when it retracts the row THIS op currently holds for the key — see
        // class doc on why any other retraction (of a non-current row) is a no-op here.
        if (Current.TryGetValue(key, out var current) && SameRow(current.Row, row))
        {
            Current.Remove(key);
            _order?.Remove((current.Ts, key));
            results.Add(new TableDelta(ProjectRow(current.Row), -1));
        }
        return results;
    }

    // ------------------------------------------------------------------
    // Plan 011 C2 — IRetentionScope. See the class doc for why this op, and not the consolidated ledger,
    // is the scope for a LATEST BY plan.
    // ------------------------------------------------------------------

    /// <summary>Starts maintaining the ordering index, seeding it from whatever is already retained (in
    /// practice nothing: a table configures retention on start, with a freshly built executor). Idempotent
    /// so a restart that re-applies the same definition is free.</summary>
    public void EnableRetention()
    {
        if (_retentionEnabled) return;
        _retentionEnabled = true;
        _order = new SortedSet<(long Ts, string Key)>(RetentionOrder.Comparer);
        foreach (var kv in Current) _order.Add((kv.Value.Ts, kv.Key));
    }

    public int RetainedCount => Current.Count;

    public long MaxObservedTs => _maxObservedTs;

    public IReadOnlyList<TableDelta> EvictOldest(int count)
    {
        var results = new List<TableDelta>();
        for (int i = 0; i < count && _order is { Count: > 0 }; i++)
        {
            EvictOne(_order.Min, results);
        }
        return results;
    }

    public IReadOnlyList<TableDelta> EvictOlderThan(long cutoffTs)
    {
        var results = new List<TableDelta>();
        while (_order is { Count: > 0 } && _order.Min.Ts < cutoffTs)
        {
            EvictOne(_order.Min, results);
        }
        return results;
    }

    /// <summary>Drops one key's retained row entirely — the map entry, the ordering entry, and (via the
    /// returned delta) the row's presence everywhere downstream. The emitted retraction is byte-identical
    /// to the one an upstream retraction of the same row produces, apart from carrying
    /// <see cref="TableDelta.Retention"/> so history can tell the two apart.</summary>
    private void EvictOne((long Ts, string Key) entry, List<TableDelta> results)
    {
        _order!.Remove(entry);
        if (!Current.Remove(entry.Key, out var state)) return;
        results.Add(new TableDelta(ProjectRow(state.Row), -1) { Retention = true });
    }

    private static bool SameRow(WorkingRow a, WorkingRow b) =>
        JsonText.SerializeCanonicalRow(a.Fields) == JsonText.SerializeCanonicalRow(b.Fields);

    /// <summary>True when <paramref name="row"/> is a client-issued key retraction rather than an
    /// ordinary delta. TableIngestOp.RetractField ("_retract") is copied into <see cref="WorkingRow"/>
    /// by <see cref="WorkingRow.FromEvent"/> exactly like every other field on the incoming
    /// <c>EventRecord</c> — alias-prefixed to "{alias}__retract" (WorkingRow's own doc: reserved fields
    /// become "{alias}__ts" etc. for the same reason). Checked against every alias this row carries, not
    /// just <see cref="_plan"/>'s FROM alias,
    /// so this still works when a join sits upstream of this op (WorkingRow.Combine unions both sides'
    /// Fields, so the flag survives under whichever alias TableIngestOp originally tagged it with).</summary>
    private static bool IsKeyRetraction(WorkingRow row)
    {
        foreach (var alias in row.Aliases)
        {
            if (row.Fields.TryGetValue(alias + "_" + TableIngestOp.RetractField, out var v) && v is true)
            {
                return true;
            }
        }
        return false;
    }

    private string EncodeKey(WorkingRow row)
    {
        var ctx = new EvalContext(row, _plan.Bindings);
        var values = _keys.Select(k => ExpressionEvaluator.Eval(k, ctx)).ToArray();
        return TableKeyEncoding.EncodeGroupKey(values);
    }

    private EventRecord ProjectRow(WorkingRow row)
    {
        var evt = new EventRecord();
        var ctx = new EvalContext(row, _plan.Bindings);
        foreach (var item in _plan.Output) evt[item.Name] = ExpressionEvaluator.Eval(item.Expression, ctx);
        evt[EventRecord.TimestampField] = row.Ts;
        evt[EventRecord.SourceField] = _plan.SourceLabel;
        return evt;
    }

    /// <summary>Pass-through — see TableReduceOp's identical OnFrontier doc: no epoch-driven state to flush
    /// today.</summary>
    public IReadOnlyList<TableDelta> OnFrontier(Epoch epoch) => [];
}
