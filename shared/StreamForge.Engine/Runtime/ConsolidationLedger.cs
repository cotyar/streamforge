namespace StreamForge.Engine.Runtime;

/// <summary>
/// Plan 009 wave D: the Z-set consolidation ledger, extracted to one place. Before this it was hand-written,
/// separately, in three sites — <see cref="TableExecutor"/>'s own `_consolidated`/`_debtWeights` fields,
/// TableGrain's coordinator-mode `_coordinatorSnapshot`/`_coordinatorDebt`, and ArrangementGrain's per-
/// partition `_index`/`_indexDebt` — all three storing the exact same shape (canonical row key -&gt; (row,
/// running weight), split across a "visible" map and a "debt" side-table) and running the exact same
/// arithmetic. A correctness fix to that arithmetic (commit 9de443e) had to land in all three by hand, and
/// nearly missed one — this type is what removes that duplication risk going forward.
///
/// THE ALGORITHM: a row's net weight decides whether it is visible, and the final state depends only on the
/// SUM of the deltas applied so far for a key, never on the order they arrived in. That order-independence
/// is why an unmatched NEGATIVE running weight is retained as debt rather than dropped: under causal
/// delivery a retraction always follows its own assertion, so the debt side-table stays empty in the common
/// case — but nothing in a Z-set/DBSP model guarantees delivery order, and a negative delta can legitimately
/// arrive for a key with no prior positive weight (out-of-order admission, replay, a restart-resume fan-in
/// from multiple partitions, or a FULL OUTER join's own retraction-driven null-pad). Discarding that
/// negative delta (the bug commit 9de443e fixed) loses information: a later positive delta for the same key
/// would then be treated as a fresh start instead of netting against the outstanding debt, so a row whose
/// true total weight is 0 could resurface at a positive weight depending on arrival order alone.
///
/// Kept as two SEPARATE dictionaries — <see cref="Visible"/> (weight &gt; 0) and an internal debt map
/// (weight &lt;= 0, not returned by anything) — rather than one dictionary that also holds non-positive
/// entries, so <see cref="Visible"/> never needs filtering: every entry in it is already exactly what a
/// caller should see, and every hot read path (TableExecutor.Snapshot()'s `/rows` consumers, TableGrain's
/// coordinator-mode reads, ArrangementGrain's index reads) can use it directly. The two maps are disjoint by
/// construction (<see cref="Apply"/> always removes from whichever one a key is NOT being written into), and
/// a key that nets to exactly 0 is removed from both, so neither structure accumulates residue for rows that
/// have fully cancelled out. That invariant — the running weight looked up before folding in a new delta is
/// always the exact sum of every delta seen so far for that key — is what makes classification
/// (positive/zero/negative) depend only on the SUM of deltas, never their arrival order, since integer
/// addition is commutative and associative.
///
/// Deliberately key-agnostic: callers supply their own already-computed canonical row key rather than this
/// type computing one itself, because the three original sites do NOT all canonicalize the same way (Engine
/// internals use <c>Runtime.JsonText.SerializeCanonicalRow</c>, exposed to Host only via the public
/// <see cref="TableExecutor.CanonicalRowKey"/> wrapper; ArrangementGrain — which has no TableExecutor
/// instance to ask — uses its own plain sorted-keys JSON dump instead, deliberately, since its key never
/// crosses a grain boundary or gets compared against another grain's canonicalization). Collapsing that
/// key-computation choice into this type would have forced a hole none of the three genuinely fits through.
/// </summary>
public sealed class ConsolidationLedger
{
    // Only ever holds weight > 0 entries. Exposed BY REFERENCE via Visible (never copied) so callers on a
    // hot read path get an allocation-free view — this dictionary already contains exactly the user-visible
    // positive rows, nothing more, at all times.
    private readonly Dictionary<string, (EventRecord Row, long Weight)> _visible = [];

    // Outstanding NEGATIVE running weight per canonical key, for keys whose net weight-so-far is <= 0 and
    // are therefore not (or no longer) in _visible. See class doc for why this exists at all.
    private readonly Dictionary<string, long> _debt = [];

    /// <summary>The current positive-weight rows, keyed by canonical row key. Returns the SAME dictionary
    /// instance on every call (never a copy) — callers depending on O(1)/allocation-free access (e.g. the
    /// `/rows` read path) may hold onto the reference across calls; it stays live-updated in place.</summary>
    public IReadOnlyDictionary<string, (EventRecord Row, long Weight)> Visible => _visible;

    /// <summary>Number of canonical keys currently holding outstanding negative debt, not yet netted against
    /// a later positive delta. Zero whenever every key's history so far is either untouched or fully
    /// cancelled out — the common case under causal delivery.</summary>
    public int DebtCount => _debt.Count;

    /// <summary>Folds one more weighted row into the ledger for the given (externally-computed) canonical
    /// key, honoring the visible/debt invariant described in the class doc: the running weight for this key
    /// BEFORE folding in <paramref name="weight"/>, wherever it currently lives (positive in
    /// <see cref="Visible"/>, negative in the debt side-table, or 0/absent from both — never in both at
    /// once), plus <paramref name="weight"/>, decides where the new running weight lands.</summary>
    public void Apply(string key, EventRecord row, long weight)
    {
        long currentWeight = _visible.TryGetValue(key, out var existing) ? existing.Weight : _debt.GetValueOrDefault(key);
        long newWeight = currentWeight + weight;

        if (newWeight > 0)
        {
            // Same canonical key => same row content, so either representative is equivalent (always store
            // the incoming row, exactly like every original site did).
            _visible[key] = (row, newWeight);
            _debt.Remove(key);
        }
        else if (newWeight < 0)
        {
            _visible.Remove(key);
            _debt[key] = newWeight;
        }
        else // newWeight == 0: fully cancelled out — no residue in either structure.
        {
            _visible.Remove(key);
            _debt.Remove(key);
        }
    }

    /// <summary>Seeds a row directly into <see cref="Visible"/> without touching the debt side-table or
    /// running the normal weight-folding arithmetic in <see cref="Apply"/> — for rebuild-from-checkpoint
    /// paths (e.g. ArrangementGrain's ActivateAsync) whose persisted checkpoint only ever contains
    /// positive-weight rows in the first place (the debt side-table is never itself persisted).</summary>
    public void Seed(string key, EventRecord row, long weight) => _visible[key] = (row, weight);

    /// <summary>Clears both maps back to empty — for a fresh start (grain reactivation reset, DEACTIVATE,
    /// StopAsync-then-StartAsync) where stale entries from a previous run/checkpoint must not linger.</summary>
    public void Clear()
    {
        _visible.Clear();
        _debt.Clear();
    }
}
