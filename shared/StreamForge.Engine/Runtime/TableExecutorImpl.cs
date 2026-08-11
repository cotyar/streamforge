using StreamForge.Engine.Dataflow;
using StreamForge.Engine.Planning;
using StreamForge.Engine.Runtime;
using StreamForge.Engine.Runtime.Ops;
using StreamForge.Engine.Sql;

namespace StreamForge.Engine;

/// <summary>Attaches the compiled, executable table plan to the frozen <see cref="TablePlan"/> DTO.</summary>
public sealed partial class TablePlan
{
    internal CompiledTablePlan Compiled { get; }

    internal TablePlan(CompiledTablePlan compiled) => Compiled = compiled;
}

/// <summary>
/// Façade over the table-mode operator chain (plan 003 M1: "TableExecutor becomes a façade: builds the
/// op chain from the compiled plan, feeds OnStreamEvent/OnTableDelta through it ... produces identical
/// outputs"). Builds one <see cref="TableIngestOp"/> per input role, a chain of <see cref="TableJoinOp"/>
/// (one per JOIN, left-to-right), a <see cref="TableFilterProjectOp"/> (WHERE, +terminal projection when
/// ungrouped) and — when the plan groups/aggregates — a <see cref="TableReduceOp"/> (GROUP BY, retraction/
/// assertion emission). Every emitted delta also folds into this table's own consolidated output Z-set,
/// exposed via Snapshot() — that bookkeeping stays here rather than becoming its own op because it plays
/// the role plan 003 assigns to a separate grain kind (TableReadGrain), not an operator in the dataflow
/// graph proper.
///
/// EPOCH: single-partition, in-process — table mode has no real partitioning yet (that's M2). Each call
/// to OnStreamEvent/OnTableDelta is stamped with its own epoch from a trivial monotonically advancing
/// counter (plan 003 M1: "epoch = a trivial advancing counter"); every op invoked while servicing one
/// call shares that call's epoch, since the whole call is one atomic admission from this table's point of
/// view. No op in table mode's OnFrontier hook does anything with epochs yet (see each op's class doc) —
/// this façade doesn't even call OnFrontier on the hot path for that reason; the hook is proven live via
/// dedicated per-op unit tests instead (see OpsTests / TableReduceOpTests etc. — OnFrontier pass-through
/// is asserted there, not exercised through this façade).
/// </summary>
public sealed partial class TableExecutor
{
    /// <summary>One role a real leaf stream/table name plays: feeds a plain FROM/JOIN alias directly, or
    /// first passes through a derived table/CTE's own nested TableExecutor (plan 004 N1) whose emitted
    /// TableDeltas THEN become this alias's input deltas — the same "table-over-table chaining" mechanism
    /// (TableExecutor.OnTableDelta) this codebase already uses for one named table depending on another,
    /// now wired automatically for an inline derived source instead of a separately-declared table.</summary>
    private sealed class RoleEntry
    {
        public required bool IsFrom;
        public required int StageIndex; // -1 = FROM
        public required string Alias;
        public TableExecutor? Derived;
    }

    private bool _initialized;
    private readonly List<ITableJoinStage> _joins = [];
    private TableFilterProjectOp? _filterProject;
    private TableReduceOp? _reduce;
    private TableLatestByOp? _latestBy;
    private readonly Dictionary<string, TableIngestOp> _ingestOps = [];

    // real leaf stream/table name -> every role it plays in the plan (FROM and/or one or more JOIN
    // aliases, directly or via a derived source's nested TableExecutor).
    private readonly Dictionary<string, List<RoleEntry>> _roles = [];

    // Consolidated output: canonical row text -> (row, weight). Only ever holds weight > 0 entries —
    // Snapshot() returns this dictionary BY REFERENCE (see SnapshotCore below) so hosts on the hot `/rows`
    // read path get an allocation-free view; that contract requires this dictionary to already contain
    // exactly the user-visible positive rows, nothing more.
    private readonly Dictionary<string, (EventRecord Row, long Weight)> _consolidated = [];

    // Outstanding NEGATIVE running weight per canonical row key, for keys whose net weight-so-far is <= 0
    // and are therefore not (or no longer) in _consolidated. Necessary for order-independence: under
    // causal delivery a retraction always follows its own assertion, so this dictionary stays empty in the
    // common case — but nothing in a Z-set/DBSP model guarantees delivery order, and a negative delta can
    // legitimately arrive for a key with no prior positive weight (e.g. a FULL OUTER join's own retraction-
    // driven pad, replay, or out-of-order admission). Discarding that negative delta (the old bug) loses
    // information: a later positive delta for the same key would then be treated as a fresh start instead
    // of netting against the outstanding debt, so a row whose true total weight is 0 could resurface at a
    // positive weight depending on arrival order alone.
    //
    // Kept as a *separate* side table rather than allowing _consolidated to hold weight <= 0 entries so
    // that _consolidated — and therefore Snapshot()'s by-reference return — never needs filtering: every
    // entry in it is already exactly what a caller should see. The two dictionaries are disjoint by
    // construction (ApplyConsolidation below always removes from whichever one a key is NOT being written
    // into) and a key that nets to exactly 0 is removed from both, so neither structure accumulates residue
    // for rows that have fully cancelled out. This is the DBSP invariant that makes the ledger order-
    // independent: for any canonical key, the value looked up before folding in a new delta is always the
    // exact running sum of every delta seen so far for that key (never a "resets to zero because we forgot
    // the negative part" approximation), so — because integer addition is commutative and associative — the
    // final classification of a key (positive/zero/negative) after the full multiset of deltas has been
    // applied depends only on the SUM of the deltas, never on the order they arrived in.
    private readonly Dictionary<string, long> _debtWeights = [];

    private long _epochCounter;

    // Plan 008 W3: set (non-null) only when this plan's CompiledTablePlan.UnionBranches is non-null — a
    // set-operation root. Every OTHER field above (`_joins`, `_filterProject`, `_reduce`, `_latestBy`,
    // `_ingestOps`, `_roles`) stays completely unused for such a plan — EnsureInit/HandleIncoming both
    // branch on `_unionRoles` FIRST and never touch them.
    private List<TableExecutor>? _unionBranchExecutors;
    private Dictionary<string, List<TableExecutor>>? _unionRoles;
    private TableDistinctOp? _distinct;

    private void EnsureInit()
    {
        if (_initialized) return;
        _initialized = true;

        var compiled = _plan.Compiled;

        if (compiled.UnionBranches is not null)
        {
            EnsureInitUnion(compiled);
            return;
        }

        // Plan 008: TableOuterJoinOp's null-padding needs every alias/schema accumulated so far on the
        // left (to build its all-NULL left row) and this join's own alias/schema (for its all-NULL right
        // row) — mirrors ExecutorImpl.EnsureInit's identical `accumulated` list for PipelineJoinOp.
        var accumulated = new List<(string Alias, SourceSchema Schema)> { (compiled.Sources[0].Alias, compiled.Sources[0].Schema) };

        for (int i = 0; i < compiled.Joins.Count; i++)
        {
            var j = compiled.Joins[i];
            // Plan 008: LEFT/RIGHT/FULL get the null-padding-aware op (composite keys — LeftKeys/
            // RightKeys, the full equi-key lists). Plan 004 N2: Semi/Anti (IN/EXISTS ↔ NOT IN/NOT EXISTS)
            // get the presence-based op; plan 002 L2's Unnest gets the 1-to-N expansion op; every other
            // kind — including N3/N4's Scalar joins — reuses TableJoinOp as-is (see its class doc and
            // Planner.BuildScalarJoin's doc comment on why a plain equi-join is already retraction-correct
            // for those), off the single-key LeftKey/RightKey (component [0], with every other equi-key
            // component already folded into Residual — see JoinKeyFolding).
            _joins.Add(j.Kind switch
            {
                JoinKind.Left or JoinKind.Right or JoinKind.Full =>
                    new TableOuterJoinOp(j.Kind, j.LeftKeys!, j.RightKeys!, j.Residual, compiled.Bindings, accumulated.ToList(), (j.Alias, j.Schema)),
                JoinKind.Semi or JoinKind.Anti => new TableSemiAntiOp(j.Kind, j.LeftKey!, j.RightKey!, compiled.Bindings),
                JoinKind.Unnest => new TableUnnestOp(j.UnnestExpr!, j.Alias, compiled.Bindings),
                _ => new TableJoinOp(j.LeftKey!, j.RightKey!, j.Residual, compiled.Bindings),
            });
            accumulated.Add((j.Alias, j.Schema));
        }

        _filterProject = new TableFilterProjectOp(compiled);

        if (compiled.GroupBy is not null || compiled.HasAggregates)
        {
            _reduce = new TableReduceOp(compiled);
        }
        else if (compiled.LatestBy is not null)
        {
            // Plan 002 L3: LATEST BY is mutually exclusive with GROUP BY/aggregates by construction (see
            // Validator's exclusivity diagnostics) — this `else` mirrors that at the runtime layer too.
            _latestBy = new TableLatestByOp(compiled, compiled.LatestBy);
        }

        AddRole(compiled.Sources[0].SourceName, isFrom: true, stageIndex: -1, compiled.Sources[0].Alias, compiled.Sources[0].DerivedPlan);
        for (int i = 0; i < compiled.Joins.Count; i++)
        {
            if (compiled.Joins[i].Kind == JoinKind.Unnest) continue; // no external driving source — see TableUnnestOp's class doc
            AddRole(compiled.Joins[i].SourceName, isFrom: false, stageIndex: i, compiled.Joins[i].Alias, compiled.Joins[i].DerivedPlan);
        }
    }

    private void AddRole(string name, bool isFrom, int stageIndex, string alias, CompiledTablePlan? derivedPlan)
    {
        if (derivedPlan is null)
        {
            AddRoleUnder(name, new RoleEntry { IsFrom = isFrom, StageIndex = stageIndex, Alias = alias, Derived = null });
        }
        else
        {
            // Derived table/CTE (plan 004 N1): one nested TableExecutor per derived source, registered
            // under every real leaf stream AND table input it transitively depends on (both already
            // flattened through any further nesting by Validator.ResolveFromItem — see its doc comment).
            var derivedExecutor = new TableExecutor(new TablePlan(derivedPlan));
            var entry = new RoleEntry { IsFrom = isFrom, StageIndex = stageIndex, Alias = alias, Derived = derivedExecutor };
            foreach (var leaf in derivedPlan.StreamInputs) AddRoleUnder(leaf, entry);
            foreach (var leaf in derivedPlan.TableInputs) AddRoleUnder(leaf, entry);
        }

        if (!_ingestOps.ContainsKey(alias))
        {
            _ingestOps[alias] = new TableIngestOp(alias);
        }
    }

    private void AddRoleUnder(string name, RoleEntry entry)
    {
        if (!_roles.TryGetValue(name, out var list))
        {
            list = [];
            _roles[name] = list;
        }
        list.Add(entry);
    }

    /// <summary>Plan 008 W3: table-mode union root. One nested TableExecutor per branch — each branch is
    /// itself a COMPLETE, independently-compiled CompiledTablePlan (its own WHERE/GROUP BY/joins/projection
    /// already applied), registered under every real leaf stream AND table input it transitively depends on
    /// (already flattened through any further nesting by Validator.ResolveFromItem — same rule the plain
    /// derived-table AddRole path above already relies on) — the SAME source feeding two branches lands in
    /// both branches' role lists, so one incoming delta fans out to every subscribing branch. UNION
    /// (distinct) additionally wires a TableDistinctOp downstream of the branch concatenation (see
    /// HandleIncomingUnion) — UNION ALL needs nothing further: TableExecutorImpl's own ApplyConsolidation
    /// already sums weights per canonical row key regardless of which branch a delta came from, which IS
    /// UNION ALL's "no dedup, straight weight concatenation" semantics for free.</summary>
    private void EnsureInitUnion(CompiledTablePlan compiled)
    {
        _unionBranchExecutors = [];
        _unionRoles = [];
        foreach (var branch in compiled.UnionBranches!)
        {
            var exec = new TableExecutor(new TablePlan(branch));
            _unionBranchExecutors.Add(exec);
            foreach (var leaf in branch.StreamInputs) AddUnionRoleUnder(leaf, exec);
            foreach (var leaf in branch.TableInputs) AddUnionRoleUnder(leaf, exec);
        }
        if (!compiled.UnionAll)
        {
            _distinct = new TableDistinctOp();
        }
    }

    private void AddUnionRoleUnder(string name, TableExecutor exec)
    {
        if (!_unionRoles!.TryGetValue(name, out var list))
        {
            list = [];
            _unionRoles[name] = list;
        }
        list.Add(exec);
    }

    /// <summary>Plan 008 W3: union-root admission — feeds <paramref name="evt"/>/<paramref name="weight"/>
    /// into every branch executor that subscribes to <paramref name="name"/> via the SAME OnTableDelta call
    /// the plain derived-table role path already uses (see HandleIncoming's `role.Derived.OnTableDelta`
    /// line — routing through OnTableDelta unconditionally reproduces whichever admission the outer caller
    /// actually used, OnStreamEvent's implicit weight=1 included), concatenates every branch's own emitted
    /// deltas, optionally dedups via <see cref="_distinct"/> (UNION only), then folds the result into this
    /// table's own consolidated output exactly like the ordinary path does.</summary>
    private List<TableDelta> HandleIncomingUnion(string name, EventRecord evt, long weight)
    {
        var output = new List<TableDelta>();
        if (_unionRoles!.TryGetValue(name, out var branches))
        {
            foreach (var branchExecutor in branches)
            {
                output.AddRange(branchExecutor.OnTableDelta(name, new TableDelta(evt, weight)));
            }
        }

        if (_distinct is not null)
        {
            var epoch = new Epoch(_epochCounter++);
            output = _distinct.OnBatch(epoch, output).ToList();
        }

        foreach (var delta in output)
        {
            ApplyConsolidation(delta);
        }

        return output;
    }

    private IReadOnlyList<TableDelta> OnStreamEventCore(string source, EventRecord evt)
    {
        EnsureInit();
        return HandleIncoming(source, evt, weight: 1);
    }

    private IReadOnlyList<TableDelta> OnTableDeltaCore(string table, TableDelta delta)
    {
        EnsureInit();
        return HandleIncoming(table, delta.Row, delta.Weight);
    }

    private IReadOnlyDictionary<string, (EventRecord Row, long Weight)> SnapshotCore()
    {
        EnsureInit();
        return _consolidated;
    }

    /// <summary>Test-only introspection hook (see TableConsolidationLedgerTests) for the debt side-table's
    /// size: the number of canonical row keys currently holding outstanding negative running weight, not yet
    /// netted against a later positive delta. Zero whenever every key's history so far is either untouched
    /// or fully cancelled out -- the common case under causal delivery. Not part of the frozen public
    /// contract (PublicApi.cs); `internal` + this assembly's InternalsVisibleTo to StreamForge.Engine.Tests
    /// is enough for the ledger itself to be provable, without adding surface area hosts could depend on.</summary>
    internal int DebtCount => _debtWeights.Count;

    private List<TableDelta> HandleIncoming(string name, EventRecord evt, long weight)
    {
        if (_unionRoles is not null) return HandleIncomingUnion(name, evt, weight);

        var output = new List<TableDelta>();
        if (!_roles.TryGetValue(name, out var roles)) return output;

        var epoch = new Epoch(_epochCounter++);

        foreach (var role in roles)
        {
            // OnStreamEvent (weight=1, always an assertion) and OnTableDelta (arbitrary signed weight)
            // both funnel through this SAME HandleIncoming — so routing a derived role's admission through
            // OnTableDelta unconditionally reproduces whichever one the outer caller actually used,
            // retraction sign and all (plan 004 N1: "table mode: an inline intermediate Z-set operator" —
            // this nested-executor wiring is the equivalent, already retraction-correct by construction
            // since it's the exact same TableExecutor machinery a real table-over-table dependency uses).
            IReadOnlyList<TableDelta> admission = role.Derived is null
                ? [new TableDelta(evt, weight)]
                : role.Derived.OnTableDelta(name, new TableDelta(evt, weight));

            if (admission.Count == 0) continue;

            var admitted = _ingestOps[role.Alias].OnBatch(epoch, admission);

            var afterJoins = role.IsFrom
                ? PropagateForward(0, epoch, admitted)
                : PropagateForward(role.StageIndex + 1, epoch, _joins[role.StageIndex].OnRightBatch(epoch, admitted));

            if (_reduce is not null)
            {
                var filtered = _filterProject!.OnBatch(epoch, afterJoins);
                output.AddRange(_reduce.OnBatch(epoch, filtered));
            }
            else if (_latestBy is not null)
            {
                var filtered = _filterProject!.OnBatch(epoch, afterJoins);
                output.AddRange(_latestBy.OnBatch(epoch, filtered));
            }
            else
            {
                output.AddRange(_filterProject!.OnBatchTerminal(epoch, afterJoins));
            }
        }

        foreach (var delta in output)
        {
            ApplyConsolidation(delta);
        }

        return output;
    }

    private IReadOnlyList<TableRowDelta> PropagateForward(int fromStageIndexInclusive, Epoch epoch, IReadOnlyList<TableRowDelta> rows)
    {
        var current = rows;
        for (int s = fromStageIndexInclusive; s < _joins.Count; s++)
        {
            current = _joins[s].OnLeftBatch(epoch, current);
        }
        return current;
    }

    private void ApplyConsolidation(TableDelta delta)
    {
        var key = JsonText.SerializeCanonicalRow(delta.Row);

        // The running weight for this key BEFORE folding in `delta`, wherever it currently lives (positive
        // in _consolidated, negative in _debtWeights, or 0/absent from both — never in both at once).
        long currentWeight = _consolidated.TryGetValue(key, out var existing)
            ? existing.Weight
            : _debtWeights.GetValueOrDefault(key);

        long newWeight = currentWeight + delta.Weight;

        if (newWeight > 0)
        {
            _consolidated[key] = (delta.Row, newWeight);
            _debtWeights.Remove(key);
        }
        else if (newWeight < 0)
        {
            _consolidated.Remove(key);
            _debtWeights[key] = newWeight;
        }
        else // newWeight == 0: fully cancelled out — no residue in either structure.
        {
            _consolidated.Remove(key);
            _debtWeights.Remove(key);
        }
    }
}
