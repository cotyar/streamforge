using StreamsForge.Engine.Dataflow;
using StreamsForge.Engine.Planning;
using StreamsForge.Engine.Runtime;
using StreamsForge.Engine.Runtime.Ops;
using StreamsForge.Engine.Sql;

namespace StreamsForge.Engine;

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
/// to OnStreamEvent/OnTableDelta/OnTableDeltaBatch is stamped with its own epoch from a trivial
/// monotonically advancing counter (plan 003 M1: "epoch = a trivial advancing counter"); every op invoked
/// while servicing one call shares that call's epoch, since the whole call is one atomic admission from
/// this table's point of view. No op in table mode's OnFrontier hook does anything with epochs yet (see
/// each op's class doc) — this façade doesn't even call OnFrontier on the hot path for that reason; the
/// hook is proven live via dedicated per-op unit tests instead (see OpsTests / TableReduceOpTests etc. —
/// OnFrontier pass-through is asserted there, not exercised through this façade).
///
/// WISHLIST #15 — ONE EPOCH FOR A WHOLE INCOMING BATCH, ADMITTED AND EMITTED ATOMICALLY.
/// <see cref="TableExecutor.OnTableDeltaBatch"/> is the batch sibling of OnStreamEvent/OnTableDelta: every
/// element of the batch shares the ONE epoch <see cref="HandleIncoming"/> allocates for the call, instead
/// of each element getting its own epoch the way a HOST-SIDE loop over single-item OnTableDelta calls used
/// to produce (see TableGrain.OnTableDeltaBatchAsync / TableActor.ProcessTableDeltasAsync, both now callers
/// of this batch entry point). OnStreamEvent/OnTableDelta themselves are unchanged in behavior — they are
/// now one-element calls into the same batch machinery (<see cref="OnTableDeltaBatchCore"/>), so a single
/// upstream delta still gets exactly the epoch it always did.
///
/// That alone is not sufficient: an op that processes a multi-element batch one input delta at a time
/// internally (<see cref="Ops.TableReduceOp"/>'s OnDelta, <see cref="Ops.TableOuterJoinOp"/>'s OnArrival)
/// can still walk its OWN output through a transient intermediate row between two elements that share this
/// epoch — a stale aggregate value, a null-padded join row — even though the epoch is atomic from THIS
/// table's point of view. <see cref="ConsolidateEpochOutput"/> is the other half: it nets this epoch's raw
/// output by canonical row key before it is returned (and before it is folded into <see cref="_ledger"/>),
/// so any row this epoch asserted and then retracted (or the reverse) before ever leaving the table
/// disappears from what a caller/consumer ever sees. See that method's own doc comment for the mechanism
/// and docs/otc-demo-wishlist.md #15 for the reported symptom (a LEFT JOIN onto a GROUP BY table observing
/// the joined column flap to NULL and back for one upstream change).
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

    // Consolidated output Z-set ledger (Plan 009 wave D: extracted — was two separate dictionaries here,
    // `_consolidated`/`_debtWeights`; see ConsolidationLedger's own class doc for the full order-
    // independence argument they used to carry locally). Snapshot() returns _ledger.Visible BY REFERENCE
    // (see SnapshotCore below) so hosts on the hot `/rows` read path get an allocation-free view.
    private readonly ConsolidationLedger _ledger = new();

    private long _epochCounter;

    /// <summary>Wishlist #14 option (a) — see <see cref="TableExecutor.LastEpoch"/>'s own doc comment (in
    /// PublicApi.cs) for the full contract this backs. Set at the SAME point <see cref="_epochCounter"/> is
    /// consumed in both <see cref="HandleIncoming"/> and <see cref="HandleIncomingUnionBatch"/> — i.e. once
    /// per call that actually admits a non-empty batch against a subscribed role, regardless of whether that
    /// call's own OUTPUT ends up empty after consolidation. -1 until the first such call.</summary>
    private long _lastEpoch = -1;

    // Plan 011 C2 — row retention. All three are inert until ConfigureRetention installs an ENABLED policy,
    // so a table without one keeps the pre-011 hot path exactly (no ordering index, no per-batch check
    // beyond a single `IsEnabled` bool test). _retentionScope is whichever structure actually owns this
    // plan's rows — the LATEST BY op's per-key map, or _ledger for a plain projection; _ledgerScope is the
    // same object as _retentionScope in the latter case, held separately only because ApplyConsolidation
    // has to keep its ordering index in step with the ledger.
    private TableRetentionPolicy _retention = TableRetentionPolicy.None;
    private IRetentionScope? _retentionScope;
    private LedgerRetentionScope? _ledgerScope;

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

    /// <summary>Plan 008 W3: union-root admission — feeds <paramref name="deltas"/> (as a single atomic
    /// batch, see wishlist #15) into every branch executor that subscribes to <paramref name="name"/> via
    /// the SAME <see cref="OnTableDeltaBatchCore"/> call the plain derived-table role path already uses
    /// (see HandleIncoming's `role.Derived.OnTableDeltaBatchCore` line — routing through the batch entry
    /// point unconditionally reproduces whichever admission the outer caller actually used, a one-element
    /// batch included, so a plain OnStreamEvent/OnTableDelta caller sees byte-identical behavior to before
    /// this batch entry point existed), concatenates every branch's own emitted deltas, optionally dedups
    /// via <see cref="_distinct"/> (UNION only), consolidates the result exactly like the ordinary path
    /// does (see <see cref="ConsolidateEpochOutput"/>), then folds it into this table's own consolidated
    /// output.</summary>
    private List<TableDelta> HandleIncomingUnionBatch(string name, IReadOnlyList<TableDelta> deltas)
    {
        var output = new List<TableDelta>();
        if (deltas.Count == 0) return output;

        // Wishlist #14 option (a): allocate (and record as LastEpoch) unconditionally, not only when
        // _distinct needs one — before this fix, a UNION ALL (no _distinct) branch of this method never
        // touched _epochCounter/_lastEpoch at all, which would have left TableExecutor.LastEpoch stuck for
        // every admission a union-without-DISTINCT table ever processed. _epochCounter is otherwise
        // Engine-private (no pre-existing consumer could observe the value directly), so widening when it
        // advances is behavior-invisible except through the new LastEpoch property.
        var epoch = new Epoch(_epochCounter++);
        _lastEpoch = epoch.Value;

        if (_unionRoles!.TryGetValue(name, out var branches))
        {
            foreach (var branchExecutor in branches)
            {
                output.AddRange(branchExecutor.OnTableDeltaBatchCore(name, deltas));
            }
        }

        if (_distinct is not null)
        {
            output = _distinct.OnBatch(epoch, output).ToList();
        }

        // See ConsolidateEpochOutput's own doc comment for why this is gated on deltas.Count (the CALL's
        // OWN admitted batch size), not on output.Count: a single admitted delta is allowed to legitimately
        // emit a same-content retract+assert pair (e.g. a branch's own GROUP BY reporting an unchanged
        // aggregate value across a contributing change) and that pair must NOT be netted away — only a
        // multi-element admission gets the netting, which is the only shape wishlist #15 is about.
        if (deltas.Count > 1) output = ConsolidateEpochOutput(output);

        foreach (var delta in output)
        {
            ApplyConsolidation(delta);
        }

        return output;
    }

    private IReadOnlyList<TableDelta> OnStreamEventCore(string source, EventRecord evt)
    {
        EnsureInit();
        return HandleIncoming(source, [new TableDelta(evt, 1)]);
    }

    private IReadOnlyList<TableDelta> OnTableDeltaCore(string table, TableDelta delta)
    {
        EnsureInit();
        return HandleIncoming(table, [delta]);
    }

    /// <summary>Wishlist #15 — the batch entry point (see <see cref="TableExecutor.OnTableDeltaBatch"/>
    /// and this class's own EPOCH doc paragraph). Identical shape to <see cref="OnTableDeltaCore"/>, just
    /// without collapsing to a one-element list first — every element of <paramref name="deltas"/> is
    /// admitted under the SAME epoch.</summary>
    private IReadOnlyList<TableDelta> OnTableDeltaBatchCore(string table, IReadOnlyList<TableDelta> deltas)
    {
        EnsureInit();
        return HandleIncoming(table, deltas);
    }

    private IReadOnlyDictionary<string, (EventRecord Row, long Weight)> SnapshotCore()
    {
        EnsureInit();
        return _ledger.Visible;
    }

    /// <summary>Plan 011 C2 — see <see cref="TableExecutor.ConfigureRetention"/> for the contract and for
    /// why the policy is installed rather than driven. Picks the scope from the plan's shape: LATEST BY
    /// owns its rows in <see cref="TableLatestByOp.Current"/>, everything else supported owns them in
    /// <see cref="_ledger"/>. An unsupported shape throws instead of quietly installing a policy that
    /// could only trim the derived copies (see <see cref="TablePlan.SupportsRetention"/>).</summary>
    private void ConfigureRetentionCore(TableRetentionPolicy policy)
    {
        EnsureInit();

        if (!policy.IsEnabled)
        {
            _retention = TableRetentionPolicy.None;
            return;
        }

        if (!TableRetentionSupport.IsSupported(_plan.Compiled))
        {
            throw new InvalidOperationException(
                "Row retention is not supported for this table's plan shape (joins, set operations, derived sources and GROUP BY/aggregates are excluded) — see TablePlan.SupportsRetention.");
        }

        _retention = policy;

        if (_latestBy is not null)
        {
            _latestBy.EnableRetention();
            _retentionScope = _latestBy;
        }
        else
        {
            _ledgerScope ??= new LedgerRetentionScope(_ledger);
            // Seed the ordering index from whatever is already consolidated. Empty in practice (a table
            // configures retention on start, against a brand-new executor), but seeding keeps the index
            // and the ledger consistent by construction rather than by call-order luck.
            foreach (var key in _ledger.Visible.Keys.ToList()) _ledgerScope.Observe(key);
            _retentionScope = _ledgerScope;
        }
    }

    /// <summary>Plan 011 C2 — applies the configured bounds after a batch has been consolidated, returning
    /// the eviction retractions to append to that batch's own output. TTL runs before MaxRows so an
    /// already-expired row is never counted against the row budget. Each eviction is folded into the
    /// ledger through the ordinary <see cref="ApplyConsolidation"/> path — a retraction is a retraction,
    /// whoever emitted it.</summary>
    private List<TableDelta> RunRetention()
    {
        var evicted = new List<TableDelta>();
        var scope = _retentionScope!;

        if (_retention.TtlMs > 0)
        {
            evicted.AddRange(scope.EvictOlderThan(scope.MaxObservedTs - _retention.TtlMs));
        }

        if (_retention.MaxRows > 0 && scope.RetainedCount > _retention.MaxRows)
        {
            evicted.AddRange(scope.EvictOldest(scope.RetainedCount - _retention.MaxRows));
        }

        foreach (var delta in evicted) ApplyConsolidation(delta);
        return evicted;
    }

    /// <summary>Test-only introspection hook (see TableConsolidationLedgerTests) for the debt side-table's
    /// size: the number of canonical row keys currently holding outstanding negative running weight, not yet
    /// netted against a later positive delta. Zero whenever every key's history so far is either untouched
    /// or fully cancelled out -- the common case under causal delivery. Not part of the frozen public
    /// contract (PublicApi.cs); `internal` + this assembly's InternalsVisibleTo to StreamsForge.Engine.Tests
    /// is enough for the ledger itself to be provable, without adding surface area hosts could depend on.</summary>
    internal int DebtCount => _ledger.DebtCount;

    /// <summary>Plan 011 C2 test-only introspection hook, in the same spirit (and with the same
    /// `internal` + InternalsVisibleTo justification) as <see cref="DebtCount"/> above: how many entries
    /// the RETENTION SCOPE currently holds — i.e. the size of the structure that actually owns this
    /// table's rows (TableLatestByOp.Current, or the ledger), not of the consolidated output copy. It
    /// exists because the one thing a retention test MUST prove is exactly the thing Snapshot() cannot
    /// show: that eviction reclaimed the operator's own per-key state and not merely the mirror. -1 when
    /// no policy is configured.</summary>
    internal int RetainedStateCount => _retentionScope?.RetainedCount ?? -1;

    /// <summary>Introspection hook in the same spirit as <see cref="DebtCount"/>: how many retractions
    /// this table's GROUP BY has received for a group it had never asserted. Non-zero means this table
    /// was attached to an upstream that ALREADY HELD ROWS and those rows were never replayed to it, so
    /// its aggregates are missing them — see TableReduceOp.UnmatchedRetractions. -1 when the plan has no
    /// GROUP BY, so "no aggregate here" is distinguishable from "an aggregate with nothing wrong".</summary>
    internal long UnmatchedRetractions => _reduce?.UnmatchedRetractions ?? -1;

    /// <summary>The one admission path every entry point (OnStreamEvent, OnTableDelta, and wishlist #15's
    /// OnTableDeltaBatch) funnels through, via <see cref="OnStreamEventCore"/>/<see cref="OnTableDeltaCore"/>
    /// wrapping their single item as a one-element <paramref name="deltas"/> list — so a plain,
    /// pre-batch-entry-point caller gets byte-identical behavior (one epoch, admitted and consolidated
    /// exactly as before) to what this method did when it only ever took one (evt, weight) pair. The whole
    /// of <paramref name="deltas"/>, however many elements, shares ONE epoch: that is wishlist #15's first
    /// half (see this class's own EPOCH doc paragraph). <see cref="ConsolidateEpochOutput"/> below is the
    /// second half.</summary>
    private List<TableDelta> HandleIncoming(string name, IReadOnlyList<TableDelta> deltas)
    {
        if (_unionRoles is not null) return HandleIncomingUnionBatch(name, deltas);

        var output = new List<TableDelta>();
        if (deltas.Count == 0) return output;
        if (!_roles.TryGetValue(name, out var roles)) return output;

        var epoch = new Epoch(_epochCounter++);
        _lastEpoch = epoch.Value; // Wishlist #14 option (a) — see TableExecutor.LastEpoch's own doc comment.

        foreach (var role in roles)
        {
            // OnStreamEvent (weight=1, always an assertion) and OnTableDelta/OnTableDeltaBatch (arbitrary
            // signed weight) all funnel through this SAME HandleIncoming — so routing a derived role's
            // admission through OnTableDeltaBatchCore unconditionally reproduces whichever the outer caller
            // actually used, retraction sign and every element of the batch included (plan 004 N1: "table
            // mode: an inline intermediate Z-set operator" — this nested-executor wiring is the equivalent,
            // already retraction-correct by construction since it's the exact same TableExecutor machinery
            // a real table-over-table dependency uses). No per-element list allocation is needed for the
            // non-derived case: `deltas` IS already the admission this role's ingest op wants.
            IReadOnlyList<TableDelta> admission = role.Derived is null
                ? deltas
                : role.Derived.OnTableDeltaBatchCore(name, deltas);

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

        // Wishlist #15, second half — see ConsolidateEpochOutput's own doc comment: nets this epoch's raw
        // output by canonical row key BEFORE it is returned or folded into _ledger, so a transient
        // intermediate row an op produced while walking a multi-element batch one input delta at a time
        // never reaches a caller/consumer of this table. Gated on deltas.Count (this CALL's own admitted
        // batch size) rather than output.Count: a SINGLE admitted delta can legitimately produce a
        // same-content retract+assert pair on its own (TableReduceOp always emits retract(old)+assert(new)
        // for any contributing change to an existing group, even one that leaves the computed aggregate
        // value unchanged — e.g. removing one of two duplicate MIN contributors) and that pair is the
        // intended, pinned-by-existing-tests shape for a one-element admission, not the flapping this
        // exists to remove. Only a genuinely multi-element admission — several deltas sharing one epoch,
        // which is exactly what TableGrain.OnTableDeltaBatchAsync/TableActor.ProcessTableDeltasAsync now
        // feed in — gets the netting.
        if (deltas.Count > 1) output = ConsolidateEpochOutput(output);

        foreach (var delta in output)
        {
            ApplyConsolidation(delta);
        }

        // Plan 011 C2: eviction retractions ride out on this call's own return value, AFTER its real
        // deltas — see TableExecutor.ConfigureRetention's doc for why that (and not a separate method) is
        // what keeps every downstream consumer consistent.
        if (_retention.IsEnabled) output.AddRange(RunRetention());

        return output;
    }

    /// <summary>
    /// Wishlist #15 — Z-set consolidation of ONE epoch's raw op output, applied here (centrally, once per
    /// call) rather than inside any individual op, BEFORE the batch is returned to the caller or folded
    /// into <see cref="_ledger"/>.
    ///
    /// WHY THIS IS NECESSARY EVEN THOUGH EVERY OP ALREADY PRODUCES A WELL-FORMED Z-SET DELTA STREAM: an op
    /// that processes more than one input delta per <c>OnBatch</c> call does so ONE INPUT DELTA AT A TIME
    /// internally (see <see cref="Ops.TableReduceOp"/>.OnDelta and <see cref="Ops.TableOuterJoinOp"/>.
    /// OnArrival's own doc comments — neither op looks ahead at the rest of the batch before emitting).
    /// So a single upstream CHANGE admitted in one epoch as, say, [retract(old), assert(new)] can still
    /// walk that op's OWN output through a transient INTERMEDIATE row — a stale aggregate value, a
    /// null-padded join row from a right-side retract that hasn't yet seen the matching assert — between
    /// processing the retract element and the assert element, even though both share this epoch. That
    /// intermediate row was only ever true of the op's own bookkeeping mid-batch, never of the input as a
    /// WHOLE; a consumer downstream of this table (a LEFT JOIN, a SignalR client) that happens to observe
    /// it sees a wrong answer for however long it takes the batch's next element to correct it — the
    /// flapping wishlist #15 reports (`scenario_trigger_monitor.threshold_headroom` reading NULL mid-tick
    /// in the demo).
    ///
    /// THE FIX: net every delta in this epoch's raw output by canonical row key (the SAME scheme
    /// <see cref="ApplyConsolidation"/>/<see cref="ConsolidationLedger"/> already use for this table's own
    /// ledger, applied here to the RETURNED batch instead) and drop any key whose net weight is exactly
    /// zero — i.e. any row this epoch asserted and then retracted (or the reverse) before it ever left this
    /// table. What survives is exactly the epoch's NET effect on each row it touched; the intermediate
    /// never happened as far as anything downstream of this call can tell. Order-preserving (first-
    /// occurrence position survives, later occurrences of the same key only contribute their weight) and
    /// safe to keep whichever literal row instance was seen first for a key: the same canonical-key-means-
    /// byte-identical-content invariant <see cref="ConsolidationLedger"/>'s class doc already relies on.
    ///
    /// CALLERS GATE THIS ON deltas.Count &gt; 1 (the ADMITTED BATCH's own size), not on this method's own
    /// input size, and that gate is load-bearing, not an optimization: a SINGLE admitted delta can, on its
    /// own, legitimately produce a same-content retract+assert pair — TableReduceOp always emits
    /// retract(old)+assert(new) for any contributing change to an existing group, even one whose computed
    /// aggregate value doesn't move (e.g. removing one of two duplicate MIN contributors leaves the
    /// reported minimum unchanged, but is still reported as retract(100)+assert(100)) — and that pair is
    /// the correct, existing-tests-pinned shape for a ONE-element admission, not an artifact of walking a
    /// multi-element batch. Consolidating it away would be observably wrong: it would make a single
    /// OnStreamEvent/OnTableDelta call sometimes return fewer/different deltas than it always has. This
    /// method itself has no way to tell that case apart from a genuine multi-delta flap by inspecting the
    /// output alone (both look like "assert then retract of the same canonical key"), so the distinction is
    /// enforced by the caller never invoking this method for a one-element admission in the first place.
    /// </summary>
    private static List<TableDelta> ConsolidateEpochOutput(List<TableDelta> raw)
    {
        if (raw.Count <= 1) return raw;

        var netWeight = new Dictionary<string, long>();
        var firstRow = new Dictionary<string, EventRecord>();
        var order = new List<string>();

        foreach (var delta in raw)
        {
            var key = JsonText.SerializeCanonicalRow(delta.Row);
            if (netWeight.TryGetValue(key, out var existing))
            {
                netWeight[key] = existing + delta.Weight;
            }
            else
            {
                netWeight[key] = delta.Weight;
                firstRow[key] = delta.Row;
                order.Add(key);
            }
        }

        var consolidated = new List<TableDelta>(order.Count);
        foreach (var key in order)
        {
            var weight = netWeight[key];
            if (weight != 0) consolidated.Add(new TableDelta(firstRow[key], weight));
        }
        return consolidated;
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
        _ledger.Apply(key, delta.Row, delta.Weight);
        // Plan 011 C2: null unless this plan's retention scope IS the ledger, in which case its ordering
        // index has to observe every visibility change the ledger just made.
        _ledgerScope?.Observe(key);
    }
}
