using StreamForge.Engine.Dataflow;
using StreamForge.Engine.Planning;
using StreamForge.Engine.Runtime;
using StreamForge.Engine.Runtime.Ops;

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
    private readonly List<TableJoinOp> _joins = [];
    private TableFilterProjectOp? _filterProject;
    private TableReduceOp? _reduce;
    private readonly Dictionary<string, TableIngestOp> _ingestOps = [];

    // real leaf stream/table name -> every role it plays in the plan (FROM and/or one or more JOIN
    // aliases, directly or via a derived source's nested TableExecutor).
    private readonly Dictionary<string, List<RoleEntry>> _roles = [];

    // Consolidated output: canonical row text -> (row, weight). Weight <= 0 entries are pruned immediately
    // (DBSP-style consolidation) — Snapshot() only ever exposes weight > 0 rows.
    private readonly Dictionary<string, (EventRecord Row, long Weight)> _consolidated = [];

    private long _epochCounter;

    private void EnsureInit()
    {
        if (_initialized) return;
        _initialized = true;

        var compiled = _plan.Compiled;

        for (int i = 0; i < compiled.Joins.Count; i++)
        {
            var j = compiled.Joins[i];
            _joins.Add(new TableJoinOp(j.LeftKey!, j.RightKey!, j.Residual, compiled.Bindings));
        }

        _filterProject = new TableFilterProjectOp(compiled);

        if (compiled.GroupBy is not null || compiled.HasAggregates)
        {
            _reduce = new TableReduceOp(compiled);
        }

        AddRole(compiled.Sources[0].SourceName, isFrom: true, stageIndex: -1, compiled.Sources[0].Alias, compiled.Sources[0].DerivedPlan);
        for (int i = 0; i < compiled.Joins.Count; i++)
        {
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

    private List<TableDelta> HandleIncoming(string name, EventRecord evt, long weight)
    {
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
        if (_consolidated.TryGetValue(key, out var existing))
        {
            long newWeight = existing.Weight + delta.Weight;
            if (newWeight <= 0) _consolidated.Remove(key);
            else _consolidated[key] = (existing.Row, newWeight);
        }
        else if (delta.Weight > 0)
        {
            _consolidated[key] = (delta.Row, delta.Weight);
        }
    }
}
