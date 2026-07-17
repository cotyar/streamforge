using StreamForge.Engine.Planning;
using StreamForge.Engine.Runtime;

namespace StreamForge.Engine;

/// <summary>Attaches the compiled, executable table plan to the frozen <see cref="TablePlan"/> DTO.</summary>
public sealed partial class TablePlan
{
    internal CompiledTablePlan Compiled { get; }

    internal TablePlan(CompiledTablePlan compiled) => Compiled = compiled;
}

/// <summary>Single-threaded Z-set runtime for a compiled table: folds JOINs left-to-right through a chain
/// of <see cref="TableJoinStage"/>s (relational equi-join, both sides fully indexed), applies WHERE
/// (weight passthrough), then hands surviving (row, weight) deltas to a <see cref="TableGroupOperator"/>
/// (if grouped/aggregated — emits retraction/assertion pairs) or projects them straight through
/// (filter/project: weight passthrough). Every emitted delta also folds into this table's own consolidated
/// output Z-set, exposed via Snapshot().</summary>
public sealed partial class TableExecutor
{
    private bool _initialized;
    private readonly List<TableJoinStage> _stages = [];
    private TableGroupOperator? _group;

    // source/table name -> every role it plays in the plan (FROM and/or one or more JOIN aliases).
    private readonly Dictionary<string, List<(bool IsFrom, int StageIndex, string Alias)>> _roles = [];

    // Consolidated output: canonical row text -> (row, weight). Weight <= 0 entries are pruned immediately
    // (DBSP-style consolidation) — Snapshot() only ever exposes weight > 0 rows.
    private readonly Dictionary<string, (EventRecord Row, long Weight)> _consolidated = [];

    private void EnsureInit()
    {
        if (_initialized) return;
        _initialized = true;

        var compiled = _plan.Compiled;

        for (int i = 0; i < compiled.Joins.Count; i++)
        {
            var j = compiled.Joins[i];
            _stages.Add(new TableJoinStage(j.LeftKey!, j.RightKey!, j.Residual, compiled.Bindings));
        }

        if (compiled.GroupBy is not null || compiled.HasAggregates)
        {
            _group = new TableGroupOperator(compiled);
        }

        AddRole(compiled.Sources[0].SourceName, isFrom: true, stageIndex: -1, compiled.Sources[0].Alias);
        for (int i = 0; i < compiled.Joins.Count; i++)
        {
            AddRole(compiled.Joins[i].SourceName, isFrom: false, stageIndex: i, compiled.Joins[i].Alias);
        }
    }

    private void AddRole(string name, bool isFrom, int stageIndex, string alias)
    {
        if (!_roles.TryGetValue(name, out var list))
        {
            list = [];
            _roles[name] = list;
        }
        list.Add((isFrom, stageIndex, alias));
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

        var compiled = _plan.Compiled;

        foreach (var role in roles)
        {
            var initial = WorkingRow.FromEvent(role.Alias, evt);
            List<(WorkingRow Row, long Weight)> combined = role.IsFrom
                ? PropagateForward(0, [(initial, weight)])
                : PropagateForward(role.StageIndex + 1, _stages[role.StageIndex].OnRight(initial, weight));

            foreach (var (row, w) in combined)
            {
                if (compiled.Where is not null)
                {
                    var ok = ExpressionEvaluator.IsTrue(ExpressionEvaluator.Eval(compiled.Where, new EvalContext(row, compiled.Bindings)));
                    if (!ok) continue;
                }

                if (_group is not null)
                {
                    output.AddRange(_group.OnDelta(row, w));
                }
                else
                {
                    output.Add(new TableDelta(ProjectRow(row, compiled), w));
                }
            }
        }

        foreach (var delta in output)
        {
            ApplyConsolidation(delta);
        }

        return output;
    }

    private List<(WorkingRow Row, long Weight)> PropagateForward(int fromStageIndexInclusive, List<(WorkingRow Row, long Weight)> rows)
    {
        var current = rows;
        for (int s = fromStageIndexInclusive; s < _stages.Count; s++)
        {
            var next = new List<(WorkingRow, long)>();
            foreach (var (r, w) in current)
            {
                next.AddRange(_stages[s].OnLeft(r, w));
            }
            current = next;
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

    private static EventRecord ProjectRow(WorkingRow row, CompiledTablePlan compiled)
    {
        var evt = new EventRecord();
        var ctx = new EvalContext(row, compiled.Bindings);
        foreach (var item in compiled.Output)
        {
            evt[item.Name] = ExpressionEvaluator.Eval(item.Expression, ctx);
        }
        evt[EventRecord.TimestampField] = row.Ts;
        evt[EventRecord.SourceField] = compiled.SourceLabel;
        return evt;
    }
}
