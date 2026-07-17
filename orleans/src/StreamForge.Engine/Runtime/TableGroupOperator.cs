using StreamForge.Engine.Planning;
using StreamForge.Engine.Sql;

namespace StreamForge.Engine.Runtime;

/// <summary>Maintains per-group running aggregate state for table-mode GROUP BY (no window: groups live
/// forever, updated by every contributing delta). A GROUP BY-less aggregate is a single implicit global
/// group (GroupValues = []). On any group change this emits a retraction (old row, −1) — skipped the very
/// first time a group produces output — followed by an assertion (new row, +1); when the group's total
/// contributing weight reaches (or starts at) zero, only the retraction is emitted and the group is
/// dropped.</summary>
internal sealed class TableGroupOperator(CompiledTablePlan plan)
{
    private sealed class GroupState
    {
        public required object?[] GroupValues;
        public required IZAggregator[] Aggregators;
        public long TotalWeight;
    }

    private readonly Dictionary<string, GroupState> _groups = [];

    public List<TableDelta> OnDelta(WorkingRow row, long weight)
    {
        var groupValues = EvalGroupValues(row);
        var key = TableKeyEncoding.EncodeGroupKey(groupValues);

        if (!_groups.TryGetValue(key, out var state))
        {
            state = new GroupState { GroupValues = groupValues, Aggregators = CreateAggregators() };
            _groups[key] = state;
        }

        var results = new List<TableDelta>();
        bool hadOutput = state.TotalWeight > 0;
        EventRecord? oldRow = hadOutput ? BuildRow(state) : null;

        for (int i = 0; i < plan.AggregateNodes.Count; i++)
        {
            var node = plan.AggregateNodes[i];
            object? val = node.IsStar ? true : ExpressionEvaluator.Eval(node.Arg!, new EvalContext(row, plan.Bindings));
            state.Aggregators[i].Apply(val, weight);
        }
        state.TotalWeight += weight;

        if (oldRow is not null) results.Add(new TableDelta(oldRow, -1));

        if (state.TotalWeight > 0)
        {
            results.Add(new TableDelta(BuildRow(state), 1));
        }
        else
        {
            _groups.Remove(key);
        }

        return results;
    }

    private IZAggregator[] CreateAggregators() =>
        plan.AggregateNodes.Select(n => ZAggregator.Create(n.Name, n.IsStar)).ToArray();

    private object?[] EvalGroupValues(WorkingRow row)
    {
        if (plan.GroupBy is null) return [];
        var ctx = new EvalContext(row, plan.Bindings);
        return plan.GroupBy.Select(g => ExpressionEvaluator.Eval(g, ctx)).ToArray();
    }

    private EventRecord BuildRow(GroupState state)
    {
        var evt = new EventRecord();
        AggregateLookup lookup = node => state.Aggregators[plan.AggregateIndex[node]].Result;
        var dummyRow = new WorkingRow { Ts = 0, Aliases = [], Fields = [] };
        var ctx = new EvalContext(dummyRow, plan.Bindings, lookup);

        foreach (var item in plan.Output)
        {
            object? value = item.GroupByIndex is int gi ? state.GroupValues[gi] : ExpressionEvaluator.Eval(item.Expression, ctx);
            evt[item.Name] = value;
        }

        evt[EventRecord.TimestampField] = 0L;
        evt[EventRecord.SourceField] = plan.SourceLabel;
        return evt;
    }
}
