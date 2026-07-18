using StreamForge.Engine.Dataflow;
using StreamForge.Engine.Planning;

namespace StreamForge.Engine.Runtime.Ops;

/// <summary>
/// Maintains per-group running aggregate state for table-mode GROUP BY (no window: groups live forever,
/// updated by every contributing delta) — plan 003 M1's suggested op set: "TableReduceOp (running
/// GROUP-BY aggregates incl. multiset MIN/MAX + retraction emission)". A GROUP BY-less aggregate is a
/// single implicit global group (GroupValues = []). On any group change this emits a retraction (old row,
/// -1) — skipped the very first time a group produces output — followed by an assertion (new row, +1);
/// when the group's total contributing weight reaches (or starts at) zero, only the retraction is emitted
/// and the group is dropped.
///
/// Mechanical relocation of the pre-M1 `TableGroupOperator` into the explicit-op shape: same per-row
/// algorithm (unchanged), `OnDelta` batched into `OnBatch`, implements <see cref="ITableOp"/>.
///
/// STATE: <see cref="Groups"/> — group-key string -> <see cref="GroupState"/> { object?[] GroupValues,
/// IZAggregator[] Aggregators, long TotalWeight }. GroupValues and TotalWeight are plain data; the
/// Aggregators array is NOT yet plain data — see this M1 pass's residue report: CountZAggregator/
/// SumZAggregator/AvgZAggregator hold private scalar accumulators, MinMaxZAggregator holds a private
/// SortedDictionary multiset. A future M2 checkpoint needs either (a) a ToState()/FromState() pair added
/// to IZAggregator (cheap: count/sum/avg state is 1-3 scalars each; MIN/MAX's multiset already IS its own
/// checkpoint, just needs exposing), or (b) rebuilding aggregator state by replaying the group's
/// contributing deltas from a checkpointed log — out of scope for M1, flagged here for M2's design.
/// </summary>
internal sealed class TableReduceOp : ITableOp
{
    internal sealed class GroupState
    {
        public required object?[] GroupValues;
        public required IZAggregator[] Aggregators;
        public long TotalWeight;
    }

    private readonly CompiledTablePlan _plan;

    /// <summary>This op's state: group-key string -> running per-group aggregate state. See class doc for
    /// the shape and its M2 checkpointing caveat.</summary>
    public Dictionary<string, GroupState> Groups { get; } = [];

    public TableReduceOp(CompiledTablePlan plan) => _plan = plan;

    public IReadOnlyList<TableDelta> OnBatch(Epoch epoch, IReadOnlyList<TableRowDelta> input)
    {
        var results = new List<TableDelta>();
        foreach (var d in input)
        {
            results.AddRange(OnDelta(d.Row, d.Weight));
        }
        return results;
    }

    private List<TableDelta> OnDelta(WorkingRow row, long weight)
    {
        var groupValues = EvalGroupValues(row);
        var key = TableKeyEncoding.EncodeGroupKey(groupValues);

        if (!Groups.TryGetValue(key, out var state))
        {
            state = new GroupState { GroupValues = groupValues, Aggregators = CreateAggregators() };
            Groups[key] = state;
        }

        var results = new List<TableDelta>();
        bool hadOutput = state.TotalWeight > 0;
        EventRecord? oldRow = hadOutput ? BuildRow(state) : null;

        for (int i = 0; i < _plan.AggregateNodes.Count; i++)
        {
            var node = _plan.AggregateNodes[i];
            object? val = node.IsStar ? true : ExpressionEvaluator.Eval(node.Arg!, new EvalContext(row, _plan.Bindings));
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
            Groups.Remove(key);
        }

        return results;
    }

    private IZAggregator[] CreateAggregators() =>
        _plan.AggregateNodes.Select(n => ZAggregator.Create(n.Name, n.IsStar)).ToArray();

    private object?[] EvalGroupValues(WorkingRow row)
    {
        if (_plan.GroupBy is null) return [];
        var ctx = new EvalContext(row, _plan.Bindings);
        return _plan.GroupBy.Select(g => ExpressionEvaluator.Eval(g, ctx)).ToArray();
    }

    private EventRecord BuildRow(GroupState state)
    {
        var evt = new EventRecord();
        AggregateLookup lookup = node => state.Aggregators[_plan.AggregateIndex[node]].Result;
        var dummyRow = new WorkingRow { Ts = 0, Aliases = [], Fields = [] };
        var ctx = new EvalContext(dummyRow, _plan.Bindings, lookup);

        foreach (var item in _plan.Output)
        {
            object? value = item.GroupByIndex is int gi ? state.GroupValues[gi] : ExpressionEvaluator.Eval(item.Expression, ctx);
            evt[item.Name] = value;
        }

        evt[EventRecord.TimestampField] = 0L;
        evt[EventRecord.SourceField] = _plan.SourceLabel;
        return evt;
    }

    /// <summary>Pass-through — see class doc: table GROUP BY is unwindowed and runs forever; nothing
    /// closes on frontier today. M4 (plan 003) will change this for EMIT FINAL table variants.</summary>
    public IReadOnlyList<TableDelta> OnFrontier(Epoch epoch) => [];
}
