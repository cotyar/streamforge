using StreamForge.Engine.Planning;
using StreamForge.Engine.Sql;

namespace StreamForge.Engine.Runtime.Ops;

/// <summary>
/// Maintains per-(window,group) aggregate state for TUMBLING/HOPPING/SESSION windows. EMIT CHANGES
/// additionally emits an update row (_final=false) on every contributing input row; closed windows always
/// emit a final row (watermark passing window end / session gap).
///
/// Mechanical relocation of the pre-M1 `WindowOperator` (Runtime/WindowOperator.cs) into the explicit-op
/// shape (plan 003 M1 Part B) — algorithm unchanged (tumbling/hopping/session window resolution, EMIT
/// CHANGES, watermark-driven close); only the type name and namespace moved.
///
/// STATE: <see cref="States"/> (window-key string -> WindowState) and <see cref="OpenSession"/>
/// (group-key string -> the currently-open session for SESSION windows, a subset view of States kept for
/// O(1) "is there an open session for this group" lookups — not independent state). Each WindowState
/// carries Start/End/GroupValues/GroupKeyStr plus a per-aggregate accumulator array (Aggregator[] —
/// see this M1 pass's residue report on TableReduceOp for the identical caveat: Aggregator's own fields
/// aren't yet plain POCO data).
/// </summary>
internal sealed class PipelineWindowOp(CompiledPlan plan)
{
    internal sealed class WindowState
    {
        public required long Start;
        public long End;
        public required object?[] GroupValues;
        public required string GroupKeyStr;
        public required Aggregator[] Aggregators;
    }

    /// <summary>This op's primary state: window-key string -> running per-window aggregate state.</summary>
    public Dictionary<string, WindowState> States { get; } = [];

    /// <summary>Derived index (group-key -> currently-open session), SESSION windows only. Not independent
    /// state — always a subset of <see cref="States"/>'s values.</summary>
    public Dictionary<string, WindowState> OpenSession { get; } = [];

    private long _sessionSeq;

    public List<EventRecord> OnRow(WorkingRow row)
    {
        var results = new List<EventRecord>();
        var groupValues = EvalGroupValues(row);
        var groupKeyStr = EncodeGroupKey(groupValues);
        var targets = ResolveTargetWindows(row.Ts, groupKeyStr, groupValues);

        foreach (var state in targets)
        {
            for (int i = 0; i < plan.AggregateNodes.Count; i++)
            {
                var node = plan.AggregateNodes[i];
                object? val = node.IsStar
                    ? true
                    : ExpressionEvaluator.Eval(node.Arg!, new EvalContext(row, plan.Bindings));
                state.Aggregators[i].Add(val);
            }

            if (plan.Emit == EmitMode.Changes)
            {
                results.Add(BuildRow(state, final: false));
            }
        }

        return results;
    }

    public List<EventRecord> Evict(long watermark)
    {
        var results = new List<EventRecord>();
        List<string>? toClose = null;

        foreach (var (key, state) in States)
        {
            bool due = plan.Window is SessionWindowSpec sw
                ? watermark > state.End + (long)Math.Round(sw.Gap.TotalMilliseconds)
                : watermark >= state.End;
            if (!due) continue;

            results.Add(BuildRow(state, final: true));
            (toClose ??= []).Add(key);
        }

        if (toClose is not null)
        {
            foreach (var key in toClose)
            {
                var state = States[key];
                States.Remove(key);
                if (OpenSession.TryGetValue(state.GroupKeyStr, out var open) && ReferenceEquals(open, state))
                {
                    OpenSession.Remove(state.GroupKeyStr);
                }
            }
        }

        return results;
    }

    private List<WindowState> ResolveTargetWindows(long ts, string groupKeyStr, object?[] groupValues)
    {
        switch (plan.Window)
        {
            case TumblingWindowSpec t:
            {
                long size = (long)Math.Round(t.Size.TotalMilliseconds);
                long start = FloorDiv(ts, size) * size;
                return [GetOrCreate(start, start + size, groupKeyStr, groupValues)];
            }
            case HoppingWindowSpec h:
            {
                long size = (long)Math.Round(h.Size.TotalMilliseconds);
                long advance = (long)Math.Round(h.Advance.TotalMilliseconds);
                var result = new List<WindowState>();
                long kMin = FloorDiv(ts - size, advance) + 1;
                long kMax = FloorDiv(ts, advance);
                for (long k = kMin; k <= kMax; k++)
                {
                    long t0 = k * advance;
                    result.Add(GetOrCreate(t0, t0 + size, groupKeyStr, groupValues));
                }
                return result;
            }
            case SessionWindowSpec s:
            {
                long gap = (long)Math.Round(s.Gap.TotalMilliseconds);
                if (OpenSession.TryGetValue(groupKeyStr, out var open) && ts <= open.End + gap)
                {
                    open.End = Math.Max(open.End, ts);
                    return [open];
                }
                var created = new WindowState
                {
                    Start = ts,
                    End = ts,
                    GroupValues = groupValues,
                    GroupKeyStr = groupKeyStr,
                    Aggregators = CreateAggregators(),
                };
                States[$"S|{groupKeyStr}|{_sessionSeq++}"] = created;
                OpenSession[groupKeyStr] = created;
                return [created];
            }
            default:
                return [];
        }
    }

    private WindowState GetOrCreate(long start, long end, string groupKeyStr, object?[] groupValues)
    {
        var key = $"{start}|{end}|{groupKeyStr}";
        if (States.TryGetValue(key, out var existing)) return existing;
        var state = new WindowState
        {
            Start = start,
            End = end,
            GroupValues = groupValues,
            GroupKeyStr = groupKeyStr,
            Aggregators = CreateAggregators(),
        };
        States[key] = state;
        return state;
    }

    private Aggregator[] CreateAggregators() =>
        plan.AggregateNodes.Select(Aggregator.Create).ToArray();

    private object?[] EvalGroupValues(WorkingRow row)
    {
        if (plan.GroupBy is null) return [];
        var ctx = new EvalContext(row, plan.Bindings);
        return plan.GroupBy.Select(g => ExpressionEvaluator.Eval(g, ctx)).ToArray();
    }

    private static string EncodeGroupKey(object?[] values)
    {
        if (values.Length == 0) return "∅";
        return string.Join("", values.Select(v => v switch
        {
            null => "N",
            long l => $"L:{l}",
            double d => $"D:{d}",
            string s => $"S:{s}",
            bool b => $"B:{b}",
            Dictionary<string, object?> or List<object?> => $"J:{JsonText.Serialize(v)}",
            _ => "?",
        }));
    }

    private EventRecord BuildRow(WindowState state, bool final)
    {
        var evt = new EventRecord();
        AggregateLookup lookup = node => state.Aggregators[plan.AggregateIndex[node]].Result;
        var dummyRow = new WorkingRow { Ts = state.End, Aliases = [], Fields = [] };
        var ctx = new EvalContext(dummyRow, plan.Bindings, lookup);

        foreach (var item in plan.Output)
        {
            object? value = item.GroupByIndex is int gi ? state.GroupValues[gi] : ExpressionEvaluator.Eval(item.Expression, ctx);
            evt[item.Name] = value;
        }

        evt[EventRecord.TimestampField] = state.End;
        evt[EventRecord.SourceField] = plan.SourceLabel;
        evt["window_start"] = state.Start;
        evt["window_end"] = state.End;
        if (plan.Emit == EmitMode.Changes) evt["_final"] = final;
        return evt;
    }

    private static long FloorDiv(long a, long b)
    {
        long q = a / b;
        long r = a % b;
        if (r != 0 && (r < 0) != (b < 0)) q--;
        return q;
    }
}
