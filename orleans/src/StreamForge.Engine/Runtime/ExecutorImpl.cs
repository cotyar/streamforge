using StreamForge.Engine.Planning;
using StreamForge.Engine.Runtime;
using StreamForge.Engine.Sql;

namespace StreamForge.Engine;

/// <summary>Attaches the compiled, executable plan to the frozen <see cref="PipelinePlan"/> DTO.</summary>
public sealed partial class PipelinePlan
{
    internal CompiledPlan Compiled { get; }

    internal PipelinePlan(CompiledPlan compiled) => Compiled = compiled;
}

/// <summary>Single-threaded runtime for a compiled pipeline: folds JOINs left-to-right through a chain of
/// <see cref="JoinStage"/>s, applies WHERE, then hands surviving rows to a <see cref="WindowOperator"/> (if
/// windowed) or projects them immediately.</summary>
public sealed partial class PipelineExecutor
{
    private const long AllowedLatenessMs = 1000;

    private bool _initialized;
    private readonly List<JoinStage> _stages = [];
    private WindowOperator? _window;

    // sourceName -> every role that source plays in the plan (FROM and/or one or more JOIN aliases;
    // a source referenced twice under different aliases gets the event delivered to every alias).
    private readonly Dictionary<string, List<(bool IsFrom, int StageIndex, string Alias)>> _roles = [];

    public long LateEvents { get; private set; }

    private void EnsureInit()
    {
        if (_initialized) return;
        _initialized = true;

        var compiled = _plan.Compiled;
        var accumulated = new List<(string Alias, SourceSchema Schema)> { (compiled.Sources[0].Alias, compiled.Sources[0].Schema) };

        for (int i = 0; i < compiled.Joins.Count; i++)
        {
            var j = compiled.Joins[i];
            _stages.Add(new JoinStage(j.Kind, j.Within, j.LeftKey, j.RightKey, j.Residual, compiled.Bindings, accumulated.ToList(), (j.Alias, j.Schema)));
            accumulated.Add((j.Alias, j.Schema));
        }

        if (compiled.Window is not null) _window = new WindowOperator(compiled);

        AddRole(compiled.Sources[0].SourceName, isFrom: true, stageIndex: -1, compiled.Sources[0].Alias);
        for (int i = 0; i < compiled.Joins.Count; i++)
        {
            AddRole(compiled.Joins[i].SourceName, isFrom: false, stageIndex: i, compiled.Joins[i].Alias);
        }
    }

    private void AddRole(string sourceName, bool isFrom, int stageIndex, string alias)
    {
        if (!_roles.TryGetValue(sourceName, out var list))
        {
            list = [];
            _roles[sourceName] = list;
        }
        list.Add((isFrom, stageIndex, alias));
    }

    private IReadOnlyList<EventRecord> OnEventCore(string sourceName, EventRecord evt)
    {
        EnsureInit();

        if (evt.Timestamp < Watermark)
        {
            LateEvents++;
            return [];
        }

        var results = new List<EventRecord>();

        if (_roles.TryGetValue(sourceName, out var roles))
        {
            foreach (var role in roles)
            {
                var initial = WorkingRow.FromEvent(role.Alias, evt);
                List<WorkingRow> combinedRows = role.IsFrom
                    ? PropagateForward(0, [initial])
                    : PropagateForward(role.StageIndex + 1, _stages[role.StageIndex].OnRight(initial));
                ProcessRows(combinedRows, results);
            }
        }

        long candidate = evt.Timestamp - AllowedLatenessMs;
        if (candidate > Watermark) Watermark = candidate;

        return results;
    }

    private IReadOnlyList<EventRecord> AdvanceWatermarkCore(long nowMs)
    {
        EnsureInit();

        long candidate = nowMs - AllowedLatenessMs;
        long newWatermark = Math.Max(Watermark, candidate);
        Watermark = newWatermark;

        var results = new List<EventRecord>();

        for (int i = 0; i < _stages.Count; i++)
        {
            var evicted = _stages[i].Evict(newWatermark);
            var propagated = PropagateForward(i + 1, evicted);
            ProcessRows(propagated, results);
        }

        if (_window is not null) results.AddRange(_window.Evict(newWatermark));

        return results;
    }

    private List<WorkingRow> PropagateForward(int fromStageIndexInclusive, List<WorkingRow> rows)
    {
        var current = rows;
        for (int s = fromStageIndexInclusive; s < _stages.Count; s++)
        {
            var next = new List<WorkingRow>();
            foreach (var r in current) next.AddRange(_stages[s].OnLeft(r));
            current = next;
        }
        return current;
    }

    private void ProcessRows(List<WorkingRow> rows, List<EventRecord> results)
    {
        var compiled = _plan.Compiled;
        foreach (var row in rows)
        {
            if (compiled.Where is not null)
            {
                var whereResult = ExpressionEvaluator.Eval(compiled.Where, new EvalContext(row, compiled.Bindings));
                if (!ExpressionEvaluator.IsTrue(whereResult)) continue;
            }

            if (_window is not null)
            {
                results.AddRange(_window.OnRow(row));
            }
            else
            {
                results.Add(ProjectRow(row, compiled));
            }
        }
    }

    private static EventRecord ProjectRow(WorkingRow row, CompiledPlan compiled)
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
