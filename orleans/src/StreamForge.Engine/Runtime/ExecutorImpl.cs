using StreamForge.Engine.Planning;
using StreamForge.Engine.Runtime;
using StreamForge.Engine.Runtime.Ops;
using StreamForge.Engine.Sql;

namespace StreamForge.Engine;

/// <summary>Attaches the compiled, executable plan to the frozen <see cref="PipelinePlan"/> DTO.</summary>
public sealed partial class PipelinePlan
{
    internal CompiledPlan Compiled { get; }

    internal PipelinePlan(CompiledPlan compiled) => Compiled = compiled;
}

/// <summary>
/// Façade over the pipeline-mode operator chain (plan 003 M1 Part B — the streaming-executor analogue of
/// TableExecutor's table-mode façade). Builds a chain of <see cref="PipelineJoinOp"/> (one per JOIN,
/// left-to-right), a <see cref="PipelineFilterProjectOp"/> (WHERE, +terminal projection when unwindowed)
/// and — when the plan windows — a <see cref="PipelineWindowOp"/>. Implements
/// <see cref="IPipelineOpChain"/> (declared on THIS partial-class part, not PublicApi.cs — PublicApi.cs's
/// frozen signatures are untouched) so a whole compiled pipeline's executor can be embedded as a node
/// feeding another chain's OnEvent calls (plan 004 N1's derived-table/windows-in-windows seam — see
/// IPipelineOpChain's doc comment and PipelineComposabilityTests for a hand-built proof).
/// </summary>
public sealed partial class PipelineExecutor : IPipelineOpChain
{
    private const long AllowedLatenessMs = 1000;

    private bool _initialized;
    private readonly List<PipelineJoinOp> _joins = [];
    private PipelineFilterProjectOp? _filterProject;
    private PipelineWindowOp? _window;

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
            _joins.Add(new PipelineJoinOp(j.Kind, j.Within, j.LeftKey, j.RightKey, j.Residual, compiled.Bindings, accumulated.ToList(), (j.Alias, j.Schema)));
            accumulated.Add((j.Alias, j.Schema));
        }

        _filterProject = new PipelineFilterProjectOp(compiled);
        if (compiled.Window is not null) _window = new PipelineWindowOp(compiled);

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
                    : PropagateForward(role.StageIndex + 1, _joins[role.StageIndex].OnRight(initial));
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

        for (int i = 0; i < _joins.Count; i++)
        {
            var evicted = _joins[i].Evict(newWatermark);
            var propagated = PropagateForward(i + 1, evicted);
            ProcessRows(propagated, results);
        }

        if (_window is not null) results.AddRange(_window.Evict(newWatermark));

        return results;
    }

    private List<WorkingRow> PropagateForward(int fromStageIndexInclusive, List<WorkingRow> rows)
    {
        var current = rows;
        for (int s = fromStageIndexInclusive; s < _joins.Count; s++)
        {
            var next = new List<WorkingRow>();
            foreach (var r in current) next.AddRange(_joins[s].OnLeft(r));
            current = next;
        }
        return current;
    }

    private void ProcessRows(List<WorkingRow> rows, List<EventRecord> results)
    {
        if (_window is not null)
        {
            var filtered = _filterProject!.OnBatch(rows);
            foreach (var row in filtered) results.AddRange(_window.OnRow(row));
        }
        else
        {
            results.AddRange(_filterProject!.OnBatchTerminal(rows));
        }
    }
}
