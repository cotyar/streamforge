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

    /// <summary>One role a real leaf source name plays: feeds a plain FROM/JOIN alias directly, or first
    /// passes through a derived table/CTE's own nested executor (plan 004 N1) whose emissions THEN become
    /// this alias's input events — the IPipelineOpChain composability seam (see PipelineComposabilityTests
    /// and IPipelineOpChain's doc comment), now wired automatically instead of by hand.</summary>
    private sealed class RoleEntry
    {
        public required bool IsFrom;
        public required int StageIndex; // -1 = FROM
        public required string Alias;
        public PipelineExecutor? Derived;
    }

    private bool _initialized;
    private readonly List<PipelineJoinOp> _joins = [];
    private PipelineFilterProjectOp? _filterProject;
    private PipelineWindowOp? _window;

    // real leaf source name -> every role that name plays in the plan (FROM and/or one or more JOIN
    // aliases, directly or via a derived source's nested executor; a source referenced twice under
    // different aliases gets the event delivered to every alias).
    private readonly Dictionary<string, List<RoleEntry>> _roles = [];

    // Distinct nested executors for this plan's derived FROM/JOIN sources (plan 004 N1) — deduplicated
    // here (unlike _roles, which is keyed per leaf source name and may reference the same nested executor
    // under several different keys) so AdvanceWatermark drives each one exactly once per call.
    private readonly List<RoleEntry> _derivedNodes = [];

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

        AddRole(compiled.Sources[0].SourceName, isFrom: true, stageIndex: -1, compiled.Sources[0].Alias, compiled.Sources[0].DerivedPlan);
        for (int i = 0; i < compiled.Joins.Count; i++)
        {
            AddRole(compiled.Joins[i].SourceName, isFrom: false, stageIndex: i, compiled.Joins[i].Alias, compiled.Joins[i].DerivedPlan);
        }
    }

    private void AddRole(string sourceName, bool isFrom, int stageIndex, string alias, CompiledPlan? derivedPlan)
    {
        if (derivedPlan is null)
        {
            var entry = new RoleEntry { IsFrom = isFrom, StageIndex = stageIndex, Alias = alias, Derived = null };
            AddRoleUnder(sourceName, entry);
            return;
        }

        // Derived table/CTE (plan 004 N1): one nested PipelineExecutor per derived source, registered
        // under every real leaf source name it transitively depends on (DerivedPlan.SourceNames is
        // already flattened through any further nesting — see Planner.BuildCompiledPlan).
        var derivedExecutor = new PipelineExecutor(new PipelinePlan(derivedPlan));
        var derivedEntry = new RoleEntry { IsFrom = isFrom, StageIndex = stageIndex, Alias = alias, Derived = derivedExecutor };
        _derivedNodes.Add(derivedEntry);
        foreach (var leaf in derivedPlan.SourceNames)
        {
            AddRoleUnder(leaf, derivedEntry);
        }
    }

    private void AddRoleUnder(string sourceName, RoleEntry entry)
    {
        if (!_roles.TryGetValue(sourceName, out var list))
        {
            list = [];
            _roles[sourceName] = list;
        }
        list.Add(entry);
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
                if (role.Derived is null)
                {
                    ProcessIncomingRow(role, WorkingRow.FromEvent(role.Alias, evt), results);
                }
                else
                {
                    // The derived source's own nested executor sees the raw event first; only what IT
                    // emits (e.g. a closed window's final row, or an immediate unwindowed passthrough)
                    // becomes this alias's input event one level up — plan 004 N1's stated windows-in-
                    // windows semantics: "emissions enter the outer level as events timestamped at window
                    // end" (or, for an unwindowed derived source, at the emission's own row timestamp).
                    foreach (var emission in role.Derived.OnEvent(sourceName, evt))
                    {
                        ProcessIncomingRow(role, WorkingRow.FromEvent(role.Alias, emission), results);
                        BumpWatermark(emission.Timestamp);
                    }
                }
            }
        }

        BumpWatermark(evt.Timestamp);

        return results;
    }

    private void ProcessIncomingRow(RoleEntry role, WorkingRow initial, List<EventRecord> results)
    {
        List<WorkingRow> combinedRows = role.IsFrom
            ? PropagateForward(0, [initial])
            : PropagateForward(role.StageIndex + 1, _joins[role.StageIndex].OnRight(initial));
        ProcessRows(combinedRows, results);
    }

    private void BumpWatermark(long eventTs)
    {
        long candidate = eventTs - AllowedLatenessMs;
        if (candidate > Watermark) Watermark = candidate;
    }

    private IReadOnlyList<EventRecord> AdvanceWatermarkCore(long nowMs)
    {
        EnsureInit();

        var results = new List<EventRecord>();

        // Advance every derived source's nested executor first, routing its closed-window (or otherwise
        // watermark-triggered) emissions through the outer chain exactly like OnEventCore does for a live
        // event — so the outer's own join/window eviction below sees a consistent picture that already
        // includes anything the derived level just emitted.
        foreach (var role in _derivedNodes)
        {
            foreach (var emission in role.Derived!.AdvanceWatermark(nowMs))
            {
                ProcessIncomingRow(role, WorkingRow.FromEvent(role.Alias, emission), results);
                BumpWatermark(emission.Timestamp);
            }
        }

        long candidate = nowMs - AllowedLatenessMs;
        long newWatermark = Math.Max(Watermark, candidate);
        Watermark = newWatermark;

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
