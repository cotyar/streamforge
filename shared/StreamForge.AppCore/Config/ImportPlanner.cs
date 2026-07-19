using System.Text.RegularExpressions;
using StreamForge.Abstractions;

namespace StreamForge.AppCore.Config;

/// <summary>One entity's planned outcome (D-J). <see cref="Action"/> is one of "created" |
/// "updated" | "skipped" | "deleted" — <see cref="ImportPlanner"/> never produces "error" (it does
/// no SQL compilation; that's an endpoint-layer concern layered on top of this pure diff).</summary>
public sealed record PlannedAction(string Kind, string Name, string Action, IReadOnlyList<string> Diagnostics);

/// <summary>
/// Plan 006 (D-J): a pure diff between a config document and the current catalog — no SQL
/// compilation (the import endpoint does that, through the real Engine compiler, against the
/// composed catalog). For every entity in the document: "created" (absent from the catalog),
/// "updated" (present, differs after canonicalization), or "skipped" (present, identical). In
/// <c>mode == "replace"</c>, catalog entities absent from the document additionally produce
/// "deleted". The returned list is ordered for SAFE APPLICATION: sources, then tables (topo-sorted
/// so a table's input table is applied before it — see <see cref="TopoSortTables"/>), then
/// pipelines; deletions LAST, in reverse dependency order (pipelines, tables reverse-topo, sources)
/// — mirroring D-J's documented stop-then-replace ordering.
/// </summary>
public static class ImportPlanner
{
    private static readonly Regex FromReferenceRegex = new(@"\bFROM\s+([A-Za-z_][A-Za-z0-9_]*)", RegexOptions.IgnoreCase | RegexOptions.Compiled);

    public static List<PlannedAction> Plan(
        ConfigDocument doc,
        IReadOnlyList<SourceDefinition> sources,
        IReadOnlyList<PipelineDefinition> pipelines,
        IReadOnlyList<TableDefinition> tables,
        string mode)
    {
        var sourceCatalog = sources.ToDictionary(s => s.Name, s => s, StringComparer.Ordinal);
        var pipelineCatalog = pipelines.ToDictionary(p => p.Name, p => p, StringComparer.Ordinal);
        var tableCatalog = tables.ToDictionary(t => t.Name, t => t, StringComparer.Ordinal);

        var result = new List<PlannedAction>();

        // ---- Sources first (no interdependencies to order by; alphabetical for determinism). ----
        foreach (var s in doc.Sources.OrderBy(s => s.Name, StringComparer.Ordinal))
        {
            result.Add(PlanSource(s, sourceCatalog.GetValueOrDefault(s.Name)));
        }

        // ---- Tables: topo-sorted by TableInputs (existing tables) / best-effort SQL scan (new). ----
        var plannedTableNames = doc.Tables.Select(t => t.Name).ToList();
        var plannedTableSet = new HashSet<string>(plannedTableNames, StringComparer.Ordinal);
        var docTablesByName = doc.Tables.ToDictionary(t => t.Name, StringComparer.Ordinal);

        var tableDeps = new Dictionary<string, List<string>>(StringComparer.Ordinal);
        foreach (var name in plannedTableNames)
        {
            tableDeps[name] = tableCatalog.TryGetValue(name, out var existingTable)
                ? [.. existingTable.TableInputs.Where(plannedTableSet.Contains)]
                : [.. ScanFromReferences(docTablesByName[name].Sql).Where(plannedTableSet.Contains)];
        }

        var (tableOrder, tableCycleDiagnostics) = TopoSortTables(plannedTableNames, tableDeps);
        foreach (var name in tableOrder)
        {
            var action = PlanTable(docTablesByName[name], tableCatalog.GetValueOrDefault(name));
            if (tableCycleDiagnostics.TryGetValue(name, out var extra))
            {
                action = action with { Diagnostics = [.. action.Diagnostics, .. extra] };
            }

            result.Add(action);
        }

        // ---- Pipelines (no interdependencies; alphabetical for determinism). ----
        foreach (var p in doc.Pipelines.OrderBy(p => p.Name, StringComparer.Ordinal))
        {
            result.Add(PlanPipeline(p, pipelineCatalog.GetValueOrDefault(p.Name)));
        }

        // ---- Deletions (replace mode only): pipelines, tables (reverse-topo), sources — LAST. ----
        if (mode == "replace")
        {
            foreach (var name in pipelines.Select(p => p.Name).Where(n => !doc.Pipelines.Any(dp => dp.Name == n)).OrderBy(n => n, StringComparer.Ordinal))
            {
                result.Add(new PlannedAction("pipeline", name, "deleted", []));
            }

            var deletedTableNames = tables.Select(t => t.Name).Where(n => !plannedTableSet.Contains(n)).ToList();
            var deletedTableSet = new HashSet<string>(deletedTableNames, StringComparer.Ordinal);
            var deleteDeps = deletedTableNames.ToDictionary(
                n => n, n => (List<string>)[.. tableCatalog[n].TableInputs.Where(deletedTableSet.Contains)], StringComparer.Ordinal);
            var (deleteOrder, _) = TopoSortTables(deletedTableNames, deleteDeps);
            deleteOrder.Reverse(); // dependents deleted before their dependencies.
            foreach (var name in deleteOrder)
            {
                result.Add(new PlannedAction("table", name, "deleted", []));
            }

            foreach (var name in sources.Select(s => s.Name).Where(n => !doc.Sources.Any(ds => ds.Name == n)).OrderBy(n => n, StringComparer.Ordinal))
            {
                result.Add(new PlannedAction("source", name, "deleted", []));
            }
        }

        return result;
    }

    private static PlannedAction PlanSource(SourceDefinition docSource, SourceDefinition? stored)
    {
        if (stored is null)
        {
            return new PlannedAction("source", docSource.Name, "created", []);
        }

        var diagnostics = new List<string>();
        var effective = docSource;
        if (SecretsMasker.HasMaskedValues(docSource))
        {
            effective = SecretsMasker.MergeSecrets(docSource, stored);
            diagnostics.Add("secrets: kept stored values");
        }

        var same = ConfigJsonMapper.ToCanonicalJsonText(ConfigJsonMapper.SourceNode(effective)) ==
                   ConfigJsonMapper.ToCanonicalJsonText(ConfigJsonMapper.SourceNode(stored));
        return new PlannedAction("source", docSource.Name, same ? "skipped" : "updated", diagnostics);
    }

    private static PlannedAction PlanPipeline(ConfigPipeline docPipeline, PipelineDefinition? stored)
    {
        if (stored is null)
        {
            return new PlannedAction("pipeline", docPipeline.Name, "created", []);
        }

        var storedAsConfig = ConfigSerializer.ToConfigPipeline(stored);
        var same = ConfigJsonMapper.ToCanonicalJsonText(ConfigJsonMapper.PipelineNode(docPipeline)) ==
                   ConfigJsonMapper.ToCanonicalJsonText(ConfigJsonMapper.PipelineNode(storedAsConfig));
        return new PlannedAction("pipeline", docPipeline.Name, same ? "skipped" : "updated", []);
    }

    private static PlannedAction PlanTable(ConfigTable docTable, TableDefinition? stored)
    {
        if (stored is null)
        {
            return new PlannedAction("table", docTable.Name, "created", []);
        }

        var storedAsConfig = ConfigSerializer.ToConfigTable(stored);
        var same = ConfigJsonMapper.ToCanonicalJsonText(ConfigJsonMapper.TableNode(docTable)) ==
                   ConfigJsonMapper.ToCanonicalJsonText(ConfigJsonMapper.TableNode(storedAsConfig));
        return new PlannedAction("table", docTable.Name, same ? "skipped" : "updated", []);
    }

    /// <summary>Best-effort dependency scan for a NEW table's SQL: every <c>FROM &lt;identifier&gt;</c>
    /// reference (case-insensitive), regardless of whether it turns out to name a stream source, a
    /// table, or nothing at all — callers filter the result down to names that are actually other
    /// planned tables. Deliberately narrow (FROM only, no JOIN, no subquery awareness) — it only
    /// needs to be good enough to order two NEW tables relative to each other; anything sturdier
    /// would require the real SQL parser, which ImportPlanner explicitly does not use.</summary>
    private static List<string> ScanFromReferences(string sql)
    {
        var names = new List<string>();
        foreach (Match m in FromReferenceRegex.Matches(sql ?? ""))
        {
            names.Add(m.Groups[1].Value);
        }

        return names;
    }

    /// <summary>Post-order DFS topological sort: a name's dependencies (per <paramref name="deps"/>,
    /// already filtered to names within <paramref name="tableNames"/>) are appended to the result
    /// BEFORE the name itself, so "a table whose input table comes later must come after it" holds.
    /// Iteration order is alphabetical wherever the graph doesn't otherwise constrain it, for a
    /// deterministic result. A cycle (revisiting a node still on the current DFS path) is broken by
    /// simply not re-descending into it — the offending name gets a diagnostic instead of a crash;
    /// the returned order is then merely "best-effort" for the nodes in that cycle.</summary>
    private static (List<string> Order, Dictionary<string, List<string>> Diagnostics) TopoSortTables(
        List<string> tableNames, Dictionary<string, List<string>> deps)
    {
        var result = new List<string>();
        var diagnostics = new Dictionary<string, List<string>>(StringComparer.Ordinal);
        var visited = new HashSet<string>(StringComparer.Ordinal);
        var visiting = new HashSet<string>(StringComparer.Ordinal);

        void Visit(string name)
        {
            if (visited.Contains(name))
            {
                return;
            }

            if (visiting.Contains(name))
            {
                if (!diagnostics.TryGetValue(name, out var list))
                {
                    list = [];
                    diagnostics[name] = list;
                }

                list.Add("table dependency cycle detected; apply order may not respect all dependencies");
                return;
            }

            visiting.Add(name);
            if (deps.TryGetValue(name, out var ds))
            {
                foreach (var d in ds.OrderBy(x => x, StringComparer.Ordinal))
                {
                    Visit(d);
                }
            }

            visiting.Remove(name);
            visited.Add(name);
            result.Add(name);
        }

        foreach (var name in tableNames.OrderBy(x => x, StringComparer.Ordinal))
        {
            Visit(name);
        }

        return (result, diagnostics);
    }
}
