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
        // Plan 016 wave 1-C — WAS ToDictionary on all three, which THREW on a duplicate-name catalog and
        // so made POST /api/config/import answer 500 on a state the catalog permits (reproduced live:
        // two pipelines named "dupe" are both creatable, the export round-trips, and the import 500s at
        // this exact line with "An item with the same key has already been added. Key: dupe").
        // ConfigImportService.FirstByName had already made this choice, and documented it, one layer up;
        // the planner it feeds had not. Same rule, same first-wins outcome, so the plan and the apply
        // loop that follows it agree about WHICH duplicate they mean.
        var sourceCatalog = FirstByName(sources, s => s.Name);
        var pipelineCatalog = FirstByName(pipelines, p => p.Name);
        var tableCatalog = FirstByName(tables, t => t.Name);

        // Which duplicate was used is not something a caller should have to infer from a silent
        // first-wins. Ordinarily this is empty and nothing below fires.
        var duplicatePipelineDiagnostics = CatalogWarnings.DuplicatePipelineDiagnostics(pipelines);

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
            result.Add(WithDuplicateDiagnostic(PlanPipeline(p, pipelineCatalog.GetValueOrDefault(p.Name)), duplicatePipelineDiagnostics));
        }

        // ---- Deletions (replace mode only): pipelines, tables (reverse-topo), sources — LAST. ----
        if (mode == "replace")
        {
            foreach (var name in pipelines.Select(p => p.Name).Where(n => !doc.Pipelines.Any(dp => dp.Name == n)).OrderBy(n => n, StringComparer.Ordinal))
            {
                result.Add(WithDuplicateDiagnostic(new PlannedAction("pipeline", name, "deleted", []), duplicatePipelineDiagnostics));
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

    /// <summary>ponytail: first-wins, character-for-character the rule
    /// <c>ConfigImportService.FirstByName</c> already applies to the same three lists — the planner and
    /// its apply loop MUST resolve a duplicated name to the same entity or the report would describe a
    /// different pipeline than the one that got written. Ceiling: the losing duplicates are invisible to
    /// the diff (they are diagnosed, not planned against). Upgrade path: none needed for sources and
    /// tables (the registries enforce uniqueness on both), and for pipelines the write path now refuses
    /// new duplicates, so this defends only against catalogs that predate that guard.</summary>
    private static Dictionary<string, T> FirstByName<T>(IEnumerable<T> items, Func<T, string> nameOf)
    {
        var result = new Dictionary<string, T>(StringComparer.Ordinal);
        foreach (var item in items)
        {
            result.TryAdd(nameOf(item), item);
        }

        return result;
    }

    /// <summary>ponytail: the duplicate-name note rides on the affected entity's own action rather than
    /// on a synthetic document-level one. Ceiling: it is only visible when the document mentions that
    /// pipeline (or replace-mode deletes it) — instance-wide visibility is
    /// <see cref="CatalogWarnings"/>'s job, not the planner's. Reason it is not a
    /// <c>PlannedAction("document", …)</c>: <c>ConfigImportService.RunImportAsync</c> switches on
    /// <c>action.Kind</c> and THROWS on an unrecognised one, so inventing a kind here would trade a
    /// 500-on-duplicates for a 500-on-every-import. Upgrade path, if a document-level channel is ever
    /// wanted: a <c>Warnings</c> list on <c>ConfigImportReport</c>, which is additive.</summary>
    private static PlannedAction WithDuplicateDiagnostic(PlannedAction action, IReadOnlyDictionary<string, string> duplicates) =>
        duplicates.TryGetValue(action.Name, out var note)
            ? action with { Diagnostics = [.. action.Diagnostics, note] }
            : action;

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

        // Plan 009 B2: same "a masked doc needs its secrets restored from stored before comparing"
        // rule PlanSource applies to source secrets — otherwise a doc round-tripped through a masked
        // export would ALWAYS compare "updated" (mask string != real credential) even when nothing
        // actually changed, and — worse — ProcessPipelineAsync would persist the literal mask string.
        var diagnostics = new List<string>();
        var effective = docPipeline;
        if (SecretsMasker.HasMaskedSinkValues(docPipeline.Sinks))
        {
            effective = ConfigJsonMapper.DeepCloneModel(docPipeline);
            effective.Sinks = SecretsMasker.MergeSinkSecrets(docPipeline.Sinks, stored.Sinks);
            diagnostics.Add("secrets: kept stored values");
        }

        var storedAsConfig = ConfigSerializer.ToConfigPipeline(stored);
        var same = ConfigJsonMapper.ToCanonicalJsonText(ConfigJsonMapper.PipelineNode(effective)) ==
                   ConfigJsonMapper.ToCanonicalJsonText(ConfigJsonMapper.PipelineNode(storedAsConfig));
        return new PlannedAction("pipeline", docPipeline.Name, same ? "skipped" : "updated", diagnostics);
    }

    private static PlannedAction PlanTable(ConfigTable docTable, TableDefinition? stored)
    {
        if (stored is null)
        {
            return new PlannedAction("table", docTable.Name, "created", []);
        }

        // Plan 009 B2: see the identical note in PlanPipeline above.
        var diagnostics = new List<string>();
        var effective = docTable;
        if (SecretsMasker.HasMaskedSinkValues(docTable.Sinks))
        {
            effective = ConfigJsonMapper.DeepCloneModel(docTable);
            effective.Sinks = SecretsMasker.MergeSinkSecrets(docTable.Sinks, stored.Sinks);
            diagnostics.Add("secrets: kept stored values");
        }

        var storedAsConfig = ConfigSerializer.ToConfigTable(stored);
        var same = ConfigJsonMapper.ToCanonicalJsonText(ConfigJsonMapper.TableNode(effective)) ==
                   ConfigJsonMapper.ToCanonicalJsonText(ConfigJsonMapper.TableNode(storedAsConfig));
        return new PlannedAction("table", docTable.Name, same ? "skipped" : "updated", diagnostics);
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

/// <summary>
/// Plan 016 wave 1-C — conditions the catalog TOLERATES but somebody should see, computed from the
/// three lists every caller already has. These are what <see cref="StreamForge.Abstractions.InstanceInfo"/>'s
/// <c>CatalogWarnings</c> is for, and wave 5's <c>GET /api/meta/instance</c> is expected to call
/// <see cref="Compute"/> and assign the result straight to that field.
///
/// <para><b>Why warnings and not a boot refusal.</b> Duplicate pipeline names were legal for the whole
/// life of the product; a catalog that was legal when it was written must not become a host that will
/// not start. The write path refuses NEW duplicates (<c>RegistryGrain</c> / <c>CatalogStore</c>) and
/// existing violators keep running until somebody edits one — at which point the same guard makes them
/// fix it. This type is how they stay visible in the meantime.</para>
///
/// <para><b>Why it lives in this file.</b> It is the same fact <see cref="ImportPlanner"/> already has
/// to compute to survive a duplicate-name catalog, and both flavours plus the API layer already
/// reference <c>StreamForge.AppCore</c> — a per-flavour copy would be the duplication
/// <c>SourceKindDispatch</c> exists to prevent. Pure: no Orleans, Dapr or ASP.NET types.</para>
/// </summary>
public static class CatalogWarnings
{
    /// <summary>Human-readable, one per condition, stable order. Empty is the normal answer.</summary>
    public static List<string> Compute(IReadOnlyList<PipelineDefinition> pipelines) =>
        [.. DuplicatePipelineDiagnostics(pipelines).OrderBy(kv => kv.Key, StringComparer.Ordinal).Select(kv => kv.Value)];

    /// <summary>Duplicated pipeline name -&gt; the message, keyed so a caller can attach it to whatever it
    /// is describing. Names that appear once are absent, so an ordinary catalog yields an empty map and
    /// costs one grouping pass.</summary>
    internal static Dictionary<string, string> DuplicatePipelineDiagnostics(IReadOnlyList<PipelineDefinition> pipelines)
    {
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var group in pipelines.GroupBy(p => p.Name, StringComparer.Ordinal).Where(g => g.Count() > 1))
        {
            var sharing = group.ToList();
            result[group.Key] =
                $"duplicate pipeline name '{group.Key}': {sharing.Count} pipelines share it (ids {string.Join(", ", sharing.Select(p => p.Id))}) — " +
                $"'{sharing[0].Id}' is the one every name-keyed read resolves to, and the only one this import plans against. " +
                "Rename the others: the write path refuses new duplicates, so editing any of them will be refused until the name is unique.";
        }

        return result;
    }
}
