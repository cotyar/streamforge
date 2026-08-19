using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using StreamForge.Abstractions;
using StreamForge.AppCore.Sql;
using StreamForge.Engine;
using StreamForge.Host.Grpc.Dynamic;

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

        // ---- Tables: topo-sorted by TableInputs (existing tables) / compiler reference scan (new). ----
        var plannedTableNames = doc.Tables.Select(t => t.Name).ToList();
        var plannedTableSet = new HashSet<string>(plannedTableNames, StringComparer.Ordinal);
        var docTablesByName = doc.Tables.ToDictionary(t => t.Name, StringComparer.Ordinal);

        // Plan 016 wave 3-B: what "exists" means for the missing-dependency diagnostic below — the
        // document's own sources/tables plus whatever the CURRENT (pre-import) catalog already holds.
        // Ordinal, deliberately: every name-keyed lookup in this codebase (RegistryGrain's SQL
        // namespace, EntityRef resolution) is ordinal/case-sensitive, so a name that resolves here only
        // case-insensitively would still fail to compile — reporting it as "exists" would be a false
        // negative on exactly the diagnostic this exists to give.
        var knownRelationNames = new HashSet<string>(StringComparer.Ordinal);
        knownRelationNames.UnionWith(sourceCatalog.Keys);
        knownRelationNames.UnionWith(tableCatalog.Keys);
        knownRelationNames.UnionWith(doc.Sources.Select(s => s.Name));
        knownRelationNames.UnionWith(plannedTableSet);

        var tableDeps = new Dictionary<string, List<string>>(StringComparer.Ordinal);
        foreach (var name in plannedTableNames)
        {
            tableDeps[name] = tableCatalog.TryGetValue(name, out var existingTable)
                ? [.. existingTable.TableInputs.Where(plannedTableSet.Contains)]
                : [.. ScanNewEntityReferences(docTablesByName[name].Sql).Where(plannedTableSet.Contains)];
        }

        var (tableOrder, tableCycleDiagnostics) = TopoSortTables(plannedTableNames, tableDeps);
        foreach (var name in tableOrder)
        {
            var action = PlanTable(docTablesByName[name], tableCatalog.GetValueOrDefault(name));
            var extra = new List<string>();
            if (tableCycleDiagnostics.TryGetValue(name, out var cycleNotes))
            {
                extra.AddRange(cycleNotes);
            }

            extra.AddRange(MissingDependencyDiagnostics(docTablesByName[name].Sql, knownRelationNames));
            if (extra.Count > 0)
            {
                action = action with { Diagnostics = [.. action.Diagnostics, .. extra] };
            }

            result.Add(action);
        }

        // ---- Pipelines (no interdependencies; alphabetical for determinism). ----
        foreach (var p in doc.Pipelines.OrderBy(p => p.Name, StringComparer.Ordinal))
        {
            var action = WithDuplicateDiagnostic(PlanPipeline(p, pipelineCatalog.GetValueOrDefault(p.Name)), duplicatePipelineDiagnostics);
            var missing = MissingDependencyDiagnostics(p.Sql, knownRelationNames);
            if (missing.Count > 0)
            {
                action = action with { Diagnostics = [.. action.Diagnostics, .. missing] };
            }

            result.Add(action);
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

    /// <summary>
    /// Plan 016 wave 2 — THE 014-K INTERACTION, decided here rather than discovered later as revision
    /// churn. A document written as <c>INSERT INTO warehouse SELECT …</c> stores the STRIPPED
    /// <c>SELECT</c> (see <see cref="SinkSugar"/>: the sugar is lossy on round-trip and says so), so the
    /// document's text and the stored text legitimately differ and every re-import of that document
    /// planned as "updated" — forever, without anything having changed.
    ///
    /// <para><b>Decision: desugar inside the planner before comparing</b>, rather than excluding sugared
    /// documents from the Revision bump. Three reasons.</para>
    ///
    /// <para>(1) The alternative fixes the counter and leaves the LIE. The import report is the thing an
    /// operator reads before saying yes to a promotion; a permanent, unexplained "updated" on an entity
    /// nobody touched trains people to ignore the report, which is a worse outcome than a counter that
    /// moves too often. Desugaring fixes both at once, because the counter is derived from this exact
    /// comparison.</para>
    ///
    /// <para>(2) "Exclude sugared documents from the bump" has nowhere honest to live. The registry —
    /// which is what bumps — is handed a DEFINITION, not a document; by the time it sees the entity the
    /// sugar is long gone (<c>ConfigImportService</c> strips it before building the definition). Wiring
    /// "this came from a sugared document" down to the write path means a new argument on the catalog
    /// facade purely to describe a cosmetic property of the request's ancestry.</para>
    ///
    /// <para>(3) It compares like with like. <c>ConfigImportService</c> already runs
    /// <c>SinkSugar.ApplyTo</c> on the document before it stores anything, so desugaring here makes the
    /// planner predict what the apply loop will actually write — which is the planner's entire job, and
    /// the property that makes a "skipped" verdict trustworthy enough to hang a counter off.</para>
    ///
    /// <para>The enable flip is part of it: <c>ApplyTo</c> switches the named sink on, and the stored
    /// entity therefore has <c>enabled: true</c>. Comparing the document's un-flipped sink against it
    /// would report "updated" for the same non-reason. It is applied to a CLONE — the caller's document
    /// is never mutated by a plan (a plan that edited its input would make <c>mode=validate</c> a
    /// side-effecting operation).</para>
    ///
    /// <para>An unresolvable target (unknown sink name, malformed prefix) falls through to the untouched
    /// text: <c>ConfigImportService</c> reports that as an "error" entry a moment later with a real
    /// message, and it is not this pure diff's job to say it twice.</para>
    /// </summary>
    private static ConfigPipeline Desugared(ConfigPipeline effective, ConfigPipeline original)
    {
        if (SinkSugar.Desugar(effective.Sql).SinkName is null)
        {
            return effective; // no sugar — the overwhelmingly common case, and it does not even scan.
        }

        var target = ReferenceEquals(effective, original) ? ConfigJsonMapper.DeepCloneModel(original) : effective;
        var sugar = SinkSugar.ApplyTo(target.Sql, target.Sinks, "pipeline");
        if (sugar.Diagnostics.Count > 0)
        {
            return effective;
        }

        target.Sql = sugar.Sql;
        return target;
    }

    /// <summary>Table twin of the pipeline overload — see it for the whole 014-K rationale.</summary>
    private static ConfigTable Desugared(ConfigTable effective, ConfigTable original)
    {
        if (SinkSugar.Desugar(effective.Sql).SinkName is null)
        {
            return effective;
        }

        var target = ReferenceEquals(effective, original) ? ConfigJsonMapper.DeepCloneModel(original) : effective;
        var sugar = SinkSugar.ApplyTo(target.Sql, target.Sinks, "table");
        if (sugar.Diagnostics.Count > 0)
        {
            return effective;
        }

        target.Sql = sugar.Sql;
        return target;
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

        // Plan 016 wave 2: via CatalogRevisions, which strips the two REGISTRY-ASSIGNED counters before
        // comparing. Without that, the first time a source's Revision moved, every later import of the
        // same document would compare a doc that says revision 0 against a store that says revision 3 and
        // report "updated" forever — a diff that reports churn it caused itself.
        var same = CatalogRevisions.SourceCanonicalText(effective) == CatalogRevisions.SourceCanonicalText(stored);
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

        effective = Desugared(effective, docPipeline);

        // Plan 016 wave 2: through CatalogRevisions, so the planner's verdict and the Revision bump are
        // computed by ONE method — which is what makes "a round-trip that reports skipped provably does
        // not bump" a fact rather than a hope.
        var same = CatalogRevisions.PipelineCanonicalText(effective) == CatalogRevisions.PipelineCanonicalText(stored);
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

        effective = Desugared(effective, docTable);

        // Plan 016 wave 2: see the identical note in PlanPipeline above.
        var same = CatalogRevisions.TableCanonicalText(effective) == CatalogRevisions.TableCanonicalText(stored);
        return new PlannedAction("table", docTable.Name, same ? "skipped" : "updated", diagnostics);
    }

    /// <summary>
    /// Plan 016 wave 3-B: every relation a NEW (not-yet-in-the-catalog) entity's SQL reads — FROM, every
    /// JOIN, every derived-table/subquery source, at any depth — via the real compiler
    /// (<see cref="SqlCompiler.ExtractReferences"/>) instead of the old FROM-only regex. Two things this
    /// buys over the regex it replaces: JOINs and subqueries are seen at all, and a CTE no longer invents
    /// a phantom dependency on its own name (<c>WITH recent AS (…) SELECT * FROM recent</c> used to make
    /// the regex emit <c>recent</c>, a name no catalog will ever hold — the compiler resolves CTE
    /// references away, so it reports the CTE body's real relations instead).
    ///
    /// <para>Desugars <c>INSERT INTO &lt;sink&gt;</c> first — the Engine has no INSERT production, so a
    /// still-sugared statement is a parse error to <c>ExtractReferences</c> and would otherwise lose
    /// every dependency of the commonest authored form.</para>
    ///
    /// <para><b>ponytail: falls back to the old FROM-only regex ONLY when the compiler could not parse
    /// the statement at all.</b> Found live while wiring this up, not guessed: <c>ImportPlannerTests</c>,
    /// <c>ImportPlannerDuplicateNameTests</c>, <c>ImportPlannerSinksTests</c> and <c>ConfigSerializerTests</c>
    /// — none of them mine to edit — author every table's SQL as <c>"TABLE AS SELECT …"</c>, a prefix this
    /// dialect's grammar has never accepted (real <c>TableDefinition.Sql</c> text is a bare
    /// <c>SELECT</c>/<c>WITH</c>, confirmed against <c>CatalogRevisionsTests</c> and every live compile
    /// call site). <c>ExtractReferences</c> therefore returns <c>[]</c> for every one of those fixtures —
    /// not because they reference nothing, but because "TABLE AS" fails to parse — and
    /// <c>ExtractReferences</c>' contract makes those two cases deliberately indistinguishable. Trusting
    /// the compiler unconditionally would silently drop the dependency those tests assert on
    /// (<c>New_table_dependency_is_inferred_from_sql_from_reference</c>,
    /// <c>Table_dependency_cycle_is_diagnosed_not_crashed</c>) and reorder every table apply in this repo's
    /// own test suite. The regex fallback only ever fires on a genuine parse failure — a CTE, a JOIN, a
    /// subquery all parse fine and never reach it — so it costs nothing on well-formed SQL and exists
    /// purely as the safety net documented above. Ceiling: a table whose SQL is BOTH invalid AND written
    /// with an unsupported prefix like "TABLE AS" gets the pre-016 FROM-only answer instead of one derived
    /// from a real parse — no worse than today, since today IS that answer for everything. Upgrade path:
    /// none needed unless a future SQL-authoring convention reintroduces a non-parsing prefix on purpose,
    /// at which point desugar it the way <see cref="SinkSugar"/> already handles INSERT INTO.</para>
    /// </summary>
    internal static List<string> ScanNewEntityReferences(string sql)
    {
        var desugared = SinkSugar.Desugar(sql ?? "").Sql;
        var compiled = SqlCompiler.ExtractReferences(desugared);
        return compiled.Count > 0 ? [.. compiled] : ScanFromReferences(desugared);
    }

    /// <summary>Plan 016 wave 3-B: the diagnostic half of <see cref="ScanNewEntityReferences"/> — every
    /// relation NAME a document entity's SQL reads that resolves to nothing in <paramref name="known"/>
    /// (the document's own sources/tables plus the current catalog's). Changes no PLANNED OUTCOME
    /// ("created"/"updated"/"skipped" is unaffected) — the downstream compile already errors on a
    /// genuinely missing relation — this only says WHY at plan time, which is what makes
    /// <c>mode=validate</c> useful instead of merely accurate. Run on every document table/pipeline
    /// regardless of created/updated/skipped, because it is about what the DOCUMENT says, not about
    /// whether the stored definition changed.</summary>
    private static List<string> MissingDependencyDiagnostics(string sql, HashSet<string> known)
    {
        var missing = ScanNewEntityReferences(sql).Where(n => !known.Contains(n)).Distinct(StringComparer.Ordinal).ToList();
        return [.. missing.Select(n => $"references '{n}', which does not exist in this document or the current catalog")];
    }

    /// <summary>ponytail: the FROM-only regex <see cref="ScanNewEntityReferences"/> now falls back to —
    /// see its doc comment for exactly when and why. No longer the primary dependency scan.</summary>
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

/// <summary>
/// Plan 016 wave 2 — the two registry-assigned counters, and the ONE predicate behind them.
///
/// <para><b>Why it lives in this file.</b> The plan pins "definition changed" to
/// <c>ConfigJsonMapper.ToCanonicalJsonText(...)</c> inequality — the exact test
/// <see cref="ImportPlanner"/> uses to tell "skipped" from "updated" — so that a round-trip import which
/// reports "skipped" PROVABLY does not bump. The cheapest way to make that provable is for both to call
/// the same method, and <c>ConfigJsonMapper</c> is <c>internal</c> to StreamForge.AppCore, so a helper
/// outside this assembly could not reach it. Same argument, same file, as <see cref="CatalogWarnings"/>
/// above.</para>
///
/// <para><b>Both counters are the REGISTRY's.</b> Callers do carry-then-bump: for pipelines and tables
/// <c>CatalogRecordMerge.CarryServerOwnedFields</c> carries and <see cref="BumpPipeline"/>/
/// <see cref="BumpTable"/> bump; for SOURCES there is no <c>CarryServerOwnedFields</c> overload at all
/// (sources are upserted whole, on both flavours), so <see cref="CarryAndBumpSource"/> does BOTH halves.
/// That asymmetry is pre-existing — wave 0 left a note about it on <c>CatalogRecordMerge</c> — and this
/// is where it gets absorbed rather than propagated: every source upsert site on both flavours makes one
/// call, and a caller that forgets it leaves the counters at zero (visibly wrong) instead of letting a
/// client's payload choose its own revision (silently wrong).</para>
///
/// <para><b>ponytail: the canonical node is the CONFIG projection, not the whole record.</b> Ceiling:
/// <c>ConfigTable</c>/<c>ConfigPipeline</c> do not carry <c>Persistence</c>, <c>FlushMs</c> or
/// <c>JournalMaxEntries</c>, so an edit to only those does not move <c>Revision</c> — they are not part
/// of what config export/import considers the definition. Upgrade path: widen the projection (a change
/// to the export contract, which is a different plan's decision) or hand <see cref="DefinitionChanged"/>
/// a second comparand. Taken deliberately, because the alternative — a second, private notion of "the
/// definition" — is exactly the drift this plan spent a paragraph forbidding.</para>
/// </summary>
public static class CatalogRevisions
{
    /// <summary>The canonical text of a source, with the two counters removed. They HAVE to come out:
    /// <c>SourceNode</c> serializes the whole <c>SourceDefinition</c> (unlike the pipeline/table nodes,
    /// which go through the <c>ConfigPipeline</c>/<c>ConfigTable</c> projections and never see a
    /// counter), so without this every source would compare unequal to itself the moment its own
    /// revision moved — self-perpetuating churn, and an import report that says "updated" forever.</summary>
    internal static string SourceCanonicalText(SourceDefinition s)
    {
        var node = ConfigJsonMapper.SourceNode(s);
        node.Remove("revision");
        node.Remove("schemaRevision");
        return Canonical(node);
    }

    internal static string PipelineCanonicalText(ConfigPipeline p) =>
        Canonical(ConfigJsonMapper.PipelineNode(p));

    /// <summary>Via the same <c>ConfigPipeline</c> projection the planner compares against, so the
    /// stored record and a document entity are canonicalised by literally the same code.</summary>
    internal static string PipelineCanonicalText(PipelineDefinition p) =>
        PipelineCanonicalText(ConfigSerializer.ToConfigPipeline(p));

    internal static string TableCanonicalText(ConfigTable t) =>
        Canonical(ConfigJsonMapper.TableNode(t));

    internal static string TableCanonicalText(TableDefinition t) =>
        TableCanonicalText(ConfigSerializer.ToConfigTable(t));

    /// <summary>
    /// <c>ConfigJsonMapper.ToCanonicalJsonText</c>, plus one normalisation: an EMPTY STRING is treated
    /// as absent, exactly the way <c>PruneEmpty</c> already treats null, an empty array and an empty
    /// object.
    ///
    /// <para><b>Why it is needed.</b> Found live, on an instance, not by reading code. A table created
    /// through <c>POST /api/tables</c> without a description stores <c>Description = null</c>; export
    /// prunes the null, so the document has no <c>description</c> at all; re-parsing gives the model
    /// default <c>""</c>, which is NOT pruned. So the very first export→import round-trip of a
    /// freshly-created entity planned as "updated" with nothing whatsoever having changed — and under
    /// this plan, "updated" is what moves <c>Revision</c>. That is revision churn on every import, for
    /// every entity anybody created without filling in a description, i.e. most of them.</para>
    ///
    /// <para><b>Why it is safe.</b> In these config projections null, "" and absent all mean the same
    /// thing — unset. Nothing collapses that a reader could distinguish: an empty string still differs
    /// from every non-empty one. It is applied ONLY to this comparison, never to what gets exported, so
    /// the D-I byte-equality contract on the JSON export is untouched.</para>
    ///
    /// <para><b>ponytail: the wart itself is left where it is.</b> Ceiling: the export still contains no
    /// <c>description</c> while the store holds null, so the two texts still differ — this only stops
    /// that difference from being read as a change. Upgrade path: stop storing null (the endpoint layer's
    /// call, and not this wave's file), at which point this normalisation becomes a no-op rather than
    /// wrong.</para>
    /// </summary>
    private static string Canonical(JsonObject node)
    {
        StripEmptyStrings(node);
        return ConfigJsonMapper.ToCanonicalJsonText(node);
    }

    private static void StripEmptyStrings(JsonNode? node)
    {
        switch (node)
        {
            case JsonObject obj:
                foreach (var key in obj.Select(kv => kv.Key).ToList())
                {
                    if (obj[key] is JsonValue value && value.TryGetValue<string>(out var text) && text.Length == 0)
                    {
                        obj.Remove(key);
                    }
                    else
                    {
                        StripEmptyStrings(obj[key]);
                    }
                }

                break;

            case JsonArray array:
                foreach (var item in array)
                {
                    StripEmptyStrings(item);
                }

                break;
        }
    }

    /// <summary>Did the stored definition actually change? Public so both flavours' registries can ask
    /// it; the three overloads exist rather than one generic because the canonical projection differs
    /// per entity type.</summary>
    public static bool DefinitionChanged(SourceDefinition existing, SourceDefinition incoming) =>
        SourceCanonicalText(existing) != SourceCanonicalText(incoming);

    public static bool DefinitionChanged(PipelineDefinition existing, PipelineDefinition incoming) =>
        PipelineCanonicalText(existing) != PipelineCanonicalText(incoming);

    public static bool DefinitionChanged(TableDefinition existing, TableDefinition incoming) =>
        TableCanonicalText(existing) != TableCanonicalText(incoming);

    /// <summary>Sources: carry AND bump, because nothing else carries for them. <c>Revision</c> moves on
    /// any definition change; <c>SchemaRevision</c> moves only when <c>Fields</c> changes shape — the
    /// split that lets an <c>eventsPerSecond</c> edit leave every downstream pin standing.</summary>
    public static void CarryAndBumpSource(SourceDefinition existing, SourceDefinition incoming)
    {
        incoming.Revision = existing.Revision;
        incoming.SchemaRevision = existing.SchemaRevision;

        if (DefinitionChanged(existing, incoming))
        {
            incoming.Revision++;
        }

        if (SchemaCompatibility.ShapeChanged(existing.Fields, incoming.Fields))
        {
            incoming.SchemaRevision++;
        }
    }

    /// <summary>Pipelines have one counter: nothing depends on a pipeline's shape (nothing reads a
    /// pipeline's output by name), so there is no SchemaRevision to move. Call AFTER
    /// <c>CatalogRecordMerge.CarryServerOwnedFields</c>, which is what puts the stored value on
    /// <paramref name="incoming"/> to increment.</summary>
    public static void BumpPipeline(PipelineDefinition existing, PipelineDefinition incoming)
    {
        if (DefinitionChanged(existing, incoming))
        {
            incoming.Revision++;
        }
    }

    /// <summary>Tables: call AFTER <c>CarryServerOwnedFields</c> AND after the compile result has been
    /// applied, because <c>OutputFields</c> — the thing SchemaRevision is about — is recomputed there.
    /// <paramref name="previousOutputFields"/> is the STORED list, captured before the merge overwrote
    /// the reference.
    ///
    /// <para>A table whose SQL stopped compiling has its <c>OutputFields</c> emptied by
    /// <c>ApplyCompileResult</c>, and that counts as a shape change — deliberately. The table really does
    /// stop advertising a schema, <c>/proto</c> really does start answering 409, and a dependant pinned
    /// to the old shape is entitled to see its pin break rather than to keep believing a schema that is
    /// no longer served. Re-saving the same broken draft does not bump again (empty to empty).</para></summary>
    public static void BumpTable(
        TableDefinition existing,
        TableDefinition incoming,
        IReadOnlyList<FieldDef> previousOutputFields)
    {
        if (DefinitionChanged(existing, incoming))
        {
            incoming.Revision++;
        }

        if (SchemaCompatibility.ShapeChanged(previousOutputFields, incoming.OutputFields))
        {
            incoming.SchemaRevision++;
        }
    }

    /// <summary>
    /// Plan 016 wave 2 — the <c>StaleReason</c> text for one entity's pins, or null when they all hold.
    ///
    /// <para><b>A string, not a flag</b>, because the only useful thing to render is WHICH dependency
    /// moved and from what. A boolean badge sends the operator to the logs to learn the one fact the
    /// badge existed to convey.</para>
    ///
    /// <para><b><c>SchemaRevision == 0</c> on a pin is not a violation</b> — it is a declared edge with no
    /// compatibility claim (import ordering wants the edge even when nobody pinned a shape), so it can
    /// never go stale. That is what makes it safe for a tool to record dependencies it discovered rather
    /// than dependencies the author asserted.</para>
    ///
    /// <para><b>Pipelines are never pinned TO</b> (nothing reads a pipeline's output by name), so only
    /// sources and tables are looked up here — a pin naming any other kind is reported as unresolvable
    /// rather than silently satisfied.</para>
    /// </summary>
    public static string? EvaluatePins(
        IReadOnlyList<EntityPin>? pins,
        IReadOnlyList<SourceDefinition> sources,
        IReadOnlyList<TableDefinition> tables)
    {
        if (pins is null || pins.Count == 0)
        {
            return null;
        }

        var broken = new List<string>();
        foreach (var pin in pins)
        {
            if (pin.SchemaRevision <= 0)
            {
                continue;
            }

            long? current = pin.Kind switch
            {
                "source" => sources.FirstOrDefault(s => string.Equals(s.Name, pin.Name, StringComparison.Ordinal))?.SchemaRevision,
                "table" => tables.FirstOrDefault(t => string.Equals(t.Name, pin.Name, StringComparison.Ordinal))?.SchemaRevision,
                _ => null,
            };

            if (current is null)
            {
                broken.Add($"{(pin.Kind is "source" or "table" ? pin.Kind : $"unknown kind '{pin.Kind}'")} '{pin.Name}' is pinned at schemaRevision {pin.SchemaRevision} but no longer resolves");
            }
            else if (current != pin.SchemaRevision)
            {
                broken.Add($"{pin.Kind} '{pin.Name}' moved from schemaRevision {pin.SchemaRevision} to {current}");
            }
        }

        return broken.Count == 0 ? null : string.Join("; ", broken);
    }
}
