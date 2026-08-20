using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Http;
using StreamForge.Abstractions;
using StreamForge.AppCore.Config;
using StreamForge.AppCore.Access;
using StreamForge.AppCore.Sql;
using StreamForge.AppCore.Transports;
using StreamForge.Engine;
using StreamForge.Host.Grpc.Dynamic;

namespace StreamForge.Api;

/// <summary>
/// Plan 006 (D-I/D-J): the config import/export apply pipeline behind <see cref="ConfigEndpoints"/> —
/// deliberately a sibling file (not inlined in the endpoint handlers) so the composition/apply logic
/// stays unit-testable without an HTTP-level test harness (this repo doesn't have one; see
/// orleans/tests/StreamForge.Host.Tests/ConfigEndpointsLogicTests.cs). <c>public</c> rather than
/// <c>internal</c> specifically so that cross-assembly test project can call it directly.
///
/// <para>Everything except <see cref="ReadDocumentAsync"/> (which touches <see cref="HttpRequest"/> for
/// I/O) and <see cref="RunImportAsync"/> (which touches <see cref="ICatalogFacade"/> for I/O) is a pure
/// function over plain data — that's the "keep handlers thin, logic testable" split the plan asks for.</para>
/// </summary>
public static class ConfigImportService
{
    // ------------------------------------------------------------------
    // Body-form detection (pure).
    // ------------------------------------------------------------------

    public static bool IsMultipartContentType(string? contentType) =>
        contentType is not null && contentType.Contains("multipart/form-data", StringComparison.OrdinalIgnoreCase);

    public static bool IsJsonContentType(string? contentType) =>
        contentType is not null && contentType.Contains("application/json", StringComparison.OrdinalIgnoreCase);

    // ------------------------------------------------------------------
    // Composition (pure) — text/bytes in, ConfigDocument (or document-level diagnostics) out.
    // ------------------------------------------------------------------

    /// <summary>A single JSON or YAML document (raw body of a text/yaml, application/x-yaml, text/plain,
    /// or unrecognized-content-type request, OR the single object case of an application/json body).
    /// Routed through <see cref="ConfigComposer.Compose"/> as a one-element list even though there's
    /// nothing to merge against — this gets the same name-dedup/diagnostic-formatting behavior a
    /// multi-document import gets, for free, instead of a separate code path. A document that declares a
    /// non-empty <c>include</c> list is rejected here (D-I: includes only resolve within a multipart file
    /// set, or the caller can pre-compose an ordered array itself) rather than silently ignored — the
    /// composer would otherwise just drop it, which would be a confusing silent no-op for whoever wrote
    /// the include.</summary>
    public static (ConfigDocument? Doc, List<string> Diagnostics) ComposeSingleDocument(string text)
    {
        var (parsed, parseDiagnostics) = ConfigSerializer.Parse(text);
        if (parsed is null)
        {
            return (null, [.. parseDiagnostics]);
        }

        if (parsed.Include.Count > 0)
        {
            return (null, ["document declares a non-empty 'include' list — includes resolve only within a multipart upload, or pre-compose an ordered array payload yourself"]);
        }

        var (composed, composeDiagnostics) = ConfigComposer.Compose([text]);
        return (composed, [.. composeDiagnostics]);
    }

    /// <summary>An application/json request body: either a single document object (delegates to
    /// <see cref="ComposeSingleDocument"/>) or a JSON array of ordered documents (each element
    /// re-serialized to text and composed via <see cref="ConfigComposer.Compose"/> — later wins,
    /// D-I).</summary>
    public static (ConfigDocument? Doc, List<string> Diagnostics) ComposeJsonBody(string rawJsonText)
    {
        JsonDocument parsed;
        try
        {
            parsed = JsonDocument.Parse(rawJsonText);
        }
        catch (JsonException ex)
        {
            return (null, [$"invalid JSON: {ex.Message}"]);
        }

        using (parsed)
        {
            var root = parsed.RootElement;
            if (root.ValueKind == JsonValueKind.Array)
            {
                var texts = new List<string>();
                foreach (var el in root.EnumerateArray())
                {
                    texts.Add(el.GetRawText());
                }

                var (composed, diagnostics) = ConfigComposer.Compose(texts);
                return (composed, [.. diagnostics]);
            }

            return ComposeSingleDocument(root.GetRawText());
        }
    }

    /// <summary>A multipart file set: <paramref name="rootFileName"/> (the FIRST uploaded file) is the
    /// root document; its own (and any transitively included document's) <c>include</c> entries resolve
    /// by exact FILE NAME within <paramref name="filesByName"/> — the server never touches its own
    /// filesystem for includes (D-I: no path traversal by construction).</summary>
    public static (ConfigDocument? Doc, List<string> Diagnostics) ComposeMultipart(
        string? rootFileName, IReadOnlyDictionary<string, string> filesByName)
    {
        if (string.IsNullOrEmpty(rootFileName))
        {
            return (null, ["multipart upload contains no files"]);
        }

        var (composed, diagnostics) = ConfigComposer.ComposeWithIncludes(
            rootFileName, name => filesByName.TryGetValue(name, out var t) ? t : null);
        return (composed, [.. diagnostics]);
    }

    // ------------------------------------------------------------------
    // Report assembly (pure).
    // ------------------------------------------------------------------

    /// <summary>Builds the 400 response for a document-level composition failure (parse error, include
    /// cycle, missing include, rejected include-on-single-document, ...) — one report entry per
    /// diagnostic, Kind="document" (D-J), Name=the diagnostic text itself (every diagnostic this codebase
    /// produces already names the offending path/index/reason inline — see ConfigComposer/ConfigJsonMapper
    /// — so the diagnostic text doubles as the most useful "Name" available without re-parsing it).</summary>
    public static ConfigImportReport DocumentErrorReport(string mode, IReadOnlyList<string> diagnostics)
    {
        var messages = diagnostics.Count > 0 ? diagnostics : ["document could not be composed"];
        return new ConfigImportReport
        {
            Mode = mode,
            Ok = false,
            Entries = [.. messages.Select(d => new ConfigImportReportEntry
            {
                Kind = "document",
                Name = d,
                Action = "error",
                Diagnostics = [d],
            })],
        };
    }

    // ------------------------------------------------------------------
    // HTTP I/O glue (thin — reads the request, delegates to the pure composers above).
    // ------------------------------------------------------------------

    public static async Task<(ConfigDocument? Doc, List<string> Diagnostics)> ReadDocumentAsync(HttpRequest request)
    {
        if (IsMultipartContentType(request.ContentType))
        {
            var form = await request.ReadFormAsync();
            if (form.Files.Count == 0)
            {
                return (null, ["multipart upload contains no files"]);
            }

            var filesByName = new Dictionary<string, string>(StringComparer.Ordinal);
            foreach (var file in form.Files)
            {
                using var fileReader = new StreamReader(file.OpenReadStream(), Encoding.UTF8);
                filesByName[file.FileName] = await fileReader.ReadToEndAsync();
            }

            return ComposeMultipart(form.Files[0].FileName, filesByName);
        }

        using var bodyReader = new StreamReader(request.Body, Encoding.UTF8);
        var body = await bodyReader.ReadToEndAsync();
        return IsJsonContentType(request.ContentType) ? ComposeJsonBody(body) : ComposeSingleDocument(body);
    }

    // ------------------------------------------------------------------
    // World-overlay schema building (pure) — mirrors PipelinesEndpoints.BuildSchemasAsync /
    // TablesEndpoints.BuildStreamSchemasAsync's FieldType -> FieldKind mapping (duplicated here on
    // purpose: this file doesn't own those endpoint files and extract-and-share isn't available across
    // ownership boundaries — see the class doc comment on ProcessTableAsync below for the same note
    // re: table create/update bodies).
    // ------------------------------------------------------------------

    public static Dictionary<string, SourceSchema> BuildSourceSchemas(IEnumerable<SourceDefinition> sources)
    {
        var schemas = new Dictionary<string, SourceSchema>(StringComparer.Ordinal);
        foreach (var src in sources)
        {
            var fields = src.Fields.ToDictionary(f => f.Name, f => MapFieldKind(f.Type));
            schemas[src.Name] = new SourceSchema(src.Name, fields);
        }

        return schemas;
    }

    public static FieldKind MapFieldKind(FieldType type) => type switch
    {
        FieldType.String => FieldKind.String,
        FieldType.Double => FieldKind.Double,
        FieldType.Long => FieldKind.Long,
        FieldType.Bool => FieldKind.Bool,
        FieldType.Timestamp => FieldKind.Timestamp,
        FieldType.Json => FieldKind.Json,
        _ => throw new ArgumentOutOfRangeException(nameof(type), type, "Unknown field type"),
    };

    private static List<string> FormatDiagnostics(IEnumerable<SqlDiagnostic> diagnostics) =>
        [.. diagnostics.Select(d => $"{d.Line}:{d.Column} {d.Message}")];

    // ------------------------------------------------------------------
    // Entitlement pre-check (plan 015 wave 3-C) — impure only in that it reads the catalog to plan.
    // ------------------------------------------------------------------

    /// <summary>
    /// Which of the changes this document would make the caller is <b>not</b> entitled to, one legible
    /// line each. Empty means the whole document may be applied.
    ///
    /// <para><b>The rule: refuse the whole import, naming the entities. Never apply the entitled
    /// subset.</b> Three reasons, in the order they convinced me.</para>
    ///
    /// <para>First, a config document is a description of a desired end state whose parts reference
    /// each other. A pipeline's SQL names sources; a table's SQL names tables the planner ordered
    /// ahead of it. Drop the un-entitled half and what gets applied is a document nobody wrote and
    /// nobody reviewed — a pipeline created against a source whose update was skipped compiles against
    /// the old schema, or stops compiling at all. The failure is not "less happened than I asked for",
    /// it is "something different happened".</para>
    ///
    /// <para>Second, <c>replace</c> mode makes partial application actively destructive: it deletes
    /// what the document omits. A partial replace would delete the entities the caller <i>is</i>
    /// entitled to while leaving the ones they are not, converging the catalog on neither the old state
    /// nor the document. There is no ordering of that operation which is safe to half-do.</para>
    ///
    /// <para>Third, refusing is recoverable and partial application is not. A caller told "you may not
    /// touch prod-feed and prod-book" edits the document or asks for a grant and runs it again; a
    /// caller handed a 200 with a report of skips has already had half a catalog rewritten, and putting
    /// it back requires the export they did not take. Between an error the caller can act on and a
    /// success they cannot undo, the error is the cheaper mistake.</para>
    ///
    /// <para><b>Why <c>validate</c> is checked too.</b> Validate is the dry run of exactly this import,
    /// and a dry run that says yes to something the real run will refuse is worse than no dry run. It
    /// writes nothing either way, so the only thing the check costs a caller is being told sooner.</para>
    ///
    /// <para><b>What is NOT checked: <c>skipped</c> actions.</b> The planner marks an entity skipped
    /// when the document restates it byte-identically, and a change that is not a change needs no
    /// entitlement. That is what makes the obvious workflow work: export the whole catalog, edit one
    /// pipeline, import it back, and be judged on the one pipeline you actually changed.</para>
    /// </summary>
    /// <param name="check">(action, scope, resourceTags) -> decision. The endpoint passes
    /// <c>AccessGuard.CheckAsync</c> bound to the caller; a test passes a stub. Anything other than
    /// <see cref="AccessDecision.Allowed"/> — including <see cref="AccessDecision.RequiresApproval"/> —
    /// counts as a refusal: parking a whole catalog rewrite behind one approval is wave 4's design
    /// question, and until it has an answer, failing closed is the only option that cannot be
    /// wrong.</param>
    public static async Task<IReadOnlyList<string>> FindUnentitledChangesAsync(
        ConfigDocument doc,
        string mode,
        ICatalogFacade registry,
        Func<string, string, IReadOnlyCollection<string>?, Task<AccessResult>> check)
    {
        var currentSources = await registry.GetSourcesAsync();
        var currentPipelines = await registry.GetPipelinesAsync();
        var currentTables = await registry.GetTablesAsync();

        // ponytail: plans a second time (RunImportAsync plans its own). Ceiling: three extra catalog
        // reads per import and a TOCTOU window in which the catalog could change between the check and
        // the apply. Both are acceptable because an import is a rare, heavyweight, human-initiated
        // operation and the window is the same one every read-then-write endpoint in the platform
        // already has. Upgrade path, if it ever matters: hand RunImportAsync the plan instead of
        // letting it build one.
        var plan = ImportPlanner.Plan(doc, currentSources, currentPipelines, currentTables, mode);

        var storedSources = currentSources.ToDictionary(s => s.Name, StringComparer.Ordinal);
        var storedPipelines = FirstByName(currentPipelines, p => p.Name);
        var storedTables = FirstByName(currentTables, t => t.Name);
        var docSources = doc.Sources.ToDictionary(s => s.Name, StringComparer.Ordinal);
        var docPipelines = doc.Pipelines.ToDictionary(p => p.Name, StringComparer.Ordinal);
        var docTables = doc.Tables.ToDictionary(t => t.Name, StringComparer.Ordinal);

        var refusals = new List<string>();
        foreach (var action in plan)
        {
            if (action.Action == "skipped")
            {
                continue;
            }

            var deleting = action.Action == "deleted";
            string permission;
            string scope;
            IReadOnlyCollection<string>? tags;

            switch (action.Kind)
            {
                case "source":
                    permission = deleting ? Actions.SourceDelete : Actions.SourceWrite;
                    // Sources are addressed by NAME everywhere (/api/sources/{name}), so that is the
                    // scope. Pipelines and tables are addressed by id, below.
                    scope = action.Name;
                    tags = TagsFor(storedSources, docSources, action.Name, s => s.Tags, s => s.Tags);
                    break;

                case "pipeline":
                    permission = deleting ? Actions.PipelineDelete : Actions.PipelineWrite;
                    scope = storedPipelines.TryGetValue(action.Name, out var sp) ? sp.Id : action.Name;
                    tags = TagsFor(storedPipelines, docPipelines, action.Name, p => p.Tags, p => p.Tags);
                    break;

                case "table":
                    permission = deleting ? Actions.TableDelete : Actions.TableWrite;
                    scope = storedTables.TryGetValue(action.Name, out var st) ? st.Id : action.Name;
                    tags = TagsFor(storedTables, docTables, action.Name, t => t.Tags, t => t.Tags);
                    break;

                default:
                    throw new InvalidOperationException($"unknown planned-action kind: {action.Kind}");
            }

            var decision = await check(permission, scope, tags);
            if (decision.Decision != AccessDecision.Allowed)
            {
                refusals.Add($"{action.Kind} '{action.Name}' ({action.Action}, needs {permission} on {scope}): {decision.Reason}");
            }
        }

        return refusals;
    }

    /// <summary>The 403 body for a refused import: every entity the caller may not touch, and an
    /// explicit statement that nothing was applied. One <c>ErrorResponse</c> string rather than a new
    /// DTO, because that is the refusal shape this endpoint already uses ("replace mode requires the
    /// Admin role") and every client in <c>clients/**</c> already reads it.</summary>
    public static string UnentitledImportMessage(IReadOnlyList<string> refusals) =>
        $"import refused — nothing was applied. You are not entitled to {refusals.Count} of the changes this document makes: " +
        string.Join("; ", refusals);

    /// <summary>The tags a decision about this entity is made against: the STORED entity's when it
    /// exists, the document's when it is being created.
    ///
    /// <para>ponytail: the stored tags win for an update. Ceiling: a caller entitled via
    /// <c>tag:dev</c> can retag a dev entity to <c>prod</c> in the same import. That is exactly the
    /// hole <c>PUT /api/sources/{name}</c> has, and closing it only here would make importing stricter
    /// than editing — a difference nobody could explain. Upgrade path: require the decision to hold for
    /// both the old and the new tag set, on every mutation surface at once.</para></summary>
    private static IReadOnlyCollection<string>? TagsFor<TStored, TDoc>(
        Dictionary<string, TStored> stored,
        Dictionary<string, TDoc> fromDoc,
        string name,
        Func<TStored, List<string>> storedTags,
        Func<TDoc, List<string>> docTags)
    {
        if (stored.TryGetValue(name, out var existing))
        {
            return storedTags(existing);
        }

        return fromDoc.TryGetValue(name, out var proposed) ? docTags(proposed) : null;
    }

    // ------------------------------------------------------------------
    // Plan 016 wave 3-C — two fatal, WHOLE-IMPORT gates. Both run before the apply loop below (and
    // before any world-schema building, so a compile that would otherwise "see" a table that should
    // never have been reached never gets the chance), and both run identically whether RunImportAsync
    // is called with apply:false (mode=validate) or apply:true (a real write) — the whole design point
    // is that validate catches these before anything is applied, so it must not be a weaker check.
    // ------------------------------------------------------------------

    /// <summary>
    /// Job 1 — cycles become fatal HERE, not in <see cref="ImportPlanner"/>. <c>ImportPlanner.Plan</c>
    /// breaks a table-dependency cycle with a per-table diagnostic and still returns a usable (if
    /// best-effort on the cyclic subset) apply order — <c>ImportPlannerTests</c> pins exactly that
    /// behavior, and that file belongs to wave 3-B, so it does not change. This mirrors
    /// <see cref="ConfigComposer"/>'s include-cycle precedent instead: a cycle here is fatal, refusing
    /// the whole import before a single row is written, and naming the FULL chain — "a cycle exists"
    /// tells an operator nothing they can act on; "a -&gt; b -&gt; c -&gt; a" does.
    ///
    /// <para><b>Deliberately independent of the planner's own diagnostic text</b> rather than parsing
    /// <c>PlannedAction.Diagnostics</c> strings for the word "cycle" — that would silently stop working
    /// the day <c>ImportPlanner</c>'s wording changes, in a file this wave does not own. Re-derives the
    /// same graph instead: for a table that already exists in the catalog, its persisted
    /// <c>TableInputs</c> — exact post-compile facts, exactly what the planner itself prefers for
    /// existing tables. For a brand-new table, the same desugar-then-<see cref="SqlCompiler.ExtractReferences"/>
    /// walk <c>ImportPlanner.ScanNewEntityReferences</c> settled on (wave 3-B, landed after this method
    /// was first written — see <see cref="ScanNewEntityReferences"/> below, a same-behavior copy rather
    /// than a cross-assembly call: that method is <c>internal</c> to <c>StreamForge.AppCore</c>, a file
    /// this wave does not own, and there is no <c>InternalsVisibleTo</c> making it reachable from here):
    /// desugar <c>INSERT INTO &lt;sink&gt;</c> first (the Engine has no INSERT production) and extract via
    /// the real compiler, with no regex fallback — an unparseable statement contributes no edges. Returns
    /// null when the graph is acyclic.</para>
    /// </summary>
    public static string? DetectTableDependencyCycle(
        ConfigDocument doc, IReadOnlyDictionary<string, TableDefinition> tableByName)
    {
        var plannedNames = doc.Tables.Select(t => t.Name).ToList();
        var plannedSet = new HashSet<string>(plannedNames, StringComparer.Ordinal);
        var docByName = doc.Tables.ToDictionary(t => t.Name, StringComparer.Ordinal);

        var deps = new Dictionary<string, List<string>>(StringComparer.Ordinal);
        foreach (var name in plannedNames)
        {
            deps[name] = tableByName.TryGetValue(name, out var existing)
                ? [.. existing.TableInputs.Where(plannedSet.Contains)]
                : [.. ScanNewEntityReferences(docByName[name].Sql).Where(plannedSet.Contains)];
        }

        var visited = new HashSet<string>(StringComparer.Ordinal);
        var onStack = new HashSet<string>(StringComparer.Ordinal);
        var stack = new List<string>();
        string? cycle = null;

        void Visit(string name)
        {
            if (cycle is not null || visited.Contains(name))
            {
                return;
            }

            stack.Add(name);
            onStack.Add(name);

            foreach (var dep in deps[name].OrderBy(x => x, StringComparer.Ordinal))
            {
                if (cycle is not null)
                {
                    break;
                }

                if (onStack.Contains(dep))
                {
                    var start = stack.IndexOf(dep);
                    cycle = string.Join(" -> ", stack.Skip(start)) + " -> " + dep;
                    break;
                }

                Visit(dep);
            }

            stack.RemoveAt(stack.Count - 1);
            onStack.Remove(name);
            visited.Add(name);
        }

        foreach (var name in plannedNames.OrderBy(x => x, StringComparer.Ordinal))
        {
            if (cycle is not null)
            {
                break;
            }

            Visit(name);
        }

        return cycle;
    }

    /// <summary>Same-behavior copy of <c>ImportPlanner.ScanNewEntityReferences</c> (wave 3-B) — desugar
    /// <c>INSERT INTO &lt;sink&gt;</c> first (the Engine has no INSERT production, so a still-sugared
    /// statement is a parse error to <see cref="SqlCompiler.ExtractReferences"/> and would otherwise
    /// lose every dependency of the commonest authored form), then extract via the real compiler and
    /// trust it — an unparseable statement contributes no edges, no regex fallback. Kept in sync by hand,
    /// the same "mirrors, does not share" rule <see cref="ProcessTableAsync"/>'s doc comment already
    /// states for the reason (<c>ImportPlanner.cs</c> and this file have different owners this
    /// wave).</summary>
    private static List<string> ScanNewEntityReferences(string sql) =>
        [.. SqlCompiler.ExtractReferences(SinkSugar.Desugar(sql ?? "").Sql)];

    /// <summary>The document-level, 200-with-<c>Ok:false</c> report for a fatal cycle — the same shape
    /// <see cref="DocumentErrorReport"/> uses for a composition failure (Kind="document"), because it is
    /// the same kind of failure: nothing single-entity to point at, so the whole document is
    /// refused.</summary>
    public static ConfigImportReport CycleErrorReport(string mode, string cycle)
    {
        var message = $"table dependency cycle detected: {cycle} — import refused, nothing applied";
        return new ConfigImportReport
        {
            Mode = mode,
            Ok = false,
            Entries = [new ConfigImportReportEntry { Kind = "document", Name = message, Action = "error", Diagnostics = [message] }],
        };
    }

    /// <summary>
    /// Job 2 — the <c>schemaPolicy</c> gate. <c>ConfigDocument.SchemaPolicy</c> is null-or-absent means
    /// "compatible" (the gate is ON) and the literal string "any" is the only value that turns it off —
    /// the same "a typo leaves enforcement on" rule <c>Auth:Mode</c> already follows, so a document that
    /// meant to write <c>schemaPolicy: any</c> and typo'd it is still gated, not silently permissive.
    /// For every source the incoming document shares a name with an existing catalog source,
    /// <see cref="SchemaCompatibility.Compare"/> (the identical removal/type-change walk
    /// <c>PUT /api/sources/{name}?allowBreaking=false</c> already uses — "compatible" means one thing
    /// everywhere) decides whether the change is breaking. A brand-new source has nothing stored to be
    /// incompatible WITH, so it is never gated here.
    ///
    /// <para><b>Decision: ONE breaking source refuses the WHOLE import, not only that source</b> — the
    /// same rule, and the same reasoning, <see cref="FindUnentitledChangesAsync"/> already applies to
    /// entitlement refusals one section up in this very file. Restated for schema instead of
    /// authorization: (1) a config document describes one coherent desired end state, and a table or
    /// pipeline ELSEWHERE in the same document may already read from the source whose change is
    /// breaking — applying everything except that one source risks compiling a dependent against a
    /// source that no longer matches what it was written against, which is "something different
    /// happened", not "less happened". (2) <c>replace</c> mode makes partial application actively
    /// destructive: skipping only the breaking source while applying every other created/updated/deleted
    /// entity in the same document converges the catalog on neither the old state nor the document.
    /// (3) refusing is recoverable — fix the source, or declare <c>schemaPolicy: "any"</c>, and re-run —
    /// while a caller handed a 200 that silently skipped one source has already had the rest of a
    /// catalog rewritten around a gap they may not notice until something downstream breaks. (4) two
    /// gates in the same import path (this one and the entitlement one) that disagreed about
    /// all-or-nothing would itself be a trap: an operator who has learned "an import error here means
    /// nothing was applied" from one gate should not have to relearn a narrower promise from the other.
    /// And <c>validate</c> agrees with a real apply for the identical reason
    /// <see cref="FindUnentitledChangesAsync"/>'s does — catching this before anything is applied IS the
    /// entire point of a dry run.</para>
    /// </summary>
    public static List<ConfigImportReportEntry> DetectBreakingSchemaChanges(
        ConfigDocument doc, IReadOnlyDictionary<string, SourceDefinition> storedSourceByName)
    {
        if (string.Equals(doc.SchemaPolicy, "any", StringComparison.Ordinal))
        {
            return [];
        }

        var entries = new List<ConfigImportReportEntry>();
        foreach (var docSource in doc.Sources.OrderBy(s => s.Name, StringComparer.Ordinal))
        {
            if (!storedSourceByName.TryGetValue(docSource.Name, out var stored))
            {
                continue; // brand new — nothing stored to be incompatible with.
            }

            var compat = SchemaCompatibility.Compare(stored.Fields, docSource.Fields);
            if (!compat.IsCompatible)
            {
                entries.Add(new ConfigImportReportEntry
                {
                    Kind = "source",
                    Name = docSource.Name,
                    Action = "error",
                    Diagnostics = [.. compat.BreakingReasons],
                });
            }
        }

        return entries;
    }

    /// <summary>
    /// Job 3 (plan 016 wave 4) — the <c>ConfigDocument.Requires</c> gate: for every declared connector
    /// kind + version-range pin, is that kind registered on THIS instance, and does its declared
    /// <c>TransportDescriptor.Version</c> (or, for the six kinds with no descriptor at all, the fixed
    /// built-in floor — see <see cref="KindVersions"/>) satisfy the range? A malformed range string is
    /// treated as unsatisfied rather than ignored — the same fail-closed choice
    /// <see cref="FindUnentitledChangesAsync"/>'s doc comment already makes for an unanswerable
    /// authorization question: a requirement this method cannot even evaluate is not one it can call
    /// satisfied.
    ///
    /// <para><b>Decision: FATAL, exactly like <see cref="DetectBreakingSchemaChanges"/> — not a warning
    /// like an unresolvable <c>@endpoint</c> (plan 016 wave 6). The two precedents pull in opposite
    /// directions and this is closer to the schema one:</b></para>
    ///
    /// <para>1. <b>An endpoint alias is a VALUE that is SUPPOSED to differ per environment</b> — that is
    /// the entire feature (a catalog exported from prod imports byte-identical into dev and connects to
    /// a different database), so refusing to import until every alias resolves would break the one thing
    /// endpoint aliasing exists to enable. A connector kind/version requirement is not that kind of
    /// value: "this document needs postgres-cdc ^2.0.0" does not mean something different in dev than in
    /// prod — it means the SAME thing everywhere, a statement about what code has to be running to
    /// interpret the document correctly, and a build either has that capability or it does not. There is
    /// no legitimate reading under which importing it anyway and discovering the gap later is the
    /// desired behavior — unlike an endpoint, promotion is not why this check exists.</para>
    ///
    /// <para>2. <b>A missing KIND fails loudly elsewhere (via <c>SourceValidation.IsKnownKind</c>'s 400
    /// at the per-source write), but that is not the whole of what this field catches.</b> It only fires
    /// for a source of that kind actually present in the SAME document — a <c>requires</c> entry can
    /// name a kind the document's pipelines/tables depend on transitively (a <c>fix-duplex</c> sink
    /// naming a session opened elsewhere) or declare a version floor with no source of that kind in this
    /// particular document at all, and — the case that matters most — <b>a VERSION mismatch on a kind
    /// that IS registered produces no error anywhere else in the system.</b> <c>IsKnownKind</c> only asks
    /// "is this string registered", not "at what version" — a document authored against
    /// <c>postgres-cdc@2.x</c>'s row mapping, imported onto an instance still running
    /// <c>postgres-cdc@1.x</c>, creates the source successfully, compiles successfully, and behaves
    /// differently with no error anywhere. That is precisely the "silently corrupts a consumer" category
    /// <see cref="DetectBreakingSchemaChanges"/>'s doc comment describes for a schema break, not the
    /// "fails loudly at start anyway" category the plan's endpoint precedent describes — so the schema
    /// gate's reasoning is the one that transfers, not the endpoint one.</para>
    ///
    /// <para>3. <b>Consistency of the promise.</b> Two whole-import gates (cycle, schema) already teach
    /// an operator "an import error here means nothing was applied, fix it and re-run." A third
    /// silent-corruption-shaped gate that instead behaves like the endpoint warning would break that
    /// single mental model for a reason nothing about THIS gate's failure mode justifies — it is
    /// recoverable (bump the requirement, or upgrade/register the connector, and re-run) exactly like
    /// the schema gate, and <c>validate</c> must agree with a real apply for the identical reason: the
    /// entire point of a dry run is catching this before anything is written.</para>
    /// </summary>
    public static List<ConfigImportReportEntry> DetectUnsatisfiedPluginRequirements(
        ConfigDocument doc, IReadOnlyDictionary<string, string> availableKindVersions)
    {
        var entries = new List<ConfigImportReportEntry>();
        foreach (var req in doc.Requires.OrderBy(r => r.Kind, StringComparer.Ordinal))
        {
            if (!availableKindVersions.TryGetValue(req.Kind, out var installed))
            {
                entries.Add(new ConfigImportReportEntry
                {
                    Kind = "requires",
                    Name = req.Kind,
                    Action = "error",
                    Diagnostics = [$"kind '{req.Kind}' is not registered on this instance (document requires version '{req.Version}')"],
                });
                continue;
            }

            if (!SemVerRange.TryParse(req.Version, out var range) || range is null)
            {
                entries.Add(new ConfigImportReportEntry
                {
                    Kind = "requires",
                    Name = req.Kind,
                    Action = "error",
                    Diagnostics = [$"'{req.Version}' is not a supported version range for kind '{req.Kind}' (see SemVerRange for the supported subset)"],
                });
                continue;
            }

            if (!range.Matches(installed))
            {
                entries.Add(new ConfigImportReportEntry
                {
                    Kind = "requires",
                    Name = req.Kind,
                    Action = "error",
                    Diagnostics = [$"kind '{req.Kind}' is installed at version '{installed}', which does not satisfy the required range '{req.Version}'"],
                });
            }
        }

        return entries;
    }

    // ------------------------------------------------------------------
    // Apply pipeline (impure — reads/writes through ICatalogFacade).
    // ------------------------------------------------------------------

    /// <summary>The D-J apply pipeline: plans via <see cref="ImportPlanner.Plan"/> (whose returned order
    /// IS the apply order), then walks the plan once, dispatching each <see cref="PlannedAction"/> to
    /// <see cref="ProcessSourceAsync"/>/<see cref="ProcessTableAsync"/>/<see cref="ProcessPipelineAsync"/>.
    /// When <paramref name="apply"/> is false ("validate" mode) nothing touches <paramref name="registry"/>
    /// beyond the initial reads — every planned created/updated pipeline/table is still compiled (against
    /// the synthetic "post-import world" built below) so validate reports the same compile diagnostics an
    /// actual apply would hit.</summary>
    public static async Task<ConfigImportReport> RunImportAsync(
        ConfigDocument doc, string mode, string createdBy, ICatalogFacade registry, bool apply)
    {
        var currentSources = await registry.GetSourcesAsync();
        var currentPipelines = await registry.GetPipelinesAsync();
        var currentTables = await registry.GetTablesAsync();

        var plan = ImportPlanner.Plan(doc, currentSources, currentPipelines, currentTables, mode);

        var sourceByName = currentSources.ToDictionary(s => s.Name, StringComparer.Ordinal);
        var pipelineByName = FirstByName(currentPipelines, p => p.Name);
        var tableByName = FirstByName(currentTables, t => t.Name);

        // Plan 016 wave 3-C/4 — three fatal, whole-import gates, BEFORE any world-schema building or the
        // apply loop below, and identically for apply:false (mode=validate) and apply:true (a real
        // write): see DetectTableDependencyCycle/DetectUnsatisfiedPluginRequirements/
        // DetectBreakingSchemaChanges above for the arguments. Order is structural (can this graph even
        // be applied) -> capability (can THIS instance run what the document needs at all) -> shape
        // (does applying it corrupt an existing consumer) — each gate answers a more specific question
        // than the one before it, so checking in that order gives the earliest, most actionable error
        // first when a document manages to trip more than one.
        var cycle = DetectTableDependencyCycle(doc, tableByName);
        if (cycle is not null)
        {
            return CycleErrorReport(mode, cycle);
        }

        var pluginEntries = DetectUnsatisfiedPluginRequirements(doc, KindVersions.All());
        if (pluginEntries.Count > 0)
        {
            return new ConfigImportReport { Mode = mode, Ok = false, Entries = pluginEntries };
        }

        var breakingSchemaEntries = DetectBreakingSchemaChanges(doc, sourceByName);
        if (breakingSchemaEntries.Count > 0)
        {
            return new ConfigImportReport { Mode = mode, Ok = false, Entries = breakingSchemaEntries };
        }

        var docSourceByName = doc.Sources.ToDictionary(s => s.Name, StringComparer.Ordinal);
        var docPipelineByName = doc.Pipelines.ToDictionary(p => p.Name, StringComparer.Ordinal);
        var docTableByName = doc.Tables.ToDictionary(t => t.Name, StringComparer.Ordinal);

        // "Post-import world" schema dict for pipeline/table SQL compilation (D-J spec: "catalog sources
        // overlaid with the doc's sources") — literal overlay, NOT filtered by replace-mode deletions.
        // A replace-deleted source is applied dead last in the plan (after every create/update compile
        // has already happened), so leaving it visible here doesn't change any compile outcome; it just
        // matches the instruction's literal wording without extra bookkeeping.
        var worldSourceDefs = new Dictionary<string, SourceDefinition>(sourceByName, StringComparer.Ordinal);
        foreach (var (name, ds) in docSourceByName)
        {
            worldSourceDefs[name] = ds;
        }

        var worldSourceSchemas = BuildSourceSchemas(worldSourceDefs.Values);

        // Table schema world, seeded from the CURRENT catalog's already-compiled OutputFields, then
        // mutated in place as each planned table is processed in the planner's topo order — so a table
        // that depends on an earlier-in-order table (just created/updated in this same loop) sees its
        // fresh schema, exactly like TablesEndpoints.CreateTableAsync would if we applied one at a time.
        var worldTableSchemas = new Dictionary<string, SourceSchema>(StringComparer.Ordinal);
        foreach (var (name, t) in tableByName)
        {
            if (t.OutputFields.Count > 0)
            {
                worldTableSchemas[name] = new SourceSchema(name, t.OutputFields.ToDictionary(f => f.Name, f => MapFieldKind(f.Type)));
            }
        }

        var entries = new List<ConfigImportReportEntry>(plan.Count);
        foreach (var action in plan)
        {
            var entry = action.Kind switch
            {
                "source" => await ProcessSourceAsync(action, docSourceByName, sourceByName, registry, apply),
                "table" => await ProcessTableAsync(action, docTableByName, tableByName, worldSourceSchemas, worldTableSchemas, registry, apply, createdBy),
                "pipeline" => await ProcessPipelineAsync(action, docPipelineByName, pipelineByName, worldSourceSchemas, registry, apply, createdBy),
                _ => throw new InvalidOperationException($"unknown planned-action kind: {action.Kind}"),
            };
            entries.Add(entry);
        }

        // Plan 016 wave 6 — non-fatal, unlike the three gates above: an unresolvable @name reference is
        // a WARNING folded onto the entity that carries it, never an "error" entry and never a reason to
        // refuse the import. Runs after the apply loop (so it sees every entity's final Action) and
        // identically for apply:true and apply:false (mode=validate) — same reason DetectTableDependencyCycle
        // et al. run identically for both: validate is a dry run of the SAME import, and a dry run that
        // stays silent about something the real run would flag is a worse dry run than none.
        AttachEndpointWarnings(entries, doc);

        // Plan 016 wave 7 follow-up. The plan promised a dependsOn pin is checked "at import ... so
        // mode=validate catches it before anything is applied"; it was not — EvaluatePins was reachable
        // ONLY from each registry's post-write RecomputeStaleReasons, so validate said nothing and the
        // break first appeared as a staleReason after a real merge. Found by the wave-7 docs agent
        // testing the plan's own sentence against the running code.
        AttachPinWarnings(entries, doc, currentSources, currentTables);

        return new ConfigImportReport
        {
            Mode = mode,
            Entries = entries,
            Ok = entries.All(e => e.Action != "error"),
        };
    }

    /// <summary>Folds <see cref="EndpointReferenceWarnings.Scan"/>'s findings into the matching report
    /// entry's <c>Diagnostics</c>, by exact (Kind, Name) match. Skipped for a "deleted" entity (its
    /// connector config is on its way out of the catalog, not something that will ever connect) and for
    /// an "error" entry (it never applied — SQL that failed to compile is not a value being asked to
    /// resolve). "created"/"updated"/"skipped" all get it: "skipped" means the document restates the
    /// entity byte-identically, and its endpoint references are just as real as a changed entity's —
    /// mode=validate answering "will this land here" should say so.</summary>
    private static void AttachEndpointWarnings(List<ConfigImportReportEntry> entries, ConfigDocument doc)
    {
        var warnings = EndpointReferenceWarnings.Scan(doc);
        if (warnings.Count == 0)
        {
            return;
        }

        var byEntity = warnings
            .GroupBy(w => (w.Kind, w.Name), EqualityComparer<(string, string)>.Default)
            .ToDictionary(g => g.Key, g => g.Select(w => w.Message).ToList());

        foreach (var entry in entries)
        {
            if (entry.Action is "deleted" or "error")
            {
                continue;
            }

            if (byEntity.TryGetValue((entry.Kind, entry.Name), out var messages))
            {
                entry.Diagnostics = [.. entry.Diagnostics, .. messages];
            }
        }
    }

    /// <summary>Plan 016 wave 7 follow-up — reports a <c>dependsOn</c> pin that will not hold here, on the
    /// entity that declares it, at PLAN time, so <c>mode=validate</c> answers "will this land here" for
    /// pins the way it already does for cycles, plugin requirements and schema breaks.
    ///
    /// <para><b>A warning, not a gate</b> — deliberately, and unlike the three fatal checks above. Wave 2
    /// already decided what a violated pin MEANS: it sets <c>staleReason</c>, badges the entity, and lets
    /// it keep running on its compiled plan. A gate here would make import stricter than the runtime it
    /// imports into, which is backwards.</para>
    ///
    /// <para><b>The honest limit:</b> only pins naming an entity the document does NOT itself declare are
    /// evaluated. Both revision counters are registry-assigned at write time, so the post-import
    /// <c>SchemaRevision</c> of an entity this very document creates or updates is not knowable here, and
    /// guessing it would produce confident nonsense. That leaves exactly the case worth reporting, and the
    /// common one: a pin against an upstream the author is not editing.</para></summary>
    private static void AttachPinWarnings(
        List<ConfigImportReportEntry> entries,
        ConfigDocument doc,
        List<SourceDefinition> currentSources,
        List<TableDefinition> currentTables)
    {
        var declaredSources = doc.Sources.Select(s => s.Name).ToHashSet(StringComparer.Ordinal);
        var declaredTables = doc.Tables.Select(t => t.Name).ToHashSet(StringComparer.Ordinal);

        bool Knowable(EntityPin pin) => pin.Kind switch
        {
            "source" => !declaredSources.Contains(pin.Name),
            "table" => !declaredTables.Contains(pin.Name),
            _ => true, // an unpinnable kind is wrong no matter what the document declares.
        };

        var byEntity = new Dictionary<(string Kind, string Name), string>();
        foreach (var (kind, name, pins) in
                 doc.Tables.Select(t => ("table", t.Name, (IReadOnlyList<EntityPin>)t.DependsOn))
                     .Concat(doc.Pipelines.Select(p => ("pipeline", p.Name, (IReadOnlyList<EntityPin>)p.DependsOn))))
        {
            var knowable = pins.Where(Knowable).ToList();
            if (CatalogRevisions.EvaluatePins(knowable, currentSources, currentTables) is { } reason)
            {
                byEntity[(kind, name)] = reason;
            }
        }

        if (byEntity.Count == 0)
        {
            return;
        }

        foreach (var entry in entries)
        {
            // Same exclusions AttachEndpointWarnings applies, for the same reasons: a deleted entity's
            // pins are on their way out of the catalog, and an "error" entry never applied at all.
            if (entry.Action is "deleted" or "error")
            {
                continue;
            }

            if (byEntity.TryGetValue((entry.Kind, entry.Name), out var reason))
            {
                entry.Diagnostics = [.. entry.Diagnostics, $"dependsOn: {reason}"];
            }
        }
    }

    private static Dictionary<string, T> FirstByName<T>(IEnumerable<T> items, Func<T, string> nameOf)
    {
        // First-wins rather than ToDictionary (which throws on a duplicate key): pipeline/table names
        // aren't enforced unique in the catalog (see PipelinesEndpoints' /proto endpoint comment) —
        // ImportPlanner.Plan itself assumes uniqueness (its own ToDictionary would already have thrown
        // before we get here if the catalog genuinely has duplicates), so this is purely defensive.
        var result = new Dictionary<string, T>(StringComparer.Ordinal);
        foreach (var item in items)
        {
            result.TryAdd(nameOf(item), item);
        }

        return result;
    }

    private static ConfigImportReportEntry ToEntry(string kind, PlannedAction action) => new()
    {
        Kind = kind,
        Name = action.Name,
        Action = action.Action,
        Diagnostics = [.. action.Diagnostics],
    };

    private static ConfigImportReportEntry ErrorEntry(string kind, string name, IReadOnlyList<string> diagnostics) => new()
    {
        Kind = kind,
        Name = name,
        Action = "error",
        Diagnostics = [.. diagnostics],
    };

    /// <summary>Source entities never compile — the only failure mode is a catalog write throwing (not
    /// expected for sources; UpsertSourceAsync has no uniqueness/dependency checks), so this never
    /// produces an "error" entry, only whatever ImportPlanner already planned.</summary>
    private static async Task<ConfigImportReportEntry> ProcessSourceAsync(
        PlannedAction action,
        Dictionary<string, SourceDefinition> docByName,
        Dictionary<string, SourceDefinition> storedByName,
        ICatalogFacade registry,
        bool apply)
    {
        if (action.Action == "deleted")
        {
            if (apply)
            {
                await registry.DeleteSourceAsync(action.Name);
            }

            return ToEntry("source", action);
        }

        if (action.Action == "skipped")
        {
            return ToEntry("source", action);
        }

        // "created" or "updated": D-J — SecretsMasker.MergeSecrets(incoming, storedIfAny) then Upsert.
        if (apply)
        {
            var docSource = docByName[action.Name];
            var stored = storedByName.GetValueOrDefault(action.Name);
            var effective = SecretsMasker.MergeSecrets(docSource, stored);
            await registry.UpsertSourceAsync(effective);
        }

        return ToEntry("source", action);
    }

    /// <summary>Table created/updated bodies deliberately MIRROR (not share) TablesEndpoints' POST/PUT
    /// handler bodies — extract-and-share isn't available since this file doesn't own TablesEndpoints.cs
    /// (ownership boundary, plan 006 W3C brief). Keep the two in sync by hand if TablesEndpoints' request
    /// -> TableDefinition mapping ever changes.</summary>
    private static async Task<ConfigImportReportEntry> ProcessTableAsync(
        PlannedAction action,
        Dictionary<string, ConfigTable> docByName,
        Dictionary<string, TableDefinition> storedByName,
        Dictionary<string, SourceSchema> worldSourceSchemas,
        Dictionary<string, SourceSchema> worldTableSchemas,
        ICatalogFacade registry,
        bool apply,
        string createdBy)
    {
        if (action.Action == "deleted")
        {
            if (apply && storedByName.TryGetValue(action.Name, out var storedForDelete))
            {
                try
                {
                    if (storedForDelete.Status != PipelineStatus.Stopped)
                    {
                        await registry.SetTableStatusAsync(storedForDelete.Id, PipelineStatus.Stopped);
                    }

                    await registry.DeleteTableAsync(storedForDelete.Id);
                }
                catch (InvalidOperationException ex)
                {
                    return ErrorEntry("table", action.Name, [ex.Message]);
                }
            }

            worldTableSchemas.Remove(action.Name);
            return ToEntry("table", action);
        }

        if (action.Action == "skipped")
        {
            return ToEntry("table", action);
        }

        var docTable = docByName[action.Name];

        // Plan 014 K: an imported document may carry 'INSERT INTO <sink> SELECT …' exactly as the console
        // editor would have saved it, so the strip happens here too — before the compile, since the naked
        // query is what has to compile, and before the apply/validate fork, so "validate" reports an
        // unknown sink target the same way an apply would rather than discovering it at write time. The
        // enable flip lands on the DOCUMENT's sink list, which is what both branches below go on to store
        // (created: copied wholesale; updated: MergeSinkSecrets clones it).
        var sugar = SinkSugar.ApplyTo(docTable.Sql, docTable.Sinks, "table");
        if (sugar.Diagnostics.Count > 0)
        {
            return ErrorEntry("table", action.Name, sugar.Diagnostics);
        }

        if (!Enum.TryParse<TableSearchMode>(docTable.SearchMode, ignoreCase: true, out var searchMode))
        {
            return ErrorEntry("table", action.Name, [$"invalid searchMode: '{docTable.SearchMode}'"]);
        }

        if (!Enum.TryParse<TableHistoryMode>(docTable.HistoryMode, ignoreCase: true, out var historyMode))
        {
            return ErrorEntry("table", action.Name, [$"invalid historyMode: '{docTable.HistoryMode}'"]);
        }

        // D-J: every imported SQL compiles through the real Engine compiler against the composed
        // (post-import-world) catalog; failure -> "error", entity skipped, keep going.
        var compile = SqlCompiler.CompileTable(sugar.Sql, worldSourceSchemas, worldTableSchemas);
        if (!compile.Ok)
        {
            return ErrorEntry("table", action.Name, FormatDiagnostics(compile.Diagnostics));
        }

        // Compiled OK: this table's schema becomes visible to later-in-topo-order tables, whether we're
        // validating (simulation only) or actually applying — TablesEndpoints.CreateTableAsync would
        // derive the identical schema once really applied, since sources/earlier tables are already live
        // in the registry by this point in the plan's apply order.
        worldTableSchemas[action.Name] = compile.OutputSchema!;

        if (!apply)
        {
            return ToEntry("table", action);
        }

        try
        {
            TableDefinition applied;
            if (action.Action == "created")
            {
                var def = new TableDefinition
                {
                    Name = docTable.Name,
                    Description = docTable.Description,
                    Sql = sugar.Sql,
                    CreatedBy = createdBy,
                    SearchEnabled = docTable.SearchEnabled,
                    SearchMode = searchMode,
                    HistoryEnabled = docTable.HistoryEnabled,
                    HistoryMode = historyMode,
                    HistoryLimit = docTable.HistoryLimit,
                    HistoryByField = docTable.HistoryByField,
                    HistoryWindowMs = docTable.HistoryWindowMs,
                    RetentionMaxRows = docTable.RetentionMaxRows,
                    RetentionTtlMs = docTable.RetentionTtlMs,
                    ShardBy = [.. docTable.ShardBy],
                    Tags = [.. docTable.Tags],
                    // Plan 016 wave 3-C: found live by wave 3-B and reproduced by it on a running
                    // instance — this create path built a TableDefinition without the document's pins,
                    // so an imported `dependsOn` silently vanished (200, entity reads back `[]`, and
                    // Revision could never reflect the pin because the record the registry compared
                    // never carried it). Same "whole replace" convention as Tags/ShardBy immediately
                    // above: the document is the desired state, not a patch.
                    DependsOn = [.. docTable.DependsOn],
                    Metadata = new Dictionary<string, string>(docTable.Metadata),
                    Parallelism = docTable.Parallelism,
                    // Plan 009 B2: a freshly-created table has no stored entity to merge secrets
                    // against — same "nothing to keep" rule as a source's own create path.
                    Sinks = [.. docTable.Sinks],
                };
                applied = await registry.CreateTableAsync(def);
            }
            else
            {
                var existing = storedByName[action.Name];
                existing.Description = docTable.Description;
                existing.Sql = sugar.Sql;
                existing.SearchEnabled = docTable.SearchEnabled;
                existing.SearchMode = searchMode;
                existing.HistoryEnabled = docTable.HistoryEnabled;
                existing.HistoryMode = historyMode;
                existing.HistoryLimit = docTable.HistoryLimit;
                existing.HistoryByField = docTable.HistoryByField;
                existing.HistoryWindowMs = docTable.HistoryWindowMs;
                existing.RetentionMaxRows = docTable.RetentionMaxRows;
                existing.RetentionTtlMs = docTable.RetentionTtlMs;
                existing.ShardBy = [.. docTable.ShardBy];
                existing.Tags = [.. docTable.Tags];
                // Plan 016 wave 3-C: the update-path twin of the create-path fix above — same bug,
                // same fix, same reasoning.
                existing.DependsOn = [.. docTable.DependsOn];
                existing.Metadata = new Dictionary<string, string>(docTable.Metadata);
                existing.Parallelism = docTable.Parallelism;
                // Plan 009 B2: D-J — SecretsMasker.MergeSinkSecrets(incoming, storedIfAny), the Sinks
                // counterpart of ProcessSourceAsync's SecretsMasker.MergeSecrets call. NOTE (Orleans
                // flavor only): RegistryGrain.UpdateTableAsync's field-copy list doesn't yet include
                // Sinks (see PipelinesEndpoints'/TablesEndpoints' PUT handlers for the same reported,
                // out-of-ownership gap) — this assignment is correct and takes effect once that grain
                // method is fixed; today it's a no-op on Orleans for the "updated" case specifically
                // (the "created" case above is unaffected on both flavors).
                existing.Sinks = SecretsMasker.MergeSinkSecrets(docTable.Sinks, existing.Sinks);

                var updated = await registry.UpdateTableAsync(existing);
                if (updated is null)
                {
                    return ErrorEntry("table", action.Name, ["table vanished from the catalog mid-import"]);
                }

                applied = updated;
            }

            var desired = docTable.Running ? PipelineStatus.Running : PipelineStatus.Stopped;
            if (applied.Status != desired)
            {
                await registry.SetTableStatusAsync(applied.Id, desired);
            }
        }
        catch (InvalidOperationException ex)
        {
            return ErrorEntry("table", action.Name, [ex.Message]);
        }

        return ToEntry("table", action);
    }

    private static async Task<ConfigImportReportEntry> ProcessPipelineAsync(
        PlannedAction action,
        Dictionary<string, ConfigPipeline> docByName,
        Dictionary<string, PipelineDefinition> storedByName,
        Dictionary<string, SourceSchema> worldSourceSchemas,
        ICatalogFacade registry,
        bool apply,
        string createdBy)
    {
        if (action.Action == "deleted")
        {
            if (apply && storedByName.TryGetValue(action.Name, out var storedForDelete))
            {
                try
                {
                    if (storedForDelete.Status != PipelineStatus.Stopped)
                    {
                        await registry.SetPipelineStatusAsync(storedForDelete.Id, PipelineStatus.Stopped);
                    }

                    await registry.DeletePipelineAsync(storedForDelete.Id);
                }
                catch (InvalidOperationException ex)
                {
                    return ErrorEntry("pipeline", action.Name, [ex.Message]);
                }
            }

            return ToEntry("pipeline", action);
        }

        if (action.Action == "skipped")
        {
            return ToEntry("pipeline", action);
        }

        var docPipeline = docByName[action.Name];

        // Plan 014 K: see the identical note in ProcessTableAsync — strip the sugar before the compile and
        // before the apply/validate fork.
        var sugar = SinkSugar.ApplyTo(docPipeline.Sql, docPipeline.Sinks, "pipeline");
        if (sugar.Diagnostics.Count > 0)
        {
            return ErrorEntry("pipeline", action.Name, sugar.Diagnostics);
        }

        // Pipelines only ever read from STREAM sources (SqlCompiler.Compile takes source schemas only —
        // see PipelinesEndpoints.BuildSchemasAsync, mirrored by worldSourceSchemas above).
        var compile = SqlCompiler.Compile(sugar.Sql, worldSourceSchemas);
        if (!compile.Ok)
        {
            return ErrorEntry("pipeline", action.Name, FormatDiagnostics(compile.Diagnostics));
        }

        if (!apply)
        {
            return ToEntry("pipeline", action);
        }

        try
        {
            PipelineDefinition applied;
            if (action.Action == "created")
            {
                var def = new PipelineDefinition
                {
                    Name = docPipeline.Name,
                    Description = docPipeline.Description,
                    Sql = sugar.Sql,
                    CreatedBy = createdBy,
                    Tags = [.. docPipeline.Tags],
                    // Plan 016 wave 3-C: see the identical note (and the coordinator-flagged live bug
                    // it fixes) in ProcessTableAsync's create path above.
                    DependsOn = [.. docPipeline.DependsOn],
                    Metadata = new Dictionary<string, string>(docPipeline.Metadata),
                    // Plan 009 B2: see the identical note in ProcessTableAsync's create path.
                    Sinks = [.. docPipeline.Sinks],
                };
                applied = await registry.CreatePipelineAsync(def);
            }
            else
            {
                var existing = storedByName[action.Name];
                existing.Description = docPipeline.Description;
                existing.Sql = sugar.Sql;
                existing.Tags = [.. docPipeline.Tags];
                // Plan 016 wave 3-C: the update-path twin of the create-path fix above.
                existing.DependsOn = [.. docPipeline.DependsOn];
                existing.Metadata = new Dictionary<string, string>(docPipeline.Metadata);
                // Plan 009 B2: see the identical note (including the Orleans RegistryGrain gap) in
                // ProcessTableAsync's update path above.
                existing.Sinks = SecretsMasker.MergeSinkSecrets(docPipeline.Sinks, existing.Sinks);

                var updated = await registry.UpdatePipelineAsync(existing);
                if (updated is null)
                {
                    return ErrorEntry("pipeline", action.Name, ["pipeline vanished from the catalog mid-import"]);
                }

                applied = updated;
            }

            var desired = docPipeline.Running ? PipelineStatus.Running : PipelineStatus.Stopped;
            if (applied.Status != desired)
            {
                await registry.SetPipelineStatusAsync(applied.Id, desired);
            }
        }
        catch (InvalidOperationException ex)
        {
            return ErrorEntry("pipeline", action.Name, [ex.Message]);
        }

        return ToEntry("pipeline", action);
    }
}
