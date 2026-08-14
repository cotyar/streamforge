using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Http;
using StreamForge.Abstractions;
using StreamForge.AppCore.Config;
using StreamForge.AppCore.Sql;
using StreamForge.Engine;

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

        return new ConfigImportReport
        {
            Mode = mode,
            Entries = entries,
            Ok = entries.All(e => e.Action != "error"),
        };
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
