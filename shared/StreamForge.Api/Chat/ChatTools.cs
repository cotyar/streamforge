using System.Security.Claims;
using System.Text.Json;
using System.Text.Json.Nodes;
using StreamForge.Abstractions;
using StreamForge.AppCore.Config;
using StreamForge.Engine;
using StreamForge.Host.Grains;

namespace StreamForge.Api;

/// <summary>
/// Plan 007 W1C, decision D-D: everything POST /api/chat's tool loop needs to execute one Gemini
/// functionCall against the EXISTING facades — mirrors SourcesEndpoints/PipelinesEndpoints/
/// TablesEndpoints semantics exactly (validation, secrets masking, id-or-name resolution) rather than
/// inventing new behavior. Only "generator"-kind sources are creatable/editable through chat
/// (connector-kind sources — url/file/folder/grpc — stay a Sources-UI-only concern; the chat surface
/// never touches ConnectorConfig).
/// </summary>
internal sealed record ChatToolContext(ICatalogFacade Catalog, ITableReadFacade Tables, ITableHistoryFacade History, ClaimsPrincipal Principal);

internal static class ChatToolCatalog
{
    /// <summary>Tool name -> one-line semantics, for the final wave report / docs; also doubles as
    /// each GeminiFunctionDeclaration.Description.</summary>
    public static readonly IReadOnlyDictionary<string, string> Descriptions = new Dictionary<string, string>
    {
        ["list_sources"] = "List every stream source (masked secrets) with its kind, enabled flag, and rate.",
        ["get_source"] = "Get one source's full definition (masked secrets) by exact name.",
        ["create_source"] = "Create a new GENERATOR-kind source (synthetic event stream). Connector-kind sources aren't supported via chat.",
        ["update_source"] = "Update an existing source's description/fields/generator settings/tags (name is immutable).",
        ["pause_source"] = "Disable a source (stops its generator/connector without deleting it).",
        ["resume_source"] = "Re-enable a previously paused source.",
        ["delete_source"] = "Permanently delete a source. DESTRUCTIVE — requires confirmed=true, set only after the user explicitly confirms in this conversation.",
        ["list_pipelines"] = "List every streaming-SQL pipeline with its status.",
        ["get_pipeline"] = "Get one pipeline's full definition by id or exact name.",
        ["create_pipeline"] = "Create a new streaming-SQL pipeline (draft-friendly: created even if the SQL doesn't currently compile; diagnostics are returned).",
        ["validate_sql"] = "Compile-check a streaming-SQL pipeline query without creating anything; returns diagnostics and a plan summary.",
        ["list_tables"] = "List every materialized table with its status.",
        ["get_table"] = "Get one table's full definition (including compiled output schema) by id or exact name.",
        ["table_rows"] = "Read a table's current rows (limit default 20, capped at 100).",
        ["search_table"] = "Full-text/fuzzy search a table's rows (only works if the table has search enabled).",
        ["table_history"] = "Look up a row's version history by its identity/GROUP BY column values (only works if the table has row history enabled).",
    }.AsReadOnly();

    public static readonly IReadOnlyList<GeminiFunctionDeclaration> Declarations = BuildDeclarations();

    private static List<GeminiFunctionDeclaration> BuildDeclarations()
    {
        var schemas = new Dictionary<string, string>
        {
            ["list_sources"] = """{"type":"object","properties":{}}""",
            ["get_source"] = """{"type":"object","properties":{"name":{"type":"string","description":"exact source name"}},"required":["name"]}""",
            ["create_source"] = """
                {
                  "type": "object",
                  "properties": {
                    "name": {"type":"string","description":"unique source name"},
                    "description": {"type":"string"},
                    "fields": {
                      "type":"array",
                      "description":"schema fields for this source",
                      "items": {
                        "type":"object",
                        "properties": {
                          "name": {"type":"string"},
                          "type": {"type":"string","enum":["String","Double","Long","Bool","Timestamp","Json"]},
                          "isArray": {"type":"boolean","description":"true if this field holds a JSON array"}
                        },
                        "required": ["name","type"]
                      }
                    },
                    "generatorProfile": {"type":"string","enum":["trades","quotes","orders","generic"],"description":"synthetic generator profile, default generic"},
                    "eventsPerSecond": {"type":"number","description":"synthetic event emission rate, must be > 0, default 5"},
                    "enabled": {"type":"boolean","description":"default true"},
                    "tags": {"type":"array","items":{"type":"string"}}
                  },
                  "required": ["name","fields"]
                }
                """,
            ["update_source"] = """
                {
                  "type": "object",
                  "properties": {
                    "name": {"type":"string","description":"exact name of the existing source to update"},
                    "description": {"type":"string"},
                    "fields": {
                      "type":"array",
                      "description":"replaces the source's full field list if provided",
                      "items": {
                        "type":"object",
                        "properties": {
                          "name": {"type":"string"},
                          "type": {"type":"string","enum":["String","Double","Long","Bool","Timestamp","Json"]},
                          "isArray": {"type":"boolean"}
                        },
                        "required": ["name","type"]
                      }
                    },
                    "generatorProfile": {"type":"string","enum":["trades","quotes","orders","generic"]},
                    "eventsPerSecond": {"type":"number"},
                    "enabled": {"type":"boolean"},
                    "tags": {"type":"array","items":{"type":"string"}}
                  },
                  "required": ["name"]
                }
                """,
            ["pause_source"] = """{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}""",
            ["resume_source"] = """{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}""",
            ["delete_source"] = """
                {
                  "type":"object",
                  "properties": {
                    "name": {"type":"string"},
                    "confirmed": {"type":"boolean","description":"set true only after the user has explicitly confirmed deleting this exact source in this conversation"}
                  },
                  "required": ["name","confirmed"]
                }
                """,
            ["list_pipelines"] = """{"type":"object","properties":{}}""",
            ["get_pipeline"] = """{"type":"object","properties":{"id":{"type":"string","description":"pipeline id, or its exact name if unambiguous"}},"required":["id"]}""",
            ["create_pipeline"] = """
                {
                  "type":"object",
                  "properties": {
                    "name": {"type":"string"},
                    "sql": {"type":"string","description":"streaming-SQL pipeline query (PIPELINE mode — windowed aggregates)"},
                    "description": {"type":"string"},
                    "tags": {"type":"array","items":{"type":"string"}}
                  },
                  "required": ["name","sql"]
                }
                """,
            ["validate_sql"] = """{"type":"object","properties":{"sql":{"type":"string"}},"required":["sql"]}""",
            ["list_tables"] = """{"type":"object","properties":{}}""",
            ["get_table"] = """{"type":"object","properties":{"table":{"type":"string","description":"table id, or its exact name if unambiguous"}},"required":["table"]}""",
            ["table_rows"] = """
                {
                  "type":"object",
                  "properties": {
                    "table": {"type":"string","description":"table id or exact name"},
                    "limit": {"type":"integer","description":"max rows to return, default 20, capped at 100"}
                  },
                  "required": ["table"]
                }
                """,
            ["search_table"] = """
                {
                  "type":"object",
                  "properties": {
                    "table": {"type":"string","description":"table id or exact name"},
                    "query": {"type":"string"},
                    "limit": {"type":"integer","description":"default 20, capped at 100"}
                  },
                  "required": ["table","query"]
                }
                """,
            ["table_history"] = """
                {
                  "type":"object",
                  "properties": {
                    "table": {"type":"string","description":"table id or exact name"},
                    "row": {"type":"object","description":"the row's field values (name: value), containing at least the table's identity/GROUP BY columns, as returned by table_rows or search_table"},
                    "limit": {"type":"integer","description":"max versions to return; 0 or omitted means all retained"}
                  },
                  "required": ["table","row"]
                }
                """,
        };

        return schemas.Select(kv => new GeminiFunctionDeclaration
        {
            Name = kv.Key,
            Description = Descriptions[kv.Key],
            Parameters = JsonDocument.Parse(kv.Value).RootElement.Clone(),
        }).ToList();
    }
}

internal static class ChatToolExecutor
{
    private static readonly JsonSerializerOptions SerializeOptions = new(JsonSerializerDefaults.Web);
    private const int TruncateCapChars = 2000;

    public static async Task<object> ExecuteAsync(string name, JsonElement args, ChatToolContext ctx)
    {
        try
        {
            return name switch
            {
                "list_sources" => await ListSourcesAsync(ctx),
                "get_source" => await GetSourceAsync(args, ctx),
                "create_source" => await CreateSourceAsync(args, ctx),
                "update_source" => await UpdateSourceAsync(args, ctx),
                "pause_source" => await SetSourceEnabledAsync(args, ctx, enabled: false),
                "resume_source" => await SetSourceEnabledAsync(args, ctx, enabled: true),
                "delete_source" => await DeleteSourceAsync(args, ctx),
                "list_pipelines" => await ListPipelinesAsync(ctx),
                "get_pipeline" => await GetPipelineAsync(args, ctx),
                "create_pipeline" => await CreatePipelineAsync(args, ctx),
                "validate_sql" => await ValidateSqlAsync(args, ctx),
                "list_tables" => await ListTablesAsync(ctx),
                "get_table" => await GetTableAsync(args, ctx),
                "table_rows" => await TableRowsAsync(args, ctx),
                "search_table" => await SearchTableAsync(args, ctx),
                "table_history" => await TableHistoryAsync(args, ctx),
                _ => new { error = $"unknown tool '{name}'" },
            };
        }
        catch (Exception ex)
        {
            return new { error = $"tool '{name}' failed: {ex.Message}" };
        }
    }

    /// <summary>Serializes <paramref name="result"/>, truncating the JSON to ~2KB (per the wave
    /// spec's ChatToolCallDto contract) before parsing it back into a self-contained (cloned)
    /// JsonElement.</summary>
    public static JsonElement TruncateToElement(object? result)
    {
        var json = JsonSerializer.Serialize(result, SerializeOptions);
        if (json.Length <= TruncateCapChars)
        {
            return JsonDocument.Parse(json).RootElement.Clone();
        }

        var preview = new { truncated = true, originalLength = json.Length, preview = json[..TruncateCapChars] };
        return JsonDocument.Parse(JsonSerializer.Serialize(preview, SerializeOptions)).RootElement.Clone();
    }

    /// <summary>Gemini requires a functionResponse's "response" to be a JSON OBJECT — wraps a
    /// non-object result (array/scalar) as {"result": ...}, leaving an already-object result as-is.</summary>
    public static JsonElement WrapAsResponseObject(JsonElement value)
    {
        if (value.ValueKind == JsonValueKind.Object)
        {
            return value;
        }

        var wrapper = new JsonObject { ["result"] = JsonNode.Parse(value.GetRawText()) };
        return JsonDocument.Parse(wrapper.ToJsonString()).RootElement.Clone();
    }

    // ------------------------------------------------------------------
    // Sources — mirrors SourcesEndpoints.cs (SourceValidation + SecretsMasker, generator-kind only).
    // ------------------------------------------------------------------

    private static async Task<object> ListSourcesAsync(ChatToolContext ctx)
    {
        var sources = await ctx.Catalog.GetSourcesAsync();
        return sources.Select(s => new
        {
            name = s.Name,
            description = s.Description,
            kind = s.Kind,
            enabled = s.Enabled,
            eventsPerSecond = s.EventsPerSecond,
            generatorProfile = s.GeneratorProfile,
            fieldCount = s.Fields.Count,
            tags = s.Tags,
        }).ToList();
    }

    private static async Task<object> GetSourceAsync(JsonElement args, ChatToolContext ctx)
    {
        var name = GetString(args, "name");
        if (string.IsNullOrWhiteSpace(name))
        {
            return new { error = "name is required" };
        }

        var src = await ctx.Catalog.GetSourceAsync(name);
        return src is null ? new { error = $"source '{name}' not found" } : SecretsMasker.Mask(src);
    }

    private static async Task<object> CreateSourceAsync(JsonElement args, ChatToolContext ctx)
    {
        var name = GetString(args, "name");
        if (string.IsNullOrWhiteSpace(name))
        {
            return new { error = "name is required" };
        }

        var (fields, fieldsError) = TryParseFields(args);
        if (fieldsError is not null)
        {
            return new { error = fieldsError };
        }

        var def = new SourceDefinition
        {
            Name = name,
            Description = GetString(args, "description") ?? "",
            Fields = fields!,
            Kind = SourceKinds.Generator,
            GeneratorProfile = GetString(args, "generatorProfile") ?? "generic",
            EventsPerSecond = GetDouble(args, "eventsPerSecond", 5),
            Enabled = GetBool(args, "enabled", true),
            Tags = GetStringArray(args, "tags"),
        };

        var errors = SourceValidation.Validate(def);
        if (errors.Count > 0)
        {
            return new { error = string.Join("; ", errors) };
        }

        var existing = await ctx.Catalog.GetSourceAsync(def.Name);
        if (existing is not null)
        {
            return new { error = "source name already exists" };
        }

        var effective = SecretsMasker.MergeSecrets(def, existing);
        await ctx.Catalog.UpsertSourceAsync(effective);
        return SecretsMasker.Mask(effective);
    }

    private static async Task<object> UpdateSourceAsync(JsonElement args, ChatToolContext ctx)
    {
        var name = GetString(args, "name");
        if (string.IsNullOrWhiteSpace(name))
        {
            return new { error = "name is required" };
        }

        var existing = await ctx.Catalog.GetSourceAsync(name);
        if (existing is null)
        {
            return new { error = $"source '{name}' not found" };
        }

        List<FieldDef> fields = existing.Fields;
        if (args.ValueKind == JsonValueKind.Object && args.TryGetProperty("fields", out var fieldsEl) && fieldsEl.ValueKind == JsonValueKind.Array)
        {
            var (parsed, err) = TryParseFields(args);
            if (err is not null)
            {
                return new { error = err };
            }

            fields = parsed!;
        }

        existing.Description = GetString(args, "description") ?? existing.Description;
        existing.Fields = fields;
        existing.GeneratorProfile = GetString(args, "generatorProfile") ?? existing.GeneratorProfile;
        existing.EventsPerSecond = GetDouble(args, "eventsPerSecond", existing.EventsPerSecond);
        existing.Enabled = GetBool(args, "enabled", existing.Enabled);
        if (args.ValueKind == JsonValueKind.Object && args.TryGetProperty("tags", out _))
        {
            existing.Tags = GetStringArray(args, "tags");
        }

        var errors = SourceValidation.Validate(existing);
        if (errors.Count > 0)
        {
            return new { error = string.Join("; ", errors) };
        }

        await ctx.Catalog.UpsertSourceAsync(existing);
        return SecretsMasker.Mask(existing);
    }

    private static async Task<object> SetSourceEnabledAsync(JsonElement args, ChatToolContext ctx, bool enabled)
    {
        var name = GetString(args, "name");
        if (string.IsNullOrWhiteSpace(name))
        {
            return new { error = "name is required" };
        }

        var existing = await ctx.Catalog.GetSourceAsync(name);
        if (existing is null)
        {
            return new { error = $"source '{name}' not found" };
        }

        existing.Enabled = enabled;
        await ctx.Catalog.UpsertSourceAsync(existing);
        return SecretsMasker.Mask(existing);
    }

    private static async Task<object> DeleteSourceAsync(JsonElement args, ChatToolContext ctx)
    {
        var name = GetString(args, "name");
        if (string.IsNullOrWhiteSpace(name))
        {
            return new { error = "name is required" };
        }

        if (!GetBool(args, "confirmed", false))
        {
            return new { error = "This deletes the source permanently. Ask the user to explicitly confirm deleting it, then call delete_source again with confirmed=true." };
        }

        var removed = await ctx.Catalog.DeleteSourceAsync(name);
        return removed ? new { deleted = true, name } : new { error = $"source '{name}' not found" };
    }

    // ------------------------------------------------------------------
    // Pipelines — mirrors PipelinesEndpoints.cs.
    // ------------------------------------------------------------------

    private static async Task<object> ListPipelinesAsync(ChatToolContext ctx)
    {
        var pipelines = await ctx.Catalog.GetPipelinesAsync();
        return pipelines.Select(p => new
        {
            id = p.Id,
            name = p.Name,
            description = p.Description,
            status = p.Status.ToString(),
            error = p.Error,
            tags = p.Tags,
        }).ToList();
    }

    private static async Task<object> GetPipelineAsync(JsonElement args, ChatToolContext ctx)
    {
        var idOrName = GetString(args, "id");
        if (string.IsNullOrWhiteSpace(idOrName))
        {
            return new { error = "id is required" };
        }

        var def = await ResolvePipelineAsync(ctx.Catalog, idOrName);
        return def is null ? new { error = $"pipeline '{idOrName}' not found" } : def;
    }

    private static async Task<object> CreatePipelineAsync(JsonElement args, ChatToolContext ctx)
    {
        var name = GetString(args, "name");
        var sql = GetString(args, "sql");
        if (string.IsNullOrWhiteSpace(name) || string.IsNullOrWhiteSpace(sql))
        {
            return new { error = "name and sql are required" };
        }

        // Draft-friendly, exactly like POST /api/pipelines: compile-check for diagnostics only —
        // never blocks creation.
        var schemas = await BuildPipelineSchemasAsync(ctx.Catalog);
        var compile = SqlCompiler.Compile(sql, schemas);

        var def = new PipelineDefinition
        {
            Name = name,
            Description = GetString(args, "description") ?? "",
            Sql = sql,
            CreatedBy = ctx.Principal.Identity?.Name ?? "",
            Tags = GetStringArray(args, "tags"),
        };
        var created = await ctx.Catalog.CreatePipelineAsync(def);

        return new
        {
            pipeline = created,
            compileOk = compile.Ok,
            diagnostics = compile.Diagnostics.Select(d => new { d.Message, d.Line, d.Column, severity = d.Severity.ToString() }).ToList(),
        };
    }

    private static async Task<object> ValidateSqlAsync(JsonElement args, ChatToolContext ctx)
    {
        var sql = GetString(args, "sql");
        if (string.IsNullOrWhiteSpace(sql))
        {
            return new { error = "sql is required" };
        }

        var schemas = await BuildPipelineSchemasAsync(ctx.Catalog);
        var result = SqlCompiler.Compile(sql, schemas);
        return new
        {
            ok = result.Ok,
            diagnostics = result.Diagnostics.Select(d => new { d.Message, d.Line, d.Column, severity = d.Severity.ToString() }).ToList(),
            planSummary = result.PlanSummary,
            sourceNames = result.SourceNames,
        };
    }

    // ------------------------------------------------------------------
    // Tables — mirrors TablesEndpoints.cs.
    // ------------------------------------------------------------------

    private static async Task<object> ListTablesAsync(ChatToolContext ctx)
    {
        var tables = await ctx.Catalog.GetTablesAsync();
        return tables.Select(t => new
        {
            id = t.Id,
            name = t.Name,
            description = t.Description,
            status = t.Status.ToString(),
            error = t.Error,
            searchEnabled = t.SearchEnabled,
            historyEnabled = t.HistoryEnabled,
            tags = t.Tags,
        }).ToList();
    }

    private static async Task<object> GetTableAsync(JsonElement args, ChatToolContext ctx)
    {
        var idOrName = GetString(args, "table");
        if (string.IsNullOrWhiteSpace(idOrName))
        {
            return new { error = "table is required" };
        }

        var def = await ResolveTableAsync(ctx.Catalog, idOrName);
        return def is null ? new { error = $"table '{idOrName}' not found" } : def;
    }

    private static async Task<object> TableRowsAsync(JsonElement args, ChatToolContext ctx)
    {
        var idOrName = GetString(args, "table");
        if (string.IsNullOrWhiteSpace(idOrName))
        {
            return new { error = "table is required" };
        }

        var def = await ResolveTableAsync(ctx.Catalog, idOrName);
        if (def is null)
        {
            return new { error = $"table '{idOrName}' not found" };
        }

        var limit = Math.Clamp((int)GetDouble(args, "limit", 20), 1, 100);
        var rows = await ctx.Tables.GetRowsAsync(def.Name, limit, 0);
        var total = await ctx.Tables.GetRowCountAsync(def.Name);
        return new { rows, totalRows = total, returned = rows.Count };
    }

    private static async Task<object> SearchTableAsync(JsonElement args, ChatToolContext ctx)
    {
        var idOrName = GetString(args, "table");
        if (string.IsNullOrWhiteSpace(idOrName))
        {
            return new { error = "table is required" };
        }

        var def = await ResolveTableAsync(ctx.Catalog, idOrName);
        if (def is null)
        {
            return new { error = $"table '{idOrName}' not found" };
        }

        if (!def.SearchEnabled)
        {
            return new { error = "search is not enabled for this table" };
        }

        var query = GetString(args, "query");
        if (string.IsNullOrWhiteSpace(query))
        {
            return new { error = "query is required" };
        }

        var limit = Math.Clamp((int)GetDouble(args, "limit", 20), 1, 100);
        var rows = await ctx.Tables.SearchAsync(def.Name, query, limit);
        return new { rows, mode = def.SearchMode.ToString(), returned = rows.Count };
    }

    private static async Task<object> TableHistoryAsync(JsonElement args, ChatToolContext ctx)
    {
        var idOrName = GetString(args, "table");
        if (string.IsNullOrWhiteSpace(idOrName))
        {
            return new { error = "table is required" };
        }

        var def = await ResolveTableAsync(ctx.Catalog, idOrName);
        if (def is null)
        {
            return new { error = $"table '{idOrName}' not found" };
        }

        if (!def.HistoryEnabled)
        {
            return new { error = "row history is not enabled for this table" };
        }

        if (args.ValueKind != JsonValueKind.Object || !args.TryGetProperty("row", out var rowEl) || rowEl.ValueKind != JsonValueKind.Object)
        {
            return new { error = "row (object) is required — the row's field values, containing at least the table's identity columns" };
        }

        var row = JsonSerializer.Deserialize<Dictionary<string, object?>>(rowEl.GetRawText()) ?? [];
        var identityColumns = TableGroupKeyExtractor.ExtractIdentityColumns(def.Sql);
        var key = RowKeyCodec.EncodeIdentity(row, identityColumns);
        var limit = (int)GetDouble(args, "limit", 0);
        return await ctx.History.GetHistoryAsync(def.Name, key, limit);
    }

    // ------------------------------------------------------------------
    // Shared helpers.
    // ------------------------------------------------------------------

    private static async Task<PipelineDefinition?> ResolvePipelineAsync(ICatalogFacade catalog, string idOrName)
    {
        var byId = await catalog.GetPipelineAsync(idOrName);
        if (byId is not null)
        {
            return byId;
        }

        var matches = (await catalog.GetPipelinesAsync()).Where(p => p.Name == idOrName).ToList();
        return matches.Count == 1 ? matches[0] : null;
    }

    private static async Task<TableDefinition?> ResolveTableAsync(ICatalogFacade catalog, string idOrName)
    {
        var byId = await catalog.GetTableAsync(idOrName);
        if (byId is not null)
        {
            return byId;
        }

        var matches = (await catalog.GetTablesAsync()).Where(t => t.Name == idOrName).ToList();
        return matches.Count == 1 ? matches[0] : null;
    }

    private static async Task<Dictionary<string, SourceSchema>> BuildPipelineSchemasAsync(ICatalogFacade catalog)
    {
        var sources = await catalog.GetSourcesAsync();
        var schemas = new Dictionary<string, SourceSchema>();
        foreach (var src in sources)
        {
            var fields = src.Fields.ToDictionary(f => f.Name, f => MapFieldKind(f.Type));
            schemas[src.Name] = new SourceSchema(src.Name, fields);
        }

        return schemas;
    }

    private static FieldKind MapFieldKind(FieldType type) => type switch
    {
        FieldType.String => FieldKind.String,
        FieldType.Double => FieldKind.Double,
        FieldType.Long => FieldKind.Long,
        FieldType.Bool => FieldKind.Bool,
        FieldType.Timestamp => FieldKind.Timestamp,
        FieldType.Json => FieldKind.Json,
        _ => throw new ArgumentOutOfRangeException(nameof(type), type, "Unknown field type"),
    };

    private static (List<FieldDef>? Fields, string? Error) TryParseFields(JsonElement args)
    {
        if (args.ValueKind != JsonValueKind.Object || !args.TryGetProperty("fields", out var arr) || arr.ValueKind != JsonValueKind.Array)
        {
            return (null, "fields (array) is required");
        }

        var result = new List<FieldDef>();
        foreach (var item in arr.EnumerateArray())
        {
            if (item.ValueKind != JsonValueKind.Object ||
                !item.TryGetProperty("name", out var nameEl) || nameEl.ValueKind != JsonValueKind.String || string.IsNullOrWhiteSpace(nameEl.GetString()))
            {
                return (null, "each field requires a non-empty 'name'");
            }

            if (!item.TryGetProperty("type", out var typeEl) || typeEl.ValueKind != JsonValueKind.String ||
                !Enum.TryParse<FieldType>(typeEl.GetString(), ignoreCase: true, out var fieldType))
            {
                return (null, $"field '{nameEl.GetString()}' has an invalid or missing 'type' (expected one of String, Double, Long, Bool, Timestamp, Json)");
            }

            var isArray = item.TryGetProperty("isArray", out var isArrayEl) && isArrayEl.ValueKind == JsonValueKind.True;
            result.Add(new FieldDef(nameEl.GetString()!, fieldType, null, isArray));
        }

        if (result.Count == 0)
        {
            return (null, "at least one field is required");
        }

        return (result, null);
    }

    private static string? GetString(JsonElement args, string prop) =>
        args.ValueKind == JsonValueKind.Object && args.TryGetProperty(prop, out var v) && v.ValueKind == JsonValueKind.String
            ? v.GetString()
            : null;

    private static double GetDouble(JsonElement args, string prop, double fallback) =>
        args.ValueKind == JsonValueKind.Object && args.TryGetProperty(prop, out var v) && v.ValueKind == JsonValueKind.Number
            ? v.GetDouble()
            : fallback;

    private static bool GetBool(JsonElement args, string prop, bool fallback)
    {
        if (args.ValueKind != JsonValueKind.Object || !args.TryGetProperty(prop, out var v))
        {
            return fallback;
        }

        return v.ValueKind switch
        {
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            _ => fallback,
        };
    }

    private static List<string> GetStringArray(JsonElement args, string prop)
    {
        if (args.ValueKind != JsonValueKind.Object || !args.TryGetProperty(prop, out var v) || v.ValueKind != JsonValueKind.Array)
        {
            return [];
        }

        return v.EnumerateArray().Where(e => e.ValueKind == JsonValueKind.String).Select(e => e.GetString()!).ToList();
    }
}
