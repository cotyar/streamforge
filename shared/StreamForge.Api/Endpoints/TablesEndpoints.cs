using System.Security.Claims;
using System.Text;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using StreamForge.Abstractions;
using StreamForge.Engine;
using StreamForge.Host.Grains;
using StreamForge.Host.Grpc.Dynamic;

namespace StreamForge.Api;

public static class TablesEndpoints
{
    public static void MapTablesEndpoints(this WebApplication app, StreamForgeApiOptions options)
    {
        var group = app.MapGroup("/api/tables");

        group.MapGet("/", async (ICatalogFacade registry) =>
            Results.Ok(await registry.GetTablesAsync())
        ).RequireAuthorization("Viewer");

        group.MapGet("/{id}", async (string id, ICatalogFacade registry) =>
        {
            var t = await registry.GetTableAsync(id);
            return t is null ? Results.NotFound() : Results.Ok(t);
        }).RequireAuthorization("Viewer");

        group.MapPost("/", async (CreateTableRequest req, ClaimsPrincipal principal, ICatalogFacade registry) =>
        {
            if (string.IsNullOrWhiteSpace(req.Name) || string.IsNullOrWhiteSpace(req.Sql))
            {
                return Results.BadRequest(new ErrorResponse("name and sql are required"));
            }

            try
            {
                var def = new TableDefinition
                {
                    Name = req.Name,
                    Description = req.Description,
                    Sql = req.Sql,
                    CreatedBy = principal.Identity?.Name ?? "",
                    SearchEnabled = req.SearchEnabled,
                    SearchMode = req.SearchMode,
                    HistoryEnabled = req.HistoryEnabled,
                    HistoryMode = req.HistoryMode,
                    HistoryLimit = req.HistoryLimit,
                    HistoryByField = req.HistoryByField,
                    HistoryWindowMs = req.HistoryWindowMs,
                    Tags = req.Tags ?? [],
                    Metadata = req.Metadata ?? [],
                    Parallelism = req.Parallelism,
                    Persistence = req.Persistence,
                    FlushMs = req.FlushMs,
                    JournalMaxEntries = req.JournalMaxEntries,
                };
                var created = await registry.CreateTableAsync(def);
                return Results.Created($"/api/tables/{created.Id}", created);
            }
            catch (InvalidOperationException ex)
            {
                return Results.Conflict(new ErrorResponse(ex.Message));
            }
        }).RequireAuthorization("Editor");

        group.MapPut("/{id}", async (string id, CreateTableRequest req, ICatalogFacade registry) =>
        {
            var existing = await registry.GetTableAsync(id);
            if (existing is null)
            {
                return Results.NotFound();
            }

            existing.Name = req.Name;
            existing.Description = req.Description;
            existing.Sql = req.Sql;
            existing.SearchEnabled = req.SearchEnabled;
            existing.SearchMode = req.SearchMode;
            existing.HistoryEnabled = req.HistoryEnabled;
            existing.HistoryMode = req.HistoryMode;
            existing.HistoryLimit = req.HistoryLimit;
            existing.HistoryByField = req.HistoryByField;
            existing.HistoryWindowMs = req.HistoryWindowMs;
            existing.Tags = req.Tags ?? existing.Tags;
            existing.Metadata = req.Metadata ?? existing.Metadata;
            existing.Parallelism = req.Parallelism;
            existing.Persistence = req.Persistence;
            existing.FlushMs = req.FlushMs;
            existing.JournalMaxEntries = req.JournalMaxEntries;

            try
            {
                var updated = await registry.UpdateTableAsync(existing);
                return updated is null ? Results.NotFound() : Results.Ok(updated);
            }
            catch (InvalidOperationException ex)
            {
                return Results.Conflict(new ErrorResponse(ex.Message));
            }
        }).RequireAuthorization("Editor");

        group.MapDelete("/{id}", async (string id, ICatalogFacade registry) =>
        {
            try
            {
                var removed = await registry.DeleteTableAsync(id);
                return removed ? Results.NoContent() : Results.NotFound();
            }
            catch (InvalidOperationException ex)
            {
                return Results.Conflict(new ErrorResponse(ex.Message));
            }
        }).RequireAuthorization("Editor");

        group.MapPost("/{id}/start", async (string id, ICatalogFacade registry) =>
        {
            try
            {
                var updated = await registry.SetTableStatusAsync(id, PipelineStatus.Running);
                return updated is null ? Results.NotFound() : Results.Ok(updated);
            }
            catch (InvalidOperationException ex)
            {
                return Results.Conflict(new ErrorResponse(ex.Message));
            }
        }).RequireAuthorization("Editor");

        group.MapPost("/{id}/stop", async (string id, ICatalogFacade registry) =>
        {
            try
            {
                var updated = await registry.SetTableStatusAsync(id, PipelineStatus.Stopped);
                return updated is null ? Results.NotFound() : Results.Ok(updated);
            }
            catch (InvalidOperationException ex)
            {
                return Results.Conflict(new ErrorResponse(ex.Message));
            }
        }).RequireAuthorization("Editor");

        group.MapPost("/validate", async (ValidateRequest req, ICatalogFacade registry) =>
        {
            var streamSchemas = await BuildStreamSchemasAsync(registry);
            var tableSchemas = await BuildTableSchemasAsync(registry);
            var result = SqlCompiler.CompileTable(req.Sql, streamSchemas, tableSchemas);
            return Results.Ok(new ValidateTableResponse(
                result.Ok,
                result.Diagnostics.Select(d => new SqlDiagnosticDto(d.Message, d.Line, d.Column, d.Severity.ToString())).ToList(),
                result.PlanSummary,
                result.StreamInputs.ToList(),
                result.TableInputs.ToList(),
                result.OutputSchema?.Fields.Select(f => new FieldDefDto(f.Key, f.Value.ToString())).ToList() ?? []));
        }).RequireAuthorization("Editor");

        // Plan 008 W5: lineage + execution-plan view for the console's React Flow page. Physical: true
        // only for a Parallelism >= 2 table on the Orleans flavor whose plan shape supports partitioning
        // (see PlanEndpointsLogic's class doc for the full degradation matrix) — otherwise the logical
        // view (planSummary + inputs) with an explanatory UnavailableReason. Recompiled fresh every call
        // (following OrleansArrangementMetaFacade.GetArrangementsAsync's precedent); never persisted.
        group.MapGet("/{id}/plan", async (string id, ICatalogFacade registry) =>
        {
            var def = await registry.GetTableAsync(id);
            if (def is null)
            {
                return Results.NotFound();
            }

            var streamSchemas = await BuildStreamSchemasAsync(registry);
            var tableSchemas = await BuildTableSchemasAsync(registry, excludeTableId: def.Id);
            return Results.Ok(PlanEndpointsLogic.BuildTablePlan(def, streamSchemas, tableSchemas, options.Flavor));
        }).RequireAuthorization("Viewer");

        group.MapGet("/{id}/rows", async (string id, int? limit, int? offset, ICatalogFacade registry, ITableReadFacade tables) =>
        {
            var def = await registry.GetTableAsync(id);
            if (def is null)
            {
                return Results.NotFound();
            }

            var rows = await tables.GetRowsAsync(def.Name, limit ?? 100, offset ?? 0);
            var total = await tables.GetRowCountAsync(def.Name);
            var seq = await tables.GetSeqAsync(def.Name);
            var frontierEpoch = await tables.GetSnapshotFrontierEpochAsync(def.Name);
            return Results.Ok(new TableRowsResponse(rows, total, seq, frontierEpoch));
        }).RequireAuthorization("Viewer");

        group.MapGet("/{id}/metrics", async (string id, ICatalogFacade registry, ITableReadFacade tables) =>
        {
            var def = await registry.GetTableAsync(id);
            if (def is null)
            {
                return Results.NotFound();
            }

            return Results.Ok(await tables.GetMetricsAsync(def.Name));
        }).RequireAuthorization("Viewer");

        // Downloadable, self-contained .proto for this table: DescriptorFactory's schema (built from
        // the already-compiled TableDefinition.OutputFields, no recompilation needed) plus the
        // DynamicStreamService streaming contract. 409 if the table has never successfully compiled
        // (no OutputFields to describe), mirroring the pipeline endpoint's compile-failure behavior.
        group.MapGet("/{id}/proto", async (string id, ICatalogFacade registry) =>
        {
            var def = await registry.GetTableAsync(id)
                ?? (await registry.GetTablesAsync()).FirstOrDefault(t => t.Name == id);
            if (def is null)
            {
                return Results.NotFound();
            }

            if (def.OutputFields.Count == 0)
            {
                return Results.Conflict(new ErrorResponse(
                    def.Error ?? "Table has no compiled output schema yet — start/re-save the table to compile its SQL."));
            }

            var numbersJson = await registry.EnsureFieldNumbersAsync(EntitySchemas.TableKey(def.Id), def.OutputFields);
            var numbers = EntitySchemas.ParseMap(numbersJson);
            var schema = DescriptorFactory.Generate(def.Name, def.OutputFields, numbers);
            var protoText = ProtoFileBuilder.Build("table", def.Name, schema);

            return Results.File(Encoding.UTF8.GetBytes(protoText), "text/plain; charset=utf-8", schema.FileProto.Name);
        }).RequireAuthorization("Viewer");

        group.MapGet("/{id}/search", async (string id, string? q, int? limit, ICatalogFacade registry, ITableReadFacade tables) =>
        {
            var def = await registry.GetTableAsync(id);
            if (def is null)
            {
                return Results.NotFound();
            }

            if (!def.SearchEnabled)
            {
                return Results.BadRequest(new ErrorResponse("Search is not enabled for this table."));
            }

            var query = q ?? "";
            List<TableRowDto> rows = string.IsNullOrWhiteSpace(query)
                ? []
                : await tables.SearchAsync(def.Name, query, limit ?? 100);
            return Results.Ok(new TableSearchResponse(rows, def.SearchMode.ToString(), def.SearchEnabled, rows.Count));
        }).RequireAuthorization("Viewer");

        // Row history (Feature B). POST (not GET) because the lookup key is the row's own content — an
        // arbitrary-shaped object, awkward to round-trip through a query string — not a resource
        // identifier; mirrors this API's existing "/validate" precedent for a read-only POST-with-body.
        // The server derives the row-identity key from req.Row via TableGroupKeyExtractor/RowKeyCodec, the
        // same way TableHistoryGrain derives it from live deltas, so the client never needs to know the
        // table's GROUP BY identity columns or the key encoding.
        group.MapPost("/{id}/history/lookup", async (string id, HistoryLookupRequest req, int? limit, ICatalogFacade registry, ITableHistoryFacade history) =>
        {
            var def = await registry.GetTableAsync(id);
            if (def is null)
            {
                return Results.NotFound();
            }

            if (!def.HistoryEnabled)
            {
                return Results.BadRequest(new ErrorResponse("Row history is not enabled for this table."));
            }

            var identityColumns = TableGroupKeyExtractor.ExtractIdentityColumns(def.Sql);
            var key = RowKeyCodec.EncodeIdentity(req.Row, identityColumns);
            var result = await history.GetHistoryAsync(def.Name, key, limit ?? 0);
            return Results.Ok(result);
        }).RequireAuthorization("Viewer");

        group.MapGet("/{id}/history/stats", async (string id, ICatalogFacade registry, ITableHistoryFacade history) =>
        {
            var def = await registry.GetTableAsync(id);
            if (def is null)
            {
                return Results.NotFound();
            }

            if (!def.HistoryEnabled)
            {
                return Results.BadRequest(new ErrorResponse("Row history is not enabled for this table."));
            }

            return Results.Ok(await history.GetStatsAsync(def.Name));
        }).RequireAuthorization("Viewer");
    }

    private static async Task<Dictionary<string, SourceSchema>> BuildStreamSchemasAsync(ICatalogFacade registry)
    {
        var sources = await registry.GetSourcesAsync();
        var schemas = new Dictionary<string, SourceSchema>();
        foreach (var src in sources)
        {
            var fields = src.Fields.ToDictionary(f => f.Name, f => MapFieldKind(f.Type));
            schemas[src.Name] = new SourceSchema(src.Name, fields);
        }
        return schemas;
    }

    // excludeTableId (plan 008 W5, additive/optional — default null keeps the pre-existing /validate
    // call site unchanged): the /{id}/plan endpoint recompiles an EXISTING table's SQL, so its own
    // OutputFields shouldn't appear as a candidate FROM/JOIN target for itself — mirrors
    // RegistryGrain.CompileTableSql/BuildTableSchemas's excludeTableId parameter for updates.
    private static async Task<Dictionary<string, SourceSchema>> BuildTableSchemasAsync(ICatalogFacade registry, string? excludeTableId = null)
    {
        var tables = await registry.GetTablesAsync();
        var schemas = new Dictionary<string, SourceSchema>();
        foreach (var t in tables.Where(t => t.Id != excludeTableId && t.OutputFields.Count > 0))
        {
            var fields = t.OutputFields.ToDictionary(f => f.Name, f => MapFieldKind(f.Type));
            schemas[t.Name] = new SourceSchema(t.Name, fields);
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
}
