using System.Security.Claims;
using Orleans;
using StreamForge.Abstractions;
using StreamForge.Engine;

namespace StreamForge.Host.Api;

public static class TablesEndpoints
{
    public static void MapTablesEndpoints(this WebApplication app)
    {
        var group = app.MapGroup("/api/tables");

        group.MapGet("/", async (IClusterClient client) =>
            Results.Ok(await Registry(client).GetTablesAsync())
        ).RequireAuthorization("Viewer");

        group.MapGet("/{id}", async (string id, IClusterClient client) =>
        {
            var t = await Registry(client).GetTableAsync(id);
            return t is null ? Results.NotFound() : Results.Ok(t);
        }).RequireAuthorization("Viewer");

        group.MapPost("/", async (CreateTableRequest req, ClaimsPrincipal principal, IClusterClient client) =>
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
                };
                var created = await Registry(client).CreateTableAsync(def);
                return Results.Created($"/api/tables/{created.Id}", created);
            }
            catch (InvalidOperationException ex)
            {
                return Results.Conflict(new ErrorResponse(ex.Message));
            }
        }).RequireAuthorization("Editor");

        group.MapPut("/{id}", async (string id, CreateTableRequest req, IClusterClient client) =>
        {
            var registry = Registry(client);
            var existing = await registry.GetTableAsync(id);
            if (existing is null)
            {
                return Results.NotFound();
            }

            existing.Name = req.Name;
            existing.Description = req.Description;
            existing.Sql = req.Sql;

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

        group.MapDelete("/{id}", async (string id, IClusterClient client) =>
        {
            try
            {
                var removed = await Registry(client).DeleteTableAsync(id);
                return removed ? Results.NoContent() : Results.NotFound();
            }
            catch (InvalidOperationException ex)
            {
                return Results.Conflict(new ErrorResponse(ex.Message));
            }
        }).RequireAuthorization("Editor");

        group.MapPost("/{id}/start", async (string id, IClusterClient client) =>
        {
            try
            {
                var updated = await Registry(client).SetTableStatusAsync(id, PipelineStatus.Running);
                return updated is null ? Results.NotFound() : Results.Ok(updated);
            }
            catch (InvalidOperationException ex)
            {
                return Results.Conflict(new ErrorResponse(ex.Message));
            }
        }).RequireAuthorization("Editor");

        group.MapPost("/{id}/stop", async (string id, IClusterClient client) =>
        {
            try
            {
                var updated = await Registry(client).SetTableStatusAsync(id, PipelineStatus.Stopped);
                return updated is null ? Results.NotFound() : Results.Ok(updated);
            }
            catch (InvalidOperationException ex)
            {
                return Results.Conflict(new ErrorResponse(ex.Message));
            }
        }).RequireAuthorization("Editor");

        group.MapPost("/validate", async (ValidateRequest req, IClusterClient client) =>
        {
            var registry = Registry(client);
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

        group.MapGet("/{id}/rows", async (string id, int? limit, int? offset, IClusterClient client) =>
        {
            var def = await Registry(client).GetTableAsync(id);
            if (def is null)
            {
                return Results.NotFound();
            }

            var grain = client.GetGrain<ITableGrain>(def.Name);
            var rows = await grain.GetRowsAsync(limit ?? 100, offset ?? 0);
            var total = await grain.GetRowCountAsync();
            var seq = await grain.GetSeqAsync();
            return Results.Ok(new TableRowsResponse(rows, total, seq));
        }).RequireAuthorization("Viewer");

        group.MapGet("/{id}/metrics", async (string id, IClusterClient client) =>
        {
            var def = await Registry(client).GetTableAsync(id);
            if (def is null)
            {
                return Results.NotFound();
            }

            return Results.Ok(await client.GetGrain<ITableGrain>(def.Name).GetMetricsAsync());
        }).RequireAuthorization("Viewer");
    }

    private static IRegistryGrain Registry(IClusterClient client) =>
        client.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);

    private static async Task<Dictionary<string, SourceSchema>> BuildStreamSchemasAsync(IRegistryGrain registry)
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

    private static async Task<Dictionary<string, SourceSchema>> BuildTableSchemasAsync(IRegistryGrain registry)
    {
        var tables = await registry.GetTablesAsync();
        var schemas = new Dictionary<string, SourceSchema>();
        foreach (var t in tables.Where(t => t.OutputFields.Count > 0))
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
