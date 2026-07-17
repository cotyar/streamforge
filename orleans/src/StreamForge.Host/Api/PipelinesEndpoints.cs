using System.Security.Claims;
using System.Text;
using Orleans;
using StreamForge.Abstractions;
using StreamForge.Engine;
using StreamForge.Host.Grpc.Dynamic;

namespace StreamForge.Host.Api;

public static class PipelinesEndpoints
{
    public static void MapPipelinesEndpoints(this WebApplication app)
    {
        var group = app.MapGroup("/api/pipelines");

        group.MapGet("/", async (IClusterClient client) =>
            Results.Ok(await Registry(client).GetPipelinesAsync())
        ).RequireAuthorization("Viewer");

        group.MapGet("/{id}", async (string id, IClusterClient client) =>
        {
            var p = await Registry(client).GetPipelineAsync(id);
            return p is null ? Results.NotFound() : Results.Ok(p);
        }).RequireAuthorization("Viewer");

        group.MapPost("/", async (CreatePipelineRequest req, ClaimsPrincipal principal, IClusterClient client) =>
        {
            if (string.IsNullOrWhiteSpace(req.Name) || string.IsNullOrWhiteSpace(req.Sql))
            {
                return Results.BadRequest(new ErrorResponse("name and sql are required"));
            }

            var registry = Registry(client);

            // Compile-check for diagnostics; draft-friendly — never blocks creation beyond the empty check above.
            var schemas = await BuildSchemasAsync(registry);
            _ = SqlCompiler.Compile(req.Sql, schemas);

            var def = new PipelineDefinition
            {
                Name = req.Name,
                Description = req.Description,
                Sql = req.Sql,
                CreatedBy = principal.Identity?.Name ?? "",
                Tags = req.Tags ?? [],
                Metadata = req.Metadata ?? [],
            };
            var created = await registry.CreatePipelineAsync(def);
            return Results.Created($"/api/pipelines/{created.Id}", created);
        }).RequireAuthorization("Editor");

        group.MapPut("/{id}", async (string id, CreatePipelineRequest req, IClusterClient client) =>
        {
            var registry = Registry(client);
            var existing = await registry.GetPipelineAsync(id);
            if (existing is null)
            {
                return Results.NotFound();
            }

            existing.Name = req.Name;
            existing.Description = req.Description;
            existing.Sql = req.Sql;
            existing.Tags = req.Tags ?? existing.Tags;
            existing.Metadata = req.Metadata ?? existing.Metadata;
            var updated = await registry.UpdatePipelineAsync(existing);
            return updated is null ? Results.NotFound() : Results.Ok(updated);
        }).RequireAuthorization("Editor");

        group.MapDelete("/{id}", async (string id, IClusterClient client) =>
        {
            var removed = await Registry(client).DeletePipelineAsync(id);
            return removed ? Results.NoContent() : Results.NotFound();
        }).RequireAuthorization("Editor");

        group.MapPost("/{id}/start", async (string id, IClusterClient client) =>
        {
            var updated = await Registry(client).SetPipelineStatusAsync(id, PipelineStatus.Running);
            return updated is null ? Results.NotFound() : Results.Ok(updated);
        }).RequireAuthorization("Editor");

        group.MapPost("/{id}/stop", async (string id, IClusterClient client) =>
        {
            var updated = await Registry(client).SetPipelineStatusAsync(id, PipelineStatus.Stopped);
            return updated is null ? Results.NotFound() : Results.Ok(updated);
        }).RequireAuthorization("Editor");

        group.MapPost("/validate", async (ValidateRequest req, IClusterClient client) =>
        {
            var schemas = await BuildSchemasAsync(Registry(client));
            var result = SqlCompiler.Compile(req.Sql, schemas);
            return Results.Ok(new ValidateResponse(
                result.Ok,
                result.Diagnostics.Select(d => new SqlDiagnosticDto(d.Message, d.Line, d.Column, d.Severity.ToString())).ToList(),
                result.PlanSummary,
                result.SourceNames.ToList()));
        }).RequireAuthorization("Editor");

        // Downloadable, self-contained .proto for this pipeline: PipelineDefinition doesn't persist an
        // output schema (unlike TableDefinition), so its SQL is recompiled here to get one. 409 with
        // compile diagnostics if the SQL doesn't currently compile.
        group.MapGet("/{id}/proto", async (string id, IClusterClient client) =>
        {
            var registry = Registry(client);
            var def = await registry.GetPipelineAsync(id);
            if (def is null)
            {
                // Name fallback (pipeline names aren't enforced unique — only resolve an unambiguous match).
                var byName = (await registry.GetPipelinesAsync()).Where(p => p.Name == id).ToList();
                if (byName.Count == 1) def = byName[0];
            }
            if (def is null)
            {
                return Results.NotFound();
            }

            var schemas = await BuildSchemasAsync(registry);
            var result = SqlCompiler.Compile(def.Sql, schemas);
            if (!result.Ok || result.OutputSchema is null)
            {
                var message = string.Join("; ", result.Diagnostics.Select(d => $"{d.Line}:{d.Column} {d.Message}"));
                return Results.Conflict(new ErrorResponse(
                    string.IsNullOrEmpty(message) ? "Pipeline SQL does not currently compile." : message));
            }

            var fields = EntitySchemas.FromOutputSchema(result.OutputSchema);
            var numbersJson = await registry.EnsureFieldNumbersAsync(EntitySchemas.PipelineKey(def.Id), fields);
            var numbers = EntitySchemas.ParseMap(numbersJson);
            var schema = DescriptorFactory.Generate(def.Name, fields, numbers);
            var protoText = ProtoFileBuilder.Build("pipeline", def.Name, schema);

            return Results.File(Encoding.UTF8.GetBytes(protoText), "text/plain; charset=utf-8", schema.FileProto.Name);
        }).RequireAuthorization("Viewer");

        group.MapGet("/{id}/results", async (string id, int? limit, IClusterClient client) =>
            Results.Ok(await client.GetGrain<IPipelineGrain>(id).GetRecentResultsAsync(limit ?? 100))
        ).RequireAuthorization("Viewer");

        group.MapGet("/{id}/metrics", async (string id, IClusterClient client) =>
            Results.Ok(await client.GetGrain<IPipelineGrain>(id).GetMetricsAsync())
        ).RequireAuthorization("Viewer");
    }

    private static IRegistryGrain Registry(IClusterClient client) =>
        client.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);

    private static async Task<Dictionary<string, SourceSchema>> BuildSchemasAsync(IRegistryGrain registry)
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
