using System.Security.Claims;
using System.Text;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using StreamForge.Abstractions;
using StreamForge.AppCore.Config;
using StreamForge.Engine;
using StreamForge.Host.Grpc.Dynamic;

namespace StreamForge.Api;

public static class PipelinesEndpoints
{
    public static void MapPipelinesEndpoints(this WebApplication app)
    {
        var group = app.MapGroup("/api/pipelines");

        group.MapGet("/", async (ICatalogFacade registry) =>
            Results.Ok((await registry.GetPipelinesAsync()).Select(SecretsMasker.MaskPipeline).ToList())
        ).RequireAuthorization("Viewer");

        group.MapGet("/{id}", async (string id, ICatalogFacade registry) =>
        {
            var p = await registry.GetPipelineAsync(id);
            return p is null ? Results.NotFound() : Results.Ok(SecretsMasker.MaskPipeline(p));
        }).RequireAuthorization("Viewer");

        group.MapPost("/", async (CreatePipelineRequest req, ClaimsPrincipal principal, ICatalogFacade registry) =>
        {
            if (string.IsNullOrWhiteSpace(req.Name) || string.IsNullOrWhiteSpace(req.Sql))
            {
                return Results.BadRequest(new ErrorResponse("name and sql are required"));
            }

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
                // Plan 009 B2: a freshly-created pipeline has no stored secrets to merge against, so a
                // "***" here (nobody would legitimately send one — the mask value only ever comes back
                // from a prior GET) would just... be a literal wrong credential. Not worth guarding —
                // mirrors sources' own CreateSourceAsync, which does the identical un-merged pass-through.
                Sinks = req.Sinks ?? [],
            };
            var created = await registry.CreatePipelineAsync(def);
            return Results.Created($"/api/pipelines/{created.Id}", SecretsMasker.MaskPipeline(created));
        }).RequireAuthorization("Editor");

        group.MapPut("/{id}", async (string id, CreatePipelineRequest req, ICatalogFacade registry) =>
        {
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
            // Plan 009 B2: null Sinks means "leave unchanged" (same convention as Tags/Metadata above);
            // a non-null Sinks that carries "***" (round-tripped from a masked GET) is restored from the
            // currently-stored value via MergeSinkSecrets — the PUT counterpart of sources'
            // SecretsMasker.MergeSecrets, so a GET -> edit-something-else -> PUT cycle never clobbers a
            // real NATS credential with the literal mask string.
            existing.Sinks = req.Sinks is null ? existing.Sinks : SecretsMasker.MergeSinkSecrets(req.Sinks, existing.Sinks);
            var updated = await registry.UpdatePipelineAsync(existing);
            return updated is null ? Results.NotFound() : Results.Ok(SecretsMasker.MaskPipeline(updated));
        }).RequireAuthorization("Editor");

        group.MapDelete("/{id}", async (string id, ICatalogFacade registry) =>
        {
            var removed = await registry.DeletePipelineAsync(id);
            return removed ? Results.NoContent() : Results.NotFound();
        }).RequireAuthorization("Editor");

        group.MapPost("/{id}/start", async (string id, ICatalogFacade registry) =>
        {
            var updated = await registry.SetPipelineStatusAsync(id, PipelineStatus.Running);
            // Plan 009 B2: this handler returns the entity too — same secrets-lite masking as every
            // other read path (found live: this endpoint was leaking an unmasked Sinks credential
            // before this fix, since it returns the entity but isn't a create/update handler).
            return updated is null ? Results.NotFound() : Results.Ok(SecretsMasker.MaskPipeline(updated));
        }).RequireAuthorization("Editor");

        group.MapPost("/{id}/stop", async (string id, ICatalogFacade registry) =>
        {
            var updated = await registry.SetPipelineStatusAsync(id, PipelineStatus.Stopped);
            return updated is null ? Results.NotFound() : Results.Ok(SecretsMasker.MaskPipeline(updated));
        }).RequireAuthorization("Editor");

        group.MapPost("/validate", async (ValidateRequest req, ICatalogFacade registry) =>
        {
            var schemas = await BuildSchemasAsync(registry);
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
        group.MapGet("/{id}/proto", async (string id, ICatalogFacade registry) =>
        {
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

        // Plan 008 W5: lineage + execution-plan view for the console's React Flow page. Always the
        // logical view (Physical: false) — pipelines have no partitioned stage/edge dataflow graph, see
        // PlanEndpointsLogic's class doc. Recompiled fresh every call; never persisted.
        group.MapGet("/{id}/plan", async (string id, ICatalogFacade registry) =>
        {
            var def = await registry.GetPipelineAsync(id);
            if (def is null)
            {
                return Results.NotFound();
            }

            var schemas = await BuildSchemasAsync(registry);
            return Results.Ok(PlanEndpointsLogic.BuildPipelinePlan(def, schemas));
        }).RequireAuthorization("Viewer");

        // .Produces<>() for the same reason as TablesEndpoints' /rows — see the note there.
        group.MapGet("/{id}/results", async (string id, int? limit, IPipelineReadFacade pipelines) =>
            Results.Ok(await pipelines.GetRecentResultsAsync(id, limit ?? 100))
        ).Produces<List<ResultEnvelope>>().RequireAuthorization("Viewer");

        // Plan 012: the recent-results buffer as a file — the pipeline twin of /api/tables/{id}/rows.csv.
        group.MapGet("/{id}/results.csv", async (string id, int? limit, ICatalogFacade registry, IPipelineReadFacade pipelines) =>
        {
            var def = await registry.GetPipelineAsync(id);
            if (def is null)
            {
                return Results.NotFound();
            }

            var results = await pipelines.GetRecentResultsAsync(id, Math.Clamp(limit ?? 10_000, 1, 100_000));
            var csv = CsvExport.Rows(results.Select(r => r.Row));
            return Results.File(Encoding.UTF8.GetBytes(csv), "text/csv; charset=utf-8", $"{def.Name}.csv");
        }).RequireAuthorization("Viewer");

        group.MapGet("/{id}/metrics", async (string id, IPipelineReadFacade pipelines) =>
            Results.Ok(await pipelines.GetMetricsAsync(id))
        ).RequireAuthorization("Viewer");
    }

    internal static async Task<Dictionary<string, SourceSchema>> BuildSchemasAsync(ICatalogFacade registry)
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
