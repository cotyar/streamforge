using System.Security.Claims;
using System.Text;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using StreamForge.Abstractions;
using StreamForge.AppCore.Config;
using StreamForge.AppCore.Sinks;
using StreamForge.AppCore.Sql;
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

            // Plan 014 K: 'INSERT INTO <sink> SELECT …' is stripped here, before anything else looks at the
            // SQL — the stored text is the naked query and the named sink is switched on. Unknown target =
            // 400, because a destination that silently goes nowhere is worse than a rejected write. Note the
            // sink list is resolved against BEFORE it is handed to the definition, so the Enabled flip the
            // sugar performs is the one that gets persisted.
            var sinks = req.Sinks ?? [];
            var sugar = SinkSugar.ApplyTo(req.Sql, sinks, "pipeline");
            if (sugar.Diagnostics.Count > 0)
            {
                return Results.BadRequest(new ErrorResponse(string.Join("; ", sugar.Diagnostics)));
            }

            // Wishlist item 13 gap 3: the missing ISinkTransport.Validate call site — see
            // SinkTransports.Validate's own doc for what this does and does not change. Always run on
            // create: 'sinks' is always the request's own (possibly empty) list here, never a
            // carried-over stored one.
            var sinkErrors = new List<string>();
            SinkTransports.Validate(sinks, sinkErrors);
            if (sinkErrors.Count > 0)
            {
                return Results.BadRequest(new ErrorResponse(string.Join("; ", sinkErrors)));
            }

            // Plan 019 D2 (wave 019-B2): the catalog-aware half — a duplex sink whose named source does
            // not exist, or is not a duplex kind, is a validation error too. entityName is null here
            // (pipeline Id does not exist until CreatePipelineAsync mints one below) — see
            // DuplexSinkCatalogValidation.ValidateAsync's own doc for why that means a templated
            // {name} source name is skipped rather than misreported on this specific call site.
            await DuplexSinkCatalogValidation.ValidateAsync(sinks, entityName: null, registry, sinkErrors);
            if (sinkErrors.Count > 0)
            {
                return Results.BadRequest(new ErrorResponse(string.Join("; ", sinkErrors)));
            }

            // Compile-check for diagnostics; draft-friendly — never blocks creation beyond the empty check above.
            var schemas = await BuildSchemasAsync(registry);
            _ = SqlCompiler.Compile(sugar.Sql, schemas);

            var def = new PipelineDefinition
            {
                Name = req.Name,
                Description = req.Description,
                Sql = sugar.Sql,
                CreatedBy = principal.Identity?.Name ?? "",
                Tags = req.Tags ?? [],
                Metadata = req.Metadata ?? [],
                // Plan 014 K: 'sinks', not 'req.Sinks' — same list, but this is the one the sugar just
                // enabled a member of.
                // Plan 009 B2: a freshly-created pipeline has no stored secrets to merge against, so a
                // "***" here (nobody would legitimately send one — the mask value only ever comes back
                // from a prior GET) would just... be a literal wrong credential. Not worth guarding —
                // mirrors sources' own CreateSourceAsync, which does the identical un-merged pass-through.
                Sinks = sinks,
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

            // Plan 009 B2: null Sinks means "leave unchanged" (same convention as Tags/Metadata below);
            // a non-null Sinks that carries "***" (round-tripped from a masked GET) is restored from the
            // currently-stored value via MergeSinkSecrets — the PUT counterpart of sources'
            // SecretsMasker.MergeSecrets, so a GET -> edit-something-else -> PUT cycle never clobbers a
            // real NATS credential with the literal mask string.
            var sinks = req.Sinks is null ? existing.Sinks : SecretsMasker.MergeSinkSecrets(req.Sinks, existing.Sinks);

            // Plan 014 K: AFTER the merge (MergeSinkSecrets deep-CLONES, so a flip applied to req.Sinks
            // would land on a list that is then thrown away) and BEFORE any field of the stored entity is
            // touched, so a rejected sugar target leaves 'existing' exactly as it was found — this is the
            // one handler where the 400 can now happen after several assignments would have run.
            var sugar = SinkSugar.ApplyTo(req.Sql, sinks, "pipeline");
            if (sugar.Diagnostics.Count > 0)
            {
                return Results.BadRequest(new ErrorResponse(string.Join("; ", sugar.Diagnostics)));
            }

            // Wishlist item 13 gap 3: only when the request is actually touching Sinks (req.Sinks is
            // null means "leave unchanged" — see the comment above) so a PUT that edits some unrelated
            // field on a pipeline whose sinks predate this validation, or were saved back before this
            // rule existed, can never be blocked by a sink spec nobody asked to change.
            if (req.Sinks is not null)
            {
                var sinkErrors = new List<string>();
                SinkTransports.Validate(sinks, sinkErrors);
                if (sinkErrors.Count > 0)
                {
                    return Results.BadRequest(new ErrorResponse(string.Join("; ", sinkErrors)));
                }

                // Plan 019 D2 (wave 019-B2): the catalog-aware half. Unlike POST, the pipeline's id is
                // already known here (it predates this PUT) so a templated {name} source name resolves
                // fully — no skip case applies on this call site.
                await DuplexSinkCatalogValidation.ValidateAsync(sinks, existing.Id, registry, sinkErrors);
                if (sinkErrors.Count > 0)
                {
                    return Results.BadRequest(new ErrorResponse(string.Join("; ", sinkErrors)));
                }
            }

            existing.Name = req.Name;
            existing.Description = req.Description;
            existing.Sql = sugar.Sql;
            existing.Tags = req.Tags ?? existing.Tags;
            existing.Metadata = req.Metadata ?? existing.Metadata;
            existing.Sinks = sinks;
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
            // Plan 014 K: the editor validates the same text the save path would store, so the sugar is
            // stripped here too. What this endpoint canNOT check is whether the named sink exists — a
            // ValidateRequest carries SQL and nothing else, and inventing a sinks field on it to make the
            // check possible would put a whole entity in a request whose entire job is "does this text
            // compile". The unknown-target 400 stays where the entity is: POST/PUT.
            var sugar = SinkSugar.Desugar(req.Sql);
            if (sugar.Diagnostics.Count > 0)
            {
                return Results.Ok(new ValidateResponse(
                    false,
                    // Line/column 1: the sugar only ever matches at the start of the statement, so that is
                    // literally where every diagnostic it produces is anchored.
                    [.. sugar.Diagnostics.Select(d => new SqlDiagnosticDto(d, 1, 1, nameof(DiagnosticSeverity.Error)))],
                    null,
                    []));
            }

            var schemas = await BuildSchemasAsync(registry);
            var result = SqlCompiler.Compile(sugar.Sql, schemas);
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
