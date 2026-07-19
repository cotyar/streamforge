using System.Text;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using StreamForge.Abstractions;
using StreamForge.AppCore.Config;
using StreamForge.Host.Grpc.Dynamic;

namespace StreamForge.Api;

public static class SourcesEndpoints
{
    public static void MapSourcesEndpoints(this WebApplication app)
    {
        var group = app.MapGroup("/api/sources");

        // D-H: every read path masks secrets (URL header values; gRPC password/token) as "***".
        group.MapGet("/", async (ICatalogFacade registry) =>
            Results.Ok((await registry.GetSourcesAsync()).Select(SecretsMasker.Mask).ToList())
        ).RequireAuthorization("Viewer");

        group.MapGet("/{name}", async (string name, ICatalogFacade registry) =>
        {
            var src = await registry.GetSourceAsync(name);
            return src is null ? Results.NotFound() : Results.Ok(SecretsMasker.Mask(src));
        }).RequireAuthorization("Viewer");

        // Downloadable, self-contained .proto for this source: DescriptorFactory's schema plus the
        // DynamicStreamService streaming contract, ready for a client to compile standalone (see
        // tools/generate-client.sh). Field numbers always come from ICatalogFacade.EnsureFieldNumbersAsync
        // so they stay identical to what the dynamic gRPC reflection surface uses.
        group.MapGet("/{name}/proto", async (string name, ICatalogFacade registry) =>
        {
            var src = await registry.GetSourceAsync(name);
            if (src is null)
            {
                return Results.NotFound();
            }

            var numbersJson = await registry.EnsureFieldNumbersAsync(EntitySchemas.SourceKey(src.Name), src.Fields);
            var numbers = EntitySchemas.ParseMap(numbersJson);
            var schema = DescriptorFactory.Generate(src.Name, src.Fields, numbers);
            var protoText = ProtoFileBuilder.Build("source", src.Name, schema);

            return Results.File(Encoding.UTF8.GetBytes(protoText), "text/plain; charset=utf-8", schema.FileProto.Name);
        }).RequireAuthorization("Viewer");

        group.MapPost("/", async (SourceDefinition def, ICatalogFacade registry) =>
        {
            var errors = SourceValidation.Validate(def);
            if (errors.Count > 0)
            {
                return Results.BadRequest(new ErrorResponse(string.Join("; ", errors)));
            }

            var existing = await registry.GetSourceAsync(def.Name);
            if (existing is not null)
            {
                return Results.BadRequest(new ErrorResponse("source name already exists"));
            }

            // D-H: incoming "***" values have nothing to restore from on a create (existing is
            // null here) — MergeSecrets is still run for compositional symmetry with PUT below
            // (it's a no-op when `stored` is null).
            var effective = SecretsMasker.MergeSecrets(def, existing);
            await registry.UpsertSourceAsync(effective);
            return Results.Created($"/api/sources/{effective.Name}", SecretsMasker.Mask(effective));
        }).RequireAuthorization("Editor");

        group.MapPut("/{name}", async (string name, SourceDefinition def, ICatalogFacade registry) =>
        {
            var existing = await registry.GetSourceAsync(name);
            if (existing is null)
            {
                return Results.NotFound();
            }

            def.Name = name;
            var errors = SourceValidation.Validate(def);
            if (errors.Count > 0)
            {
                return Results.BadRequest(new ErrorResponse(string.Join("; ", errors)));
            }

            // D-H CRITICAL invariant: an incoming "***" (the SPA's GET-then-edit-then-PUT-whole-
            // object cycle round-tripping a masked value verbatim) is replaced with the STORED real
            // secret before it ever reaches UpsertSourceAsync — never persist the literal mask.
            var effective = SecretsMasker.MergeSecrets(def, existing);
            await registry.UpsertSourceAsync(effective);
            return Results.Ok(SecretsMasker.Mask(effective));
        }).RequireAuthorization("Editor");

        group.MapDelete("/{name}", async (string name, ICatalogFacade registry) =>
        {
            var removed = await registry.DeleteSourceAsync(name);
            return removed ? Results.NoContent() : Results.NotFound();
        }).RequireAuthorization("Editor");

        // GET /{name}/status — connector runtime status (D-C). null (no connector status tracked,
        // e.g. generator-kind sources) -> 204; missing source -> 404.
        group.MapGet("/{name}/status", async (string name, ICatalogFacade registry, IConnectorStatusFacade statusFacade) =>
        {
            var src = await registry.GetSourceAsync(name);
            var status = src is null ? null : await statusFacade.GetStatusAsync(name);
            return SourceSchemaService.DecideStatusOutcome(src is not null, status) switch
            {
                SourceStatusOutcome.NotFound => Results.NotFound(),
                SourceStatusOutcome.NoContent => Results.NoContent(),
                _ => Results.Ok(status),
            };
        }).RequireAuthorization("Viewer");

        // ---- Schema helper endpoints for the UI (Editor — even mapping-validate/derive-openapi,
        // which don't themselves dial out, since they're part of the connector-authoring flow that
        // from-remote (which DOES dial out) also belongs to; keeps all three under one auth policy). ----

        group.MapPost("/schema/mapping-validate", (MappingValidateRequest request) =>
            Results.Ok(SourceSchemaService.ValidateMappingDocument(request))
        ).RequireAuthorization("Editor");

        group.MapPost("/schema/derive-openapi", async (SchemaDeriveRequest request, CancellationToken ct) =>
            Results.Ok(await SourceSchemaService.DeriveOpenApiAsync(request, ct))
        ).RequireAuthorization("Editor");

        // Dials out to a remote gRPC endpoint per the request body — Editor-only (D-G).
        group.MapPost("/schema/from-remote", async (RemoteSchemaRequest request, CancellationToken ct) =>
            Results.Ok(await SourceSchemaService.FromRemoteAsync(request, ct))
        ).RequireAuthorization("Editor");
    }
}
