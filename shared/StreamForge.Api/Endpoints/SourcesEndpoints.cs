using System.Globalization;
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

        // ---- Client-push ingress (plan 008 W4). POST holds NO policy of its own — every branch below
        // is a 1:1 mapping of IngestOutcome onto an HTTP status per IngestModels.cs's own doc comments;
        // all the actual admission/coercion decisions live in IIngressFacade/AppCore.Ingest. ----

        group.MapPost("/{name}/events", async (string name, IngestEventsRequest req, IIngressFacade ingress, HttpContext http) =>
        {
            var result = await ingress.PushAsync(name, req.Events, req.Partial);
            switch (result.Outcome)
            {
                case IngestOutcome.Accepted:
                    // DepthRows/CapacityRows aren't on IngestResult (that contract is frozen) — a
                    // second, in-memory-only GetStatusAsync call (no extra I/O; same buffer instance)
                    // fills them in for the 202 body.
                    var status = await ingress.GetStatusAsync(name);
                    return Results.Json(
                        new IngestAcceptedResponse(result.Accepted, result.Dropped, result.Invalid, status?.DepthRows ?? 0, status?.CapacityRows ?? 0),
                        statusCode: StatusCodes.Status202Accepted);

                case IngestOutcome.Invalid:
                    return Results.Json(
                        new IngestErrorResponse(result.Error ?? "one or more rows failed coercion", 0, result.RowErrors),
                        statusCode: StatusCodes.Status400BadRequest);

                case IngestOutcome.NotFound:
                    return Results.NotFound();

                case IngestOutcome.WrongKind:
                    return Results.Json(
                        new IngestErrorResponse(result.Error ?? $"source '{name}' is not ingest-kind", 0, result.RowErrors),
                        statusCode: StatusCodes.Status409Conflict);

                case IngestOutcome.TooLarge:
                    return Results.Json(
                        new IngestErrorResponse(result.Error ?? "batch exceeds the source's ingest limits", 0, result.RowErrors),
                        statusCode: StatusCodes.Status413PayloadTooLarge);

                case IngestOutcome.Overloaded:
                default:
                    // Retry-After: whole seconds clamped to [1,30] — IngestResult.RetryAfterMs is the
                    // unclamped estimate (IngressAdmission.Decision doc), this is where it becomes an
                    // honest HTTP header.
                    var retryAfterSeconds = Math.Clamp((int)Math.Ceiling(result.RetryAfterMs / 1000.0), 1, 30);
                    http.Response.Headers.RetryAfter = retryAfterSeconds.ToString(CultureInfo.InvariantCulture);
                    return Results.Json(
                        new IngestErrorResponse(result.Error ?? "ingress buffer has no room for this batch", retryAfterSeconds * 1000, result.RowErrors),
                        statusCode: StatusCodes.Status429TooManyRequests);
            }
        }).RequireAuthorization("Editor");

        // GET /{name}/ingest — ingress buffer status (plan 008 W4). Deliberately NOT overloaded onto
        // /status above, which is the connector-runtime surface; reuses the same three-way
        // NotFound/NoContent/Ok decision (SourceSchemaService.DecideStatusOutcome's bool overload).
        group.MapGet("/{name}/ingest", async (string name, ICatalogFacade registry, IIngressFacade ingress) =>
        {
            var src = await registry.GetSourceAsync(name);
            var status = src is null ? null : await ingress.GetStatusAsync(name);
            return SourceSchemaService.DecideStatusOutcome(src is not null, status is not null) switch
            {
                SourceStatusOutcome.NotFound => Results.NotFound(),
                SourceStatusOutcome.NoContent => Results.NoContent(),
                _ => Results.Ok(ToIngestStatusResponse(status!)),
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

    private static IngestStatusResponse ToIngestStatusResponse(IngestStatus s) => new(
        s.Policy.ToString(),
        s.CapacityRows,
        s.DepthRows,
        s.MaxBatchRows,
        s.TotalAccepted,
        s.TotalRejected,
        s.TotalDropped,
        s.TotalInvalid,
        s.TotalPublished,
        s.DownstreamDropped,
        s.LastPushMs);
}
