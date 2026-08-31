using System.Security.Claims;
using System.Text.Json.Nodes;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.OpenApi;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.OpenApi;
using StreamsForge.Abstractions;
using StreamsForge.Api.Auth;
using StreamsForge.Engine;
using StreamsForge.Host.Grpc.Dynamic;

namespace StreamsForge.Api;

/// <summary>
/// <c>GET /api/{tables|pipelines|sources}/{id-or-name}/openapi.json</c> — the interactive-reference twin
/// of the per-entity <c>/proto</c> downloads. Each returns a standalone OpenAPI 3.1 document describing
/// <em>only that entity's</em> REST surface, with the concrete id/name already substituted into every
/// path and the entity's real output schema on the row-shaped payloads. Rendered by the per-entity
/// Scalar pages mapped in <see cref="StreamsForgeApiExtensions.MapStreamsForgeApi"/>
/// (<c>/scalar/tables/{id}</c> and friends), and linked from the console's API Explorer.
///
/// <para>The document is derived from the application's own generated document — obtained at runtime
/// from <see cref="IOpenApiDocumentProvider"/>, the same source <c>/openapi/v1.json</c> is built from —
/// rather than hand-authored, so it cannot drift from the routes it describes. All the reshaping lives
/// in <see cref="EntityOpenApiLogic"/>.</para>
///
/// <para><b>Reachability note.</b> These routes are <c>Viewer</c>-gated, like the catalog reads they
/// describe, and unlike the whole-application <c>/openapi/v1.json</c> they are carved out of. A
/// per-entity document names one entity's output fields and their types, and a source is addressed by
/// name rather than by a GUID, so the set of them is guessable in a way an id is not — not something to
/// serve to the open internet from a deployed instance.</para>
///
/// <para>What makes that gate compatible with the feature is <see cref="Auth.DocsAuthCookie"/>: a Scalar
/// page fetches its document from the browser with a plain <c>fetch</c> and no Authorization header
/// (verified against Scalar 2.16's own bundle), but a plain <c>fetch</c> does send a same-origin cookie,
/// and login issues an httpOnly one carrying the same JWT. The JWT wiring honours that cookie on these
/// paths and on the Scalar pages, and on nothing else — so a signed-in browser renders the page, an
/// anonymous caller gets 401, and no state-changing endpoint has become cookie-reachable. Every
/// operation inside the document still carries the Bearer requirement, which is what "try it" uses.</para>
///
/// <para>Plan 015 wave 3-A: each route keeps its <c>Viewer</c> policy as the compatibility floor and
/// additionally asks <see cref="AccessGuard"/> for the READ action of the entity it describes, at that
/// entity's own name and with its tags — the same question the catalog route next to it asks. A document
/// that named a table's output fields to somebody entitled to read nothing of that table would be the
/// obvious way around the whole wave, and it is reachable from a browser with only the docs cookie, so
/// it is the last route where the gate should be coarse. Note the cookie is a JWT: the principal
/// resolved here is the signed-in user, not an anonymous docs reader.</para>
/// </summary>
public static class EntityOpenApiEndpoints
{
    /// <summary>Route segment these documents are served under, and the suffix the console links to.</summary>
    public const string RouteSuffix = "openapi.json";

    public static void MapEntityOpenApiEndpoints(this WebApplication app)
    {
        app.MapGet($"/api/tables/{{id}}/{RouteSuffix}", async (
            string id, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry,
            IServiceProvider services, CancellationToken ct) =>
        {
            // Plan 016 wave 1: id-or-name, via the one resolver — a duplicate table name used to serve
            // the FIRST match silently here and now answers 409. Guard FIRST, against the raw route
            // segment: an ambiguous result has no single name/tags, and a 409 emitted before the guard
            // would tell a caller entitled to read neither candidate that both exist. This document
            // names an entity's output fields, so it is the last route where that should leak.
            var hit = await EntityLookup.TableAsync(registry, id);
            if (await RefuseAsync(guard, principal, Actions.TableRead, hit.Value?.Name ?? id, hit.Value?.Tags) is { } refusal)
            {
                return refusal;
            }

            if (EntityLookup.Reject(hit) is { } miss)
            {
                return miss;
            }

            var def = hit.Value!;

            return await DocumentAsync(
                services,
                "/api/tables",
                "id",
                id,
                def.Name,
                "table",
                // TableDefinition persists its compiled output schema, so no recompilation is needed —
                // same source the .proto download uses. Empty until the table's SQL has compiled once.
                EntityOpenApiLogic.RowSchemaFromFields(def.OutputFields, $"One row of table \"{def.Name}\"."),
                ct);
        }).RequireAuthorization("Viewer");

        app.MapGet($"/api/pipelines/{{id}}/{RouteSuffix}", async (
            string id, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry,
            IServiceProvider services, CancellationToken ct) =>
        {
            // Plan 016 wave 1: id-or-name, via the one resolver — a duplicate pipeline name used to be
            // reported as 404 here and now answers 409 naming both ids. Guard first; see the table
            // route above for why that order matters.
            var hit = await EntityLookup.PipelineAsync(registry, id);
            if (await RefuseAsync(guard, principal, Actions.PipelineRead, hit.Value?.Name ?? id, hit.Value?.Tags) is { } refusal)
            {
                return refusal;
            }

            if (EntityLookup.Reject(hit) is { } miss)
            {
                return miss;
            }

            var def = hit.Value!;

            // PipelineDefinition doesn't persist an output schema, so recompile for one — but unlike the
            // .proto download, a pipeline whose SQL no longer compiles still gets its document (minus the
            // typed row schema) rather than a 409: the routes are worth documenting either way.
            List<FieldDef>? fields = null;
            var compiled = SqlCompiler.Compile(def.Sql, await PipelinesEndpoints.BuildSchemasAsync(registry));
            if (compiled.Ok && compiled.OutputSchema is not null)
            {
                fields = EntitySchemas.FromOutputSchema(compiled.OutputSchema);
            }

            return await DocumentAsync(
                services,
                "/api/pipelines",
                "id",
                id,
                def.Name,
                "pipeline",
                fields is null
                    ? null
                    : EntityOpenApiLogic.RowSchemaFromFields(fields, $"One output row of pipeline \"{def.Name}\"."),
                ct);
        }).RequireAuthorization("Viewer");

        app.MapGet($"/api/sources/{{name}}/{RouteSuffix}", async (
            string name, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry,
            IServiceProvider services, CancellationToken ct) =>
        {
            var src = await registry.GetSourceAsync(name);
            if (await RefuseAsync(guard, principal, Actions.SourceRead, src?.Name ?? name, src?.Tags) is { } refusal)
            {
                return refusal;
            }

            if (src is null)
            {
                return Results.NotFound();
            }

            return await DocumentAsync(
                services,
                "/api/sources",
                "name",
                name,
                src.Name,
                "source",
                // A source has no REST rows endpoint (its events are SignalR/gRPC only), so this schema
                // lands on the ingest push body — which is exactly where a caller needs it.
                EntityOpenApiLogic.RowSchemaFromFields(src.Fields, $"One event of source \"{src.Name}\"."),
                ct);
        }).RequireAuthorization("Viewer");
    }

    /// <summary>Null when the caller may proceed; the ready-made 403 when they may not — the same helper
    /// <c>AccessEndpoints.RefuseAsync</c> is, with the resource's tags threaded through. A
    /// <see cref="AccessDecision.RequiresApproval"/> answer is refused rather than treated as a yes;
    /// filing the request is waves 4-5's job.</summary>
    private static async Task<IResult?> RefuseAsync(
        AccessGuard guard, ClaimsPrincipal principal, string action, string scope, IReadOnlyCollection<string>? tags = null)
    {
        var result = await guard.CheckAsync(principal, action, scope, tags);
        return result.IsAllowed ? null : AccessGuard.Deny(result);
    }

    private static async Task<IResult> DocumentAsync(
        IServiceProvider services,
        string pathPrefix,
        string parameterName,
        string parameterValue,
        string entityName,
        string kind,
        JsonObject? rowSchema,
        CancellationToken ct)
    {
        // "v1" is the document name AddOpenApi registers by default; the provider is keyed by it.
        var provider = services.GetRequiredKeyedService<IOpenApiDocumentProvider>("v1");
        var appDocument = await provider.GetOpenApiDocumentAsync(ct);
        var json = await appDocument.SerializeAsJsonAsync(OpenApiSpecVersion.OpenApi3_1, ct);

        var document = EntityOpenApiLogic.BuildEntityDocument(
            JsonNode.Parse(json)!,
            pathPrefix,
            parameterName,
            parameterValue,
            $"StreamsForge {kind} \"{entityName}\"",
            $"Every REST operation of the StreamsForge {kind} \"{entityName}\", with its id already " +
            "substituted into each path. Derived from the application document at /openapi/v1.json — " +
            $"see that one for the whole API. Authenticate via POST {EntityOpenApiLogic.LoginPath}, then " +
            "send the returned JWT as a Bearer token.",
            entityName,
            rowSchema,
            rowSchema is null ? null : SchemaName(entityName, kind));

        return Results.Text(document.ToJsonString(), "application/json", System.Text.Encoding.UTF8);
    }

    /// <summary>Component name for the injected row schema. Sanitized to what OpenAPI allows in a
    /// component key, and suffixed by kind so a table and a pipeline of the same name can't collide.</summary>
    private static string SchemaName(string entityName, string kind)
    {
        var safe = new string([.. entityName.Select(c => char.IsLetterOrDigit(c) || c is '_' or '-' or '.' ? c : '_')]);
        return $"{safe}_{kind}_row";
    }
}
