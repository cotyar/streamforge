using System.Text.Json.Nodes;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.OpenApi;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.OpenApi;
using StreamForge.Abstractions;
using StreamForge.Engine;
using StreamForge.Host.Grpc.Dynamic;

namespace StreamForge.Api;

/// <summary>
/// <c>GET /api/{tables|pipelines|sources}/{id-or-name}/openapi.json</c> — the interactive-reference twin
/// of the per-entity <c>/proto</c> downloads. Each returns a standalone OpenAPI 3.1 document describing
/// <em>only that entity's</em> REST surface, with the concrete id/name already substituted into every
/// path and the entity's real output schema on the row-shaped payloads. Rendered by the per-entity
/// Scalar pages mapped in <see cref="StreamForgeApiExtensions.MapStreamForgeApi"/>
/// (<c>/scalar/tables/{id}</c> and friends), and linked from the console's API Explorer.
///
/// <para>The document is derived from the application's own generated document — obtained at runtime
/// from <see cref="IOpenApiDocumentProvider"/>, the same source <c>/openapi/v1.json</c> is built from —
/// rather than hand-authored, so it cannot drift from the routes it describes. All the reshaping lives
/// in <see cref="EntityOpenApiLogic"/>.</para>
///
/// <para><b>Reachability note.</b> These routes are anonymous, exactly like <c>/openapi/v1.json</c> that
/// they are carved out of, and unlike the catalog reads they describe. This is forced, not chosen: a
/// Scalar page fetches its document from the browser with no Authorization header (verified against
/// Scalar 2.16's <c>scalar.aspnetcore.js</c>), so a Viewer-gated document is a page that renders 401.
/// The additional exposure over the already-anonymous application document is the entity's output field
/// names and types for an id the caller must already know; the data itself stays Viewer-gated, and every
/// operation in the document still carries the Bearer requirement.</para>
/// </summary>
public static class EntityOpenApiEndpoints
{
    /// <summary>Route segment these documents are served under, and the suffix the console links to.</summary>
    public const string RouteSuffix = "openapi.json";

    public static void MapEntityOpenApiEndpoints(this WebApplication app)
    {
        app.MapGet($"/api/tables/{{id}}/{RouteSuffix}", async (
            string id, ICatalogFacade registry, IServiceProvider services, CancellationToken ct) =>
        {
            var def = await registry.GetTableAsync(id)
                ?? (await registry.GetTablesAsync()).FirstOrDefault(t => t.Name == id);
            if (def is null)
            {
                return Results.NotFound();
            }

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
        }).AllowAnonymous();

        app.MapGet($"/api/pipelines/{{id}}/{RouteSuffix}", async (
            string id, ICatalogFacade registry, IServiceProvider services, CancellationToken ct) =>
        {
            var def = await registry.GetPipelineAsync(id);
            if (def is null)
            {
                var byName = (await registry.GetPipelinesAsync()).Where(p => p.Name == id).ToList();
                if (byName.Count == 1)
                {
                    def = byName[0];
                }
            }

            if (def is null)
            {
                return Results.NotFound();
            }

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
        }).AllowAnonymous();

        app.MapGet($"/api/sources/{{name}}/{RouteSuffix}", async (
            string name, ICatalogFacade registry, IServiceProvider services, CancellationToken ct) =>
        {
            var src = await registry.GetSourceAsync(name);
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
        }).AllowAnonymous();
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
            $"StreamForge {kind} \"{entityName}\"",
            $"Every REST operation of the StreamForge {kind} \"{entityName}\", with its id already " +
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
