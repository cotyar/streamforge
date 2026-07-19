using System.Security.Claims;
using System.Text;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using StreamForge.Abstractions;
using StreamForge.AppCore.Config;

namespace StreamForge.Api;

/// <summary>Config export/import endpoints (plan 006 W3C — decisions D-I/D-J).
/// GET /api/config/export (Viewer) and POST /api/config/import (Editor; replace mode Admin).
/// Handlers stay thin — the composition/apply pipeline lives in <see cref="ConfigImportService"/>,
/// whose pure parts are unit-tested directly (no HTTP-level test harness in this repo; see
/// orleans/tests/StreamForge.Host.Tests/ConfigEndpointsLogicTests.cs).</summary>
public static class ConfigEndpoints
{
    public static void MapConfigEndpoints(this WebApplication app)
    {
        var group = app.MapGroup("/api/config");

        // GET /api/config/export?format=json|yaml&includeSecrets=true|false — Viewer; includeSecrets=true
        // additionally requires Admin (D-H: secrets never leave the process in a plain export).
        group.MapGet("/export", async (string? format, bool? includeSecrets, ClaimsPrincipal principal, ICatalogFacade registry) =>
        {
            var wantSecrets = includeSecrets ?? false;
            if (wantSecrets && !principal.IsInRole("Admin"))
            {
                return Results.Json(new ErrorResponse("includeSecrets=true requires the Admin role"), statusCode: StatusCodes.Status403Forbidden);
            }

            var sources = await registry.GetSourcesAsync();
            var pipelines = await registry.GetPipelinesAsync();
            var tables = await registry.GetTablesAsync();
            var doc = ConfigSerializer.FromCatalog(sources, pipelines, tables, wantSecrets);

            if (string.Equals(format, "yaml", StringComparison.OrdinalIgnoreCase))
            {
                var yaml = ConfigSerializer.ToYaml(doc);
                return Results.File(Encoding.UTF8.GetBytes(yaml), "application/yaml", "streamforge-config.yaml");
            }

            var json = ConfigSerializer.ToCanonicalJson(doc);
            return Results.File(Encoding.UTF8.GetBytes(json), "application/json", "streamforge-config.json");
        }).RequireAuthorization("Viewer");

        // POST /api/config/import?mode=validate|merge|replace — Editor; replace additionally requires
        // Admin. Body form is detected by Content-Type (single doc / ordered JSON array / multipart file
        // set — see ConfigImportService.ReadDocumentAsync). Composition failures are document-level
        // (400); entity-level SQL-compile failures are per-entity "error" report entries (200).
        group.MapPost("/import", async (HttpRequest request, string? mode, ClaimsPrincipal principal, ICatalogFacade registry) =>
        {
            var importMode = string.IsNullOrWhiteSpace(mode) ? "merge" : mode;
            if (importMode is not ("validate" or "merge" or "replace"))
            {
                return Results.BadRequest(new ErrorResponse($"unknown mode '{importMode}' (expected validate|merge|replace)"));
            }

            if (importMode == "replace" && !principal.IsInRole("Admin"))
            {
                return Results.Json(new ErrorResponse("replace mode requires the Admin role"), statusCode: StatusCodes.Status403Forbidden);
            }

            var (doc, diagnostics) = await ConfigImportService.ReadDocumentAsync(request);
            if (doc is null)
            {
                return Results.BadRequest(ConfigImportService.DocumentErrorReport(importMode, diagnostics));
            }

            var createdBy = principal.Identity?.Name ?? "";
            var report = await ConfigImportService.RunImportAsync(doc, importMode, createdBy, registry, apply: importMode != "validate");
            return Results.Ok(report);
        }).RequireAuthorization("Editor");
    }
}
