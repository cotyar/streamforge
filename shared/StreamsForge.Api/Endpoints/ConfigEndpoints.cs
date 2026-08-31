using System.Security.Claims;
using System.Text;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using StreamsForge.Abstractions;
using StreamsForge.Api.Auth;
using StreamsForge.AppCore.Access;
using StreamsForge.AppCore.Config;

namespace StreamsForge.Api;

/// <summary>Config export/import endpoints (plan 006 W3C — decisions D-I/D-J).
/// GET /api/config/export (Viewer) and POST /api/config/import (Editor; replace mode Admin).
/// Handlers stay thin — the composition/apply pipeline lives in <see cref="ConfigImportService"/>,
/// whose pure parts are unit-tested directly (no HTTP-level test harness in this repo; see
/// orleans/tests/StreamsForge.Host.Tests/ConfigEndpointsLogicTests.cs).
///
/// <para><b>Plan 015 wave 3-C.</b> These two routes were the widest pair of holes left in the
/// entitlement model, and for opposite reasons. Import could rewrite the entire catalog behind one
/// coarse <c>Editor</c> gate — including entities the caller has no entitlement to touch. Export could
/// read the entire catalog behind one coarse <c>Viewer</c> gate, which is what a read of everything at
/// once is. Both keep their group-level policy as the compatibility floor (the wave 2-C pattern; the
/// route metadata <c>AuthorizationCoverageTests</c> pins does not move) and both now ask a specific
/// question inside the handler:</para>
/// <list type="bullet">
///   <item><b>export</b> checks <see cref="Actions.ConfigExport"/> at <c>*</c> and then <i>filters</i>
///   what it serialises to what the caller may read, entity by entity.</item>
///   <item><b>import</b> checks <see cref="Actions.ConfigReplace"/> at <c>*</c> — the action the wave
///   2-B legacy-equivalence matrix already assigns to this route, for every mode, not only
///   <c>replace</c> — and then refuses the whole document if it touches anything the caller is not
///   entitled to change. See <see cref="ConfigImportService.FindUnentitledChangesAsync"/> for why the
///   answer is "refuse the whole thing" and not "apply the entitled subset".</item>
/// </list>
///
/// <para><b>Why export filters and import refuses.</b> The asymmetry is deliberate and it is the whole
/// design decision of this half of the wave. A partial <i>read</i> is a coherent thing: "here is the
/// part of the catalog you can see" is an answer with a meaning, it is what every list route in the
/// platform already returns, and nothing is changed by it. A partial <i>write</i> is not: a config
/// document's parts reference each other, so applying half of one applies a document nobody wrote. So
/// the read narrows silently and the write refuses loudly.</para></summary>
public static class ConfigEndpoints
{
    public static void MapConfigEndpoints(this WebApplication app)
    {
        var group = app.MapGroup("/api/config");

        // GET /api/config/export?format=json|yaml&includeSecrets=true|false — Viewer; includeSecrets=true
        // additionally requires Admin (D-H: secrets never leave the process in a plain export).
        group.MapGet("/export", async (string? format, bool? includeSecrets, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry) =>
        {
            var mayExport = await guard.CheckAsync(principal, Actions.ConfigExport, "*");
            if (!mayExport.IsAllowed)
            {
                return AccessGuard.Deny(mayExport);
            }

            var wantSecrets = includeSecrets ?? false;
            // ponytail: still the legacy role check, not an entitlement. Ceiling: "may export" and "may
            // export secrets" cannot be granted apart. Upgrade path is one more Actions constant, and
            // AccessModels.cs is frozen for this wave — inventing `config.export.secrets` in a
            // wave-local type would put a permission name outside the vocabulary every other surface
            // reads, which is worse than the ceiling.
            if (wantSecrets && !principal.IsInRole("Admin"))
            {
                return Results.Json(new ErrorResponse("includeSecrets=true requires the Admin role"), statusCode: StatusCodes.Status403Forbidden);
            }

            // An export is a read of everything at once, so it is filtered by exactly the read
            // entitlements the individual GET routes are gated by — sources by name, pipelines and
            // tables by id, which is what /api/sources/{name} and /api/{pipelines,tables}/{id} are
            // scoped by in the wave 2-B matrix. Under the built-in roles every grant is written at *,
            // so nothing about an unmodified deployment's export changes.
            var sources = await FilterAsync(await registry.GetSourcesAsync(), guard, principal, Actions.SourceRead, s => s.Name, s => s.Tags);
            var pipelines = await FilterAsync(await registry.GetPipelinesAsync(), guard, principal, Actions.PipelineRead, p => p.Id, p => p.Tags);
            var tables = await FilterAsync(await registry.GetTablesAsync(), guard, principal, Actions.TableRead, t => t.Id, t => t.Tags);
            var doc = ConfigSerializer.FromCatalog(sources, pipelines, tables, wantSecrets);

            if (string.Equals(format, "yaml", StringComparison.OrdinalIgnoreCase))
            {
                var yaml = ConfigSerializer.ToYaml(doc);
                return Results.File(Encoding.UTF8.GetBytes(yaml), "application/yaml", "streamsforge-config.yaml");
            }

            var json = ConfigSerializer.ToCanonicalJson(doc);
            return Results.File(Encoding.UTF8.GetBytes(json), "application/json", "streamsforge-config.json");
        }).RequireAuthorization("Viewer");

        // POST /api/config/import?mode=validate|merge|replace — Editor; replace additionally requires
        // Admin. Body form is detected by Content-Type (single doc / ordered JSON array / multipart file
        // set — see ConfigImportService.ReadDocumentAsync). Composition failures are document-level
        // (400); entity-level SQL-compile failures are per-entity "error" report entries (200).
        group.MapPost("/import", async (HttpRequest request, string? mode, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry) =>
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

            // config.replace at *, for every mode. That is the action the legacy-equivalence matrix
            // assigns to POST /api/config/import as a whole, and it is the honest one: the route's
            // capability is "hand me a document and I will reconcile the catalog to it", which is a
            // catalog-wide power even when a particular document happens to touch one entity.
            // Deliberately BEFORE the body is read — an unentitled caller should not get a multipart
            // upload parsed on their behalf.
            var mayImport = await guard.CheckAsync(principal, Actions.ConfigReplace, "*");
            if (!mayImport.IsAllowed)
            {
                return AccessGuard.Deny(mayImport);
            }

            var (doc, diagnostics) = await ConfigImportService.ReadDocumentAsync(request);
            if (doc is null)
            {
                return Results.BadRequest(ConfigImportService.DocumentErrorReport(importMode, diagnostics));
            }

            // …and then the per-entity question, which is the one that matters: config.replace says the
            // caller may run an import, not that they may rewrite prod. All-or-nothing, named entities,
            // nothing applied — the argument is on FindUnentitledChangesAsync.
            var refusals = await ConfigImportService.FindUnentitledChangesAsync(
                doc, importMode, registry, (action, scope, tags) => guard.CheckAsync(principal, action, scope, tags));
            if (refusals.Count > 0)
            {
                return Results.Json(
                    new ErrorResponse(ConfigImportService.UnentitledImportMessage(refusals)),
                    statusCode: StatusCodes.Status403Forbidden);
            }

            var createdBy = principal.Identity?.Name ?? "";
            var report = await ConfigImportService.RunImportAsync(doc, importMode, createdBy, registry, apply: importMode != "validate");
            return Results.Ok(report);
        }).RequireAuthorization("Editor");
    }

    /// <summary>The entities of one kind the caller may read. Sequential on purpose: the guard answers
    /// from an in-memory snapshot with a linear walk over tens of grants, so a catalog-sized loop is
    /// cheaper than the machinery to parallelise it.</summary>
    private static async Task<List<T>> FilterAsync<T>(
        IReadOnlyList<T> items,
        AccessGuard guard,
        ClaimsPrincipal principal,
        string action,
        Func<T, string> scopeOf,
        Func<T, List<string>> tagsOf)
    {
        var kept = new List<T>(items.Count);
        foreach (var item in items)
        {
            // RequiresApproval is not a read: an entitlement that needs a second pair of eyes has not
            // been given yet, so the entity stays out of the export rather than leaking through the one
            // route that hands over everything in a single file.
            if ((await guard.CheckAsync(principal, action, scopeOf(item), tagsOf(item))).IsAllowed)
            {
                kept.Add(item);
            }
        }

        return kept;
    }
}
