using System.Security.Claims;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using StreamForge.Abstractions;
using StreamForge.Api.Auth;

namespace StreamForge.Api;

/// <summary>
/// Plan 020 wave B-2 — the CRDT document intake route. Follows <c>SourcesEndpoints</c>'s own guard idiom
/// verbatim (that class's doc comment: the <see cref="AccessGuard"/> check runs BEFORE the 404, so an
/// unentitled caller cannot use 403-vs-404 to enumerate source names) and its ingest-keys handlers'
/// "wrong kind is a 409" shape.
///
/// <para><b>Status codes, and why the order is what it is</b> (plan 020's own wording): <b>501</b> when
/// <see cref="ICrdtFacade.Enabled"/> is false — this build has no document runtime at all (the Dapr
/// flavor, D9) — checked FIRST and before any registry lookup, because it is true or false for every
/// source name uniformly and doing a lookup+guard check first would cost a registry round trip to answer
/// a question that doesn't depend on one. Then the usual guard-before-404 ordering; <b>404</b> unknown
/// source; <b>409</b> the source exists but is not <see cref="SourceKinds.Crdt"/> kind; a malformed
/// base64 update is a <b>400</b>, checked last (after authorization and existence — so an invalid body
/// never leaks ahead of those). <b>200</b> otherwise.</para>
/// </summary>
public static class CrdtEndpoints
{
    public static void MapCrdtEndpoints(this WebApplication app)
    {
        var group = app.MapGroup("/api/sources");

        group.MapPost("/{name}/crdt/updates", async (
            string name, CrdtUpdatesRequest req, ClaimsPrincipal principal, AccessGuard guard,
            ICatalogFacade registry, ICrdtFacade crdt) =>
        {
            if (!crdt.Enabled)
            {
                return Results.Json(
                    new ErrorResponse("this build has no CRDT document runtime"),
                    statusCode: StatusCodes.Status501NotImplemented);
            }

            var src = await registry.GetSourceAsync(name);
            if (await RefuseAsync(guard, principal, Actions.SourceWrite, src?.Name ?? name, src?.Tags) is { } refusal)
            {
                return refusal;
            }

            if (src is null)
            {
                return Results.NotFound();
            }

            if (src.Kind != SourceKinds.Crdt)
            {
                return Results.Json(
                    new ErrorResponse($"source '{name}' is not crdt-kind"),
                    statusCode: StatusCodes.Status409Conflict);
            }

            var updates = new List<byte[]>(req.Updates.Count);
            foreach (var encoded in req.Updates)
            {
                byte[] bytes;
                try
                {
                    bytes = Convert.FromBase64String(encoded);
                }
                catch (FormatException)
                {
                    return Results.BadRequest(new ErrorResponse("one or more updates are not valid base64"));
                }
                updates.Add(bytes);
            }

            var result = await crdt.MergeAsync(name, updates);
            if (result is null)
            {
                // Existence + kind were just checked above — this means the source was deleted or its
                // kind changed underneath this request. Same reading as every other "checked, then gone"
                // race in this file: 404, not a surfaced null turning into a 500.
                return Results.NotFound();
            }

            return Results.Ok(result);
        }).RequireAuthorization("Editor");

        // Plan 020 wave C. Same 501 -> guard -> 404 -> 409 ordering as the intake route above, and the
        // same Editor/SourceWrite gate: it publishes rows onto the source's stream, which is a write to
        // everything downstream even though it does not touch the document.
        group.MapPost("/{name}/crdt/replay", async (
            string name, ClaimsPrincipal principal, AccessGuard guard,
            ICatalogFacade registry, ICrdtFacade crdt) =>
        {
            if (!crdt.Enabled)
            {
                return Results.Json(
                    new ErrorResponse("this build has no CRDT document runtime"),
                    statusCode: StatusCodes.Status501NotImplemented);
            }

            var src = await registry.GetSourceAsync(name);
            if (await RefuseAsync(guard, principal, Actions.SourceWrite, src?.Name ?? name, src?.Tags) is { } refusal)
            {
                return refusal;
            }

            if (src is null)
            {
                return Results.NotFound();
            }

            if (src.Kind != SourceKinds.Crdt)
            {
                return Results.Json(
                    new ErrorResponse($"source '{name}' is not crdt-kind"),
                    statusCode: StatusCodes.Status409Conflict);
            }

            var result = await crdt.ReplayAsync(name);
            return result is null ? Results.NotFound() : Results.Ok(result);
        }).RequireAuthorization("Editor");

        group.MapGet("/{name}/crdt", async (
            string name, ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry, ICrdtFacade crdt) =>
        {
            if (!crdt.Enabled)
            {
                return Results.Json(
                    new ErrorResponse("this build has no CRDT document runtime"),
                    statusCode: StatusCodes.Status501NotImplemented);
            }

            var src = await registry.GetSourceAsync(name);
            if (await RefuseAsync(guard, principal, Actions.SourceRead, src?.Name ?? name, src?.Tags) is { } refusal)
            {
                return refusal;
            }

            if (src is null)
            {
                return Results.NotFound();
            }

            if (src.Kind != SourceKinds.Crdt)
            {
                return Results.Json(
                    new ErrorResponse($"source '{name}' is not crdt-kind"),
                    statusCode: StatusCodes.Status409Conflict);
            }

            var status = await crdt.GetStatusAsync(name);
            if (status is null)
            {
                return Results.NotFound();
            }

            return Results.Ok(status);
        }).RequireAuthorization("Viewer");
    }

    /// <summary>Null when the caller may proceed; the ready-made 403 when they may not — the same helper
    /// <c>SourcesEndpoints.RefuseAsync</c>/<c>AccessEndpoints.RefuseAsync</c> are, duplicated per that
    /// pattern's own precedent (each endpoints file owns its copy rather than sharing one across a public
    /// surface boundary).</summary>
    private static async Task<IResult?> RefuseAsync(
        AccessGuard guard, ClaimsPrincipal principal, string action, string scope, IReadOnlyCollection<string>? tags = null)
    {
        var result = await guard.CheckAsync(principal, action, scope, tags);
        return result.IsAllowed ? null : AccessGuard.Deny(result);
    }
}
