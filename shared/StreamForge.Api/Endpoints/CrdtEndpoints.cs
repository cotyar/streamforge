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
///
/// <para><b>Plan 020 wave D.</b> Three findings, each landing here rather than in the grain (the grain
/// has no ASP.NET DI and no request-scoped principal — see <c>AGENTS.md</c>'s own note on this file):
/// <list type="number">
///   <item><b>Finding 1, verified rather than rebuilt.</b> The coarse per-document ACL this file already
///   had (the <see cref="Actions.SourceWrite"/>/<see cref="Actions.SourceRead"/> check below, scoped by
///   <c>src.Name</c> and <c>src.Tags</c>) already gates one document from another under plan 015's grant
///   model — name, prefix, and tag scopes all apply to it unmodified. <c>CrdtAuthzTests</c> proves this
///   live; nothing new was built for it.</item>
///   <item><b>Finding 2 — <see cref="CrdtSourceConfig.RequireEntityAuthorization"/>.</b> Opt-in. When
///   set, every update is inspected (<see cref="ICrdtFacade.Inspect"/>, decode-only — nothing is applied
///   here) BEFORE it reaches <see cref="ICrdtFacade.MergeAsync"/>, and an update whose touched entity
///   key the caller is not granted <see cref="Actions.SourceWrite"/> on — scope
///   <c>"{sourceName}/{entityKey}"</c> — or that the inspector cannot decide at all, is refused
///   individually: it is left OUT of the batch actually merged, and named in
///   <see cref="CrdtMergeResult.Diagnostics"/>, exactly like a corrupt frame already is
///   (<c>CrdtDocGrain.MergeAsync</c>'s own precedent) — one bad update never strands the good ones
///   behind it. <c>/crdt/replay</c> does NOT apply this filter: it re-asserts the WHOLE document by
///   design (its own Editor gate exists because it publishes everything downstream), and a per-entity
///   filter there would silently under-deliver a replay rather than refuse it honestly.</item>
///   <item><b>Finding 3 — audit and attribution.</b> <see cref="AccessGuard.CheckAsync"/> already writes
///   a generic allow/deny row for the coarse check below (<see cref="AuditActionPolicy.RecordsAllowed"/>
///   says <c>source.write</c> qualifies) — that machinery is NOT duplicated here. What this file adds on
///   top, only after a merge/replay actually executes, is ONE richer "executed" row (real batch detail:
///   counts, refusals, entity keys) following <see cref="CatalogChangeAudit"/>'s own established
///   allowed-vs-executed distinction, plus (opt-in, <see cref="CrdtSourceConfig.AttributeChanges"/>)
///   forwarding the caller's identity into the document itself via
///   <see cref="ICrdtFacade.MergeAttributedAsync"/>.</item>
/// </list></para>
/// </summary>
public static class CrdtEndpoints
{
    public static void MapCrdtEndpoints(this WebApplication app)
    {
        var group = app.MapGroup("/api/sources");

        group.MapPost("/{name}/crdt/updates", async (
            string name, CrdtUpdatesRequest req, ClaimsPrincipal principal, AccessGuard guard,
            ICatalogFacade registry, ICrdtFacade crdt, IAuditSink audit) =>
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

            // Plan 020 wave D, finding 2 — opt-in, and off by default (this class's own doc comment
            // names the exact scope string and the boundary ICrdtFacade.Inspect enforces). A refused
            // update is dropped from what actually reaches the facade and named in the returned
            // Diagnostics, per D7's "a flaky link (or here, a missing grant) must not strand every good
            // update behind it" — it never turns the whole call into a 403.
            var config = src.Connector?.Crdt ?? new CrdtSourceConfig();
            var toMerge = updates;
            var preMergeDiagnostics = new List<string>();
            if (config.RequireEntityAuthorization)
            {
                toMerge = new List<byte[]>(updates.Count);
                for (var i = 0; i < updates.Count; i++)
                {
                    var refusalReason = await EntityAuthorizationRefusalAsync(guard, principal, name, src, crdt, updates[i]);
                    if (refusalReason is null)
                    {
                        toMerge.Add(updates[i]);
                    }
                    else
                    {
                        // "original request position" because the index a facade/grain diagnostic uses
                        // is the position within the FILTERED batch actually forwarded, not this one —
                        // stated so the two numbering schemes are never silently conflated.
                        preMergeDiagnostics.Add($"update[{i}] (original request position): refused pre-merge — {refusalReason}");
                    }
                }
            }

            var result = config.AttributeChanges
                ? await crdt.MergeAttributedAsync(name, toMerge, principal.Identity?.Name ?? "(anonymous)")
                : await crdt.MergeAsync(name, toMerge);
            if (result is null)
            {
                // Existence + kind were just checked above — this means the source was deleted or its
                // kind changed underneath this request. Same reading as every other "checked, then gone"
                // race in this file: 404, not a surfaced null turning into a 500.
                return Results.NotFound();
            }

            if (preMergeDiagnostics.Count > 0)
            {
                result.Diagnostics.InsertRange(0, preMergeDiagnostics);
            }

            RecordExecutedAudit(audit, principal, name, isReplay: false, result);

            return Results.Ok(result);
        }).RequireAuthorization("Editor");

        // Plan 020 wave C. Same 501 -> guard -> 404 -> 409 ordering as the intake route above, and the
        // same Editor/SourceWrite gate: it publishes rows onto the source's stream, which is a write to
        // everything downstream even though it does not touch the document. Plan 020 wave D, finding 2:
        // deliberately NOT filtered by RequireEntityAuthorization — see this class's own doc comment for
        // why (it re-asserts the WHOLE document by design; a per-entity filter here would silently
        // under-deliver a replay instead of refusing it honestly).
        group.MapPost("/{name}/crdt/replay", async (
            string name, ClaimsPrincipal principal, AccessGuard guard,
            ICatalogFacade registry, ICrdtFacade crdt, IAuditSink audit) =>
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
            if (result is null)
            {
                return Results.NotFound();
            }

            RecordExecutedAudit(audit, principal, name, isReplay: true, result);

            return Results.Ok(result);
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

    /// <summary>Plan 020 wave D, finding 2. Null when this one update may merge; a human-readable
    /// refusal reason otherwise. <see cref="ICrdtFacade.Inspect"/> decodes without applying, so an
    /// undecidable frame here costs nothing the actual merge would not also have paid decoding it.
    ///
    /// <para><b>The scope string is <c>"{sourceName}/{entityKey}"</c>, deliberately not the bare entity
    /// key.</b> Plan 015's grants are per-action-per-pattern with NO implicit namespace — an entity key
    /// alone (e.g. <c>"AAPL"</c>) could collide with an unrelated resource of a different kind entirely.
    /// The one caveat, stated rather than fixed: an entity key that itself contains <c>/</c> can produce
    /// the same composed scope string as a DIFFERENT (source, entity-key) pair — e.g. source <c>"s"</c>
    /// entity <c>"a/b"</c> composes to <c>"s/a/b"</c>, identical to source <c>"s/a"</c> entity <c>"b"</c>
    /// would (were <c>/</c> a legal source-name character, which plan 021 already forbids for <c>.</c> but
    /// not for <c>/</c>). Not escaped or otherwise defended against beyond naming it here — bounded and
    /// honest for this wave, not gold-plated; an operator whose entity keys contain <c>/</c> should read
    /// this before turning the flag on for that source.</para>
    ///
    /// <para>Also note what this does NOT do: <see cref="AccessGuard.CheckAsync"/> writes its own audit
    /// row for every one of these calls when entitlements are enforced (the guard's documented policy,
    /// unconditional here — see <see cref="CrdtSourceConfig.RequireEntityAuthorization"/>'s own doc
    /// comment for the volume this implies), so this method does not duplicate that.</para></summary>
    private static async Task<string?> EntityAuthorizationRefusalAsync(
        AccessGuard guard, ClaimsPrincipal principal, string sourceName, SourceDefinition src, ICrdtFacade crdt, byte[] updateBytes)
    {
        var inspection = crdt.Inspect(src, updateBytes);
        if (inspection.Undecidable)
        {
            return $"cannot determine which entity (and field) this update touches — {inspection.UndecidableReason}";
        }

        foreach (var touch in inspection.Touches)
        {
            var scope = $"{sourceName}/{touch.EntityKey}";
            var check = await guard.CheckAsync(principal, Actions.SourceWrite, scope, src.Tags);
            if (!check.IsAllowed)
            {
                return $"entity '{touch.EntityKey}': {check.Reason}";
            }
        }

        return null;
    }

    /// <summary>Plan 020 wave D, finding 3. The "executed" row: distinct from
    /// <see cref="AccessGuard.CheckAsync"/>'s own generic allow/deny row (that one answers "was this
    /// caller permitted"; this one answers "what did the merge/replay actually do"), following
    /// <see cref="CatalogChangeAudit"/>'s own allowed-vs-executed convention. Written unconditionally on
    /// every successful call — including a zero-effect idempotent replay (D7) — because the fact that a
    /// merge EXECUTED is itself worth one row regardless of whether it changed anything, the same way
    /// <see cref="CatalogChangeAudit"/>'s own rows are written on the mutation succeeding, not on it
    /// mattering. Never throws past the caller: matches <see cref="AccessGuard"/>'s own audit contract
    /// ("audit must never make a request fail or slow" — this file's own header note).</summary>
    private static void RecordExecutedAudit(IAuditSink audit, ClaimsPrincipal principal, string sourceName, bool isReplay, CrdtMergeResult result)
    {
        try
        {
            var row = CatalogChangeAudit.RestRow(principal, Actions.SourceWrite, sourceName);
            row.Detail = isReplay
                ? $"crdt replay: {result.RowsEmitted} row(s) re-asserted"
                : $"crdt merge: {result.UpdatesApplied} update(s) applied, {result.RowsEmitted} row(s) emitted"
                    + (result.Diagnostics.Count > 0 ? $", {result.Diagnostics.Count} diagnostic(s)" : "");
            audit.Record(row);
        }
        catch (Exception)
        {
            // Swallowed for the same reason AccessGuard.Audit and CatalogChangeAudit.Record swallow: the
            // merge/replay already happened, and nothing about recording it may change what the caller
            // sees.
        }
    }
}
