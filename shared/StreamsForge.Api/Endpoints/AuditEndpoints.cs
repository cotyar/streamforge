using System.Security.Claims;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using StreamsForge.Abstractions;
using StreamsForge.Api.Auth;
using StreamsForge.AppCore.Access;

namespace StreamsForge.Api;

/// <summary>
/// Plan 015 wave 5-A — the REST surface over the audit log. <b>Read-only, and structurally so.</b>
///
/// <para>There is no write route here and there must never be one: the ONLY writer is
/// <see cref="AuditChannelSink"/>'s drain (<see cref="AuditWriterService"/>), and the value of the log
/// is exactly that nothing else can put a row in it or take one out. <see cref="IAuditFacade"/> is
/// injected for its two read members; <c>AppendAsync</c> is deliberately never called from this file.
/// A route that let a caller append would let a caller forge the record of their own actions, which is
/// worse than having no log at all — the forged one is believed.</para>
///
/// <para><b>Two gates, the wave 2-C pattern.</b> The group carries
/// <c>RequireAuthorization("Admin")</c> and each handler checks <see cref="Actions.AuditRead"/> through
/// <see cref="AccessGuard"/>. The floor is Admin rather than Viewer because these routes are NEW and
/// therefore have no legacy behaviour to preserve — which makes the choice a pure fail-closed decision.
/// In <c>Auth:Mode=legacy</c> the in-handler guard allows everything by definition, so the floor is the
/// only control that survives a rollback, and "any authenticated user may read every action anybody has
/// taken" is not a state this repo should be one config flag away from.</para>
///
/// <para>ponytail: consequently a bespoke read-only auditor role holding <see cref="Actions.AuditRead"/>
/// and nothing else cannot reach these routes — the same ceiling wave 2-C wrote down for the
/// <c>/api/access</c> reads, with the same upgrade path: drop the group's floor to <c>Viewer</c> on the
/// day <c>Auth:Mode=legacy</c> stops being a supported rollback, and the per-action guards already
/// written below become the whole control with no change to a handler. Over-granting and under-granting
/// are not symmetric mistakes (015 wave 2), and this is the direction that is merely inconvenient.</para>
///
/// <para><b><see cref="AuditPage.Truncated"/> reaches the response body and a client cannot omit it.</b>
/// That counter is the entire reason the day shard's drop-oldest cap is honest — it exists so silence is
/// never mistaken for absence — and an API that quietly dropped it would undo the whole of wave 4's
/// persistence work. It is a required field of <see cref="AuditPageResponse"/> and a required field of
/// the TypeScript <c>AuditPageResponse</c>, not an optional one.</para>
/// </summary>
public static class AuditEndpoints
{
    /// <summary>Page size when the caller names none, and the ceiling. A day holds up to
    /// <c>Audit:MaxEntriesPerDay</c> (20 000) rows; handing all of them to a browser in one response is
    /// not a page, it is a download.</summary>
    private const int DefaultLimit = 200;
    private const int MaxLimit = 2000;

    public static void MapAuditEndpoints(this WebApplication app)
    {
        var group = app.MapGroup("/api/audit").RequireAuthorization("Admin");

        // GET /api/audit/days — which days have entries. Reads the index, wakes no day shard, which is
        // what makes it the cheap first call for a console that then asks for one day.
        //
        // A literal segment beats a route parameter in ASP.NET routing, so this cannot be swallowed by
        // GET /{day} below however the two are ordered.
        group.MapGet("/days", async (ClaimsPrincipal principal, AccessGuard guard, IAuditFacade audit) =>
        {
            if (await RefuseAsync(guard, principal) is { } refusal)
            {
                return refusal;
            }

            return Results.Ok(await audit.GetDaysAsync());
        });

        // GET /api/audit/{day}?actor=&action=&limit=&offset=&includeChanges=
        //
        // The filters are the facade's, verbatim and no richer: exact actor, action PREFIX, limit and
        // offset. Anything more expressive is a query engine, and this platform already is one — a day's
        // rows are a stream somebody can point SQL at, one layer up.
        group.MapGet("/{day}", async (
            string day,
            ClaimsPrincipal principal,
            AccessGuard guard,
            PermissionResolver resolver,
            IAuditFacade audit,
            string? actor = null,
            string? action = null,
            int limit = DefaultLimit,
            int offset = 0,
            bool includeChanges = false) =>
        {
            if (await RefuseAsync(guard, principal) is { } refusal)
            {
                return refusal;
            }

            if (!IsDay(day))
            {
                // Validated rather than forwarded: the day IS a storage key (a grain key on Orleans, an
                // actor id on Dapr), so an unvalidated one lets a caller activate arbitrary keys by
                // asking about them. Eight digits is the whole grammar — StreamConstants.AuditKeyFor
                // never produces anything else.
                return Results.BadRequest(new ErrorResponse($"'{day}' is not a day; expected yyyyMMdd (UTC), e.g. {Today()}"));
            }

            var page = await audit.QueryAsync(day, Trim(actor), Trim(action), Math.Clamp(limit, 1, MaxLimit), Math.Max(0, offset));

            // Whether this caller gets the before/after payloads — see RedactChanges.
            var mayReadChanges = PermissionEvaluator
                .Evaluate(await resolver.ResolveAsync(principal), Actions.AccessRead, "*")
                .IsAllowed;
            var withChanges = includeChanges && mayReadChanges;

            var entries = withChanges ? page.Entries : [.. page.Entries.Select(RedactChanges)];
            var withheld = withChanges
                ? 0
                : page.Entries.Count(e => e.BeforeJson is not null || e.AfterJson is not null);

            return Results.Ok(new AuditPageResponse(
                day,
                entries,
                page.Truncated,
                page.Total,
                withChanges,
                withheld));
        });
    }

    // ==============================================================================================
    // Before/after payloads
    // ==============================================================================================

    /// <summary>
    /// The same entry with <see cref="AuditEntry.BeforeJson"/> and <see cref="AuditEntry.AfterJson"/>
    /// removed.
    ///
    /// <para><b>Why they are off by default.</b> A sibling wave populates those two fields with the
    /// serialized entity a mutation changed — which for a source definition includes its stored
    /// credential fields. This platform already decided how it feels about that on the one other route
    /// that can emit them: <c>GET /api/config/export</c> masks secrets unless the caller explicitly asks
    /// for <c>includeSecrets</c>. An audit reader that handed the same values back unasked would simply
    /// be the way around that decision. So the payloads are opt-in twice — <c>?includeChanges=true</c>
    /// AND <see cref="Actions.AccessRead"/> at <c>*</c>, the entitlement that already reads the whole
    /// policy document — and the default response, the one a console polls, carries neither.</para>
    ///
    /// <para><b>Withholding is reported, never silent.</b> <see cref="AuditPageResponse.ChangesIncluded"/>
    /// says whether this response carries them and
    /// <see cref="AuditPageResponse.ChangesWithheld"/> counts the rows that had something to carry —
    /// the same argument <see cref="AuditPage.Truncated"/> makes one layer down. A reader who cannot see
    /// a diff at least knows a diff exists.</para>
    ///
    /// <para><b>A whitelist copy, and the drift is deliberately in the safe direction.</b> The redacted
    /// entry is a NEW object rather than the stored one with two fields nulled, because the stored one
    /// may be the store's own instance and an audit reader must not be able to erase the audit log by
    /// reading it. Listing the fields by hand means a field added to the frozen
    /// <see cref="AuditEntry"/> later is dropped from the redacted copy until somebody adds it here —
    /// which is exactly the direction <see cref="ApprovalStateMachine.CreateRequest"/>'s whitelist
    /// chooses for the same reason: a new field defaults to withheld rather than to leaked.</para>
    /// </summary>
    private static AuditEntry RedactChanges(AuditEntry e) => new()
    {
        Id = e.Id,
        AtMs = e.AtMs,
        Actor = e.Actor,
        Action = e.Action,
        Scope = e.Scope,
        Outcome = e.Outcome,
        Detail = e.Detail,
        OnBehalfOf = e.OnBehalfOf,
        ApprovalId = e.ApprovalId,
        Origin = e.Origin,
        // BeforeJson / AfterJson deliberately absent.
    };

    // ==============================================================================================
    // Helpers
    // ==============================================================================================

    /// <summary>Eight ASCII digits. <c>DateTime.TryParseExact</c> would additionally reject 20261332,
    /// which the store would answer with an empty page anyway — this is a key-shape check, not a
    /// calendar.</summary>
    private static bool IsDay(string day) =>
        day.Length == 8 && day.All(char.IsAsciiDigit);

    private static string Today() => DateTime.UtcNow.ToString("yyyyMMdd");

    private static string? Trim(string? value) => string.IsNullOrWhiteSpace(value) ? null : value;

    /// <summary>Null when the caller may read the log; the ready-made 403 when they may not.
    /// <see cref="Actions.AuditRead"/> is asked at <c>*</c> and not at the day: a day is a storage
    /// shard, not a resource anybody would write a scoped entitlement about, and a
    /// <c>scope=20260819</c> grant would be a trap rather than a feature.</summary>
    private static async Task<IResult?> RefuseAsync(AccessGuard guard, ClaimsPrincipal principal)
    {
        var result = await guard.CheckAsync(principal, Actions.AuditRead, "*");
        return result.IsAllowed ? null : AccessGuard.Deny(result);
    }
}
