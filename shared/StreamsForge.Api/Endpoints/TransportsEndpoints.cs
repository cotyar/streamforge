using System.Security.Claims;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using StreamsForge.Abstractions;
using StreamsForge.Api.Auth;
using StreamsForge.AppCore.Sinks;
using StreamsForge.AppCore.Transports;

namespace StreamsForge.Api;

/// <summary>
/// Plan 010 (console wave): <c>GET /api/transports</c> — the registered transports and their form
/// descriptors, so the SPA renders a config editor for a transport it has never heard of.
///
/// <para>This is the last of the places that used to need editing per transport. With it, the cost of a new
/// transport is one <see cref="IInboundTransport"/> (and/or one <see cref="ISinkTransport"/>) plus one line
/// in the registry — validation, secret masking, both connector drivers, both publisher services and now the
/// console all derive from that.</para>
///
/// <para>Viewer, like every other read: a descriptor is field metadata, never a value. Secrets never appear
/// here — the SPA learns that a field IS a secret (so it renders masked and honors the "*** keeps the stored
/// value" rule), not what it contains.</para>
///
/// <para>Plan 015 wave 3-A: both routes keep their policy as the compatibility floor and additionally
/// ask <see cref="AccessGuard"/> — the listing for <see cref="Actions.CatalogRead"/>, the probe for
/// <see cref="Actions.CatalogWrite"/>, both at <c>*</c>. A transport DESCRIPTOR belongs to the build,
/// not to any entity, so there is no narrower scope to ask at; the probe is <c>catalog.write</c> and
/// not a read because of the security note below — it makes the server dial a host the caller named,
/// which is a write-shaped capability however the response reads.</para>
/// </summary>
public static class TransportsEndpoints
{
    /// <summary>Default bound on one <c>POST /api/transports/{kind}/probe</c> call — overridable via
    /// <c>Transports:ProbeTimeoutSeconds</c>. A probe dials a host an Editor supplied in the request body;
    /// 15 s mirrors <c>SourceSchemaService.DeriveHttpClient</c>'s bound on the same kind of "server reaches
    /// out on the caller's word" call, and keeps a stalled/black-holed connector from parking a request
    /// thread indefinitely.</summary>
    private const int DefaultProbeTimeoutSeconds = 15;

    public static void MapTransportsEndpoints(this WebApplication app)
    {
        // Plan 014: Inbound is BOTH registries. A polled kind and a message kind are both "a source the
        // console has to draw a form for", and the two are separate registries for a driver-side reason
        // (one arms a subscriber, the other arms a timer) that the console has no business knowing about.
        // Merging them here is what makes the descriptor's own Polled flag useful — it exists precisely so
        // one list can carry both and the form decides per entry whether to render a schedule editor.
        // Without this the database kinds would validate, start and run while being invisible to the only
        // UI that can configure them.
        app.MapGet("/api/transports", async (ClaimsPrincipal principal, AccessGuard guard) =>
            {
                var decision = await guard.CheckAsync(principal, Actions.CatalogRead, "*");
                if (!decision.IsAllowed)
                {
                    return AccessGuard.Deny(decision);
                }

                return Results.Ok(new TransportCatalog(
                    Inbound:
                    [
                        .. InboundTransports.Kinds.Select(k => InboundTransports.Find(k)!.Describe()),
                        .. PolledTransports.Kinds.Select(k => PolledTransports.Find(k)!.Describe()),
                    ],
                    Outbound: [.. SinkTransports.Kinds.Select(k => SinkTransports.Find(k)!.Describe())]));
            })
            .RequireAuthorization("Viewer");

        // Plan 014: generic schema discovery for any registered POLLED kind that also implements
        // ISchemaProbe. This is the entire database-awareness surface StreamsForge.Api carries — it knows
        // ISchemaProbe exists and nothing about what implements it (Postgres, MS SQL, or a kind this
        // assembly has never heard of). The branching itself lives in SourceSchemaService.ProbeAsync (same
        // split as mapping-validate/derive-openapi/from-remote in that file), so this handler is just HTTP
        // plumbing: read the configured timeout, call it, map the three-way outcome to a status code.
        //
        // Security note: a probe means the server opens an OUTBOUND connection to a host supplied by the
        // Editor calling this endpoint, using the request body's connector config verbatim. That is the
        // same trust the url/file/folder source kinds already place in an Editor (their config is also a
        // caller-supplied address the server reaches out to) — this endpoint raises no new ceiling, but the
        // trust boundary is worth stating rather than leaving implicit.
        app.MapPost("/api/transports/{kind}/probe", async (
            string kind, SourceDefinition def, ClaimsPrincipal principal, AccessGuard guard,
            IConfiguration config, CancellationToken ct) =>
        {
            // Asked at `*`, and with the body's own Tags deliberately NOT passed: a probe has no stored
            // entity behind it, so the only tags available are the ones the caller just typed — an
            // entitlement that a caller could satisfy by writing the right tag into their own request
            // would not be an entitlement.
            var decision = await guard.CheckAsync(principal, Actions.CatalogWrite, "*");
            if (!decision.IsAllowed)
            {
                return AccessGuard.Deny(decision);
            }

            var timeoutSeconds = config.GetValue("Transports:ProbeTimeoutSeconds", DefaultProbeTimeoutSeconds);
            var outcome = await SourceSchemaService.ProbeAsync(kind, def, TimeSpan.FromSeconds(timeoutSeconds), ct)
                .ConfigureAwait(false);

            return outcome.Kind switch
            {
                // 404: nobody registered this kind. 400: it's registered but never implemented ISchemaProbe.
                // Deliberately different statuses — see ProbeOutcomeKind's doc comment.
                ProbeOutcomeKind.UnknownKind => Results.NotFound(new ErrorResponse(outcome.Message!)),
                ProbeOutcomeKind.CannotProbe => Results.BadRequest(new ErrorResponse(outcome.Message!)),
                _ => Results.Ok(outcome.Result),
            };
        }).RequireAuthorization("Editor");
    }
}
