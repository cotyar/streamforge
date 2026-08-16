using Grpc.Core;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using StreamForge.Abstractions;
using StreamForge.AppCore.Ingest;
using V1 = StreamForge.Host.Grpc.V1;

namespace StreamForge.Host.Grpc;

/// <summary>
/// gRPC bidi ingest surface for client-push sources (plan 008 W4) — see streamforge.proto's
/// IngestService section for the wire-shape rationale.
///
/// <para>Plan 009 A1.2: NO method-level [Authorize] — per-message dual auth instead (Editor JWT via
/// the real "Editor" policy, resolved through <see cref="IAuthorizationService"/> so this can't drift
/// from what SourcesEndpoints.IsAuthorizedToPushAsync checks; else the "x-sf-ingest-key" call metadata
/// against THAT message's source). Per-MESSAGE, not per-call, because <see cref="V1.IngestRequest.SourceName"/>
/// travels on every message, not just the first — the same bidi stream could in principle carry
/// pushes for different sources, and a key only ever authorizes ONE source.</para>
///
/// Bidi (not unary/client-streaming) so the server can ack per pushed batch and signal overload
/// WITHOUT ending the call — overload rides an ack field (retry_after_ms), never an RPC error status
/// (mirrors IngestOutcome.Overloaded being a 429 body, not a REST 5xx). The loop below is
/// deliberately sequential: it awaits IIngressFacade.PushAsync (which, under
/// IngressOverflowPolicy.Block, can itself await up to the policy's timeout) BEFORE reading the next
/// request. That single fact is what makes Block's backpressure real on this path: while the await is
/// pending, this service simply doesn't call MoveNext on the request reader, so gRPC's receive buffer
/// fills, HTTP/2 flow control stops advertising window credit on the wire, and the client's own
/// RequestStream.WriteAsync genuinely blocks — no client-side polling or throttle needed.
///
/// <para>Wishlist "explicit key retraction through ingest": this used to call
/// <see cref="IIngressFacade.PushAsync"/> straight off the wire, which meant a <c>"_retract": true</c>
/// row landed with no gate at all — <see cref="StreamForge.Api.SourcesEndpoints"/>'s
/// <c>POST /{name}/events</c> is the only place that ran <see cref="RetractConsumerValidation.FindNonLatestByConsumer"/>
/// before admission. Fixed by running the exact same check here, against the exact same method (not a
/// second copy of "is this table LATEST BY" that could quietly drift from the REST one) — see
/// <see cref="Ingest"/>'s body.</para>
/// </summary>
public sealed class IngestGrpcService(IIngressFacade ingress, IAuthorizationService authz, ICatalogFacade registry) : V1.IngestService.IngestServiceBase
{
    public override async Task Ingest(
        IAsyncStreamReader<V1.IngestRequest> requestStream,
        IServerStreamWriter<V1.IngestAck> responseStream,
        ServerCallContext context)
    {
        var httpContext = context.GetHttpContext();

        await foreach (var request in requestStream.ReadAllAsync(context.CancellationToken).ConfigureAwait(false))
        {
            if (!await IsAuthorizedAsync(httpContext, context, request.SourceName).ConfigureAwait(false))
            {
                throw new RpcException(new Status(StatusCode.Unauthenticated, "an Editor JWT or a valid X-SF-Ingest-Key for this source is required"));
            }

            var rows = request.Rows.Select(GrpcValueConverter.FromStruct).ToList();
            var idempotencyKey = string.IsNullOrEmpty(request.IdempotencyKey) ? null : request.IdempotencyKey;

            // Wishlist "explicit key retraction through ingest": mirrors SourcesEndpoints.cs's
            // POST /{name}/events gate 1:1 — same RetractConsumerValidation.FindNonLatestByConsumer
            // call, same partial/whole-batch split, same message text. Closes the gap that commit
            // c5847be named plainly: this path used to accept a "_retract" row silently instead of
            // refusing it.
            var retractRowIndexes = RetractConsumerValidation.CollectRetractRowIndexes(rows);
            if (retractRowIndexes.Count > 0)
            {
                var offendingTable = RetractConsumerValidation.FindNonLatestByConsumer(
                    request.SourceName, await registry.GetSourcesAsync().ConfigureAwait(false), await registry.GetTablesAsync().ConfigureAwait(false));
                if (offendingTable is not null)
                {
                    var retractErrors = BuildRetractErrors(request.SourceName, offendingTable, retractRowIndexes);

                    if (!request.Partial)
                    {
                        // Whole-batch rejection: no PushAsync call at all — same "a partially-admitted
                        // batch would have no safe retry" reasoning IngressOverflowPolicy.Reject's own
                        // doc states, applied to a validate-time rejection instead of a capacity one.
                        // The stream itself stays open (an RpcException would end the call for every
                        // future message, not just this one) — the bad batch gets an Invalid ack and
                        // the client decides what to do next, exactly like a 400 leaves the HTTP
                        // connection open for the next request.
                        await responseStream.WriteAsync(ToAck(RejectedResult(retractErrors))).ConfigureAwait(false);
                        continue;
                    }

                    // Partial: admit everything else, fold the offending rows into Invalid/RowErrors —
                    // identical to SourcesEndpoints.cs's own partial branch.
                    var offendingSet = new HashSet<int>(retractRowIndexes);
                    var filteredRows = rows.Where((_, i) => !offendingSet.Contains(i)).ToList();
                    var partialResult = await ingress.PushAsync(request.SourceName, filteredRows, request.Partial, idempotencyKey).ConfigureAwait(false);
                    partialResult.Invalid += retractErrors.Count;
                    partialResult.RowErrors = [.. partialResult.RowErrors, .. retractErrors];
                    await responseStream.WriteAsync(ToAck(partialResult)).ConfigureAwait(false);
                    continue;
                }
            }

            var result = await ingress.PushAsync(request.SourceName, rows, request.Partial, idempotencyKey).ConfigureAwait(false);
            await responseStream.WriteAsync(ToAck(result)).ConfigureAwait(false);
        }
    }

    /// <summary>Same two-step check as SourcesEndpoints.IsAuthorizedToPushAsync — resolved through
    /// the real "Editor" policy first (so Admin still counts, and role-claim shape can't drift between
    /// REST and gRPC), then a per-source key. Metadata lookup is case-insensitive
    /// (<see cref="Metadata.GetValue"/>).</summary>
    private async Task<bool> IsAuthorizedAsync(HttpContext httpContext, ServerCallContext context, string sourceName)
    {
        var editorResult = await authz.AuthorizeAsync(httpContext.User, "Editor").ConfigureAwait(false);
        if (editorResult.Succeeded)
        {
            return true;
        }

        var presentedKey = context.RequestHeaders.GetValue("x-sf-ingest-key");
        return await ingress.ValidateKeyAsync(sourceName, presentedKey).ConfigureAwait(false);
    }

    /// <summary>The exact per-row message SourcesEndpoints.cs's REST handler builds for the identical
    /// situation — extracted to its own testable method (this class has no HTTP/gRPC test harness to
    /// exercise <see cref="Ingest"/> end-to-end against, so this is the seam a unit test can pin the wording
    /// through without one) rather than left inline, so a future edit to either copy is at least visible as
    /// a diff to this one method instead of two independently-typed string interpolations silently drifting
    /// apart. See <see cref="Ingest"/>'s own class-doc paragraph on why the CHECK itself
    /// (<see cref="RetractConsumerValidation.FindNonLatestByConsumer"/>) is shared code, not just
    /// shared wording.</summary>
    public static List<string> BuildRetractErrors(string sourceName, string offendingTable, IReadOnlyList<int> retractRowIndexes)
    {
        var message = $"\"_retract\" is only valid when every running table reading source '{sourceName}' directly is a LATEST BY table; '{offendingTable}' is not";
        return retractRowIndexes.Select(i => $"row {i}: {message}").ToList();
    }

    /// <summary>The whole-batch-rejection <see cref="IngestResult"/> shape for a request that failed
    /// retract validation and did not ask for <c>partial</c> — no <see cref="IIngressFacade.PushAsync"/>
    /// call happens for this shape (see <see cref="Ingest"/>'s own comment on why), so this is the entire
    /// answer the client gets back on this message.</summary>
    public static IngestResult RejectedResult(IReadOnlyList<string> retractErrors) => new()
    {
        Outcome = IngestOutcome.Invalid,
        Invalid = retractErrors.Count,
        Error = $"{retractErrors.Count} row(s) failed retract validation",
        RowErrors = [.. retractErrors],
    };

    private static V1.IngestAck ToAck(IngestResult result)
    {
        var ack = new V1.IngestAck
        {
            Outcome = ToProto(result.Outcome),
            Accepted = result.Accepted,
            Dropped = result.Dropped,
            Invalid = result.Invalid,
            RetryAfterMs = result.RetryAfterMs,
            Error = result.Error ?? "",
            Duplicate = result.Duplicate,
            Replayed = result.Replayed,
        };
        ack.RowErrors.AddRange(result.RowErrors);
        return ack;
    }

    private static V1.IngestOutcome ToProto(IngestOutcome outcome) => outcome switch
    {
        IngestOutcome.Accepted => V1.IngestOutcome.Accepted,
        IngestOutcome.Invalid => V1.IngestOutcome.Invalid,
        IngestOutcome.NotFound => V1.IngestOutcome.NotFound,
        IngestOutcome.WrongKind => V1.IngestOutcome.WrongKind,
        IngestOutcome.TooLarge => V1.IngestOutcome.TooLarge,
        IngestOutcome.Overloaded => V1.IngestOutcome.Overloaded,
        _ => throw new ArgumentOutOfRangeException(nameof(outcome), outcome, "unknown IngestOutcome"),
    };
}
