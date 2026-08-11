using Grpc.Core;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using StreamForge.Abstractions;
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
/// </summary>
public sealed class IngestGrpcService(IIngressFacade ingress, IAuthorizationService authz) : V1.IngestService.IngestServiceBase
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
