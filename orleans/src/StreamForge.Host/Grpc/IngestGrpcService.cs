using Grpc.Core;
using Microsoft.AspNetCore.Authorization;
using StreamForge.Abstractions;
using V1 = StreamForge.Host.Grpc.V1;

namespace StreamForge.Host.Grpc;

/// <summary>
/// gRPC bidi ingest surface for client-push sources (plan 008 W4) — see streamforge.proto's
/// IngestService section for the wire-shape rationale. Method-level [Authorize(Policy = "Editor")]:
/// every other gRPC RPC in this codebase that reaches actual row data (StreamGrpcService's three
/// server-streaming subscriptions) is Viewer-only, since they only ever read; this is the first one
/// that writes, so it needs the same bar POST /api/sources/{name}/events does.
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
public sealed class IngestGrpcService(IIngressFacade ingress) : V1.IngestService.IngestServiceBase
{
    [Authorize(Policy = "Editor")]
    public override async Task Ingest(
        IAsyncStreamReader<V1.IngestRequest> requestStream,
        IServerStreamWriter<V1.IngestAck> responseStream,
        ServerCallContext context)
    {
        await foreach (var request in requestStream.ReadAllAsync(context.CancellationToken).ConfigureAwait(false))
        {
            var rows = request.Rows.Select(GrpcValueConverter.FromStruct).ToList();
            var result = await ingress.PushAsync(request.SourceName, rows, request.Partial).ConfigureAwait(false);
            await responseStream.WriteAsync(ToAck(result)).ConfigureAwait(false);
        }
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
