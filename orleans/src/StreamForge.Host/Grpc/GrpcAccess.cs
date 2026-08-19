using Grpc.Core;
using StreamForge.Abstractions;
using StreamForge.Api.Auth;

namespace StreamForge.Host.Grpc;

/// <summary>
/// Plan 015 wave 3-B — the gRPC translation of <see cref="AccessGuard"/>'s answer, and nothing else.
///
/// <para><b>Why this exists at all, given the ponytail rule against abstractions spanning files.</b> The
/// block it replaces is five lines, but it appears once per gRPC method — thirty times across six
/// services — and every copy would have to agree on the same two status codes. A single static method in
/// the folder that owns all six services is cheaper than thirty chances to write
/// <c>Unauthenticated</c> where the rest wrote <c>PermissionDenied</c>. It is deliberately NOT in
/// <c>shared/</c>: nothing outside <c>Grpc/</c> speaks <see cref="RpcException"/>.</para>
///
/// <para><b>The refusal shape.</b> A denial is <see cref="StatusCode.PermissionDenied"/>, the gRPC
/// analogue of the REST 403 <see cref="AccessGuard.Deny"/> returns, and it carries
/// <see cref="StreamForge.AppCore.Access.AccessResult.Reason"/> verbatim as the status detail — a caller
/// staring at a bare <c>PermissionDenied</c> with no detail is precisely the failure mode plan 015 exists
/// to remove, and the reason string is written to be read by a human (it names the grant that denied, or
/// says that none matched).</para>
///
/// <para><b>RequiresApproval is refused DISTINCTLY, and it is not a denial.</b> It comes back as
/// <see cref="StatusCode.FailedPrecondition"/> — "the system is not in a state where this can run yet",
/// which is exactly what "somebody has to approve this first" means, and which a client can tell apart
/// from <c>PermissionDenied</c> without parsing text. Waves 4-5 own filing the approval request; until
/// that machinery exists, refusing is the only answer that cannot be wrong, and it fails closed rather
/// than treating "needs a second pair of eyes" as yes. When wave 5 lands, this method is the one place
/// that has to change to file-and-report-an-id instead.</para>
/// </summary>
internal static class GrpcAccess
{
    /// <summary>Throws the appropriate <see cref="RpcException"/> unless the caller is entitled.</summary>
    /// <param name="scope">The entity's id or name, or <c>"*"</c> for a method with no single resource
    /// (a list, a SQL validation). Asking with <c>"*"</c> is answered only by a <c>*</c>-scoped grant.</param>
    /// <param name="resourceTags">The entity's <c>Tags</c>, so <c>tag:finance</c> scopes match. Omitting
    /// them can only ever narrow the answer, never widen it.</param>
    public static async Task EnsureAsync(
        AccessGuard guard,
        ServerCallContext context,
        string action,
        string scope,
        IReadOnlyCollection<string>? resourceTags = null)
    {
        var result = await guard.CheckAsync(context.GetHttpContext().User, action, scope, resourceTags)
            .ConfigureAwait(false);
        if (result.IsAllowed)
        {
            return;
        }

        throw new RpcException(new Status(
            result.Decision == AccessDecision.RequiresApproval
                ? StatusCode.FailedPrecondition
                : StatusCode.PermissionDenied,
            result.Reason));
    }
}
