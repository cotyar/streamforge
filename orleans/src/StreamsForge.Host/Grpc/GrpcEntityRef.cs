using Grpc.Core;
using StreamsForge.Abstractions;
using StreamsForge.Api.Auth;
using StreamsForge.AppCore;

namespace StreamsForge.Host.Grpc;

/// <summary>
/// Plan 016 wave 1 — the gRPC translation of <see cref="EntityRef"/>, the twin of the REST side's
/// <c>EntityLookup</c>. Same rule (exact ordinal id wins outright, else exact ordinal name; 0 →
/// NotFound, ≥2 → Ambiguous), same rendered messages, different transport vocabulary.
///
/// <para><b>Ambiguous is <see cref="StatusCode.FailedPrecondition"/>, never
/// <see cref="StatusCode.NotFound"/>.</b> "Two of these exist" and "none of these exists" are opposite
/// facts, and a client that retried on NotFound would retry forever against a catalog state only an
/// operator can fix. FailedPrecondition says exactly what is true: the catalog is not in a state where
/// this call can be answered. It sits next to <c>GrpcAccess</c>'s RequiresApproval mapping for the same
/// reason.</para>
///
/// <para><b>The entitlement check comes first on the ambiguous branch</b> — at the raw query, since an
/// ambiguous result has no single name or tags — and only then is the message with the candidate ids
/// thrown. Answering before the guard would let a caller entitled to read neither candidate learn that
/// both exist and learn both their ids. The not-found branch keeps its pre-existing order (thrown before
/// the guard, exactly as every one of these RPCs already did) because changing it would turn today's
/// NotFound into PermissionDenied for callers, which is a plan-015 question, not a plan-016 one.</para>
///
/// <para>ponytail: no generic "resolve any entity" abstraction over the registry — two methods, because
/// the two getters have different names and sources have no id at all. Ceiling: a third id-keyed kind
/// wants a third method.</para>
/// </summary>
// Public, unlike its neighbour GrpcAccess, for one reason: the Host test project has no
// InternalsVisibleTo and EntityRefRouteTests pins the resolution rule against this class directly.
// Adding an InternalsVisibleTo to the whole assembly to keep one class internal is the more expensive
// of the two options.
public static class GrpcEntityRef
{
    /// <summary>Id fast path (an id wins outright, so a hit never fetches the catalog), else exact name.</summary>
    public static async Task<EntityRefResult<TableDefinition>> TableAsync(IRegistryGrain registry, string idOrName) =>
        await registry.GetTableAsync(idOrName) is { } byId
            ? new EntityRefResult<TableDefinition>(EntityRefOutcome.Found, byId, EntityRef.TableKind, idOrName, [])
            : EntityRef.Resolve(await registry.GetTablesAsync(), idOrName);

    public static async Task<EntityRefResult<PipelineDefinition>> PipelineAsync(IRegistryGrain registry, string idOrName) =>
        await registry.GetPipelineAsync(idOrName) is { } byId
            ? new EntityRefResult<PipelineDefinition>(EntityRefOutcome.Found, byId, EntityRef.PipelineKind, idOrName, [])
            : EntityRef.Resolve(await registry.GetPipelinesAsync(), idOrName);

    /// <summary>The resolved entity, or the right <see cref="RpcException"/>. See the class remarks for
    /// why the guard runs before the ambiguous throw and not before the not-found one.</summary>
    internal static async Task<T> RequireAsync<T>(
        EntityRefResult<T> hit, AccessGuard guard, ServerCallContext context, string readAction)
        where T : class
    {
        if (hit.Outcome == EntityRefOutcome.Ambiguous)
        {
            await GrpcAccess.EnsureAsync(guard, context, readAction, hit.Query, null);
            throw new RpcException(new Status(StatusCode.FailedPrecondition, hit.Message));
        }

        return hit.Value ?? throw new RpcException(new Status(StatusCode.NotFound, hit.Message));
    }
}
