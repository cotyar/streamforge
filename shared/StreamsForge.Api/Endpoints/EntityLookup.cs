using Microsoft.AspNetCore.Http;
using StreamsForge.Abstractions;
using StreamsForge.AppCore;

namespace StreamsForge.Api;

/// <summary>
/// Plan 016 wave 1 — the HTTP-side adapter over <see cref="EntityRef"/>, and the only place in this
/// assembly that turns an id-or-name route segment into an entity.
///
/// <para><b>Why an adapter and not a direct <c>EntityRef.Resolve</c> call at each site.</b> Two reasons,
/// both operational. First, the fast path: <c>ICatalogFacade.GetTableAsync</c>/<c>GetPipelineAsync</c>
/// match on <c>Id</c> only, and the pinned rule says an id wins outright — so an id hit can
/// short-circuit and the whole catalog never has to be fetched on the common path (the console addresses
/// everything by id). Falling through to <c>GetTablesAsync()</c> only when that misses is exactly
/// equivalent to the rule, not an approximation of it. Second, the status mapping: <b>Ambiguous is 409,
/// never 404</b>, and having one <see cref="Reject{T}"/> means no route can quietly word it differently
/// from the route next to it.</para>
///
/// <para><b>The ordering every call site follows, and why it is not negotiable.</b>
/// <code>
///   var hit = await EntityLookup.TableAsync(registry, id);
///   if (await RefuseAsync(guard, principal, action, hit.Value?.Name ?? id, hit.Value?.Tags) is { } refusal) return refusal;
///   if (EntityLookup.Reject(hit) is { } miss) return miss;
///   var def = hit.Value!;
/// </code>
/// The entitlement guard runs FIRST, against the raw route segment when nothing resolved. An
/// <see cref="EntityRefOutcome.Ambiguous"/> result has no single name and no tags to authorize against,
/// so a 409 emitted before the guard would tell a caller entitled to read <i>neither</i> candidate that
/// two entities exist and hand them both ids — a catalog-enumeration oracle on a route whose entire
/// purpose is to be scoped. Authorize against the query, then answer. This is the same order the
/// pre-existing <c>def?.Name ?? id</c> guard calls already used; the 409 simply slots in where the 404
/// was, after it.</para>
///
/// <para>ponytail: two methods and a switch, no generic facade abstraction over "catalog entity" — the
/// two facade getters have different names and sources have no id at all, so the abstraction would cost
/// more than the twelve lines it removed. Ceiling: a third id-keyed entity kind would want a third
/// method here. Upgrade path: write it.</para>
/// </summary>
internal static class EntityLookup
{
    /// <summary>Resolve a table by id (fast path, no catalog fetch) or else by exact name.</summary>
    public static async Task<EntityRefResult<TableDefinition>> TableAsync(ICatalogFacade registry, string idOrName)
    {
        var byId = await registry.GetTableAsync(idOrName);
        return byId is not null
            ? new EntityRefResult<TableDefinition>(EntityRefOutcome.Found, byId, EntityRef.TableKind, idOrName, [])
            : EntityRef.Resolve(await registry.GetTablesAsync(), idOrName);
    }

    /// <summary>Resolve a pipeline by id (fast path, no catalog fetch) or else by exact name.</summary>
    public static async Task<EntityRefResult<PipelineDefinition>> PipelineAsync(ICatalogFacade registry, string idOrName)
    {
        var byId = await registry.GetPipelineAsync(idOrName);
        return byId is not null
            ? new EntityRefResult<PipelineDefinition>(EntityRefOutcome.Found, byId, EntityRef.PipelineKind, idOrName, [])
            : EntityRef.Resolve(await registry.GetPipelinesAsync(), idOrName);
    }

    /// <summary>Null when the caller may proceed (<see cref="EntityRefResult{T}.Value"/> is non-null);
    /// otherwise the response to return. 404 stays bodiless, as every route here already answered it;
    /// the 409 carries <see cref="EntityRefResult{T}.Message"/> verbatim, which names the candidate ids
    /// because addressing one by id is the caller's only way out.
    ///
    /// <para><b>Call this AFTER the entitlement guard</b> — see the class remarks.</para></summary>
    public static IResult? Reject<T>(EntityRefResult<T> hit)
        where T : class => hit.Outcome switch
        {
            EntityRefOutcome.Found => null,
            EntityRefOutcome.Ambiguous => Results.Conflict(new ErrorResponse(hit.Message)),
            _ => Results.NotFound(),
        };
}
