using System.Security.Claims;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using StreamForge.Abstractions;
using StreamForge.Api.Auth;
using StreamForge.Engine.Sql;

namespace StreamForge.Api;

/// <summary>
/// <c>GET /api/sql/functions</c> — which scalar functions and aggregates this deployment's SQL dialect
/// actually has. The console's completion list used to be a hardcoded copy of the Engine's built-in
/// switches, so a function registered through <see cref="SqlFunctions"/> (the pricing scalars, anything
/// a future assembly adds) existed in the language but not in the editor. Same reasoning as
/// <c>GET /api/transports</c>: the surface that draws a UI reads the registry rather than duplicating it.
///
/// <para>Built-ins and registered entries are reported separately rather than merged, because the
/// distinction is real to an operator: a built-in is in every deployment, a registered one is there only
/// because that host loaded the assembly that registers it. A missing pricing function is then a
/// deployment question, not a syntax one.</para>
///
/// <para>Plan 015 wave 3-A: the route keeps its <c>Viewer</c> policy as the compatibility floor and
/// additionally asks <see cref="AccessGuard"/> for <see cref="Actions.CatalogRead"/> at <c>*</c>. There
/// is no <c>meta.read</c> action and there deliberately isn't going to be one — wave 1 folded every
/// platform-metadata route onto <c>catalog.read</c> (see <c>BuiltInRoleCatalog</c>'s class doc), and this
/// list is platform metadata: it describes the dialect, not any entity in the catalog. <c>*</c> is
/// therefore the honest scope, and it is the one case where asking at <c>*</c> is not a cop-out.</para>
/// </summary>
public static class SqlFunctionsEndpoints
{
    public sealed record SqlFunctionCatalog(
        IReadOnlyList<string> Scalars,
        IReadOnlyList<string> Aggregates,
        IReadOnlyList<string> RegisteredScalars,
        IReadOnlyList<string> RegisteredAggregates);

    public static void MapSqlFunctionsEndpoints(this WebApplication app)
    {
        app.MapGet("/api/sql/functions", async (ClaimsPrincipal principal, AccessGuard guard) =>
            {
                var decision = await guard.CheckAsync(principal, Actions.CatalogRead, "*");
                if (!decision.IsAllowed)
                {
                    return AccessGuard.Deny(decision);
                }

                return Results.Ok(new SqlFunctionCatalog(
                    Scalars: SqlFunctions.BuiltInScalarNames,
                    Aggregates: SqlFunctions.BuiltInAggregateNames,
                    RegisteredScalars: SqlFunctions.RegisteredScalarNames(),
                    RegisteredAggregates: SqlFunctions.RegisteredAggregateNames()));
            })
            .RequireAuthorization("Viewer");
    }
}
