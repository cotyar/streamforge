using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
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
        app.MapGet("/api/sql/functions", () => Results.Ok(new SqlFunctionCatalog(
                Scalars: SqlFunctions.BuiltInScalarNames,
                Aggregates: SqlFunctions.BuiltInAggregateNames,
                RegisteredScalars: SqlFunctions.RegisteredScalarNames(),
                RegisteredAggregates: SqlFunctions.RegisteredAggregateNames())))
            .RequireAuthorization("Viewer");
    }
}
