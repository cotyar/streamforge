using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using StreamForge.Abstractions;
using StreamForge.Api;
using StreamForge.Api.Auth;
using StreamForge.AppCore.Ingest;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// Plan 015 wave 0 spike, and the highest-leverage test in the plan: build a <see cref="WebApplication"/>
/// in-process, call <see cref="StreamForgeApiExtensions.MapStreamForgeApi"/>, and read the resulting
/// <see cref="EndpointDataSource"/>. It never calls <c>Run()</c>, never binds a port and never starts a
/// silo — the whole REST surface's authorization metadata becomes a table-driven assertion, which today
/// is pinned by nothing at all.
///
/// <para><c>WebApplicationFactory</c> was declined: it needs both flavours' <c>Program.cs</c> startable
/// without their runtimes, which is a refactor of the two most dangerous files in the repo.</para>
/// </summary>
public class AuthorizationCoverageTests
{
    private static IReadOnlyList<Endpoint> BuildEndpoints()
    {
        var builder = WebApplication.CreateBuilder();
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Jwt:Key"] = new string('k', 64),
            ["Jwt:Issuer"] = "streamforge-test",
            ["Jwt:Audience"] = "streamforge-test",
        });
        builder.Services.AddStreamForgeApi(builder.Configuration);
        RegisterHandlerDependencies(builder.Services);

        var app = builder.Build();
        // No protos dir, no docs file, no SPA dist: those three route groups are host-specific and
        // deliberately out of the authorization surface this test pins.
        app.MapStreamForgeApi(new StreamForgeApiOptions(
            ProtosDir: Path.Combine(Path.GetTempPath(), "sf-authz-spike-protos"),
            GrpcPort: 0,
            GrpcStaticServices: [],
            DocsFilePath: null,
            SpaDistPath: null,
            Flavor: "test"));

        // The routes live on the WebApplication's own IEndpointRouteBuilder.DataSources until the
        // routing middleware folds them into the composite EndpointDataSource at Run() time — and Run()
        // is precisely what this test refuses to call. So read the data sources directly.
        return ((IEndpointRouteBuilder)app).DataSources.SelectMany(d => d.Endpoints).ToList();
    }

    /// <summary>Minimal-API parameter binding decides "service or request body?" by asking the service
    /// provider whether the type is registered — at MAP time, before any request exists. So every
    /// handler dependency has to be registered here or the endpoint fails to build; none of them is ever
    /// resolved, which is why a throwing factory is the honest registration (an accidental resolve
    /// becomes a loud failure rather than a null reference).</summary>
    private static void RegisterHandlerDependencies(IServiceCollection services)
    {
        var types = typeof(ICatalogFacade).Assembly.GetTypes()
            .Where(t => t.IsInterface && t.IsPublic && t.Name.EndsWith("Facade", StringComparison.Ordinal))
            .Concat(typeof(StreamForgeApiExtensions).Assembly.GetTypes()
                .Where(t => t.IsClass && t.IsPublic && !t.IsAbstract && !t.IsGenericType))
            .Concat(typeof(IngestKeyUsageTracker).Assembly.GetTypes()
                .Where(t => t.IsClass && t.IsPublic && !t.IsAbstract && !t.IsGenericType
                            && t.Name.EndsWith("Tracker", StringComparison.Ordinal)))
            .Distinct();

        foreach (var t in types)
        {
            services.AddSingleton(t, _ => throw new InvalidOperationException(
                $"{t.Name} was resolved — this test builds endpoints, it never serves a request."));
        }
    }

    [Fact]
    public void EveryApiEndpointIsEitherAuthorizedOrExplicitlyAnonymous()
    {
        var unguarded = BuildEndpoints()
            .OfType<RouteEndpoint>()
            .Where(e => e.RoutePattern.RawText?.StartsWith("/api/", StringComparison.Ordinal) == true)
            .Where(e => e.Metadata.GetMetadata<IAuthorizeData>() is null
                        && e.Metadata.GetMetadata<IAllowAnonymous>() is null)
            .Select(e => $"{string.Join(",", e.Metadata.GetMetadata<HttpMethodMetadata>()?.HttpMethods ?? [])} {e.RoutePattern.RawText}")
            .OrderBy(s => s, StringComparer.Ordinal)
            .ToList();

        Assert.True(unguarded.Count == 0,
            "Endpoints under /api/ with neither an authorize policy nor an explicit AllowAnonymous:\n  "
            + string.Join("\n  ", unguarded));
    }

    [Fact]
    public void TheApiSurfaceIsNotAccidentallyEmpty()
    {
        // Without this, the assertion above passes trivially the day MapStreamForgeApi stops mapping.
        var apiRoutes = BuildEndpoints()
            .OfType<RouteEndpoint>()
            .Count(e => e.RoutePattern.RawText?.StartsWith("/api/", StringComparison.Ordinal) == true);

        Assert.True(apiRoutes > 50, $"expected the whole REST surface, saw {apiRoutes} routes under /api/");
    }
}
