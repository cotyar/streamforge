using System.Reflection;
using System.Security.Claims;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using StreamForge.Abstractions;
using StreamForge.Api;
using StreamForge.Api.Auth;
using StreamForge.AppCore.Access;
using StreamForge.AppCore.Ingest;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// Plan 016 wave 1-C — <b>a source cannot be renamed, and this is the test that keeps it that way.</b>
///
/// <para>Plan 016 declines to give sources an id, and that decision rests entirely on one line:
/// <c>SourcesEndpoints</c>' <c>PUT /api/sources/{name}</c> does <c>def.Name = name;</c> before it
/// validates or persists anything, so the body's name is discarded and the route segment wins. Delete
/// that line and a source silently becomes renameable — while its name is still simultaneously its REST
/// route, its grain/actor key, its Orleans stream key, its entry in the SQL namespace, its
/// <c>EntitySchemas.SourceKey</c> field-number key, a member of every
/// <c>PipelineDefinition.SourceNames</c>, and every federated peer's <c>EntityKey</c>. None of those
/// move. The result would not be an error; it would be a fork, and a quiet one.</para>
///
/// <para><b>Run, not read.</b> The handler is invoked against a hand-built
/// <see cref="DefaultHttpContext"/> — the technique <c>CatalogEntitlementEndpointTests</c> established
/// (its own doc comment explains why <c>WebApplicationFactory</c> was declined for this repo). Reading
/// the route's metadata could never see the assignment; running it does. Entitlements are switched OFF
/// here (<c>Auth:Mode=legacy</c>'s behaviour) because this test is about the write, not the guard —
/// <c>CatalogEntitlementEndpointTests</c> owns the guard.</para>
///
/// <para>Verified live as well, against an isolated instance: <c>PUT /api/sources/trades</c> with
/// <c>"name":"renamed_src"</c> answered 200 with <c>"name":"trades"</c>, and
/// <c>GET /api/sources/renamed_src</c> answered 404.</para>
/// </summary>
public class SourceRenameInvariantTests
{
    // ---------------------------------------------------------------------------------------------
    // Harness — per-file by convention in this assembly (see the three existing copies).
    // ---------------------------------------------------------------------------------------------

    private static readonly JsonSerializerOptions JsonOpts = new(JsonSerializerDefaults.Web);

    private sealed class BodyAllowed : IHttpRequestBodyDetectionFeature
    {
        public bool CanHaveBody => true;
    }

    /// <summary>The compiler emits <c>&lt;Clone&gt;$</c> on records and nothing else. Registering a
    /// record in the container would make minimal API bind that parameter from the container instead of
    /// the request body — fatal for a test that actually PUTs something.</summary>
    private static bool IsRecord(Type t) =>
        t.GetMethod("<Clone>$", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic) is not null;

    private static (IReadOnlyList<Endpoint> Endpoints, IServiceProvider Services, FakeCatalog Catalog) Build()
    {
        var catalog = new FakeCatalog();
        catalog.Sources.Add(new SourceDefinition
        {
            Name = "trades",
            Kind = SourceKinds.Generator,
            GeneratorProfile = "trades",
            Fields = [new FieldDef("symbol", FieldType.String), new FieldDef("price", FieldType.Double)],
        });

        var builder = WebApplication.CreateBuilder();
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Jwt:Key"] = new string('k', 64),
            ["Jwt:Issuer"] = "streamforge-test",
            ["Jwt:Audience"] = "streamforge-test",
        });
        builder.Services.AddStreamForgeApi(builder.Configuration);

        foreach (var t in typeof(ICatalogFacade).Assembly.GetTypes()
                     .Where(t => t.IsInterface && t.IsPublic && t.Name.EndsWith("Facade", StringComparison.Ordinal))
                     .Concat(typeof(StreamForgeApiExtensions).Assembly.GetTypes()
                         .Where(t => t.IsClass && t.IsPublic && !t.IsAbstract && !t.IsGenericType && !IsRecord(t)))
                     .Concat(typeof(IngestKeyUsageTracker).Assembly.GetTypes()
                         .Where(t => t.IsClass && t.IsPublic && !t.IsAbstract && !t.IsGenericType
                                     && t.Name.EndsWith("Tracker", StringComparison.Ordinal)))
                     .Distinct())
        {
            var type = t;
            builder.Services.AddSingleton(type, _ => throw new InvalidOperationException(
                $"{type.Name} was resolved but this test never registered a real one."));
        }

        var policyFacade = new EmptyAccessPolicyFacade();
        builder.Services.AddSingleton<IAccessPolicyFacade>(policyFacade);
        builder.Services.AddSingleton(new PermissionResolver(policyFacade, NullLogger<PermissionResolver>.Instance, 600));
        builder.Services.AddSingleton(sp => new AccessGuard(sp.GetRequiredService<PermissionResolver>(), entitlementsEnabled: false));
        builder.Services.AddSingleton<ICatalogFacade>(catalog);
        builder.Services.AddSingleton(new IngestKeyUsageTracker());

        var app = builder.Build();
        app.MapStreamForgeApi(new StreamForgeApiOptions(
            ProtosDir: Path.Combine(Path.GetTempPath(), "sf-source-rename-protos"),
            GrpcPort: 0,
            GrpcStaticServices: [],
            DocsFilePath: null,
            SpaDistPath: null,
            Flavor: "test"));

        return ([.. ((IEndpointRouteBuilder)app).DataSources.SelectMany(d => d.Endpoints)], app.Services, catalog);
    }

    private static async Task<(int Status, string Body)> CallAsync(
        IReadOnlyList<Endpoint> endpoints, IServiceProvider services,
        string key, (string Name, string Value)[] routeValues, object? body)
    {
        var endpoint = endpoints.OfType<RouteEndpoint>().Single(e => KeyOf(e) == key);

        var http = new DefaultHttpContext
        {
            RequestServices = services,
            User = new ClaimsPrincipal(new ClaimsIdentity([new Claim(ClaimTypes.Name, "tester")], "test")),
        };
        var responseBody = new MemoryStream();
        http.Response.Body = responseBody;
        http.Features.Set<IHttpRequestBodyDetectionFeature>(new BodyAllowed());
        http.Request.Method = key.Split(' ')[0];
        http.Request.Path = key.Split(' ')[1];
        foreach (var (name, value) in routeValues)
        {
            http.Request.RouteValues[name] = value;
        }

        if (body is not null)
        {
            var json = JsonSerializer.SerializeToUtf8Bytes(body, JsonOpts);
            http.Request.Body = new MemoryStream(json);
            http.Request.ContentType = "application/json";
            http.Request.ContentLength = json.Length;
        }

        await endpoint.RequestDelegate!(http);
        responseBody.Position = 0;
        return (http.Response.StatusCode, new StreamReader(responseBody).ReadToEnd());
    }

    private static string KeyOf(RouteEndpoint endpoint)
    {
        var methods = endpoint.Metadata.GetMetadata<HttpMethodMetadata>()?.HttpMethods;
        var method = methods is null || methods.Count == 0 ? "(any)" : string.Join("|", methods);
        return $"{method} /{endpoint.RoutePattern.RawText?.TrimStart('/')}";
    }

    // ---------------------------------------------------------------------------------------------
    // The invariant.
    // ---------------------------------------------------------------------------------------------

    /// <summary>The one that must never start failing: a PUT whose body carries a DIFFERENT name updates
    /// the source named by the ROUTE, under that name. Nothing is created under the body's name.</summary>
    [Fact]
    public async Task PutSource_ignores_the_bodys_name_and_writes_under_the_route_name()
    {
        var (endpoints, services, catalog) = Build();

        var (status, responseBody) = await CallAsync(endpoints, services, "PUT /api/sources/{name}", [("name", "trades")],
            new SourceDefinition
            {
                Name = "renamed_by_the_body",
                Kind = SourceKinds.Generator,
                GeneratorProfile = "trades",
                EventsPerSecond = 5,
                Enabled = false,
                Fields = [new FieldDef("symbol", FieldType.String), new FieldDef("price", FieldType.Double)],
            });

        Assert.True(status == 200, $"expected 200, got {status}: {responseBody}");
        Assert.Contains("\"name\":\"trades\"", responseBody, StringComparison.Ordinal);
        Assert.DoesNotContain("renamed_by_the_body", responseBody, StringComparison.Ordinal);

        // What actually reached the catalog is the load-bearing half: the response could be masked or
        // re-read, the upsert cannot.
        var upserted = Assert.Single(catalog.Upserted);
        Assert.Equal("trades", upserted.Name);
    }

    /// <summary>The route itself is the second half of the invariant: a source is addressed by NAME, so
    /// there is no id-addressed PUT that could carry a new name in its body without contradiction.</summary>
    [Fact]
    public void The_only_source_write_routes_are_name_addressed()
    {
        var (endpoints, _, _) = Build();

        var sourceRoutes = endpoints.OfType<RouteEndpoint>()
            .Select(KeyOf)
            .Where(k => k.Contains("/api/sources", StringComparison.Ordinal))
            .Where(k => k.StartsWith("PUT ", StringComparison.Ordinal) || k.StartsWith("DELETE ", StringComparison.Ordinal))
            .ToList();

        Assert.NotEmpty(sourceRoutes);
        Assert.All(sourceRoutes, k => Assert.Contains("{name}", k, StringComparison.Ordinal));
        Assert.DoesNotContain(sourceRoutes, k => k.Contains("rename", StringComparison.OrdinalIgnoreCase));
    }

    // ---------------------------------------------------------------------------------------------
    // Fakes.
    // ---------------------------------------------------------------------------------------------

    private sealed class FakeCatalog : ICatalogFacade
    {
        public List<SourceDefinition> Sources { get; } = [];
        public List<SourceDefinition> Upserted { get; } = [];

        public Task<List<SourceDefinition>> GetSourcesAsync() => Task.FromResult(Sources);
        public Task<SourceDefinition?> GetSourceAsync(string name) =>
            Task.FromResult(Sources.FirstOrDefault(s => s.Name == name));

        public Task UpsertSourceAsync(SourceDefinition def)
        {
            Upserted.Add(def);
            var idx = Sources.FindIndex(s => s.Name == def.Name);
            if (idx >= 0)
            {
                Sources[idx] = def;
            }
            else
            {
                Sources.Add(def);
            }

            return Task.CompletedTask;
        }

        public Task<bool> DeleteSourceAsync(string name) => Task.FromResult(Sources.RemoveAll(s => s.Name == name) > 0);

        public Task<List<PipelineDefinition>> GetPipelinesAsync() => Task.FromResult(new List<PipelineDefinition>());
        public Task<PipelineDefinition?> GetPipelineAsync(string id) => Task.FromResult<PipelineDefinition?>(null);
        public Task<PipelineDefinition> CreatePipelineAsync(PipelineDefinition def) => throw new NotSupportedException();
        public Task<PipelineDefinition?> UpdatePipelineAsync(PipelineDefinition def) => throw new NotSupportedException();
        public Task<bool> DeletePipelineAsync(string id) => throw new NotSupportedException();
        public Task<PipelineDefinition?> SetPipelineStatusAsync(string id, PipelineStatus status) => throw new NotSupportedException();

        public Task<List<TableDefinition>> GetTablesAsync() => Task.FromResult(new List<TableDefinition>());
        public Task<TableDefinition?> GetTableAsync(string id) => Task.FromResult<TableDefinition?>(null);
        public Task<TableDefinition> CreateTableAsync(TableDefinition def) => throw new NotSupportedException();
        public Task<TableDefinition?> UpdateTableAsync(TableDefinition def) => throw new NotSupportedException();
        public Task<bool> DeleteTableAsync(string id) => throw new NotSupportedException();
        public Task<TableDefinition?> SetTableStatusAsync(string id, PipelineStatus status) => throw new NotSupportedException();

        public Task<string> EnsureFieldNumbersAsync(string entityKey, List<FieldDef> fields) => Task.FromResult("{}");
        public Task<ScenarioRunResult> RunSourceAsync(string name, ScenarioRunRequest request) => throw new NotSupportedException();
    }

    private sealed class EmptyAccessPolicyFacade : IAccessPolicyFacade
    {
        private readonly AccessPolicyDocument _document = new();

        public Task<long> GetVersionAsync() => Task.FromResult(_document.Version);
        public Task<AccessPolicyDocument> GetPolicyAsync() => Task.FromResult(_document);
        public Task<RoleDefinition?> UpsertRoleAsync(RoleDefinition role, string actor) => throw new NotSupportedException();
        public Task<bool> DeleteRoleAsync(string name) => throw new NotSupportedException();
        public Task<GroupDefinition?> UpsertGroupAsync(GroupDefinition group, string actor) => throw new NotSupportedException();
        public Task<bool> DeleteGroupAsync(string name) => throw new NotSupportedException();
        public Task<UserAccessEntry?> UpsertUserAccessAsync(UserAccessEntry entry, string actor) => throw new NotSupportedException();
        public Task<bool> DeleteUserAccessAsync(string username) => throw new NotSupportedException();
        public Task<ApprovalTemplate?> UpsertApprovalTemplateAsync(ApprovalTemplate template, string actor) => throw new NotSupportedException();
        public Task<bool> DeleteApprovalTemplateAsync(string name) => throw new NotSupportedException();
    }
}
