using System.Reflection;
using System.Security.Claims;
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
using StreamForge.AppCore;
using StreamForge.AppCore.Access;
using StreamForge.AppCore.Ingest;
using StreamForge.Host.Grpc;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// Plan 016 wave 1 — the id-or-name rule as the REST and gRPC surfaces actually apply it, run through
/// the real mapped handlers (the in-process technique <c>CatalogEntitlementEndpointTests</c>
/// established: build and map a <see cref="WebApplication"/>, never <c>Run()</c> it, and invoke one
/// endpoint's <see cref="RouteEndpoint.RequestDelegate"/> against a hand-built context).
///
/// <para>Three things are pinned here, and the third is the one worth having:</para>
/// <list type="number">
/// <item>a read route resolves an id AND a name, where it used to take only an id;</item>
/// <item>two entities sharing the queried name is <b>409</b> naming both candidate ids — not a 404
/// (which claims nothing exists when two do) and not the silent first-wins the table routes used to
/// do;</item>
/// <item><b>an unentitled caller gets 403 for that same request, and the response never names the
/// candidates.</b> The 409 must be emitted AFTER the entitlement guard; emitting it first would make
/// every ambiguous name a catalog-enumeration oracle on the routes whose whole purpose is to be
/// scoped.</item>
/// </list>
/// </summary>
public class EntityRefRouteTests
{
    // -------------------------------------------------------------------------------------------
    // Fixture.
    // -------------------------------------------------------------------------------------------

    /// <summary>Ids that look nothing like the names, so an assertion can tell which one answered.</summary>
    private static FakeReadCatalog Catalog() => new()
    {
        Tables =
        [
            new TableDefinition { Id = "t-aaaa", Name = "dev-positions", Tags = ["sandbox"] },
            new TableDefinition { Id = "t-bbbb", Name = "prod-positions", Tags = ["finance"] },
        ],
        Pipelines =
        [
            new PipelineDefinition { Id = "p-1111", Name = "dev-enrich", Tags = ["sandbox"] },
            new PipelineDefinition { Id = "p-2222", Name = "prod-enrich", Tags = ["finance"] },
        ],
    };

    /// <summary>The same catalog with a DUPLICATE name — the state plan 016 exists to answer for. Two
    /// tables cannot actually reach this through the write path today (the registry rejects a colliding
    /// table name ordinally), which is exactly why the branch needs a test: it is a dead branch on the
    /// table routes and a live one on the pipeline routes.</summary>
    private static FakeReadCatalog DuplicateCatalog() => new()
    {
        Tables =
        [
            new TableDefinition { Id = "t-aaaa", Name = "twins", Tags = ["finance"] },
            new TableDefinition { Id = "t-bbbb", Name = "twins", Tags = ["finance"] },
        ],
        Pipelines =
        [
            new PipelineDefinition { Id = "p-1111", Name = "twins", Tags = ["finance"] },
            new PipelineDefinition { Id = "p-2222", Name = "twins", Tags = ["finance"] },
        ],
    };

    private static AccessPolicyDocument Document(params UserAccessEntry[] users)
    {
        var document = new AccessPolicyDocument { Roles = BuiltInRoleCatalog.Create(), Version = 1 };
        document.Users.AddRange(users);
        return document;
    }

    private static UserAccessEntry User(string username, params PermissionGrant[] grants) =>
        new() { Username = username, Grants = [.. grants] };

    private static PermissionGrant Allow(string action, string scope) =>
        new() { Action = action, Scope = scope };

    // -------------------------------------------------------------------------------------------
    // 1. Id-or-name on read routes.
    // -------------------------------------------------------------------------------------------

    [Theory]
    [InlineData("t-bbbb")]      // by id, as before
    [InlineData("prod-positions")] // by NAME — plan 016 wave 1
    public async Task TableReadRouteResolvesByIdAndByName(string query)
    {
        var harness = Build(Document(User("ops", Allow(Actions.TableRead, "*"))), Catalog());

        var (status, body) = await harness.CallAsync(
            "GET /api/tables/{id}", Principal("ops"), [("id", query)]);

        Assert.Equal(200, status);
        Assert.Contains("prod-positions", body, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("p-2222")]
    [InlineData("prod-enrich")]
    public async Task PipelineReadRouteResolvesByIdAndByName(string query)
    {
        var harness = Build(Document(User("ops", Allow(Actions.PipelineRead, "*"))), Catalog());

        var (status, body) = await harness.CallAsync(
            "GET /api/pipelines/{id}", Principal("ops"), [("id", query)]);

        Assert.Equal(200, status);
        Assert.Contains("prod-enrich", body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AnUnknownNameIsStillNotFound()
    {
        var harness = Build(Document(User("ops", Allow(Actions.TableRead, "*"))), Catalog());

        var (status, _) = await harness.CallAsync(
            "GET /api/tables/{id}", Principal("ops"), [("id", "no-such-table")]);

        Assert.Equal(404, status);
    }

    /// <summary>The rule is ORDINAL and exact — no case-insensitive or prefix matching, because the
    /// registry builds the SQL namespace with ordinal dictionaries and a looser resolver would let
    /// <c>GET /api/tables/Prod-Positions</c> and <c>FROM prod_positions</c> disagree.</summary>
    [Theory]
    [InlineData("PROD-POSITIONS")]
    [InlineData("prod-")]
    public async Task NameMatchingIsOrdinalAndExact(string query)
    {
        var harness = Build(Document(User("ops", Allow(Actions.TableRead, "*"))), Catalog());

        var (status, _) = await harness.CallAsync(
            "GET /api/tables/{id}", Principal("ops"), [("id", query)]);

        Assert.Equal(404, status);
    }

    // -------------------------------------------------------------------------------------------
    // 2. Ambiguity is 409, naming both ids.
    // -------------------------------------------------------------------------------------------

    [Fact]
    public async Task TwoTablesOfOneNameAreAConflictNamingBothIds()
    {
        var harness = Build(Document(User("ops", Allow(Actions.TableRead, "*"))), DuplicateCatalog());

        var (status, body) = await harness.CallAsync(
            "GET /api/tables/{id}", Principal("ops"), [("id", "twins")]);

        Assert.Equal(409, status);
        Assert.Contains("t-aaaa", body, StringComparison.Ordinal);
        Assert.Contains("t-bbbb", body, StringComparison.Ordinal);
    }

    /// <summary>Was a 404 before plan 016 — the one live status-code change in this wave, because
    /// pipeline names are unenforced today and duplicates already exist in real catalogs.</summary>
    [Fact]
    public async Task TwoPipelinesOfOneNameAreAConflictNotANotFound()
    {
        var harness = Build(Document(User("ops", Allow(Actions.PipelineRead, "*"))), DuplicateCatalog());

        var (status, body) = await harness.CallAsync(
            "GET /api/pipelines/{id}", Principal("ops"), [("id", "twins")]);

        Assert.Equal(409, status);
        Assert.Contains("p-1111", body, StringComparison.Ordinal);
        Assert.Contains("p-2222", body, StringComparison.Ordinal);
    }

    // -------------------------------------------------------------------------------------------
    // 3. THE ORDERING. The guard runs before the 409.
    // -------------------------------------------------------------------------------------------

    [Fact]
    public async Task AnUnentitledCallerGets403AndLearnsNoCandidateIds()
    {
        // Entitled to read a DIFFERENT scope, so the guard is genuinely consulted and genuinely says no.
        var harness = Build(
            Document(User("nosy", Allow(Actions.TableRead, "something-else"))), DuplicateCatalog());

        var (status, body) = await harness.CallAsync(
            "GET /api/tables/{id}", Principal("nosy"), [("id", "twins")]);

        Assert.Equal(403, status);
        Assert.DoesNotContain("t-aaaa", body, StringComparison.Ordinal);
        Assert.DoesNotContain("t-bbbb", body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AnUnentitledCallerGets403OnAnAmbiguousPipelineToo()
    {
        var harness = Build(
            Document(User("nosy", Allow(Actions.PipelineRead, "something-else"))), DuplicateCatalog());

        var (status, body) = await harness.CallAsync(
            "GET /api/pipelines/{id}", Principal("nosy"), [("id", "twins")]);

        Assert.Equal(403, status);
        Assert.DoesNotContain("p-1111", body, StringComparison.Ordinal);
        Assert.DoesNotContain("p-2222", body, StringComparison.Ordinal);
    }

    // -------------------------------------------------------------------------------------------
    // 4. The gRPC side resolves by the same rule (its status mapping needs a ServerCallContext, so
    //    what is pinned here is the resolution itself — FailedPrecondition-vs-NotFound lives in
    //    GrpcEntityRef.RequireAsync and is exercised live, see the plan's verification notes).
    // -------------------------------------------------------------------------------------------

    [Fact]
    public async Task GrpcResolvesByIdByNameAndReportsAmbiguity()
    {
        var registry = new FakeRegistryGrain();
        registry.Tables.Add(new TableDefinition { Id = "t-aaaa", Name = "twins" });
        registry.Tables.Add(new TableDefinition { Id = "t-bbbb", Name = "twins" });
        registry.Tables.Add(new TableDefinition { Id = "t-cccc", Name = "unique" });

        Assert.Equal("t-aaaa", (await GrpcEntityRef.TableAsync(registry, "t-aaaa")).Value?.Id);
        Assert.Equal("t-cccc", (await GrpcEntityRef.TableAsync(registry, "unique")).Value?.Id);

        var ambiguous = await GrpcEntityRef.TableAsync(registry, "twins");
        Assert.Equal(EntityRefOutcome.Ambiguous, ambiguous.Outcome);
        Assert.Equal(["t-aaaa", "t-bbbb"], ambiguous.CandidateIds);

        Assert.Equal(EntityRefOutcome.NotFound, (await GrpcEntityRef.TableAsync(registry, "nope")).Outcome);
    }

    // -------------------------------------------------------------------------------------------
    // Harness — the same in-process shape CatalogEntitlementEndpointTests uses, trimmed to the two
    // facades these routes touch.
    // -------------------------------------------------------------------------------------------

    private sealed class Harness(IReadOnlyList<Endpoint> endpoints, IServiceProvider services)
    {
        public async Task<(int Status, string Body)> CallAsync(
            string key, ClaimsPrincipal user, (string Name, string Value)[]? routeValues = null)
        {
            var endpoint = endpoints.OfType<RouteEndpoint>().Single(e => KeyOf(e) == key);

            var http = new DefaultHttpContext { RequestServices = services, User = user };
            var responseBody = new MemoryStream();
            http.Response.Body = responseBody;
            http.Features.Set<IHttpRequestBodyDetectionFeature>(new BodyAllowed());
            http.Request.Method = key.Split(' ')[0];
            http.Request.Path = key.Split(' ')[1];
            foreach (var (name, value) in routeValues ?? [])
            {
                http.Request.RouteValues[name] = value;
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
    }

    private static Harness Build(AccessPolicyDocument document, FakeReadCatalog catalog)
    {
        var builder = WebApplication.CreateBuilder();
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Jwt:Key"] = new string('k', 64),
            ["Jwt:Issuer"] = "streamforge-test",
            ["Jwt:Audience"] = "streamforge-test",
            ["Auth:PolicyCacheSeconds"] = "600",
        });
        builder.Services.AddStreamForgeApi(builder.Configuration);

        // Minimal API decides "service or body?" at MAP time by asking the container, so every facade a
        // handler names has to resolve. A DispatchProxy that throws on the first METHOD call is the
        // honest stand-in: being handed a facade is fine, reaching for its data is the failure — and no
        // route under test here should ever get that far.
        foreach (var iface in typeof(ICatalogFacade).Assembly.GetTypes()
                     .Where(t => t.IsInterface && t.IsPublic && t.Name.EndsWith("Facade", StringComparison.Ordinal)))
        {
            var captured = iface;
            builder.Services.AddSingleton(captured, _ => DispatchProxy.Create(captured, typeof(UntouchedFacade)));
        }

        foreach (var t in typeof(StreamForgeApiExtensions).Assembly.GetTypes()
                     .Where(t => t.IsClass && t.IsPublic && !t.IsAbstract && !t.IsGenericType && !IsRecord(t)))
        {
            var captured = t;
            builder.Services.AddSingleton(captured, _ => throw new InvalidOperationException(
                $"{captured.Name} was resolved but this test never registered a real one."));
        }

        var policyFacade = new FrozenPolicyFacade(document);
        var resolver = new PermissionResolver(policyFacade, NullLogger<PermissionResolver>.Instance, 600);
        builder.Services.AddSingleton<IAccessPolicyFacade>(policyFacade);
        builder.Services.AddSingleton(resolver);
        builder.Services.AddSingleton(new AccessGuard(resolver, entitlementsEnabled: true));
        builder.Services.AddSingleton<ICatalogFacade>(catalog);
        // Named directly by a source route's handler signature, so minimal API must be able to resolve
        // it at MAP time even though nothing in this file calls it.
        builder.Services.AddSingleton(new IngestKeyUsageTracker());

        var app = builder.Build();
        app.MapStreamForgeApi(new StreamForgeApiOptions(
            ProtosDir: Path.Combine(Path.GetTempPath(), "sf-entityref-protos"),
            GrpcPort: 0,
            GrpcStaticServices: [],
            DocsFilePath: null,
            SpaDistPath: null,
            Flavor: "test"));

        return new Harness([.. ((IEndpointRouteBuilder)app).DataSources.SelectMany(d => d.Endpoints)], app.Services);
    }

    public class UntouchedFacade : DispatchProxy
    {
        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args) =>
            throw new InvalidOperationException(
                $"{targetMethod?.DeclaringType?.Name}.{targetMethod?.Name} was CALLED — no route in this " +
                "file should reach a facade other than the catalog.");
    }

    private sealed class BodyAllowed : IHttpRequestBodyDetectionFeature
    {
        public bool CanHaveBody => true;
    }

    private static bool IsRecord(Type t) =>
        t.GetMethod("<Clone>$", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic) is not null;

    private static ClaimsPrincipal Principal(string name) => PermissionResolverTests.Principal(name);

    /// <summary>Only the reads these routes make; everything else refuses, so a handler that grew a new
    /// dependency shows up as a failure rather than a plausible-looking pass.</summary>
    private sealed class FakeReadCatalog : ICatalogFacade
    {
        public List<SourceDefinition> Sources { get; init; } = [];
        public List<PipelineDefinition> Pipelines { get; init; } = [];
        public List<TableDefinition> Tables { get; init; } = [];

        public Task<List<SourceDefinition>> GetSourcesAsync() => Task.FromResult(Sources);
        public Task<SourceDefinition?> GetSourceAsync(string name) =>
            Task.FromResult(Sources.FirstOrDefault(s => s.Name == name));
        public Task UpsertSourceAsync(SourceDefinition def) => throw new NotSupportedException();
        public Task<bool> DeleteSourceAsync(string name) => throw new NotSupportedException();

        public Task<List<PipelineDefinition>> GetPipelinesAsync() => Task.FromResult(Pipelines);

        // Id-only, exactly like RegistryGrain/CatalogStore — the point of the fast path under test.
        public Task<PipelineDefinition?> GetPipelineAsync(string id) =>
            Task.FromResult(Pipelines.FirstOrDefault(p => p.Id == id));
        public Task<PipelineDefinition> CreatePipelineAsync(PipelineDefinition def) => throw new NotSupportedException();
        public Task<PipelineDefinition?> UpdatePipelineAsync(PipelineDefinition def) => throw new NotSupportedException();
        public Task<bool> DeletePipelineAsync(string id) => throw new NotSupportedException();
        public Task<PipelineDefinition?> SetPipelineStatusAsync(string id, PipelineStatus status) => throw new NotSupportedException();

        public Task<List<TableDefinition>> GetTablesAsync() => Task.FromResult(Tables);
        public Task<TableDefinition?> GetTableAsync(string id) =>
            Task.FromResult(Tables.FirstOrDefault(t => t.Id == id));
        public Task<TableDefinition> CreateTableAsync(TableDefinition def) => throw new NotSupportedException();
        public Task<TableDefinition?> UpdateTableAsync(TableDefinition def) => throw new NotSupportedException();
        public Task<bool> DeleteTableAsync(string id) => throw new NotSupportedException();
        public Task<TableDefinition?> SetTableStatusAsync(string id, PipelineStatus status) => throw new NotSupportedException();
        public Task<string> EnsureFieldNumbersAsync(string entityKey, List<FieldDef> fields) => Task.FromResult("{}");
        public Task<ScenarioRunResult> RunSourceAsync(string name, ScenarioRunRequest request) => throw new NotSupportedException();
    }

    private sealed class FrozenPolicyFacade(AccessPolicyDocument document) : IAccessPolicyFacade
    {
        public Task<long> GetVersionAsync() => Task.FromResult(document.Version);
        public Task<AccessPolicyDocument> GetPolicyAsync() => Task.FromResult(document);
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
