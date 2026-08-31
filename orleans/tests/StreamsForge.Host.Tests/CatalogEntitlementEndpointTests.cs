using System.Net;
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
using StreamsForge.Abstractions;
using StreamsForge.Api;
using StreamsForge.Api.Auth;
using StreamsForge.AppCore.Access;
using StreamsForge.AppCore.Ingest;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>
/// Plan 015 wave 3-A — the catalog REST surface actually enforcing entitlements, tested by RUNNING the
/// handlers rather than by reading their metadata.
///
/// <para><b>How, without a server.</b> <see cref="AuthorizationCoverageTests"/> established that a
/// <see cref="WebApplication"/> can be built, mapped and read in-process without <c>Run()</c>, a port or
/// a silo. This test goes one step further and INVOKES the mapped
/// <see cref="RouteEndpoint.RequestDelegate"/> against a hand-built <see cref="DefaultHttpContext"/>.
/// That runs the real handler — real minimal-API parameter binding, the real
/// <see cref="AccessGuard"/>, the real <see cref="PermissionEvaluator"/> — and therefore proves the
/// thing the metadata test structurally cannot: that the guard is CALLED, with the scope and the tags of
/// the entity the route is about. Nothing else in this repo can prove that short of
/// <c>tools/authz-matrix.sh</c>, which starts a whole host and can only speak in terms of the three
/// legacy roles.</para>
///
/// <para>Authorization MIDDLEWARE is not in an endpoint's RequestDelegate — the coarse
/// <c>RequireAuthorization("Viewer"/"Editor")</c> policy is metadata that the pipeline applies earlier.
/// That is exactly right here: these tests are about the second gate, the in-handler one, and
/// <c>AuthorizationCoverageTests</c> plus <c>AccessPolicyWiringTests</c> already own the first.</para>
/// </summary>
public class CatalogEntitlementEndpointTests
{
    // ---------------------------------------------------------------------------------------------
    // The fixture: one small catalog, one access document, and a way to call a route.
    // ---------------------------------------------------------------------------------------------

    /// <summary>Two of each entity type, deliberately named so that a <c>dev-*</c> / <c>prod-*</c>
    /// prefix scope separates them, and tagged so a <c>tag:</c> scope separates them differently. The
    /// two axes must not coincide, or a test could pass for the wrong reason.</summary>
    private static FakeCatalog Catalog() => new()
    {
        Sources =
        [
            new SourceDefinition { Name = "dev-trades", Tags = ["sandbox"] },
            new SourceDefinition { Name = "prod-trades", Tags = ["finance"] },
        ],
        Pipelines =
        [
            new PipelineDefinition { Id = "1111", Name = "dev-enrich", Tags = ["sandbox"] },
            new PipelineDefinition { Id = "2222", Name = "prod-enrich", Tags = ["finance"] },
        ],
        Tables =
        [
            new TableDefinition { Id = "aaaa", Name = "dev-positions", Tags = ["sandbox"] },
            new TableDefinition { Id = "bbbb", Name = "prod-positions", Tags = ["finance"] },
        ],
    };

    /// <summary>A migrated access document (the three built-in roles present) plus whatever per-user
    /// entries a test adds.</summary>
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

    private sealed class Harness(IReadOnlyList<Endpoint> endpoints, IServiceProvider services)
    {
        public FakeCatalog Catalog { get; init; } = null!;
        public FakeIngress Ingress { get; init; } = null!;

        /// <summary>Call one mapped route. <paramref name="key"/> is the same
        /// <c>"METHOD /pattern"</c> string <see cref="AuthorizationCoverageTests"/> keys its table by,
        /// so a route renamed here fails loudly rather than silently matching nothing.</summary>
        public async Task<(int Status, string Body)> CallAsync(
            string key,
            ClaimsPrincipal user,
            (string Name, string Value)[]? routeValues = null,
            object? body = null,
            (string Name, string Value)[]? headers = null)
        {
            var endpoint = endpoints.OfType<RouteEndpoint>().Single(e => KeyOf(e) == key);

            var http = new DefaultHttpContext { RequestServices = services, User = user };
            var responseBody = new MemoryStream();
            http.Response.Body = responseBody;
            // Without this, minimal API's body binder finds no IHttpRequestBodyDetectionFeature, decides
            // the request cannot have a body at all, and answers 400 before the handler ever runs — a
            // DefaultHttpContext carries no such feature. Found the hard way.
            http.Features.Set<IHttpRequestBodyDetectionFeature>(new BodyAllowed());

            var (method, pattern) = (key.Split(' ')[0], key.Split(' ')[1]);
            http.Request.Method = method;
            http.Request.Path = pattern;
            foreach (var (name, value) in routeValues ?? [])
            {
                http.Request.RouteValues[name] = value;
            }

            foreach (var (name, value) in headers ?? [])
            {
                http.Request.Headers[name] = value;
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
    }

    private static readonly JsonSerializerOptions JsonOpts =
        new(JsonSerializerDefaults.Web);

    private static Harness Build(AccessPolicyDocument document, FakeCatalog? catalog = null)
    {
        catalog ??= Catalog();
        var ingress = new FakeIngress();

        var builder = WebApplication.CreateBuilder();
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Jwt:Key"] = new string('k', 64),
            ["Jwt:Issuer"] = "streamsforge-test",
            ["Jwt:Audience"] = "streamsforge-test",
            // Long TTL: these tests are about the decision, not the refresh cadence.
            ["Auth:PolicyCacheSeconds"] = "600",
        });
        builder.Services.AddStreamsForgeApi(builder.Configuration);

        // Same trick as AuthorizationCoverageTests: minimal-API binding decides "service or body?" at
        // MAP time by asking the container, so every handler dependency has to be registered. A
        // throwing factory is the honest default — anything a test actually needs is re-registered
        // below, and MS DI resolves the LAST registration, so the real ones win.
        //
        // …with one difference that only matters because this test RUNS the handlers: RECORDS are
        // skipped. Every request DTO in StreamsForge.Api is a record, and registering one makes minimal
        // API bind that parameter from the container instead of the request body — harmless when you
        // only ever read metadata, fatal when you actually POST something.
        // Facade INTERFACES get a DispatchProxy stub rather than a throwing factory, because minimal
        // API resolves every service parameter of a handler BEFORE running its body — a factory that
        // threw would take down a route whose whole assertion is that it refuses before touching a
        // facade. The proxy throws on the first METHOD call instead, which is the honest line: reaching
        // for the data is the failure, being handed the facade is not.
        foreach (var t in typeof(ICatalogFacade).Assembly.GetTypes()
                     .Where(t => t.IsInterface && t.IsPublic && t.Name.EndsWith("Facade", StringComparison.Ordinal)))
        {
            var iface = t;
            builder.Services.AddSingleton(iface, _ => UntouchableProxy(iface));
        }

        foreach (var t in typeof(StreamsForgeApiExtensions).Assembly.GetTypes()
                     .Where(t => t.IsClass && t.IsPublic && !t.IsAbstract && !t.IsGenericType && !IsRecord(t))
                     .Concat(typeof(IngestKeyUsageTracker).Assembly.GetTypes()
                         .Where(t => t.IsClass && t.IsPublic && !t.IsAbstract && !t.IsGenericType
                                     && t.Name.EndsWith("Tracker", StringComparison.Ordinal)))
                     .Distinct())
        {
            services_AddThrowing(builder.Services, t);
        }

        var policyFacade = new StaticAccessPolicyFacade(document);
        var resolver = new PermissionResolver(policyFacade, NullLogger<PermissionResolver>.Instance, 600);

        builder.Services.AddSingleton<IAccessPolicyFacade>(policyFacade);
        builder.Services.AddSingleton(resolver);
        builder.Services.AddSingleton(new AccessGuard(resolver, entitlementsEnabled: true));
        builder.Services.AddSingleton<ICatalogFacade>(catalog);
        builder.Services.AddSingleton<IIngressFacade>(ingress);
        builder.Services.AddSingleton(new IngestKeyUsageTracker());

        var app = builder.Build();
        app.MapStreamsForgeApi(new StreamsForgeApiOptions(
            ProtosDir: Path.Combine(Path.GetTempPath(), "sf-entitlement-protos"),
            GrpcPort: 0,
            GrpcStaticServices: [],
            DocsFilePath: null,
            SpaDistPath: null,
            Flavor: "test"));

        return new Harness(
            [.. ((IEndpointRouteBuilder)app).DataSources.SelectMany(d => d.Endpoints)],
            app.Services)
        {
            Catalog = catalog,
            Ingress = ingress,
        };
    }

    /// <summary>An interface implementation that exists only to be handed to a handler and never
    /// called. <see cref="DispatchProxy"/> is BCL — no mocking library enters the repo for this.</summary>
    private static object UntouchableProxy(Type interfaceType) =>
        DispatchProxy.Create(interfaceType, typeof(UntouchedFacade));

    public class UntouchedFacade : DispatchProxy
    {
        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args) =>
            throw new InvalidOperationException(
                $"{targetMethod?.DeclaringType?.Name}.{targetMethod?.Name} was CALLED. The route under " +
                "test was expected to refuse before reading anything; register a real fake if it should not.");
    }

    private sealed class BodyAllowed : IHttpRequestBodyDetectionFeature
    {
        public bool CanHaveBody => true;
    }

    /// <summary>The compiler emits a <c>&lt;Clone&gt;$</c> method on every record and on nothing else,
    /// which is the only reliable runtime marker C# gives for "this was declared as a record".</summary>
    private static bool IsRecord(Type t) =>
        t.GetMethod("<Clone>$", System.Reflection.BindingFlags.Instance
                                | System.Reflection.BindingFlags.Public
                                | System.Reflection.BindingFlags.NonPublic) is not null;

    private static void services_AddThrowing(IServiceCollection services, Type t) =>
        services.AddSingleton(t, _ => throw new InvalidOperationException(
            $"{t.Name} was resolved but this test never registered a real one."));

    private static ClaimsPrincipal Principal(string name, string? role = null) =>
        PermissionResolverTests.Principal(name, role);

    // ---------------------------------------------------------------------------------------------
    // 1. A scope-limited entitlement is honoured — the whole point of the wave.
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task APrefixScopedReadIsAllowedOnDevAndRefusedOnProd()
    {
        var harness = Build(Document(User("dev", Allow(Actions.TableRead, "dev-*"))));
        var dev = Principal("dev");

        var (okStatus, okBody) = await harness.CallAsync(
            "GET /api/tables/{id}", dev, [("id", "aaaa")]);
        Assert.Equal(200, okStatus);
        Assert.Contains("dev-positions", okBody, StringComparison.Ordinal);

        var (deniedStatus, deniedBody) = await harness.CallAsync(
            "GET /api/tables/{id}", dev, [("id", "bbbb")]);
        Assert.Equal((int)HttpStatusCode.Forbidden, deniedStatus);
        // The reason, not a bare 403 — an operator staring at this has to know WHY.
        Assert.Contains("prod-positions", deniedBody, StringComparison.Ordinal);
        Assert.DoesNotContain("dev-positions", deniedBody, StringComparison.Ordinal);
    }

    /// <summary>The same entitlement, on the WRITE half — proving the actions are not one bundle. A
    /// <c>table.read</c> on <c>dev-*</c> does not delete a dev table, and a <c>table.delete</c> on
    /// <c>dev-*</c> does not delete a prod one.</summary>
    [Fact]
    public async Task ScopeAndActionAreBothEnforcedOnAMutatingRoute()
    {
        var harness = Build(Document(User(
            "dev",
            Allow(Actions.TableRead, "*"),
            Allow(Actions.TableDelete, "dev-*"))));
        var dev = Principal("dev");

        Assert.Equal(403,
            (await harness.CallAsync("DELETE /api/tables/{id}", dev, [("id", "bbbb")])).Status);
        Assert.Empty(harness.Catalog.DeletedTables);

        Assert.Equal(204,
            (await harness.CallAsync("DELETE /api/tables/{id}", dev, [("id", "aaaa")])).Status);
        Assert.Equal(["aaaa"], harness.Catalog.DeletedTables);
    }

    /// <summary>A control action is its own action: "may start and stop" is a different grant from "may
    /// edit", which is most of why <see cref="Actions"/> has four verbs per entity and not two.</summary>
    [Fact]
    public async Task ControlIsSeparableFromWrite()
    {
        var harness = Build(Document(User(
            "ops",
            Allow(Actions.PipelineRead, "*"),
            Allow(Actions.PipelineControl, "prod-*"))));
        var ops = Principal("ops");

        Assert.Equal(200,
            (await harness.CallAsync("POST /api/pipelines/{id}/stop", ops, [("id", "2222")])).Status);

        // …but editing it is refused, from the same token, on the same entity.
        var edit = await harness.CallAsync(
            "PUT /api/pipelines/{id}", ops, [("id", "2222")],
            new { name = "prod-enrich", description = "", sql = "SELECT 1" });
        Assert.Equal(403, edit.Status);
    }

    // ---------------------------------------------------------------------------------------------
    // 2. A tag-scoped entitlement works — which is only true if the handler passes the entity's Tags.
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task ATagScopedEntitlementMatchesTheEntitysOwnTags()
    {
        var harness = Build(Document(User("fin", Allow(Actions.PipelineRead, "tag:finance"))));
        var fin = Principal("fin");

        // prod-enrich carries Tags = ["finance"]; dev-enrich carries ["sandbox"]. Note the tag axis
        // and the name axis are deliberately different partitions of the same two entities, so this
        // cannot pass because of the prefix.
        Assert.Equal(200,
            (await harness.CallAsync("GET /api/pipelines/{id}", fin, [("id", "2222")])).Status);
        Assert.Equal(403,
            (await harness.CallAsync("GET /api/pipelines/{id}", fin, [("id", "1111")])).Status);
    }

    /// <summary>Tags reach the guard on the sub-resource routes too, not only on <c>GET /{id}</c> —
    /// those are the routes that serve the actual DATA, so a tag scope that stopped at the definition
    /// would be decorative.</summary>
    [Fact]
    public async Task TagScopeReachesTheDataRoutesAndNotOnlyTheDefinition()
    {
        var harness = Build(Document(User("fin", Allow(Actions.SourceRead, "tag:finance"))));
        var fin = Principal("fin");

        Assert.Equal(403,
            (await harness.CallAsync("GET /api/sources/{name}/status", fin, [("name", "dev-trades")])).Status);
        Assert.Equal(403,
            (await harness.CallAsync("GET /api/sources/{name}/openapi.json", fin, [("name", "dev-trades")])).Status);
    }

    // ---------------------------------------------------------------------------------------------
    // 3. List routes FILTER; they do not refuse.
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task ListRoutesReturnTheEntriesTheCallerMaySeeRatherThanA403()
    {
        var harness = Build(Document(User("dev", Allow(Actions.SourceRead, "dev-*"))));

        var (status, body) = await harness.CallAsync("GET /api/sources/", Principal("dev"));

        Assert.Equal(200, status);
        Assert.Contains("dev-trades", body, StringComparison.Ordinal);
        Assert.DoesNotContain("prod-trades", body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AListForACallerEntitledToNothingIsEmptyAndNotForbidden()
    {
        // Empty is the same statement as 403 and it is the one a console can render. It also keeps the
        // page that lists three entity types from failing whole because one of them is off limits.
        var harness = Build(Document(User("nobody")));

        var (status, body) = await harness.CallAsync("GET /api/tables/", Principal("nobody"));

        Assert.Equal(200, status);
        Assert.Equal("[]", body);
    }

    [Fact]
    public async Task ADenyGrantRemovesJustThatEntryFromTheListing()
    {
        var harness = Build(Document(User(
            "most",
            Allow(Actions.PipelineRead, "*"),
            new PermissionGrant
            {
                Action = Actions.PipelineRead,
                Scope = "tag:finance",
                Effect = PermissionEffect.Deny,
                Note = "frozen for the close",
            })));

        var (status, body) = await harness.CallAsync("GET /api/pipelines/", Principal("most"));

        Assert.Equal(200, status);
        Assert.Contains("dev-enrich", body, StringComparison.Ordinal);
        Assert.DoesNotContain("prod-enrich", body, StringComparison.Ordinal);
    }

    // ---------------------------------------------------------------------------------------------
    // 4. RequiresApproval is refused with its OWN reason — never collapsed into a plain denial.
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task RequiresApprovalIsRefusedWithItsOwnReasonUntilWavesFourAndFive()
    {
        var harness = Build(Document(User("carol", new PermissionGrant
        {
            Action = Actions.TableWrite,
            Scope = "prod-*",
            RequiresApproval = true,
        })));

        var (status, body) = await harness.CallAsync(
            "PUT /api/tables/{id}", Principal("carol"), [("id", "bbbb")],
            new { name = "prod-positions", description = "", sql = "SELECT 1" });

        // Refused — fail-closed — but the body says why, so the SPA can one day render "Request
        // approval…" from it rather than "Forbidden".
        Assert.Equal(403, status);
        Assert.Contains("requires approval", body, StringComparison.Ordinal);
        Assert.Empty(harness.Catalog.UpdatedTables);
    }

    // ---------------------------------------------------------------------------------------------
    // 5. Backward compatibility: the three legacy roles still get exactly what they got.
    // ---------------------------------------------------------------------------------------------

    [Theory]
    [InlineData(BuiltInRoles.Viewer)]
    [InlineData(BuiltInRoles.Editor)]
    [InlineData(BuiltInRoles.Admin)]
    public async Task ABuiltInRoleSeesTheWholeCatalogUnfiltered(string role)
    {
        var harness = Build(Document(new UserAccessEntry { Username = "u", Roles = [role] }));
        var user = Principal("u", role);

        foreach (var listRoute in new[] { "GET /api/sources/", "GET /api/pipelines/", "GET /api/tables/" })
        {
            var (status, body) = await harness.CallAsync(listRoute, user);
            Assert.Equal(200, status);
            Assert.Contains("dev-", body, StringComparison.Ordinal);
            Assert.Contains("prod-", body, StringComparison.Ordinal);
        }

        // …and the per-entity reads, which is where the built-in roles' `*` scope has to hold.
        Assert.Equal(200, (await harness.CallAsync("GET /api/tables/{id}", user, [("id", "bbbb")])).Status);
        Assert.Equal(200, (await harness.CallAsync("GET /api/sql/functions", user)).Status);
        Assert.Equal(200, (await harness.CallAsync("GET /api/transports", user)).Status);
    }

    /// <summary>A pre-upgrade token: no entry in the access document at all, only the legacy role claim.
    /// The evaluator falls back to it, which is what keeps a catalog whose migration has not run from
    /// going dark the moment this wave lands.</summary>
    [Fact]
    public async Task ATokenWithNoDocumentEntryStillWorksThroughTheLegacyRoleClaim()
    {
        var harness = Build(Document());

        var (status, body) = await harness.CallAsync(
            "GET /api/tables/", Principal("never-migrated", BuiltInRoles.Viewer));

        Assert.Equal(200, status);
        Assert.Contains("prod-positions", body, StringComparison.Ordinal);
    }

    // ---------------------------------------------------------------------------------------------
    // 6. Platform metadata folds onto catalog.read / catalog.write, and refuses without it.
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task PlatformMetadataAsksForCatalogReadAndTheProbeForCatalogWrite()
    {
        var harness = Build(Document(User("reader", Allow(Actions.CatalogRead, "*"))));
        var reader = Principal("reader");

        Assert.Equal(200, (await harness.CallAsync("GET /api/sql/functions", reader)).Status);
        Assert.Equal(200, (await harness.CallAsync("GET /api/transports", reader)).Status);

        // catalog.read is not catalog.write: the probe makes the server dial a host the caller named.
        var probe = await harness.CallAsync(
            "POST /api/transports/{kind}/probe", reader, [("kind", "postgres")],
            new { name = "x", kind = "postgres" });
        Assert.Equal(403, probe.Status);

        // And a caller with only an entity-scoped read cannot reach platform metadata at all — `*` is
        // answered only by a `*`-scoped grant.
        var scoped = Build(Document(User("scoped", Allow(Actions.CatalogRead, "dev-*"))));
        Assert.Equal(403, (await scoped.CallAsync("GET /api/sql/functions", Principal("scoped"))).Status);
    }

    // ---------------------------------------------------------------------------------------------
    // 7. The dual-auth ingest route: the entitlement gates the JWT branch ONLY.
    // ---------------------------------------------------------------------------------------------

    /// <summary>Plan 009 A1.2 made <c>POST /api/sources/{name}/events</c> anonymous at the metadata layer
    /// with a manual Editor-JWT-or-ingest-key check inside. Wave 3-A added
    /// <see cref="Actions.SourceIngest"/> to the JWT branch, and these three assertions are the whole
    /// contract: a scoped Editor is refused where their scope does not reach, allowed where it does, and
    /// a key holder is completely unaffected by either.</summary>
    [Fact]
    public async Task TheIngestEntitlementGatesTheJwtBranchAndLeavesTheKeyBranchAlone()
    {
        var harness = Build(Document(new UserAccessEntry
        {
            Username = "pusher",
            Roles = [BuiltInRoles.Editor],
            Grants = [new PermissionGrant { Action = Actions.SourceIngest, Scope = "prod-*", Effect = PermissionEffect.Deny }],
        }));
        var pusher = Principal("pusher", BuiltInRoles.Editor);
        object Batch() => new { events = new[] { new Dictionary<string, object?> { ["v"] = 1 } } };

        // Allowed where the Deny does not reach: Editor's own source.ingest on `*` carries it.
        Assert.Equal(202,
            (await harness.CallAsync("POST /api/sources/{name}/events", pusher, [("name", "dev-trades")], Batch())).Status);

        // Refused where it does. 401, not 403, because this route's contract is "authorized or not" —
        // it has two credential shapes and only one of them has a principal to explain a 403 with.
        Assert.Equal(401,
            (await harness.CallAsync("POST /api/sources/{name}/events", pusher, [("name", "prod-trades")], Batch())).Status);

        // The key branch is untouched: an anonymous caller with a valid X-SF-Ingest-Key still pushes to
        // the very source the JWT branch was just refused.
        harness.Ingress.ValidKey = "sfk_good";
        var anonymous = new ClaimsPrincipal(new ClaimsIdentity());
        Assert.Equal(202, (await harness.CallAsync(
            "POST /api/sources/{name}/events", anonymous, [("name", "prod-trades")], Batch(),
            [("X-SF-Ingest-Key", "sfk_good")])).Status);
    }

    // ---------------------------------------------------------------------------------------------
    // Fakes
    // ---------------------------------------------------------------------------------------------

    /// <summary>Everything the guarded handlers read, and nothing else — the unused half of
    /// <see cref="ICatalogFacade"/> throws rather than answering plausibly, so a handler that started
    /// calling something new shows up as a failure instead of a silent pass.</summary>
    private sealed class FakeCatalog : ICatalogFacade
    {
        public List<SourceDefinition> Sources { get; init; } = [];
        public List<PipelineDefinition> Pipelines { get; init; } = [];
        public List<TableDefinition> Tables { get; init; } = [];

        public List<string> DeletedTables { get; } = [];
        public List<string> UpdatedTables { get; } = [];

        public Task<List<SourceDefinition>> GetSourcesAsync() => Task.FromResult(Sources);
        public Task<SourceDefinition?> GetSourceAsync(string name) =>
            Task.FromResult(Sources.FirstOrDefault(s => s.Name == name));
        public Task UpsertSourceAsync(SourceDefinition def) => Task.CompletedTask;
        public Task<bool> DeleteSourceAsync(string name) => Task.FromResult(true);

        public Task<List<PipelineDefinition>> GetPipelinesAsync() => Task.FromResult(Pipelines);
        public Task<PipelineDefinition?> GetPipelineAsync(string id) =>
            Task.FromResult(Pipelines.FirstOrDefault(p => p.Id == id));
        public Task<PipelineDefinition> CreatePipelineAsync(PipelineDefinition def) => Task.FromResult(def);
        public Task<PipelineDefinition?> UpdatePipelineAsync(PipelineDefinition def) =>
            Task.FromResult<PipelineDefinition?>(def);
        public Task<bool> DeletePipelineAsync(string id) => Task.FromResult(true);
        public Task<PipelineDefinition?> SetPipelineStatusAsync(string id, PipelineStatus status) =>
            Task.FromResult(Pipelines.FirstOrDefault(p => p.Id == id));

        public Task<List<TableDefinition>> GetTablesAsync() => Task.FromResult(Tables);
        public Task<TableDefinition?> GetTableAsync(string id) =>
            Task.FromResult(Tables.FirstOrDefault(t => t.Id == id));
        public Task<TableDefinition> CreateTableAsync(TableDefinition def) => Task.FromResult(def);
        public Task<TableDefinition?> UpdateTableAsync(TableDefinition def)
        {
            UpdatedTables.Add(def.Id);
            return Task.FromResult<TableDefinition?>(def);
        }

        public Task<bool> DeleteTableAsync(string id)
        {
            DeletedTables.Add(id);
            return Task.FromResult(true);
        }

        public Task<TableDefinition?> SetTableStatusAsync(string id, PipelineStatus status) =>
            Task.FromResult(Tables.FirstOrDefault(t => t.Id == id));

        public Task<string> EnsureFieldNumbersAsync(string entityKey, List<FieldDef> fields) =>
            Task.FromResult("{}");
        public Task<ScenarioRunResult> RunSourceAsync(string name, ScenarioRunRequest request) =>
            throw new NotSupportedException();
    }

    private sealed class FakeIngress : IIngressFacade
    {
        public string? ValidKey { get; set; }

        public Task<IngestResult> PushAsync(
            string sourceName, IReadOnlyList<Dictionary<string, object?>> events, bool partial, string? idempotencyKey = null) =>
            Task.FromResult(new IngestResult { Outcome = IngestOutcome.Accepted, Accepted = events.Count });

        public Task<bool> ValidateKeyAsync(string sourceName, string? presentedKey) =>
            Task.FromResult(ValidKey is not null && presentedKey == ValidKey);

        public Task<IngestStatus?> GetStatusAsync(string sourceName) => Task.FromResult<IngestStatus?>(null);
    }

    /// <summary>A document that never changes and never throws — the resolver's own behaviour is
    /// <see cref="PermissionResolverTests"/>' subject, not this file's.</summary>
    private sealed class StaticAccessPolicyFacade(AccessPolicyDocument document) : IAccessPolicyFacade
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
