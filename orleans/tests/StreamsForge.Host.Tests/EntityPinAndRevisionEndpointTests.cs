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
/// Plan 016 wave 2-B — the four gaps wave 2-A found and left for this wave, run through the real mapped
/// REST handlers (the in-process technique <see cref="CatalogEntitlementEndpointTests"/> established):
/// <list type="number">
/// <item><c>dependsOn</c> now reaches the registry from POST/PUT on both tables and pipelines, and a
/// pin naming a pipeline is refused rather than stored unresolvable;</item>
/// <item><c>PUT /api/sources/{name}?allowBreaking=false</c> refuses a breaking field change with 409 and
/// names the field; the default (no query param) stays permissive;</item>
/// <item>POST/PUT <c>/api/sources</c> echo the REGISTRY-assigned revision, not the caller's stale input
/// — this file's <see cref="FakeCatalog.UpsertSourceAsync"/> deliberately reproduces the Orleans
/// by-value boundary (the stored record is a CLONE, so mutating its Revision can never be observed
/// through the caller's own reference) so a regression back to "return the input" fails these tests the
/// same way it failed in production;</item>
/// <item>the audit row for a source write carries the STORED definition, not the pre-write input.</item>
/// </list>
/// </summary>
public class EntityPinAndRevisionEndpointTests
{
    // ---------------------------------------------------------------------------------------------
    // Fixture — one user entitled to everything these routes need.
    // ---------------------------------------------------------------------------------------------

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

    private static ClaimsPrincipal Principal(string name) => PermissionResolverTests.Principal(name);

    private static readonly JsonSerializerOptions JsonOpts = new(JsonSerializerDefaults.Web);

    private static UserAccessEntry Operator() => User(
        "op",
        Allow(Actions.SourceWrite, "*"),
        Allow(Actions.SourceRead, "*"),
        Allow(Actions.TableWrite, "*"),
        Allow(Actions.TableRead, "*"),
        Allow(Actions.PipelineWrite, "*"),
        Allow(Actions.PipelineRead, "*"));

    // ---------------------------------------------------------------------------------------------
    // 1. dependsOn reaches the registry — tables.
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task PostTableWithDependsOnPinStoresIt()
    {
        var (harness, catalog) = Build(Document(Operator()));

        var req = new CreateTableRequest(
            "positions", "", "SELECT * FROM trades",
            DependsOn: [new EntityPin { Kind = "source", Name = "trades", SchemaRevision = 1 }]);

        var (status, body) = await harness.CallAsync("POST /api/tables/", Principal("op"), body: req);

        Assert.Equal(201, status);
        using var doc = JsonDocument.Parse(body);
        var pins = doc.RootElement.GetProperty("dependsOn");
        Assert.Equal(1, pins.GetArrayLength());
        Assert.Equal("source", pins[0].GetProperty("kind").GetString());
        Assert.Equal("trades", pins[0].GetProperty("name").GetString());
        Assert.Single(catalog.Tables[0].DependsOn);
    }

    [Fact]
    public async Task PostTableWithPipelineKindPinIsRejected()
    {
        var (harness, catalog) = Build(Document(Operator()));

        var req = new CreateTableRequest(
            "positions", "", "SELECT * FROM trades",
            DependsOn: [new EntityPin { Kind = "pipeline", Name = "enrich" }]);

        var (status, body) = await harness.CallAsync("POST /api/tables/", Principal("op"), body: req);

        Assert.Equal(400, status);
        Assert.Contains("pipeline", body, StringComparison.Ordinal);
        Assert.Empty(catalog.Tables);
    }

    [Fact]
    public async Task PutTableWithOmittedDependsOnLeavesExistingPinUnchanged()
    {
        var (harness, catalog) = Build(Document(Operator()));
        catalog.Tables.Add(new TableDefinition
        {
            Id = "t1",
            Name = "positions",
            Sql = "SELECT * FROM trades",
            DependsOn = [new EntityPin { Kind = "source", Name = "trades", SchemaRevision = 1 }],
        });

        // DependsOn omitted (defaults to null) — the null-means-unchanged convention every other
        // optional list field on this request already follows.
        var req = new CreateTableRequest("positions", "", "SELECT * FROM trades");

        var (status, body) = await harness.CallAsync(
            "PUT /api/tables/{id}", Principal("op"), [("id", "t1")], body: req);

        Assert.Equal(200, status);
        using var doc = JsonDocument.Parse(body);
        var pins = doc.RootElement.GetProperty("dependsOn");
        Assert.Equal(1, pins.GetArrayLength());
        Assert.Equal("trades", pins[0].GetProperty("name").GetString());
    }

    [Fact]
    public async Task PutTableWithExplicitEmptyDependsOnClearsPins()
    {
        var (harness, catalog) = Build(Document(Operator()));
        catalog.Tables.Add(new TableDefinition
        {
            Id = "t1",
            Name = "positions",
            Sql = "SELECT * FROM trades",
            DependsOn = [new EntityPin { Kind = "source", Name = "trades", SchemaRevision = 1 }],
        });

        var req = new CreateTableRequest("positions", "", "SELECT * FROM trades", DependsOn: []);

        var (status, body) = await harness.CallAsync(
            "PUT /api/tables/{id}", Principal("op"), [("id", "t1")], body: req);

        Assert.Equal(200, status);
        using var doc = JsonDocument.Parse(body);
        Assert.Equal(0, doc.RootElement.GetProperty("dependsOn").GetArrayLength());
    }

    // ---------------------------------------------------------------------------------------------
    // 1. dependsOn reaches the registry — pipelines. Nothing reads a pipeline's output by name, so a
    //    pin naming a TABLE or SOURCE is legal here but a pin naming a pipeline is not (mirrors the
    //    table tests above; only the invalid-kind case is repeated to prove both routes share the gate).
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task PostPipelineWithDependsOnPinStoresIt()
    {
        var (harness, catalog) = Build(Document(Operator()));

        var req = new CreatePipelineRequest(
            "enrich", "", "SELECT * FROM trades",
            DependsOn: [new EntityPin { Kind = "table", Name = "positions", SchemaRevision = 2 }]);

        var (status, body) = await harness.CallAsync("POST /api/pipelines/", Principal("op"), body: req);

        Assert.Equal(201, status);
        using var doc = JsonDocument.Parse(body);
        var pins = doc.RootElement.GetProperty("dependsOn");
        Assert.Equal(1, pins.GetArrayLength());
        Assert.Equal("table", pins[0].GetProperty("kind").GetString());
        Assert.Single(catalog.Pipelines[0].DependsOn);
    }

    [Fact]
    public async Task PostPipelineWithPipelineKindPinIsRejected()
    {
        var (harness, catalog) = Build(Document(Operator()));

        var req = new CreatePipelineRequest(
            "enrich", "", "SELECT * FROM trades",
            DependsOn: [new EntityPin { Kind = "pipeline", Name = "other" }]);

        var (status, _) = await harness.CallAsync("POST /api/pipelines/", Principal("op"), body: req);

        Assert.Equal(400, status);
        Assert.Empty(catalog.Pipelines);
    }

    // ---------------------------------------------------------------------------------------------
    // 2. ?allowBreaking on the source PUT. Default permissive; ?allowBreaking=false opts INTO the gate.
    // ---------------------------------------------------------------------------------------------

    private static SourceDefinition PriceSource(FieldType priceType) => new()
    {
        Name = "trades",
        Kind = SourceKinds.Generator,
        Fields = [new FieldDef("price", priceType)],
    };

    [Fact]
    public async Task PutSourceAllowsBreakingChangeByDefault()
    {
        var (harness, catalog) = Build(Document(Operator()));
        catalog.Sources.Add(PriceSource(FieldType.Double));

        // price: Double -> String is a breaking type change, and no ?allowBreaking is sent.
        var (status, _) = await harness.CallAsync(
            "PUT /api/sources/{name}", Principal("op"), [("name", "trades")], body: PriceSource(FieldType.String));

        Assert.Equal(200, status);
        Assert.Equal(FieldType.String, catalog.Sources.Single(s => s.Name == "trades").Fields[0].Type);
    }

    [Fact]
    public async Task PutSourceWithAllowBreakingFalseRefusesBreakingChangeWith409NamingTheField()
    {
        var (harness, catalog) = Build(Document(Operator()));
        catalog.Sources.Add(PriceSource(FieldType.Double));

        var (status, body) = await harness.CallAsync(
            "PUT /api/sources/{name}", Principal("op"), [("name", "trades")],
            body: PriceSource(FieldType.String), query: "allowBreaking=false");

        Assert.Equal(409, status);
        Assert.Contains("price", body, StringComparison.Ordinal);
        // Refused: the stored definition never moved.
        Assert.Equal(FieldType.Double, catalog.Sources.Single(s => s.Name == "trades").Fields[0].Type);

        var parsed = JsonSerializer.Deserialize<SchemaBreakingChangeResponse>(body, JsonOpts)!;
        Assert.Contains(parsed.BreakingReasons, r => r.Contains("price", StringComparison.Ordinal));
    }

    [Fact]
    public async Task PutSourceWithAllowBreakingFalseAllowsACompatibleChange()
    {
        var (harness, catalog) = Build(Document(Operator()));
        catalog.Sources.Add(PriceSource(FieldType.Double));

        var addedField = PriceSource(FieldType.Double);
        addedField.Fields.Add(new FieldDef("qty", FieldType.Long));

        var (status, _) = await harness.CallAsync(
            "PUT /api/sources/{name}", Principal("op"), [("name", "trades")],
            body: addedField, query: "allowBreaking=false");

        Assert.Equal(200, status);
        Assert.Equal(2, catalog.Sources.Single(s => s.Name == "trades").Fields.Count);
    }

    // ---------------------------------------------------------------------------------------------
    // 3 & 4. POST/PUT /api/sources echo the STORED revision, and the audit row carries the stored
    // definition — both proven against a fake that reproduces the Orleans by-value boundary: the stored
    // record is a CLONE assigned its Revision AFTER the copy, so the caller's own `def`/`effective`
    // reference is stuck at Revision 0 exactly like it was on the live grain. A regression to "return
    // the input" (or "audit the input") fails these the same way it failed live.
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task PostSourceEchoesTheStoredRevisionNotZero()
    {
        var (harness, _) = Build(Document(Operator()));

        var (status, body) = await harness.CallAsync(
            "POST /api/sources/", Principal("op"), body: PriceSource(FieldType.Double));

        Assert.Equal(201, status);
        using var doc = JsonDocument.Parse(body);
        Assert.Equal(1, doc.RootElement.GetProperty("revision").GetInt64());
    }

    [Fact]
    public async Task PutSourceEchoesTheBumpedStoredRevision()
    {
        var (harness, catalog) = Build(Document(Operator()));
        catalog.Sources.Add(new SourceDefinition
        {
            Name = "trades", Kind = SourceKinds.Generator,
            Fields = [new FieldDef("price", FieldType.Double)],
            Revision = 1, SchemaRevision = 1,
        });

        var (status, body) = await harness.CallAsync(
            "PUT /api/sources/{name}", Principal("op"), [("name", "trades")], body: PriceSource(FieldType.Double));

        Assert.Equal(200, status);
        using var doc = JsonDocument.Parse(body);
        Assert.Equal(2, doc.RootElement.GetProperty("revision").GetInt64());
    }

    [Fact]
    public async Task AuditRowForASourceCreateRecordsTheStoredRevisionNotTheStaleInput()
    {
        var (harness, _, audit) = BuildWithAudit(Document(Operator()));

        var (status, _) = await harness.CallAsync(
            "POST /api/sources/", Principal("op"), body: PriceSource(FieldType.Double));

        Assert.Equal(201, status);
        var row = Assert.Single(audit.Entries);
        Assert.NotNull(row.AfterJson);
        Assert.Contains("\"revision\":1", row.AfterJson, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AuditRowForASourceUpdateRecordsTheStoredRevisionNotTheStaleInput()
    {
        var (harness, catalog, audit) = BuildWithAudit(Document(Operator()));
        catalog.Sources.Add(new SourceDefinition
        {
            Name = "trades", Kind = SourceKinds.Generator,
            Fields = [new FieldDef("price", FieldType.Double)],
            Revision = 1, SchemaRevision = 1,
        });

        var (status, _) = await harness.CallAsync(
            "PUT /api/sources/{name}", Principal("op"), [("name", "trades")], body: PriceSource(FieldType.Double));

        Assert.Equal(200, status);
        var row = Assert.Single(audit.Entries);
        Assert.NotNull(row.AfterJson);
        Assert.Contains("\"revision\":2", row.AfterJson, StringComparison.Ordinal);
    }

    // ---------------------------------------------------------------------------------------------
    // Harness — the in-process shape CatalogEntitlementEndpointTests established, trimmed to what
    // these routes need, plus a FakeCatalog that actually simulates revision assignment (the pre-
    // existing FakeCatalog in that file no-ops UpsertSourceAsync, which would make the revision-echo
    // regression invisible to a test built on it).
    // ---------------------------------------------------------------------------------------------

    private sealed class Harness(IReadOnlyList<Endpoint> endpoints, IServiceProvider services)
    {
        public async Task<(int Status, string Body)> CallAsync(
            string key,
            ClaimsPrincipal user,
            (string Name, string Value)[]? routeValues = null,
            object? body = null,
            string? query = null)
        {
            var endpoint = endpoints.OfType<RouteEndpoint>().Single(e => KeyOf(e) == key);

            var http = new DefaultHttpContext { RequestServices = services, User = user };
            var responseBody = new MemoryStream();
            http.Response.Body = responseBody;
            http.Features.Set<IHttpRequestBodyDetectionFeature>(new BodyAllowed());

            var (method, pattern) = (key.Split(' ')[0], key.Split(' ')[1]);
            http.Request.Method = method;
            http.Request.Path = pattern;
            if (query is not null)
            {
                http.Request.QueryString = new QueryString("?" + query);
            }

            foreach (var (name, value) in routeValues ?? [])
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
    }

    private static (Harness, FakeCatalog) Build(AccessPolicyDocument document)
    {
        var (harness, catalog, _) = BuildInternal(document, new FakeAuditSink());
        return (harness, catalog);
    }

    private static (Harness, FakeCatalog, FakeAuditSink) BuildWithAudit(AccessPolicyDocument document)
    {
        var audit = new FakeAuditSink();
        return BuildInternal(document, audit);
    }

    private static (Harness, FakeCatalog, FakeAuditSink) BuildInternal(AccessPolicyDocument document, FakeAuditSink audit)
    {
        var catalog = new FakeCatalog();

        var builder = WebApplication.CreateBuilder();
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Jwt:Key"] = new string('k', 64),
            ["Jwt:Issuer"] = "streamsforge-test",
            ["Jwt:Audience"] = "streamsforge-test",
            ["Auth:PolicyCacheSeconds"] = "600",
        });
        builder.Services.AddStreamsForgeApi(builder.Configuration);

        // Same technique as CatalogEntitlementEndpointTests: every facade interface gets an
        // UntouchableProxy stub, every concrete class in the API/Ingest assemblies gets a throwing
        // stub, and then the specific singletons these tests actually need are registered afterward —
        // MS DI resolves the LAST registration, so the real ones win.
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
            AddThrowing(builder.Services, t);
        }

        var policyFacade = new StaticAccessPolicyFacade(document);
        var resolver = new PermissionResolver(policyFacade, NullLogger<PermissionResolver>.Instance, 600);

        builder.Services.AddSingleton<IAccessPolicyFacade>(policyFacade);
        builder.Services.AddSingleton(resolver);
        builder.Services.AddSingleton(new AccessGuard(resolver, entitlementsEnabled: true));
        builder.Services.AddSingleton<ICatalogFacade>(catalog);
        builder.Services.AddSingleton<IIngressFacade>(new UnusedIngress());
        builder.Services.AddSingleton(new IngestKeyUsageTracker());
        // Overrides the throwing stub AuditChannelSink resolution would otherwise hit — see
        // CatalogChangeAudit's own doc: a throwing IAuditSink factory is swallowed (audit is never why
        // a request fails), which would make an audit-content assertion silently vacuous without this.
        builder.Services.AddSingleton<IAuditSink>(audit);

        var app = builder.Build();
        app.MapStreamsForgeApi(new StreamsForgeApiOptions(
            ProtosDir: Path.Combine(Path.GetTempPath(), "sf-pin-revision-protos"),
            GrpcPort: 0,
            GrpcStaticServices: [],
            DocsFilePath: null,
            SpaDistPath: null,
            Flavor: "test"));

        var harness = new Harness(
            [.. ((IEndpointRouteBuilder)app).DataSources.SelectMany(d => d.Endpoints)],
            app.Services);

        return (harness, catalog, audit);
    }

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

    private static bool IsRecord(Type t) =>
        t.GetMethod("<Clone>$", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic) is not null;

    private static void AddThrowing(IServiceCollection services, Type t) =>
        services.AddSingleton(t, _ => throw new InvalidOperationException(
            $"{t.Name} was resolved but this test never registered a real one."));

    private sealed class UnusedIngress : IIngressFacade
    {
        public Task<IngestResult> PushAsync(
            string sourceName, IReadOnlyList<Dictionary<string, object?>> events, bool partial, string? idempotencyKey = null) =>
            throw new NotSupportedException();
        public Task<bool> ValidateKeyAsync(string sourceName, string? presentedKey) => Task.FromResult(false);
        public Task<IngestStatus?> GetStatusAsync(string sourceName) => Task.FromResult<IngestStatus?>(null);
    }

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

    private sealed class FakeAuditSink : IAuditSink
    {
        public List<AuditEntry> Entries { get; } = [];
        public void Record(AuditEntry entry) => Entries.Add(entry);
    }

    /// <summary>The one facade in this file that is NOT a thin pass-through: <see cref="UpsertSourceAsync"/>
    /// deliberately mirrors <c>RegistryGrain.UpsertSourceAsync</c>'s real shape — the record it stores is
    /// a CLONE of the caller's <see cref="SourceDefinition"/>, and <c>Revision</c> is assigned only on
    /// that clone, AFTER the copy. That is exactly the Orleans by-value grain-call boundary this wave's
    /// fix depends on: the caller's own <c>def</c>/<c>effective</c> reference can never observe the
    /// assigned revision, so a handler that returns it instead of re-reading fails these tests the same
    /// way it failed against the real grain. Tables/pipelines need no such fidelity — the REST handlers
    /// mutate the SAME <c>existing</c> reference this fake also stores, which is realistic for both
    /// flavours on those two entity types (neither wave 2-A gap was about them).</summary>
    private sealed class FakeCatalog : ICatalogFacade
    {
        public List<SourceDefinition> Sources { get; } = [];
        public List<PipelineDefinition> Pipelines { get; } = [];
        public List<TableDefinition> Tables { get; } = [];

        public Task<List<SourceDefinition>> GetSourcesAsync() => Task.FromResult(Sources.ToList());
        public Task<SourceDefinition?> GetSourceAsync(string name) =>
            Task.FromResult(Sources.FirstOrDefault(s => s.Name == name));

        public Task UpsertSourceAsync(SourceDefinition def)
        {
            var idx = Sources.FindIndex(s => s.Name == def.Name);
            var stored = Clone(def);
            stored.Revision = idx >= 0 ? Sources[idx].Revision + 1 : 1;
            stored.SchemaRevision = stored.Revision;
            if (idx >= 0) Sources[idx] = stored; else Sources.Add(stored);
            return Task.CompletedTask;
        }

        public Task<bool> DeleteSourceAsync(string name) => Task.FromResult(Sources.RemoveAll(s => s.Name == name) > 0);

        public Task<List<PipelineDefinition>> GetPipelinesAsync() => Task.FromResult(Pipelines.ToList());
        public Task<PipelineDefinition?> GetPipelineAsync(string id) =>
            Task.FromResult(Pipelines.FirstOrDefault(p => p.Id == id));

        public Task<PipelineDefinition> CreatePipelineAsync(PipelineDefinition def)
        {
            def.Id = $"pl-{Pipelines.Count + 1}";
            Pipelines.Add(def);
            return Task.FromResult(def);
        }

        public Task<PipelineDefinition?> UpdatePipelineAsync(PipelineDefinition def)
        {
            var idx = Pipelines.FindIndex(p => p.Id == def.Id);
            if (idx < 0) return Task.FromResult<PipelineDefinition?>(null);
            Pipelines[idx] = def;
            return Task.FromResult<PipelineDefinition?>(def);
        }

        public Task<bool> DeletePipelineAsync(string id) => Task.FromResult(Pipelines.RemoveAll(p => p.Id == id) > 0);

        public Task<PipelineDefinition?> SetPipelineStatusAsync(string id, PipelineStatus status) =>
            Task.FromResult(Pipelines.FirstOrDefault(p => p.Id == id));

        public Task<List<TableDefinition>> GetTablesAsync() => Task.FromResult(Tables.ToList());
        public Task<TableDefinition?> GetTableAsync(string id) =>
            Task.FromResult(Tables.FirstOrDefault(t => t.Id == id));

        public Task<TableDefinition> CreateTableAsync(TableDefinition def)
        {
            def.Id = $"tbl-{Tables.Count + 1}";
            Tables.Add(def);
            return Task.FromResult(def);
        }

        public Task<TableDefinition?> UpdateTableAsync(TableDefinition def)
        {
            var idx = Tables.FindIndex(t => t.Id == def.Id);
            if (idx < 0) return Task.FromResult<TableDefinition?>(null);
            Tables[idx] = def;
            return Task.FromResult<TableDefinition?>(def);
        }

        public Task<bool> DeleteTableAsync(string id) => Task.FromResult(Tables.RemoveAll(t => t.Id == id) > 0);

        public Task<TableDefinition?> SetTableStatusAsync(string id, PipelineStatus status) =>
            Task.FromResult(Tables.FirstOrDefault(t => t.Id == id));

        public Task<string> EnsureFieldNumbersAsync(string entityKey, List<FieldDef> fields) => Task.FromResult("{}");
        public Task<ScenarioRunResult> RunSourceAsync(string name, ScenarioRunRequest request) =>
            throw new NotSupportedException();

        private static SourceDefinition Clone(SourceDefinition def) =>
            JsonSerializer.Deserialize<SourceDefinition>(JsonSerializer.Serialize(def))!;
    }
}
