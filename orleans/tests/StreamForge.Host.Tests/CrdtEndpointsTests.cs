using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using StreamForge.Abstractions;
using StreamForge.Api;
using StreamForge.Api.Auth;
using StreamForge.AppCore.Access;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// Plan 020 wave B-2 — <c>/api/sources/{name}/crdt[/updates]</c> exercised over real HTTP, the
/// <see cref="DiscoveryEndpointsTests"/>/<see cref="EnvironmentEndpointsTests"/> pattern: a real Kestrel
/// listener on a dynamic port, fakes behind every facade the route touches, no Orleans silo — this file
/// is about the ROUTE's status-code mapping (<see cref="CrdtEndpoints"/>'s own doc comment: 501/404/409/
/// 400/200), not the merge algorithm (that is <c>CrdtDocGrainClusterTests</c>' job, against a real grain).
///
/// <para>Authenticated calls use a real JWT minted by <see cref="JwtTokenService"/> against a document
/// whose only roles are the built-in three — the same legacy-equivalence shortcut
/// <see cref="EnvironmentEndpointsTests"/> uses, sufficient to prove the Viewer/Editor split without
/// standing up a user store.</para>
/// </summary>
public sealed class CrdtEndpointsTests : IAsyncDisposable
{
    private readonly string _dataDir = Path.Combine(Path.GetTempPath(), "sf-crdt-endpoints-tests-" + Guid.NewGuid().ToString("n"));
    private WebApplication? _app;

    public async ValueTask DisposeAsync()
    {
        if (_app is not null)
        {
            await _app.StopAsync();
            await _app.DisposeAsync();
        }

        if (Directory.Exists(_dataDir))
        {
            Directory.Delete(_dataDir, recursive: true);
        }
    }

    /// <summary>Sources are seeded directly into the dictionary before <see cref="StartAsync"/> — enough
    /// to exercise the route's "unknown name" / "wrong kind" / "crdt kind" three-way split. Every other
    /// member throws: nothing this file asserts reaches them.</summary>
    private sealed class FakeCatalogFacade : ICatalogFacade
    {
        public Dictionary<string, SourceDefinition> Sources { get; } = new(StringComparer.Ordinal);

        public Task<List<SourceDefinition>> GetSourcesAsync() => Task.FromResult(Sources.Values.ToList());
        public Task<SourceDefinition?> GetSourceAsync(string name) =>
            Task.FromResult(Sources.TryGetValue(name, out var def) ? def : null);
        public Task UpsertSourceAsync(SourceDefinition def) => throw new NotSupportedException();
        public Task<bool> DeleteSourceAsync(string name) => throw new NotSupportedException();
        public Task<List<PipelineDefinition>> GetPipelinesAsync() => throw new NotSupportedException();
        public Task<PipelineDefinition?> GetPipelineAsync(string id) => throw new NotSupportedException();
        public Task<PipelineDefinition> CreatePipelineAsync(PipelineDefinition def) => throw new NotSupportedException();
        public Task<PipelineDefinition?> UpdatePipelineAsync(PipelineDefinition def) => throw new NotSupportedException();
        public Task<bool> DeletePipelineAsync(string id) => throw new NotSupportedException();
        public Task<PipelineDefinition?> SetPipelineStatusAsync(string id, PipelineStatus status) => throw new NotSupportedException();
        public Task<List<TableDefinition>> GetTablesAsync() => throw new NotSupportedException();
        public Task<TableDefinition?> GetTableAsync(string id) => throw new NotSupportedException();
        public Task<TableDefinition> CreateTableAsync(TableDefinition def) => throw new NotSupportedException();
        public Task<TableDefinition?> UpdateTableAsync(TableDefinition def) => throw new NotSupportedException();
        public Task<bool> DeleteTableAsync(string id) => throw new NotSupportedException();
        public Task<TableDefinition?> SetTableStatusAsync(string id, PipelineStatus status) => throw new NotSupportedException();
        public Task<string> EnsureFieldNumbersAsync(string entityKey, List<FieldDef> fields) => throw new NotSupportedException();
        public Task<ScenarioRunResult> RunSourceAsync(string name, ScenarioRunRequest request) => throw new NotSupportedException();
    }

    private sealed class FakeCrdtFacade : ICrdtFacade
    {
        public bool Enabled { get; set; } = true;

        public Task<CrdtMergeResult?> MergeAsync(string sourceName, IReadOnlyList<byte[]> updates) =>
            Task.FromResult<CrdtMergeResult?>(new CrdtMergeResult { UpdatesApplied = updates.Count, RowsEmitted = updates.Count });

        public Task<CrdtDocStatus?> GetStatusAsync(string sourceName) =>
            Task.FromResult<CrdtDocStatus?>(new CrdtDocStatus { EntityCount = 3, UpdatesMerged = 5, RowsEmitted = 5 });

        // Plan 020 wave C — the replay route's fake. Reports rows but zero updates applied, which is what
        // a real replay returns: it re-asserts the projection and merges nothing.
        public Task<CrdtMergeResult?> ReplayAsync(string sourceName) =>
            Task.FromResult<CrdtMergeResult?>(new CrdtMergeResult { UpdatesApplied = 0, RowsEmitted = 3 });
    }

    private sealed class FakeUserStoreFacade : IUserStoreFacade
    {
        public Task<List<UserRecord>> GetUsersAsync() => Task.FromResult(new List<UserRecord>());
        public Task<UserRecord?> ValidateCredentialsAsync(string username, string password) => throw new NotSupportedException();
        public Task<bool> CreateUserAsync(string username, string displayName, string role, string password) => throw new NotSupportedException();
        public Task<bool> UpdateUserAsync(string username, string? displayName, string? role, string? password) => throw new NotSupportedException();
        public Task<bool> DeleteUserAsync(string username) => throw new NotSupportedException();
    }

    private sealed class FakeAccessPolicyFacade : IAccessPolicyFacade
    {
        private readonly AccessPolicyDocument _document = new() { Roles = BuiltInRoleCatalog.Create(), Version = 1 };

        public Task<AccessPolicyDocument> GetPolicyAsync() => Task.FromResult(_document);
        public Task<long> GetVersionAsync() => Task.FromResult(_document.Version);
        public Task<RoleDefinition?> UpsertRoleAsync(RoleDefinition role, string actor) => throw new NotSupportedException();
        public Task<bool> DeleteRoleAsync(string name) => throw new NotSupportedException();
        public Task<GroupDefinition?> UpsertGroupAsync(GroupDefinition group, string actor) => throw new NotSupportedException();
        public Task<bool> DeleteGroupAsync(string name) => throw new NotSupportedException();
        public Task<UserAccessEntry?> UpsertUserAccessAsync(UserAccessEntry entry, string actor) => throw new NotSupportedException();
        public Task<bool> DeleteUserAccessAsync(string username) => throw new NotSupportedException();
        public Task<ApprovalTemplate?> UpsertApprovalTemplateAsync(ApprovalTemplate template, string actor) => throw new NotSupportedException();
        public Task<bool> DeleteApprovalTemplateAsync(string name) => throw new NotSupportedException();
    }

    private readonly FakeCatalogFacade _catalog = new();
    private readonly FakeCrdtFacade _crdt = new();

    private async Task<HttpClient> StartAsync()
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Jwt:Key"] = new string('k', 64),
            ["Jwt:Issuer"] = "streamforge-test",
            ["Jwt:Audience"] = "streamforge-test",
        });
        builder.Services.AddStreamForgeApi(builder.Configuration);

        // Same "every facade/tracker needs a resolvable-in-principle registration" requirement
        // DiscoveryEndpointsTests/EnvironmentEndpointsTests document on their own StartAsync — Minimal
        // API infers a handler parameter's binding source by asking the container whether the TYPE is
        // registered, for every mapped route, not only the ones this file exercises.
        var throwingStubTypes = typeof(ICatalogFacade).Assembly.GetTypes()
            .Where(t => t.IsInterface && t.IsPublic && t.Name.EndsWith("Facade", StringComparison.Ordinal))
            .Concat(typeof(StreamForge.AppCore.Ingest.IngestKeyUsageTracker).Assembly.GetTypes()
                .Where(t => t.IsClass && t.IsPublic && !t.IsAbstract && !t.IsGenericType
                            && t.Name.EndsWith("Tracker", StringComparison.Ordinal)));
        foreach (var t in throwingStubTypes)
        {
            builder.Services.AddSingleton(t, _ => throw new InvalidOperationException(
                $"{t.Name} was resolved — this file only drives /api/sources/{{name}}/crdt*."));
        }

        builder.Services.AddSingleton<ICatalogFacade>(_catalog);
        builder.Services.AddSingleton<ICrdtFacade>(_crdt);
        builder.Services.AddSingleton<IAccessPolicyFacade>(new FakeAccessPolicyFacade());
        builder.Services.AddSingleton<IUserStoreFacade>(new FakeUserStoreFacade());

        _app = builder.Build();
        _app.MapStreamForgeApi(new StreamForgeApiOptions(
            ProtosDir: Path.Combine(Path.GetTempPath(), "sf-crdt-endpoints-tests-protos"),
            GrpcPort: 7298,
            GrpcStaticServices: [],
            DocsFilePath: null,
            SpaDistPath: null,
            Flavor: "test",
            DataDir: _dataDir));

        await _app.StartAsync();
        var address = _app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First();
        return new HttpClient { BaseAddress = new Uri(address) };
    }

    private HttpRequestMessage AuthedRequest(HttpMethod method, string url, string role, object? body = null)
    {
        var token = _app!.Services.GetRequiredService<JwtTokenService>()
            .CreateToken(new UserRecord { Username = role.ToLowerInvariant(), DisplayName = role, Role = role });

        var request = new HttpRequestMessage(method, url)
        {
            Headers = { Authorization = new AuthenticationHeaderValue("Bearer", token) },
        };
        if (body is not null)
        {
            request.Content = JsonContent.Create(body);
        }

        return request;
    }

    private static SourceDefinition CrdtSource(string name) => new()
    {
        Name = name,
        Kind = SourceKinds.Crdt,
        Fields = [new FieldDef("id", FieldType.String)],
        Connector = new ConnectorConfig { Crdt = new CrdtSourceConfig { RootMap = "root", KeyField = "id" } },
    };

    // ---------------------------------------------------------------------------------------------
    // 501 — no CRDT document runtime in this build (the Dapr flavor, D9).
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task Disabled_facade_answers_501_before_touching_the_catalog_at_all()
    {
        _crdt.Enabled = false;
        // Deliberately no source seeded — 501 must not depend on the name resolving at all.
        using var client = await StartAsync();

        var getResponse = await client.SendAsync(AuthedRequest(HttpMethod.Get, "/api/sources/whatever/crdt", "Viewer"));
        Assert.Equal(HttpStatusCode.NotImplemented, getResponse.StatusCode);

        var postResponse = await client.SendAsync(AuthedRequest(
            HttpMethod.Post, "/api/sources/whatever/crdt/updates", "Editor", new CrdtUpdatesRequest([])));
        Assert.Equal(HttpStatusCode.NotImplemented, postResponse.StatusCode);
    }

    // ---------------------------------------------------------------------------------------------
    // 404 — no such source.
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task Unknown_source_is_404()
    {
        using var client = await StartAsync();

        var getResponse = await client.SendAsync(AuthedRequest(HttpMethod.Get, "/api/sources/nope/crdt", "Viewer"));
        Assert.Equal(HttpStatusCode.NotFound, getResponse.StatusCode);

        var postResponse = await client.SendAsync(AuthedRequest(
            HttpMethod.Post, "/api/sources/nope/crdt/updates", "Editor", new CrdtUpdatesRequest([])));
        Assert.Equal(HttpStatusCode.NotFound, postResponse.StatusCode);
    }

    // ---------------------------------------------------------------------------------------------
    // 409 — the source exists but is not crdt-kind.
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task Wrong_kind_source_is_409()
    {
        _catalog.Sources["notcrdt"] = new SourceDefinition { Name = "notcrdt", Kind = SourceKinds.Generator };
        using var client = await StartAsync();

        var getResponse = await client.SendAsync(AuthedRequest(HttpMethod.Get, "/api/sources/notcrdt/crdt", "Viewer"));
        Assert.Equal(HttpStatusCode.Conflict, getResponse.StatusCode);

        var postResponse = await client.SendAsync(AuthedRequest(
            HttpMethod.Post, "/api/sources/notcrdt/crdt/updates", "Editor", new CrdtUpdatesRequest([])));
        Assert.Equal(HttpStatusCode.Conflict, postResponse.StatusCode);
    }

    // ---------------------------------------------------------------------------------------------
    // 200 — the happy path, both routes.
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task Crdt_kind_source_answers_200_on_both_routes()
    {
        _catalog.Sources["doc1"] = CrdtSource("doc1");
        using var client = await StartAsync();

        var getResponse = await client.SendAsync(AuthedRequest(HttpMethod.Get, "/api/sources/doc1/crdt", "Viewer"));
        Assert.Equal(HttpStatusCode.OK, getResponse.StatusCode);
        var status = await getResponse.Content.ReadFromJsonAsync<CrdtDocStatus>();
        Assert.Equal(3, status!.EntityCount);

        var updates = new CrdtUpdatesRequest([Convert.ToBase64String([1, 2, 3])]);
        var postResponse = await client.SendAsync(AuthedRequest(HttpMethod.Post, "/api/sources/doc1/crdt/updates", "Editor", updates));
        Assert.Equal(HttpStatusCode.OK, postResponse.StatusCode);
        var result = await postResponse.Content.ReadFromJsonAsync<CrdtMergeResult>();
        Assert.Equal(1, result!.UpdatesApplied);
    }

    // ---------------------------------------------------------------------------------------------
    // 400 — malformed base64, not a 500.
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task Invalid_base64_update_is_400_not_500()
    {
        _catalog.Sources["doc1"] = CrdtSource("doc1");
        using var client = await StartAsync();

        var updates = new CrdtUpdatesRequest(["not valid base64 !!!"]);
        var response = await client.SendAsync(AuthedRequest(HttpMethod.Post, "/api/sources/doc1/crdt/updates", "Editor", updates));

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    // ---------------------------------------------------------------------------------------------
    // Authorization floor: Viewer cannot push updates.
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task Viewer_cannot_push_updates()
    {
        _catalog.Sources["doc1"] = CrdtSource("doc1");
        using var client = await StartAsync();

        var updates = new CrdtUpdatesRequest([]);
        var response = await client.SendAsync(AuthedRequest(HttpMethod.Post, "/api/sources/doc1/crdt/updates", "Viewer", updates));

        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
    }
}
