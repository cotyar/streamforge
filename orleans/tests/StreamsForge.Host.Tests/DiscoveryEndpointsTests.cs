using System.Net.Http.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using StreamsForge.Abstractions;
using StreamsForge.Api;
using StreamsForge.AppCore.Discovery;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>
/// Plan 016 wave 5 — <c>GET /api/meta/instance</c> exercised end-to-end over real HTTP (not just
/// endpoint metadata, unlike <see cref="AuthorizationCoverageTests"/>): a real Kestrel listener on a
/// dynamic port, a fake <see cref="ICatalogFacade"/> so the catalog-count/catalog-warning logic runs
/// against known inputs, no Orleans silo. <c>WebApplicationFactory</c> is declined for the same reason
/// <see cref="AuthorizationCoverageTests"/> declines it (neither host's <c>Program.cs</c> is startable
/// without its runtime); starting the built <see cref="WebApplication"/> for real with
/// <c>StartAsync</c> and reading its bound address back is the cheapest way to get a genuine HTTP round
/// trip without that refactor.
///
/// <para>Scope note: <c>GET /api/meta/peers</c> and <c>POST /api/meta/peers/{name}/probe</c> are NOT
/// exercised here — they sit behind the full Viewer+AccessGuard stack (JWT auth, the access-policy
/// facade), which would drag the whole entitlements DI surface into what is otherwise a two-dependency
/// test file. Their gating is pinned by <c>LegacyEquivalenceMatrixTests</c> (policy-answer equivalence)
/// and exercised for real by this track's live-check script
/// (<c>scratchpad/wave5-track-a-live-check.sh</c>), which logs in for real. Only the anonymous route
/// gets an in-process HTTP test.</para>
/// </summary>
public sealed class DiscoveryEndpointsTests : IAsyncDisposable
{
    private readonly string _dataDir = Path.Combine(Path.GetTempPath(), "sf-discovery-endpoints-tests-" + Guid.NewGuid().ToString("n"));
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

        PeerDirectory.Clear();
    }

    /// <summary>A source with an unregistered kind, a duplicated pipeline name, and a table with a
    /// stale pin — one instance of each of the three catalogWarnings categories
    /// <c>MetaEndpoints.CollectCatalogWarnings</c> is documented to detect, in one fixture.</summary>
    private sealed class FakeCatalogFacade : ICatalogFacade
    {
        public Task<List<SourceDefinition>> GetSourcesAsync() => Task.FromResult(new List<SourceDefinition>
        {
            new() { Name = "trades", Kind = SourceKinds.Generator },
            new() { Name = "quotes", Kind = "not-a-real-kind" },
        });

        public Task<List<PipelineDefinition>> GetPipelinesAsync() => Task.FromResult(new List<PipelineDefinition>
        {
            new() { Id = "p1", Name = "fx_desk" },
            new() { Id = "p2", Name = "fx_desk" },
            new() { Id = "p3", Name = "rates_desk" },
        });

        public Task<List<TableDefinition>> GetTablesAsync() => Task.FromResult(new List<TableDefinition>
        {
            new() { Id = "t1", Name = "daily_pnl", StaleReason = "source 'trades' schema changed" },
        });

        public Task<SourceDefinition?> GetSourceAsync(string name) => throw new NotSupportedException();
        public Task UpsertSourceAsync(SourceDefinition def) => throw new NotSupportedException();
        public Task<bool> DeleteSourceAsync(string name) => throw new NotSupportedException();
        public Task<PipelineDefinition?> GetPipelineAsync(string id) => throw new NotSupportedException();
        public Task<PipelineDefinition> CreatePipelineAsync(PipelineDefinition def) => throw new NotSupportedException();
        public Task<PipelineDefinition?> UpdatePipelineAsync(PipelineDefinition def) => throw new NotSupportedException();
        public Task<bool> DeletePipelineAsync(string id) => throw new NotSupportedException();
        public Task<PipelineDefinition?> SetPipelineStatusAsync(string id, PipelineStatus status) => throw new NotSupportedException();
        public Task<TableDefinition?> GetTableAsync(string id) => throw new NotSupportedException();
        public Task<TableDefinition> CreateTableAsync(TableDefinition def) => throw new NotSupportedException();
        public Task<TableDefinition?> UpdateTableAsync(TableDefinition def) => throw new NotSupportedException();
        public Task<bool> DeleteTableAsync(string id) => throw new NotSupportedException();
        public Task<TableDefinition?> SetTableStatusAsync(string id, PipelineStatus status) => throw new NotSupportedException();
        public Task<string> EnsureFieldNumbersAsync(string entityKey, List<FieldDef> fields) => throw new NotSupportedException();
        public Task<ScenarioRunResult> RunSourceAsync(string name, ScenarioRunRequest request) => throw new NotSupportedException();
    }

    /// <summary>Never exercised by anything this file asserts — <c>GetUsersAsync</c> returning empty is
    /// what keeps <c>LegacyRoleMigration.Apply</c> (run for real by <c>AccessBootstrapService</c>, a
    /// hosted service <c>AddStreamsForgeApi</c> registers unconditionally, since this test calls
    /// <c>StartAsync</c> for a genuine HTTP round trip rather than only reading endpoint metadata the
    /// way <see cref="AuthorizationCoverageTests"/> does) a no-op. Every other member only exists to
    /// satisfy the interface.</summary>
    private sealed class FakeUserStoreFacade : IUserStoreFacade
    {
        public Task<List<UserRecord>> GetUsersAsync() => Task.FromResult(new List<UserRecord>());
        public Task<UserRecord?> ValidateCredentialsAsync(string username, string password) => throw new NotSupportedException();
        public Task<bool> CreateUserAsync(string username, string displayName, string role, string password) => throw new NotSupportedException();
        public Task<bool> UpdateUserAsync(string username, string? displayName, string? role, string? password) => throw new NotSupportedException();
        public Task<bool> DeleteUserAsync(string username) => throw new NotSupportedException();
    }

    /// <summary>Same story as <see cref="FakeUserStoreFacade"/>: an empty policy document is what makes
    /// <c>LegacyRoleMigration.Apply</c> a no-op against an empty user list. Nothing this file asserts
    /// reaches the write members.</summary>
    private sealed class FakeAccessPolicyFacade : IAccessPolicyFacade
    {
        public Task<AccessPolicyDocument> GetPolicyAsync() => Task.FromResult(new AccessPolicyDocument());
        public Task<long> GetVersionAsync() => Task.FromResult(0L);
        public Task<RoleDefinition?> UpsertRoleAsync(RoleDefinition role, string actor) => throw new NotSupportedException();
        public Task<bool> DeleteRoleAsync(string name) => throw new NotSupportedException();
        public Task<GroupDefinition?> UpsertGroupAsync(GroupDefinition group, string actor) => throw new NotSupportedException();
        public Task<bool> DeleteGroupAsync(string name) => throw new NotSupportedException();
        public Task<UserAccessEntry?> UpsertUserAccessAsync(UserAccessEntry entry, string actor) => throw new NotSupportedException();
        public Task<bool> DeleteUserAccessAsync(string username) => throw new NotSupportedException();
        public Task<ApprovalTemplate?> UpsertApprovalTemplateAsync(ApprovalTemplate template, string actor) => throw new NotSupportedException();
        public Task<bool> DeleteApprovalTemplateAsync(string name) => throw new NotSupportedException();
    }

    private async Task<HttpClient> StartAsync(string flavor = "orleans", IReadOnlyList<string>? grpcServices = null)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Jwt:Key"] = new string('k', 64),
            ["Jwt:Issuer"] = "streamsforge-test",
            ["Jwt:Audience"] = "streamsforge-test",
        });
        builder.Services.AddStreamsForgeApi(builder.Configuration);

        // A real StartAsync() (unlike AuthorizationCoverageTests, which never calls it) makes ASP.NET
        // build the full endpoint route table eagerly — Minimal API's RequestDelegateFactory infers a
        // parameter's binding SOURCE (service vs. body) by asking the container WHETHER THE TYPE IS
        // REGISTERED (IServiceProviderIsService — it does not invoke the factory), for EVERY mapped
        // route, not only the one this file calls. So every facade interface every *Endpoints.cs handler
        // in the whole surface takes needs to be resolvable-in-principle, or it gets mis-inferred as a
        // [FromBody] parameter and endpoint construction throws. Deliberately narrower than
        // AuthorizationCoverageTests.RegisterHandlerDependencies (which also stubs every public class in
        // the assembly): that test never calls StartAsync, so a stubbed concrete class's factory is never
        // actually INVOKED. This one does call StartAsync, so hosted services DO invoke the real
        // AddStreamsForgeApi-registered factories for concrete types like AuditChannelSink — stubbing
        // those over would break the very machinery this test needs to actually start.
        var throwingStubTypes = typeof(ICatalogFacade).Assembly.GetTypes()
            .Where(t => t.IsInterface && t.IsPublic && t.Name.EndsWith("Facade", StringComparison.Ordinal))
            // Same narrow addition AuthorizationCoverageTests makes for the same reason: at least one
            // non-facade handler parameter (a *Tracker class, e.g. IngestKeyUsageTracker) is bound as a
            // service too. Concrete, not just interfaces — but still name-filtered rather than "every
            // public class", which is what broke AuditChannelSink above.
            .Concat(typeof(StreamsForge.AppCore.Ingest.IngestKeyUsageTracker).Assembly.GetTypes()
                .Where(t => t.IsClass && t.IsPublic && !t.IsAbstract && !t.IsGenericType
                            && t.Name.EndsWith("Tracker", StringComparison.Ordinal)));
        foreach (var t in throwingStubTypes)
        {
            builder.Services.AddSingleton(t, _ => throw new InvalidOperationException(
                $"{t.Name} was resolved — this test only drives /api/meta/instance."));
        }

        // Registered AFTER the throwing stubs above so these three win at resolution time (the built-in
        // container resolves the LAST registration for a singular GetRequiredService<T>() call) — the
        // only three real fakes this file needs for the routes it actually exercises. AccessBootstrapService
        // (a hosted service AddStreamsForgeApi registers unconditionally) eagerly constructor-injects
        // IAccessPolicyFacade/IUserStoreFacade, so those two must work enough to make
        // LegacyRoleMigration.Apply a no-op, not merely fail to throw.
        builder.Services.AddSingleton<ICatalogFacade>(new FakeCatalogFacade());
        builder.Services.AddSingleton<IAccessPolicyFacade>(new FakeAccessPolicyFacade());
        builder.Services.AddSingleton<IUserStoreFacade>(new FakeUserStoreFacade());

        _app = builder.Build();
        _app.MapStreamsForgeApi(new StreamsForgeApiOptions(
            ProtosDir: Path.Combine(Path.GetTempPath(), "sf-discovery-endpoints-tests-protos"),
            GrpcPort: 7299,
            GrpcStaticServices: grpcServices ?? [],
            DocsFilePath: null,
            SpaDistPath: null,
            Flavor: flavor,
            DataDir: _dataDir,
            InstanceName: "test-instance",
            Version: "9.9.9-test"));

        await _app.StartAsync();
        var address = _app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First();
        return new HttpClient { BaseAddress = new Uri(address) };
    }

    [Fact]
    public async Task Instance_route_is_reachable_with_no_authorization_header_at_all()
    {
        using var client = await StartAsync();

        using var response = await client.GetAsync("/api/meta/instance");

        Assert.Equal(System.Net.HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task Instance_route_reports_identity_flavor_version_and_name_from_options()
    {
        using var client = await StartAsync();

        var info = await client.GetFromJsonAsync<InstanceInfo>("/api/meta/instance");

        Assert.NotNull(info);
        Assert.False(string.IsNullOrEmpty(info!.InstanceId));
        Assert.Equal("orleans", info.Flavor);
        Assert.Equal("9.9.9-test", info.Version);
        Assert.Equal("test-instance", info.Name);
        Assert.True(info.StartedAtMs > 0);
    }

    [Fact]
    public async Task Instance_id_is_stable_across_two_requests_and_persisted_to_the_data_dir()
    {
        using var client = await StartAsync();

        var first = await client.GetFromJsonAsync<InstanceInfo>("/api/meta/instance");
        var second = await client.GetFromJsonAsync<InstanceInfo>("/api/meta/instance");

        Assert.Equal(first!.InstanceId, second!.InstanceId);
        Assert.True(File.Exists(Path.Combine(_dataDir, InstanceIdentity.FileName)));
    }

    [Fact]
    public async Task Instance_route_reports_catalog_counts_from_the_facade()
    {
        using var client = await StartAsync();

        var info = await client.GetFromJsonAsync<InstanceInfo>("/api/meta/instance");

        Assert.Equal(2, info!.CatalogCounts["sources"]);
        Assert.Equal(3, info.CatalogCounts["pipelines"]);
        Assert.Equal(1, info.CatalogCounts["tables"]);
    }

    [Fact]
    public async Task Instance_route_surfaces_all_three_catalog_warning_categories()
    {
        using var client = await StartAsync();

        var info = await client.GetFromJsonAsync<InstanceInfo>("/api/meta/instance");

        Assert.Contains(info!.CatalogWarnings, w => w.Contains("more than one pipeline", StringComparison.Ordinal));
        Assert.Contains(info.CatalogWarnings, w => w.Contains("table(s) have a stale pin", StringComparison.Ordinal));
        Assert.Contains(info.CatalogWarnings, w => w.Contains("not-a-real-kind", StringComparison.Ordinal));
    }

    /// <summary>The security property behind the shape of those warnings, pinned separately so it cannot
    /// be lost to a later "make the warning more helpful" edit: this route is ANONYMOUS, and the fixture
    /// above is built entirely out of entities in a warning state — so if any warning named its entity,
    /// an unauthenticated caller would read the catalog's contents off a route that exists to say what
    /// this INSTANCE is. Counts and connector-kind names only; the operator who needs to know which
    /// entity reads it off the catalog routes they already hold a Viewer grant for.</summary>
    [Fact]
    public async Task Instance_route_warnings_name_no_entity_even_though_every_fixture_entity_is_in_a_warning_state()
    {
        using var client = await StartAsync();

        var info = await client.GetFromJsonAsync<InstanceInfo>("/api/meta/instance");

        var joined = string.Join("\n", info!.CatalogWarnings);
        Assert.NotEmpty(info.CatalogWarnings);
        foreach (var entityName in new[] { "trades", "quotes", "fx_desk", "rates_desk", "daily_pnl" })
        {
            Assert.DoesNotContain(entityName, joined, StringComparison.Ordinal);
        }
    }

    [Fact]
    public async Task Instance_route_omits_the_grpc_endpoint_when_the_flavor_serves_no_static_grpc_services()
    {
        // Mirrors the Dapr host's actual Program.cs shape: GrpcStaticServices is empty there (gRPC
        // serving is phase 2 on that flavor) — the whole point of the servesGrpc check.
        using var client = await StartAsync(flavor: "dapr", grpcServices: []);

        var info = await client.GetFromJsonAsync<InstanceInfo>("/api/meta/instance");

        Assert.False(info!.Endpoints.ContainsKey("grpc"));
        Assert.DoesNotContain("grpc", info.Capabilities);
    }

    [Fact]
    public async Task Instance_route_reports_a_grpc_endpoint_when_the_flavor_serves_static_grpc_services()
    {
        using var client = await StartAsync(flavor: "orleans", grpcServices: ["SourceService"]);

        var info = await client.GetFromJsonAsync<InstanceInfo>("/api/meta/instance");

        Assert.True(info!.Endpoints.ContainsKey("grpc"));
        Assert.Contains("7299", info.Endpoints["grpc"]);
        Assert.Contains("grpc", info.Capabilities);
    }
}
