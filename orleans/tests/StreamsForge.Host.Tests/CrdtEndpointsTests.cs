using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using StreamsForge.Abstractions;
using StreamsForge.Connectors.Crdt;
using StreamsForge.Api;
using StreamsForge.Api.Auth;
using StreamsForge.AppCore.Access;
using Xunit;
using Ycs;

namespace StreamsForge.Host.Tests;

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

        // Plan 020 wave D, finding 3 — records the actor it was called with so
        // Attribution_actor_reaches_the_facade_when_AttributeChanges_is_on can assert on it without a
        // real grain in this file (this file is about the ROUTE, per its own class doc).
        public string? LastAttributedActor { get; private set; }

        public Task<CrdtMergeResult?> MergeAttributedAsync(string sourceName, IReadOnlyList<byte[]> updates, string actor)
        {
            LastAttributedActor = actor;
            return Task.FromResult<CrdtMergeResult?>(new CrdtMergeResult { UpdatesApplied = updates.Count, RowsEmitted = updates.Count });
        }

        public Task<CrdtDocStatus?> GetStatusAsync(string sourceName) =>
            Task.FromResult<CrdtDocStatus?>(new CrdtDocStatus { EntityCount = 3, UpdatesMerged = 5, RowsEmitted = 5 });

        // Plan 020 wave C — the replay route's fake. Reports rows but zero updates applied, which is what
        // a real replay returns: it re-asserts the projection and merges nothing.
        public Task<CrdtMergeResult?> ReplayAsync(string sourceName) =>
            Task.FromResult<CrdtMergeResult?>(new CrdtMergeResult { UpdatesApplied = 0, RowsEmitted = 3 });

        // Plan 020 wave D — the inspection seam. This fake DELEGATES to the real decoder rather than
        // stubbing it: the route tests below feed genuine Yjs bytes and genuine garbage, and what they
        // are asserting is that a real undecidable frame is refused without aborting the batch. A stub
        // returning a canned answer would pass those tests while the decode was broken. The production
        // reason this call goes through ICrdtFacade at all is the project boundary (StreamsForge.Api holds
        // no reference to a connector — see that method's own doc comment); a test assembly that already
        // links the connector has no such constraint.
        public CrdtUpdateInspection Inspect(SourceDefinition source, byte[] update) =>
            CrdtUpdateInspector.Inspect(update, source.Connector?.Crdt ?? new CrdtSourceConfig());

        // Plan 020 wave F — the rebalance route's fake. Records the args it was called with so a test can
        // assert on what reached the facade without a real grain (this file is about the ROUTE, per its
        // own class doc); RebalanceRefuses toggles a canned refusal so the route's "business refusal is a
        // 200 with Ok:false" behavior is testable without a real EscrowCounter.
        public (string From, string To, long Amount)? LastRebalanceArgs { get; private set; }
        public bool RebalanceRefuses { get; set; }

        public Task<EscrowRebalanceResult?> RebalanceAsync(string sourceName, string from, string to, long amount)
        {
            LastRebalanceArgs = (from, to, amount);
            return Task.FromResult<EscrowRebalanceResult?>(RebalanceRefuses
                ? new EscrowRebalanceResult { Ok = false, Reason = "canned refusal", FromAllowance = 1, ToAllowance = 2 }
                : new EscrowRebalanceResult { Ok = true, FromAllowance = 3, ToAllowance = 4 });
        }
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

    /// <summary>Plan 020 wave D, finding 3 — captures whatever <c>CrdtEndpoints</c> hands
    /// <see cref="IAuditSink"/> directly, in-process, so a test does not have to race
    /// <c>AuditWriterService</c>'s own background drain to see a row land. Registered AFTER
    /// <c>AddStreamsForgeApi</c> so it replaces the real <c>AuditChannelSink</c> for this file's tests,
    /// same override idiom the fake facades above already use.</summary>
    private sealed class FakeAuditSink : IAuditSink
    {
        public List<AuditEntry> Entries { get; } = [];
        public void Record(AuditEntry entry) { lock (Entries) Entries.Add(entry); }
    }

    private readonly FakeCatalogFacade _catalog = new();
    private readonly FakeCrdtFacade _crdt = new();
    private readonly FakeAuditSink _audit = new();

    private async Task<HttpClient> StartAsync()
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

        // Same "every facade/tracker needs a resolvable-in-principle registration" requirement
        // DiscoveryEndpointsTests/EnvironmentEndpointsTests document on their own StartAsync — Minimal
        // API infers a handler parameter's binding source by asking the container whether the TYPE is
        // registered, for every mapped route, not only the ones this file exercises.
        var throwingStubTypes = typeof(ICatalogFacade).Assembly.GetTypes()
            .Where(t => t.IsInterface && t.IsPublic && t.Name.EndsWith("Facade", StringComparison.Ordinal))
            .Concat(typeof(StreamsForge.AppCore.Ingest.IngestKeyUsageTracker).Assembly.GetTypes()
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
        builder.Services.AddSingleton<IAuditSink>(_audit);

        _app = builder.Build();
        _app.MapStreamsForgeApi(new StreamsForgeApiOptions(
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

    private static SourceDefinition CrdtSource(string name, CrdtSourceConfig? config = null) => new()
    {
        Name = name,
        Kind = SourceKinds.Crdt,
        Fields = [new FieldDef("id", FieldType.String)],
        Connector = new ConnectorConfig { Crdt = config ?? new CrdtSourceConfig { RootMap = "root", KeyField = "id" } },
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

    // ---------------------------------------------------------------------------------------------
    // Plan 020 wave D, finding 3 — the "executed" audit row, distinct from AccessGuard's own
    // allow/deny row (that one is asserted here only by absence-of-a-second-mechanism; its own
    // coverage lives in AccessGuardTests/ChatToolGate's suite).
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task Executed_audit_row_is_written_after_a_successful_merge()
    {
        _catalog.Sources["doc1"] = CrdtSource("doc1");
        using var client = await StartAsync();

        var updates = new CrdtUpdatesRequest([Convert.ToBase64String([1, 2, 3])]);
        var response = await client.SendAsync(AuthedRequest(HttpMethod.Post, "/api/sources/doc1/crdt/updates", "Editor", updates));
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var executed = Assert.Single(_audit.Entries, e => e.Outcome == "executed" && e.Scope == "doc1");
        Assert.Equal(Actions.SourceWrite, executed.Action);
        Assert.Contains("crdt merge", executed.Detail);
        Assert.Contains("1 update(s) applied", executed.Detail); // FakeCrdtFacade echoes updates.Count
    }

    [Fact]
    public async Task Executed_audit_row_is_written_after_a_successful_replay()
    {
        _catalog.Sources["doc1"] = CrdtSource("doc1");
        using var client = await StartAsync();

        var response = await client.SendAsync(AuthedRequest(HttpMethod.Post, "/api/sources/doc1/crdt/replay", "Editor"));
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var executed = Assert.Single(_audit.Entries, e => e.Outcome == "executed" && e.Scope == "doc1");
        Assert.Contains("crdt replay", executed.Detail);
        Assert.Contains("3 row(s) re-asserted", executed.Detail); // FakeCrdtFacade.ReplayAsync's fixed 3
    }

    // ---------------------------------------------------------------------------------------------
    // Plan 020 wave D, finding 3 — attribution actor threading. AttributeChanges is off by default
    // (the config in CrdtSource(name) with no override), so the plain path must be untouched.
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task Attribution_actor_reaches_the_facade_when_AttributeChanges_is_on()
    {
        _catalog.Sources["doc1"] = CrdtSource("doc1", new CrdtSourceConfig { RootMap = "root", KeyField = "id", AttributeChanges = true });
        using var client = await StartAsync();

        var updates = new CrdtUpdatesRequest([Convert.ToBase64String([1, 2, 3])]);
        var response = await client.SendAsync(AuthedRequest(HttpMethod.Post, "/api/sources/doc1/crdt/updates", "Editor", updates));

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("editor", _crdt.LastAttributedActor); // AuthedRequest mints role.ToLowerInvariant() as the username
    }

    [Fact]
    public async Task Attribution_actor_is_not_forwarded_when_AttributeChanges_is_off()
    {
        _catalog.Sources["doc1"] = CrdtSource("doc1"); // AttributeChanges defaults to false
        using var client = await StartAsync();

        var updates = new CrdtUpdatesRequest([Convert.ToBase64String([1, 2, 3])]);
        var response = await client.SendAsync(AuthedRequest(HttpMethod.Post, "/api/sources/doc1/crdt/updates", "Editor", updates));

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Null(_crdt.LastAttributedActor); // MergeAsync was called, not MergeAttributedAsync
    }

    // ---------------------------------------------------------------------------------------------
    // Plan 020 wave D, finding 2 — pre-merge entity-level authorization. Off by default; when on, an
    // undecidable update is refused individually and the rest of the batch still merges (D7's own
    // "a flaky link must not strand every good one behind it", applied to a missing/undecidable grant).
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task EntityAuthorization_off_by_default_forwards_every_update_unfiltered()
    {
        _catalog.Sources["doc1"] = CrdtSource("doc1"); // RequireEntityAuthorization defaults to false
        using var client = await StartAsync();

        // Garbage bytes — CrdtUpdateInspector would call this Undecidable, but the flag is off so the
        // inspector never runs at all and the byte-opaque FakeCrdtFacade sees it unfiltered.
        var updates = new CrdtUpdatesRequest([Convert.ToBase64String([1, 2, 3])]);
        var response = await client.SendAsync(AuthedRequest(HttpMethod.Post, "/api/sources/doc1/crdt/updates", "Editor", updates));

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var result = await response.Content.ReadFromJsonAsync<CrdtMergeResult>();
        Assert.Equal(1, result!.UpdatesApplied);
        Assert.Empty(result.Diagnostics);
    }

    [Fact]
    public async Task EntityAuthorization_refuses_an_undecidable_update_without_aborting_the_batch()
    {
        var config = new CrdtSourceConfig { RootMap = "root", KeyField = "id", RequireEntityAuthorization = true };
        _catalog.Sources["doc1"] = CrdtSource("doc1", config);
        using var client = await StartAsync();

        var doc = new YDoc();
        doc.Transact(_ =>
        {
            var e1 = new YMap();
            doc.GetMap("root").Set("e1", e1);
            e1.Set("name", "Ann");
        });
        var decidableUpdate = Convert.ToBase64String(doc.EncodeStateAsUpdateV1());
        var undecidableUpdate = Convert.ToBase64String([0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);

        var updates = new CrdtUpdatesRequest([undecidableUpdate, decidableUpdate]);
        var response = await client.SendAsync(AuthedRequest(HttpMethod.Post, "/api/sources/doc1/crdt/updates", "Editor", updates));
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var result = await response.Content.ReadFromJsonAsync<CrdtMergeResult>();
        // Only the decidable update reached FakeCrdtFacade — it echoes the COUNT it was called with.
        Assert.Equal(1, result!.UpdatesApplied);
        Assert.Contains(result.Diagnostics, d => d.Contains("update[0]") && d.Contains("refused pre-merge"));
    }

    [Fact]
    public async Task EntityAuthorization_forwards_a_decidable_update_the_caller_is_granted_on()
    {
        var config = new CrdtSourceConfig { RootMap = "root", KeyField = "id", RequireEntityAuthorization = true };
        _catalog.Sources["doc1"] = CrdtSource("doc1", config);
        using var client = await StartAsync();

        var doc = new YDoc();
        doc.GetMap("root").Set("e1", "whole-entity-scalar");

        // The built-in Editor role's Actions.SourceWrite grant is scoped "*" (BuiltInRoleCatalog), which
        // matches the composite "doc1/e1" scope too — this is the "operator did not need to add a new
        // grant just to keep working with a blanket role" case; EntityAuthorization_refuses_an_
        // undecidable_update... above and the live check cover the narrower-grant boundary this harness
        // cannot construct without a custom role.
        var updates = new CrdtUpdatesRequest([Convert.ToBase64String(doc.EncodeStateAsUpdateV1())]);
        var response = await client.SendAsync(AuthedRequest(HttpMethod.Post, "/api/sources/doc1/crdt/updates", "Editor", updates));

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var result = await response.Content.ReadFromJsonAsync<CrdtMergeResult>();
        Assert.Equal(1, result!.UpdatesApplied);
        Assert.Empty(result.Diagnostics);
    }

    // ---------------------------------------------------------------------------------------------
    // Plan 020 wave F — /crdt/escrow/rebalance. Same 501/404/409 shape as the other two routes
    // (proven once here rather than duplicating every status-code test — the ordering is identical
    // code in CrdtEndpoints.cs); what is specific to this route is the 200-with-Ok:false business
    // refusal, the Editor floor, and the "executed" audit row for BOTH outcomes.
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task Rebalance_disabled_facade_answers_501()
    {
        _crdt.Enabled = false;
        using var client = await StartAsync();

        var response = await client.SendAsync(AuthedRequest(
            HttpMethod.Post, "/api/sources/whatever/crdt/escrow/rebalance", "Editor",
            new EscrowRebalanceRequest("a", "b", 1)));
        Assert.Equal(HttpStatusCode.NotImplemented, response.StatusCode);
    }

    [Fact]
    public async Task Rebalance_unknown_source_is_404()
    {
        using var client = await StartAsync();

        var response = await client.SendAsync(AuthedRequest(
            HttpMethod.Post, "/api/sources/nope/crdt/escrow/rebalance", "Editor",
            new EscrowRebalanceRequest("a", "b", 1)));
        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task Rebalance_wrong_kind_source_is_409()
    {
        _catalog.Sources["notcrdt"] = new SourceDefinition { Name = "notcrdt", Kind = SourceKinds.Generator };
        using var client = await StartAsync();

        var response = await client.SendAsync(AuthedRequest(
            HttpMethod.Post, "/api/sources/notcrdt/crdt/escrow/rebalance", "Editor",
            new EscrowRebalanceRequest("a", "b", 1)));
        Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
    }

    [Fact]
    public async Task Rebalance_viewer_is_forbidden()
    {
        _catalog.Sources["doc1"] = CrdtSource("doc1");
        using var client = await StartAsync();

        var response = await client.SendAsync(AuthedRequest(
            HttpMethod.Post, "/api/sources/doc1/crdt/escrow/rebalance", "Viewer",
            new EscrowRebalanceRequest("a", "b", 1)));
        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
    }

    [Fact]
    public async Task Rebalance_success_is_200_with_Ok_true_and_the_facade_sees_the_exact_args()
    {
        _catalog.Sources["doc1"] = CrdtSource("doc1");
        using var client = await StartAsync();

        var response = await client.SendAsync(AuthedRequest(
            HttpMethod.Post, "/api/sources/doc1/crdt/escrow/rebalance", "Editor",
            new EscrowRebalanceRequest("site-a", "site-b", 5)));

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var result = await response.Content.ReadFromJsonAsync<EscrowRebalanceResult>();
        Assert.True(result!.Ok);
        Assert.Equal(3, result.FromAllowance);
        Assert.Equal(4, result.ToAllowance);
        Assert.Equal(("site-a", "site-b", 5L), _crdt.LastRebalanceArgs);
    }

    [Fact]
    public async Task Rebalance_refusal_is_200_with_Ok_false_and_a_reason_not_an_HTTP_error()
    {
        _catalog.Sources["doc1"] = CrdtSource("doc1");
        _crdt.RebalanceRefuses = true;
        using var client = await StartAsync();

        var response = await client.SendAsync(AuthedRequest(
            HttpMethod.Post, "/api/sources/doc1/crdt/escrow/rebalance", "Editor",
            new EscrowRebalanceRequest("site-a", "site-b", 999)));

        // A business refusal (insufficient allowance, unknown replica, ...) is reported IN the body,
        // exactly like a per-update merge diagnostic is — never an HTTP error status.
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var result = await response.Content.ReadFromJsonAsync<EscrowRebalanceResult>();
        Assert.False(result!.Ok);
        Assert.Equal("canned refusal", result.Reason);
    }

    [Fact]
    public async Task Rebalance_writes_an_executed_audit_row_on_success_and_on_refusal()
    {
        _catalog.Sources["doc1"] = CrdtSource("doc1");
        using var client = await StartAsync();

        var ok = await client.SendAsync(AuthedRequest(
            HttpMethod.Post, "/api/sources/doc1/crdt/escrow/rebalance", "Editor",
            new EscrowRebalanceRequest("site-a", "site-b", 5)));
        Assert.Equal(HttpStatusCode.OK, ok.StatusCode);

        var executedOk = Assert.Single(_audit.Entries, e => e.Outcome == "executed" && e.Scope == "doc1");
        Assert.Contains("escrow rebalance:", executedOk.Detail);
        Assert.Contains("'site-a' -> 'site-b'", executedOk.Detail);

        _audit.Entries.Clear();
        _crdt.RebalanceRefuses = true;
        var refused = await client.SendAsync(AuthedRequest(
            HttpMethod.Post, "/api/sources/doc1/crdt/escrow/rebalance", "Editor",
            new EscrowRebalanceRequest("site-a", "site-b", 999)));
        Assert.Equal(HttpStatusCode.OK, refused.StatusCode);

        var executedRefused = Assert.Single(_audit.Entries, e => e.Outcome == "executed" && e.Scope == "doc1");
        Assert.Contains("REFUSED", executedRefused.Detail);
        Assert.Contains("canned refusal", executedRefused.Detail);
    }
}
