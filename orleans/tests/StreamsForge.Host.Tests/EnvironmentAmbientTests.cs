using System.Net;
using System.Net.Http.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using StreamsForge.Abstractions;
using StreamsForge.Api;
using StreamsForge.AppCore.Environments;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>
/// Plan 021 wave 1 (track C, decision D4) — the three tests D4 itself says the wave owes, exercised over
/// a real Kestrel listener (the <see cref="DiscoveryEndpointsTests"/> pattern: <c>StartAsync()</c> for
/// real, no <c>WebApplicationFactory</c>, because neither host's <c>Program.cs</c> is startable without
/// its runtime) so the assertions are about the actual ASP.NET Core middleware pipeline
/// (<c>EnvironmentSelectionMiddleware</c>) rather than about one handler invoked in isolation —
/// <see cref="CatalogEntitlementEndpointTests"/>'s <c>endpoint.RequestDelegate!(http)</c> trick would
/// SKIP the middleware entirely, which is exactly the thing under test here.
///
/// <para>Two tiny test-only routes (<c>/__test/echo</c>, GET and POST) are mapped directly on the built
/// <see cref="WebApplication"/> AFTER <see cref="StreamsForgeApiExtensions.MapStreamsForgeApi"/>, and
/// report <see cref="EnvironmentAmbient.Current"/> back as JSON. They exist so the ambient can be
/// observed from outside the process without adding a diagnostic endpoint to the real API surface, and
/// so the "creates nothing" claim in the unknown-environment tests can be checked with a plain counter
/// instead of standing up the full entitlements/JWT stack <c>EnvironmentEndpointsTests</c> needs for the
/// real <c>/api/environments</c> routes.</para>
/// </summary>
public sealed class EnvironmentAmbientTests : IAsyncDisposable
{
    private readonly string _dataDir = Path.Combine(Path.GetTempPath(), "sf-env-ambient-tests-" + Guid.NewGuid().ToString("n"));
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

    private sealed record EchoResponse(string Env);

    /// <summary>Answers <c>ExistsAsync</c> true for <see cref="EnvKeys.Default"/> and for whatever names
    /// were passed to the constructor, false otherwise — and counts every call, which is the whole point
    /// of this fake: <see cref="ExistsCalls"/> being zero after a request is how the D2 "costs nothing"
    /// claim is checked, not merely asserted in prose.</summary>
    private sealed class FakeEnvironmentFacade(params string[] known) : IEnvironmentFacade
    {
        private readonly HashSet<string> _known = new(known, StringComparer.Ordinal);

        public int ExistsCalls { get; private set; }

        public List<string> ExistsCallNames { get; } = [];

        public Task<List<EnvironmentRecord>> ListAsync() => Task.FromResult(new List<EnvironmentRecord>());

        public Task<bool> ExistsAsync(string name)
        {
            ExistsCalls++;
            ExistsCallNames.Add(name);
            return Task.FromResult(name == EnvKeys.Default || _known.Contains(name));
        }

        public Task<EnvironmentRecord> CreateAsync(string name, string description, string createdBy) =>
            throw new NotSupportedException("this file only drives the ambient middleware, never the CRUD routes");

        public Task<bool> DeleteAsync(string name, bool force) =>
            throw new NotSupportedException("this file only drives the ambient middleware, never the CRUD routes");
    }

    /// <summary>Never exercised by anything this file asserts — see <see cref="DiscoveryEndpointsTests"/>'s
    /// identical fixture for why an empty answer is what keeps <c>AccessBootstrapService</c>'s
    /// <c>LegacyRoleMigration.Apply</c> a no-op.</summary>
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

    private int _testWriteCalls;

    private async Task<HttpClient> StartAsync(FakeEnvironmentFacade environments)
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

        // Same reasoning as DiscoveryEndpointsTests.StartAsync: a real StartAsync() makes minimal API's
        // RequestDelegateFactory ask, for EVERY mapped route (not only the ones this file calls), whether
        // each handler-parameter TYPE is registered — so every facade/tracker interface in the whole REST
        // surface needs to be resolvable-in-principle, or endpoint construction throws before any test
        // runs. Stub every one to throw if actually INVOKED; then register the few real fakes needed.
        var throwingStubTypes = typeof(ICatalogFacade).Assembly.GetTypes()
            .Where(t => t.IsInterface && t.IsPublic && t.Name.EndsWith("Facade", StringComparison.Ordinal))
            .Concat(typeof(StreamsForge.AppCore.Ingest.IngestKeyUsageTracker).Assembly.GetTypes()
                .Where(t => t.IsClass && t.IsPublic && !t.IsAbstract && !t.IsGenericType
                            && t.Name.EndsWith("Tracker", StringComparison.Ordinal)));
        foreach (var t in throwingStubTypes)
        {
            builder.Services.AddSingleton(t, _ => throw new InvalidOperationException(
                $"{t.Name} was resolved — this test only drives the environment middleware."));
        }

        builder.Services.AddSingleton<IEnvironmentFacade>(environments);
        builder.Services.AddSingleton<IAccessPolicyFacade>(new FakeAccessPolicyFacade());
        builder.Services.AddSingleton<IUserStoreFacade>(new FakeUserStoreFacade());

        _app = builder.Build();
        _app.MapStreamsForgeApi(new StreamsForgeApiOptions(
            ProtosDir: Path.Combine(Path.GetTempPath(), "sf-env-ambient-tests-protos"),
            GrpcPort: 7298,
            GrpcStaticServices: [],
            DocsFilePath: null,
            SpaDistPath: null,
            Flavor: "test",
            DataDir: _dataDir));

        // Test-only echo routes: NOT part of the real API surface, mapped straight on the built app so
        // they still go through EnvironmentSelectionMiddleware (registered inside MapStreamsForgeApi)
        // exactly like every real route does. GET is the "read" shape, POST is the "write" shape — the
        // POST handler incrementing _testWriteCalls is the "creates nothing" check: if the middleware's
        // 404 short-circuits the pipeline, this line never runs.
        _app.MapGet("/__test/echo", () => Results.Ok(new EchoResponse(EnvironmentAmbient.Current)));
        _app.MapPost("/__test/echo", () =>
        {
            _testWriteCalls++;
            return Results.Ok(new EchoResponse(EnvironmentAmbient.Current));
        });

        await _app.StartAsync();
        var address = _app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First();
        return new HttpClient { BaseAddress = new Uri(address) };
    }

    [Fact]
    public async Task No_header_resolves_to_default_and_calls_ExistsAsync_zero_times()
    {
        var environments = new FakeEnvironmentFacade("staging");
        using var client = await StartAsync(environments);

        var echo = await client.GetFromJsonAsync<EchoResponse>("/__test/echo");

        Assert.Equal(EnvKeys.Default, echo!.Env);
        Assert.Equal("", echo.Env);
        // D2's acceptance criterion, checked as a fact rather than left as an assertion about behaviour:
        // the untouched request never even asked whether an environment exists.
        Assert.Equal(0, environments.ExistsCalls);
    }

    [Fact]
    public async Task The_literal_default_header_also_costs_nothing()
    {
        // "default" is explicitly one of the three spellings EnvKeys.Normalize maps back to
        // EnvKeys.Default (alongside absent and empty) — so it must be exactly as free as no header.
        var environments = new FakeEnvironmentFacade("staging");
        using var client = await StartAsync(environments);

        var request = new HttpRequestMessage(HttpMethod.Get, "/__test/echo");
        request.Headers.Add(EnvironmentSelectionMiddleware.HeaderName, "default");
        var response = await client.SendAsync(request);
        var echo = await response.Content.ReadFromJsonAsync<EchoResponse>();

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(EnvKeys.Default, echo!.Env);
        Assert.Equal(0, environments.ExistsCalls);
    }

    [Fact]
    public async Task Unknown_environment_is_404_on_a_read()
    {
        var environments = new FakeEnvironmentFacade("staging");
        using var client = await StartAsync(environments);

        var request = new HttpRequestMessage(HttpMethod.Get, "/__test/echo");
        request.Headers.Add(EnvironmentSelectionMiddleware.HeaderName, "nope");
        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        var body = await response.Content.ReadAsStringAsync();
        Assert.Contains("nope", body, StringComparison.Ordinal);
        Assert.Equal(1, environments.ExistsCalls);
        Assert.Equal(["nope"], environments.ExistsCallNames);
    }

    [Fact]
    public async Task Unknown_environment_is_404_on_a_write_and_creates_nothing()
    {
        var environments = new FakeEnvironmentFacade("staging");
        using var client = await StartAsync(environments);

        var request = new HttpRequestMessage(HttpMethod.Post, "/__test/echo")
        {
            Content = JsonContent.Create(new { anything = "goes" }),
        };
        request.Headers.Add(EnvironmentSelectionMiddleware.HeaderName, "nope");
        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        // The handler never ran: the middleware refused the request before the endpoint that would have
        // "created" anything was ever reached.
        Assert.Equal(0, _testWriteCalls);
    }

    [Fact]
    public async Task Query_env_overrides_the_header()
    {
        var environments = new FakeEnvironmentFacade("staging", "other");
        using var client = await StartAsync(environments);

        var request = new HttpRequestMessage(HttpMethod.Get, "/__test/echo?env=other");
        request.Headers.Add(EnvironmentSelectionMiddleware.HeaderName, "staging");
        var response = await client.SendAsync(request);
        var echo = await response.Content.ReadFromJsonAsync<EchoResponse>();

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("other", echo!.Env);
    }

    [Fact]
    public async Task The_ambient_does_not_leak_across_requests_on_a_reused_thread()
    {
        var environments = new FakeEnvironmentFacade("staging");
        using var client = await StartAsync(environments);

        // Two requests back to back over the SAME HttpClient (so Kestrel/HttpClient are free to reuse
        // whatever thread/connection they like) — one naming an environment, one not. If the ambient
        // were anything other than a per-request AsyncLocal write, the second request would observe the
        // first's value.
        var namedRequest = new HttpRequestMessage(HttpMethod.Get, "/__test/echo");
        namedRequest.Headers.Add(EnvironmentSelectionMiddleware.HeaderName, "staging");
        var namedResponse = await client.SendAsync(namedRequest);
        var namedEcho = await namedResponse.Content.ReadFromJsonAsync<EchoResponse>();

        var defaultEcho = await client.GetFromJsonAsync<EchoResponse>("/__test/echo");

        Assert.Equal("staging", namedEcho!.Env);
        Assert.Equal(EnvKeys.Default, defaultEcho!.Env);
    }
}
