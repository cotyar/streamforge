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
using StreamForge.AppCore.Environments;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// Plan 021 wave 1 (track C) — <c>/api/environments</c> exercised over real HTTP, the
/// <see cref="DiscoveryEndpointsTests"/> pattern: a real Kestrel listener on a dynamic port, fakes behind
/// every facade the route touches, no Orleans silo. <c>WebApplicationFactory</c> is declined for the
/// same reason that file declines it.
///
/// <para>Authenticated calls use a real JWT minted by <see cref="JwtTokenService"/> against a document
/// whose only roles are the built-in three (<see cref="BuiltInRoleCatalog.Create"/>) — no per-user
/// <see cref="UserAccessEntry"/> — so <see cref="StreamForge.Api.Auth.PermissionResolver"/> resolves
/// grants purely off the token's <c>ClaimTypes.Role</c>, the pre-015 legacy-equivalence path every seeded
/// login still exercises today. That is enough to prove the Viewer/Admin split this file is about,
/// without standing up a user store.</para>
/// </summary>
public sealed class EnvironmentEndpointsTests : IAsyncDisposable
{
    private readonly string _dataDir = Path.Combine(Path.GetTempPath(), "sf-env-endpoints-tests-" + Guid.NewGuid().ToString("n"));
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

    /// <summary>An in-memory store good enough to exercise the route's status-code mapping: unknown name
    /// on create → normal create; a name already present → <see cref="InvalidOperationException"/> (409,
    /// the convention every other catalog refusal in this codebase uses — see
    /// <c>EnvironmentsEndpoints</c>'s own doc comment); a name that fails <see cref="EnvKeys.IsValidName"/>
    /// → <see cref="ArgumentException"/> (400); deleting the default environment refuses, always.</summary>
    private sealed class FakeEnvironmentFacade : IEnvironmentFacade
    {
        private readonly Dictionary<string, EnvironmentRecord> _store = new(StringComparer.Ordinal);

        public Task<List<EnvironmentRecord>> ListAsync() =>
            Task.FromResult(_store.Values.OrderBy(e => e.Name, StringComparer.Ordinal).ToList());

        public Task<bool> ExistsAsync(string name) =>
            Task.FromResult(name == EnvKeys.Default || _store.ContainsKey(name));

        public Task<EnvironmentRecord> CreateAsync(string name, string description, string createdBy)
        {
            if (!EnvKeys.IsValidName(name))
            {
                throw new ArgumentException($"'{name}' is not a valid environment name");
            }

            if (_store.ContainsKey(name))
            {
                throw new InvalidOperationException($"environment '{name}' already exists");
            }

            var record = new EnvironmentRecord { Name = name, Description = description, CreatedBy = createdBy, CreatedAtMs = 1 };
            _store[name] = record;
            return Task.FromResult(record);
        }

        public Task<bool> DeleteAsync(string name, bool force)
        {
            if (name == EnvKeys.Default)
            {
                throw new InvalidOperationException("the default environment cannot be deleted");
            }

            return Task.FromResult(_store.Remove(name));
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

    /// <summary>Only the three built-in roles — no per-user entries — so every decision below runs off
    /// the token's role claim, per the class remarks.</summary>
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

        // See DiscoveryEndpointsTests.StartAsync for why every facade/tracker interface needs a
        // resolvable-in-principle registration before StartAsync() can build the route table at all.
        var throwingStubTypes = typeof(ICatalogFacade).Assembly.GetTypes()
            .Where(t => t.IsInterface && t.IsPublic && t.Name.EndsWith("Facade", StringComparison.Ordinal))
            .Concat(typeof(StreamForge.AppCore.Ingest.IngestKeyUsageTracker).Assembly.GetTypes()
                .Where(t => t.IsClass && t.IsPublic && !t.IsAbstract && !t.IsGenericType
                            && t.Name.EndsWith("Tracker", StringComparison.Ordinal)));
        foreach (var t in throwingStubTypes)
        {
            builder.Services.AddSingleton(t, _ => throw new InvalidOperationException(
                $"{t.Name} was resolved — this file only drives /api/environments."));
        }

        builder.Services.AddSingleton<IEnvironmentFacade, FakeEnvironmentFacade>();
        builder.Services.AddSingleton<IAccessPolicyFacade>(new FakeAccessPolicyFacade());
        builder.Services.AddSingleton<IUserStoreFacade>(new FakeUserStoreFacade());

        _app = builder.Build();
        _app.MapStreamForgeApi(new StreamForgeApiOptions(
            ProtosDir: Path.Combine(Path.GetTempPath(), "sf-env-endpoints-tests-protos"),
            GrpcPort: 7297,
            GrpcStaticServices: [],
            DocsFilePath: null,
            SpaDistPath: null,
            Flavor: "test",
            DataDir: _dataDir));

        await _app.StartAsync();
        var address = _app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First();
        return new HttpClient { BaseAddress = new Uri(address) };
    }

    /// <summary>A bearer token for a user whose ONLY grants come from <paramref name="role"/> via the
    /// legacy-equivalence path (see the class remarks) — no X-StreamForge-Environment header, so every
    /// call in this file resolves the ambient to <see cref="EnvKeys.Default"/> for free (D2) and the
    /// fake's <c>ExistsAsync</c> is never consulted by the middleware.</summary>
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

    // ---------------------------------------------------------------------------------------------
    // Happy paths
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task Viewer_can_list_environments()
    {
        using var client = await StartAsync();

        var response = await client.SendAsync(AuthedRequest(HttpMethod.Get, "/api/environments", "Viewer"));

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var list = await response.Content.ReadFromJsonAsync<List<EnvironmentRecord>>();
        Assert.NotNull(list);
    }

    [Fact]
    public async Task Admin_can_create_an_environment()
    {
        using var client = await StartAsync();

        var response = await client.SendAsync(AuthedRequest(
            HttpMethod.Post, "/api/environments", "Admin", new CreateEnvironmentRequest("staging", "for testing")));

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var created = await response.Content.ReadFromJsonAsync<EnvironmentRecord>();
        Assert.Equal("staging", created!.Name);
        Assert.Equal("admin", created.CreatedBy);

        var listResponse = await client.SendAsync(AuthedRequest(HttpMethod.Get, "/api/environments", "Viewer"));
        var list = await listResponse.Content.ReadFromJsonAsync<List<EnvironmentRecord>>();
        Assert.Contains(list!, e => e.Name == "staging");
    }

    [Fact]
    public async Task Admin_can_delete_an_environment()
    {
        using var client = await StartAsync();
        await client.SendAsync(AuthedRequest(
            HttpMethod.Post, "/api/environments", "Admin", new CreateEnvironmentRequest("staging", "")));

        var response = await client.SendAsync(AuthedRequest(HttpMethod.Delete, "/api/environments/staging", "Admin"));

        Assert.Equal(HttpStatusCode.NoContent, response.StatusCode);
        var listResponse = await client.SendAsync(AuthedRequest(HttpMethod.Get, "/api/environments", "Viewer"));
        var list = await listResponse.Content.ReadFromJsonAsync<List<EnvironmentRecord>>();
        Assert.DoesNotContain(list!, e => e.Name == "staging");
    }

    [Fact]
    public async Task Deleting_an_unknown_environment_is_404()
    {
        using var client = await StartAsync();

        var response = await client.SendAsync(AuthedRequest(HttpMethod.Delete, "/api/environments/nope", "Admin"));

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    // ---------------------------------------------------------------------------------------------
    // Refusals
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task Create_with_an_invalid_name_is_400()
    {
        using var client = await StartAsync();

        var response = await client.SendAsync(AuthedRequest(
            HttpMethod.Post, "/api/environments", "Admin", new CreateEnvironmentRequest("NOT VALID!", "")));

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task Create_with_a_duplicate_name_is_409()
    {
        using var client = await StartAsync();
        await client.SendAsync(AuthedRequest(
            HttpMethod.Post, "/api/environments", "Admin", new CreateEnvironmentRequest("staging", "")));

        var response = await client.SendAsync(AuthedRequest(
            HttpMethod.Post, "/api/environments", "Admin", new CreateEnvironmentRequest("staging", "again")));

        Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
    }

    [Fact]
    public async Task Viewer_is_refused_on_create()
    {
        using var client = await StartAsync();

        var response = await client.SendAsync(AuthedRequest(
            HttpMethod.Post, "/api/environments", "Viewer", new CreateEnvironmentRequest("staging", "")));

        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
    }

    [Fact]
    public async Task Viewer_is_refused_on_delete()
    {
        using var client = await StartAsync();
        await client.SendAsync(AuthedRequest(
            HttpMethod.Post, "/api/environments", "Admin", new CreateEnvironmentRequest("staging", "")));

        var response = await client.SendAsync(AuthedRequest(HttpMethod.Delete, "/api/environments/staging", "Viewer"));

        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);

        // And it is really still there — a 403 that quietly deleted anyway would be worse than no route.
        var listResponse = await client.SendAsync(AuthedRequest(HttpMethod.Get, "/api/environments", "Viewer"));
        var list = await listResponse.Content.ReadFromJsonAsync<List<EnvironmentRecord>>();
        Assert.Contains(list!, e => e.Name == "staging");
    }

    /// <summary>D7: renaming an environment is refused outright, by not existing — there is no
    /// <c>PUT /api/environments/{name}</c> route at all. <c>/api/environments/{name}</c> IS a matched
    /// route pattern (DELETE owns it), so ASP.NET's routing layer answers PUT there with 405 Method Not
    /// Allowed rather than 404 — which is the routing-layer proof that no PUT handler was ever mapped,
    /// not a facade refusal reachable at runtime.</summary>
    [Fact]
    public async Task There_is_no_rename_route()
    {
        using var client = await StartAsync();

        var response = await client.SendAsync(AuthedRequest(
            HttpMethod.Put, "/api/environments/staging", "Admin", new CreateEnvironmentRequest("staging-renamed", "")));

        Assert.Equal(HttpStatusCode.MethodNotAllowed, response.StatusCode);
    }
}
