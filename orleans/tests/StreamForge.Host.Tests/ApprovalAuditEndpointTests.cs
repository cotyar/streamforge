using System.Reflection;
using System.Security.Claims;
using System.Text.Json;
using System.Text.Json.Serialization;
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
/// Plan 015 wave 5-A — the approvals and audit REST surface, tested by RUNNING the handlers.
///
/// <para>Same in-process harness <see cref="CatalogEntitlementEndpointTests"/> established: build a
/// <see cref="WebApplication"/>, map the real routes, invoke a mapped
/// <see cref="RouteEndpoint.RequestDelegate"/> against a hand-built <see cref="DefaultHttpContext"/>.
/// No port, no silo, no <c>Run()</c>. What that buys here is the four properties that cannot be read
/// off metadata: that a caller cannot file a request as somebody else, that a requester is refused
/// their own approval THROUGH THE ROUTE (the state machine's own tests prove the rule; these prove the
/// route reaches it), that the listing rule shows an inbox to the people who can act on it and to
/// nobody else, and that <see cref="AuditPage.Truncated"/> survives into the response body.</para>
///
/// <para>The approvals fake below is the real <see cref="ApprovalStateMachine"/> plus the real
/// <see cref="EffectivePermissionsBuilder"/> over an in-memory list — i.e. the same two calls both
/// shipped stores make. A fake that decided anything for itself would be testing the fake.</para>
/// </summary>
public class ApprovalAuditEndpointTests
{
    // ---------------------------------------------------------------------------------------------
    // Fixture
    // ---------------------------------------------------------------------------------------------

    private const string Reviewers = "reviewers";

    /// <summary>One template covering everything, decided by one member of <c>reviewers</c>.</summary>
    private static ApprovalTemplate Template() => new()
    {
        Name = "everything",
        ActionPattern = "*",
        ScopePattern = "*",
        RequiredApprovals = 1,
        ApproverGroups = [Reviewers],
        ExpiresAfterSeconds = 3600,
        Enabled = true,
    };

    /// <summary>Five principals, chosen so that every clause of the visibility rule is separable:
    /// <list type="bullet">
    ///   <item><c>alice</c> — may file, is in no group, holds no decide entitlement.</item>
    ///   <item><c>bob</c> — in <c>reviewers</c> AND entitled to decide: the real approver.</item>
    ///   <item><c>carol</c> — in <c>reviewers</c>, NOT entitled to decide.</item>
    ///   <item><c>dave</c> — entitled to decide, NOT in <c>reviewers</c>.</item>
    ///   <item><c>root</c> — <c>access.read</c> at <c>*</c>: the administrator.</item>
    /// </list></summary>
    private static AccessPolicyDocument Document()
    {
        var document = new AccessPolicyDocument
        {
            Roles = BuiltInRoleCatalog.Create(),
            Version = 1,
            ApprovalTemplates = [Template()],
        };

        document.Groups.Add(new GroupDefinition { Name = Reviewers, Members = ["bob", "carol"] });

        document.Users.AddRange(
            User("alice", Allow(Actions.ApprovalRequest, "*")),
            User("bob", Allow(Actions.ApprovalRequest, "*"), Allow(Actions.ApprovalDecide, "*")),
            User("carol", Allow(Actions.ApprovalRequest, "*")),
            User("dave", Allow(Actions.ApprovalDecide, "*")),
            User("root", Allow(Actions.AccessRead, "*"), Allow(Actions.AuditRead, "*")),
            User("nosy"));

        return document;
    }

    private static UserAccessEntry User(string username, params PermissionGrant[] grants) =>
        new() { Username = username, Grants = [.. grants] };

    private static PermissionGrant Allow(string action, string scope) =>
        new() { Action = action, Scope = scope };

    private static ClaimsPrincipal Principal(string name) => PermissionResolverTests.Principal(name);

    /// <summary>An empty decision body. The vote routes' body is optional, and a real bodyless POST
    /// (Content-Length 0) never reaches the JSON reader at all — but this harness tells minimal API the
    /// request CAN have a body, which then wants a JSON content type. Sending <c>{}</c> is the honest
    /// way to keep the harness's fiction consistent rather than weakening it.</summary>
    private static readonly object NoComment = new { };

    /// <summary>The enum converter matters: the app registers <see cref="JsonStringEnumConverter"/> in
    /// its HTTP JSON options, so an <see cref="ApprovalState"/> crosses the wire as "Pending" and a test
    /// reading it back without the converter would be reading a different contract than the client
    /// does.</summary>
    private static readonly JsonSerializerOptions JsonOpts = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter() },
    };

    private sealed class Harness(IReadOnlyList<Endpoint> endpoints, IServiceProvider services)
    {
        public FakeApprovals Approvals { get; init; } = null!;
        public FakeAudit Audit { get; init; } = null!;

        /// <summary>Every mapped endpoint, so a test can assert about the SHAPE of a route group and
        /// not only about what one handler answers.</summary>
        public IReadOnlyList<Endpoint> Endpoints => endpoints;

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

        public async Task<T> ReadAsync<T>(string key, ClaimsPrincipal user, (string Name, string Value)[]? routeValues = null, object? body = null, string? query = null)
        {
            var (status, text) = await CallAsync(key, user, routeValues, body, query);
            Assert.InRange(status, 200, 299);
            return JsonSerializer.Deserialize<T>(text, JsonOpts)!;
        }

        private static string KeyOf(RouteEndpoint endpoint)
        {
            var methods = endpoint.Metadata.GetMetadata<HttpMethodMetadata>()?.HttpMethods;
            var method = methods is null || methods.Count == 0 ? "(any)" : string.Join("|", methods);
            return $"{method} /{endpoint.RoutePattern.RawText?.TrimStart('/')}";
        }
    }

    private static Harness Build(AccessPolicyDocument? document = null, bool approvalsEnabled = true, FakeAudit? audit = null)
    {
        document ??= Document();
        audit ??= new FakeAudit();

        var builder = WebApplication.CreateBuilder();
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Jwt:Key"] = new string('k', 64),
            ["Jwt:Issuer"] = "streamforge-test",
            ["Jwt:Audience"] = "streamforge-test",
            ["Auth:PolicyCacheSeconds"] = "600",
        });
        builder.Services.AddStreamForgeApi(builder.Configuration);

        // Minimal-API binding decides "service or request body?" at MAP time by asking the container, so
        // every handler dependency has to be registered — records excluded, or a request DTO binds from
        // DI instead of the body. Verbatim from CatalogEntitlementEndpointTests, including the
        // DispatchProxy for facade interfaces: being HANDED a facade is not the failure, reaching for
        // its data is.
        foreach (var t in typeof(ICatalogFacade).Assembly.GetTypes()
                     .Where(t => t.IsInterface && t.IsPublic && t.Name.EndsWith("Facade", StringComparison.Ordinal)))
        {
            var iface = t;
            builder.Services.AddSingleton(iface, _ => UntouchableProxy(iface));
        }

        foreach (var t in typeof(StreamForgeApiExtensions).Assembly.GetTypes()
                     .Where(t => t.IsClass && t.IsPublic && !t.IsAbstract && !t.IsGenericType && !IsRecord(t))
                     // The ingest routes take an AppCore tracker that lives in neither assembly above;
                     // unregistered, minimal API infers it as a BODY parameter and the whole map fails.
                     .Concat(typeof(IngestKeyUsageTracker).Assembly.GetTypes()
                         .Where(t => t.IsClass && t.IsPublic && !t.IsAbstract && !t.IsGenericType
                                     && t.Name.EndsWith("Tracker", StringComparison.Ordinal)))
                     .Distinct())
        {
            var type = t;
            builder.Services.AddSingleton(type, _ => throw new InvalidOperationException(
                $"{type.Name} was resolved but this test never registered a real one."));
        }

        var policyFacade = new StaticAccessPolicyFacade(document);
        var resolver = new PermissionResolver(policyFacade, NullLogger<PermissionResolver>.Instance, 600);
        var approvals = new FakeApprovals(document);

        builder.Services.AddSingleton<IAccessPolicyFacade>(policyFacade);
        builder.Services.AddSingleton(resolver);
        builder.Services.AddSingleton(new AccessGuard(resolver, entitlementsEnabled: true));
        builder.Services.AddSingleton<IApprovalFacade>(approvals);
        builder.Services.AddSingleton<IAuditFacade>(audit);
        // Last registration wins: AddStreamForgeApi already added the configured (disabled) options.
        builder.Services.AddSingleton(new ApprovalOptions(approvalsEnabled, ApprovalOptions.DefaultSweepSeconds));

        var app = builder.Build();
        app.MapStreamForgeApi(new StreamForgeApiOptions(
            ProtosDir: Path.Combine(Path.GetTempPath(), "sf-approval-protos"),
            GrpcPort: 0,
            GrpcStaticServices: [],
            DocsFilePath: null,
            SpaDistPath: null,
            Flavor: "test"));

        return new Harness(
            [.. ((IEndpointRouteBuilder)app).DataSources.SelectMany(d => d.Endpoints)],
            app.Services)
        {
            Approvals = approvals,
            Audit = audit,
        };
    }

    private static object UntouchableProxy(Type interfaceType) =>
        DispatchProxy.Create(interfaceType, typeof(CatalogEntitlementEndpointTests.UntouchedFacade));

    private sealed class BodyAllowed : IHttpRequestBodyDetectionFeature
    {
        public bool CanHaveBody => true;
    }

    private static bool IsRecord(Type t) =>
        t.GetMethod("<Clone>$", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic) is not null;

    /// <summary>File one request as <paramref name="who"/> and return its id.</summary>
    private static async Task<string> FileAsync(Harness harness, string who, string action = Actions.PipelineWrite, string scope = "prod-enrich")
    {
        var filed = await harness.ReadAsync<ApprovalRequest>(
            "POST /api/approvals/", Principal(who), body: new { action, scope, reason = "because" });
        return filed.Id;
    }

    // ---------------------------------------------------------------------------------------------
    // 1. The requester is the authenticated principal, whatever the body says.
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task ACallerCannotFileARequestAsSomebodyElse()
    {
        var harness = Build();

        // Every field a forger would reach for, sent anyway: the DTO has no requestedBy at all and the
        // state machine's whitelist discards the rest. What comes back must be alice's request, filed by
        // alice, with no votes and nothing pre-decided.
        var filed = await harness.ReadAsync<ApprovalRequest>(
            "POST /api/approvals/",
            Principal("alice"),
            body: new
            {
                action = Actions.PipelineWrite,
                scope = "prod-enrich",
                reason = "because",
                requestedBy = "bob",
                votes = new[] { new { username = "bob", approve = true, atMs = 1 } },
                state = "Approved",
                requiredApprovals = 0,
                approverGroups = new[] { "alice-only" },
            });

        Assert.Equal("alice", filed.RequestedBy);
        Assert.Empty(filed.Votes);
        Assert.Equal(ApprovalState.Pending, filed.State);
        Assert.Equal(1, filed.RequiredApprovals);
        Assert.Equal([Reviewers], filed.ApproverGroups);
        Assert.Equal("rest", filed.Origin);

        // …and the stored copy agrees, not merely the response body.
        Assert.Equal("alice", harness.Approvals.Requests.Single().RequestedBy);
    }

    [Fact]
    public async Task FilingIsRefusedWithoutTheRequestEntitlement()
    {
        var harness = Build();

        // dave holds approval.decide and NOT approval.request — the two are separable, which is the
        // point of having both constants.
        var (status, body) = await harness.CallAsync(
            "POST /api/approvals/", Principal("dave"),
            body: new { action = Actions.PipelineWrite, scope = "prod-enrich" });

        Assert.Equal(403, status);
        Assert.Contains(Actions.ApprovalRequest, body, StringComparison.Ordinal);
        Assert.Empty(harness.Approvals.Requests);
    }

    // ---------------------------------------------------------------------------------------------
    // 2. A requester cannot approve their own request through the route.
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task ARequesterCannotApproveTheirOwnRequest()
    {
        var harness = Build();

        // bob is a genuine, entitled approver — a member of reviewers holding approval.decide. He is
        // refused anyway, because he filed it. That ordering (refused for filing it, not for being an
        // outsider) is the case the state machine's comment calls the reason the self-vote check runs
        // first, and it only reaches a user if the route asks the store rather than deciding itself.
        var id = await FileAsync(harness, "bob");

        var (status, body) = await harness.CallAsync(
            "POST /api/approvals/{id}/approve", Principal("bob"), [("id", id)], NoComment);

        Assert.Equal(403, status);
        Assert.Contains("filed request", body, StringComparison.Ordinal);

        var stored = harness.Approvals.Requests.Single();
        Assert.Equal(ApprovalState.Pending, stored.State);
        Assert.Empty(stored.Votes);
    }

    [Fact]
    public async Task AnEntitledApproverInTheGroupApproves()
    {
        var harness = Build();
        var id = await FileAsync(harness, "alice");

        var approved = await harness.ReadAsync<ApprovalRequest>(
            "POST /api/approvals/{id}/approve", Principal("bob"), [("id", id)],
            body: new { comment = "looks fine" });

        Assert.Equal(ApprovalState.Approved, approved.State);
        var vote = Assert.Single(approved.Votes);
        Assert.Equal("bob", vote.Username);
        Assert.True(vote.Approve);
        Assert.Equal("looks fine", vote.Comment);
        // Server-stamped, not taken from a body that never carried it.
        Assert.True(vote.AtMs > 0);
    }

    [Fact]
    public async Task AGroupMemberWithoutTheDecideEntitlementIsRefusedByTheRoute()
    {
        var harness = Build();
        var id = await FileAsync(harness, "alice");

        // carol is in reviewers, so the STORE would accept her vote. The route's entitlement check is
        // the other control, and both are required.
        var (status, body) = await harness.CallAsync(
            "POST /api/approvals/{id}/approve", Principal("carol"), [("id", id)], NoComment);

        Assert.Equal(403, status);
        Assert.Contains(Actions.ApprovalDecide, body, StringComparison.Ordinal);
        Assert.Empty(harness.Approvals.Requests.Single().Votes);
    }

    [Fact]
    public async Task AnEntitledOutsiderIsRefusedByTheStoreAndToldWhy()
    {
        var harness = Build();
        var id = await FileAsync(harness, "alice");

        // dave holds approval.decide at * and is in no approver group: past the route, refused by the
        // store. The route has to turn that null-or-unchanged answer into a sentence.
        var (status, body) = await harness.CallAsync(
            "POST /api/approvals/{id}/approve", Principal("dave"), [("id", id)], NoComment);

        Assert.Equal(403, status);
        Assert.Contains("not an approver", body, StringComparison.Ordinal);
        Assert.Contains(Reviewers, body, StringComparison.Ordinal);
        Assert.Empty(harness.Approvals.Requests.Single().Votes);
    }

    [Fact]
    public async Task RejectIsTheSameTransitionWithTheOtherBoolean()
    {
        var harness = Build();
        var id = await FileAsync(harness, "alice");

        var rejected = await harness.ReadAsync<ApprovalRequest>(
            "POST /api/approvals/{id}/reject", Principal("bob"), [("id", id)], NoComment);

        Assert.Equal(ApprovalState.Rejected, rejected.State);
        Assert.False(Assert.Single(rejected.Votes).Approve);

        // A decided request accepts nothing further, and says so as a 409 rather than a bare 404.
        var (status, body) = await harness.CallAsync(
            "POST /api/approvals/{id}/approve", Principal("bob"), [("id", id)], NoComment);
        Assert.Equal(409, status);
        Assert.Contains("rejected", body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task CancelIsTheRequestersOwnAndNeedsNoDecideEntitlement()
    {
        var harness = Build();
        var id = await FileAsync(harness, "alice");

        // alice holds approval.request only.
        var cancelled = await harness.ReadAsync<ApprovalRequest>(
            "POST /api/approvals/{id}/cancel", Principal("alice"), [("id", id)]);
        Assert.Equal(ApprovalState.Cancelled, cancelled.State);
    }

    [Fact]
    public async Task NobodyElseCanCancelSomebodyElsesRequest()
    {
        var harness = Build();
        var id = await FileAsync(harness, "alice");

        var (status, body) = await harness.CallAsync(
            "POST /api/approvals/{id}/cancel", Principal("bob"), [("id", id)]);

        Assert.Equal(403, status);
        Assert.Contains("only 'alice'", body, StringComparison.Ordinal);
        Assert.Equal(ApprovalState.Pending, harness.Approvals.Requests.Single().State);
    }

    [Fact]
    public async Task AnUnknownRequestIsAFourOhFourOnEveryTransition()
    {
        var harness = Build();

        Assert.Equal(404, (await harness.CallAsync("GET /api/approvals/{id}", Principal("root"), [("id", "nope")])).Status);
        Assert.Equal(404, (await harness.CallAsync("POST /api/approvals/{id}/approve", Principal("bob"), [("id", "nope")], NoComment)).Status);
        Assert.Equal(404, (await harness.CallAsync("POST /api/approvals/{id}/cancel", Principal("alice"), [("id", "nope")])).Status);
    }

    // ---------------------------------------------------------------------------------------------
    // 3. The listing rule.
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task TheInboxShowsARequestToItsRequesterToEntitledApproversAndToAnAdministrator()
    {
        var harness = Build();
        var id = await FileAsync(harness, "alice");

        // The requester: always their own.
        Assert.Equal([id], await ListIdsAsync(harness, "alice"));

        // The entitled approver: in reviewers AND holding approval.decide.
        Assert.Equal([id], await ListIdsAsync(harness, "bob"));

        // The administrator: access.read at * sees everything, filed by anyone.
        Assert.Equal([id], await ListIdsAsync(harness, "root"));
    }

    [Fact]
    public async Task TheInboxIsNotASideChannel()
    {
        var harness = Build();
        await FileAsync(harness, "alice");

        // carol IS in reviewers but cannot act — group membership alone would show her every request
        // routed to her team that she is not entitled to decide.
        Assert.Empty(await ListIdsAsync(harness, "carol"));

        // dave IS entitled to decide at * but is in no approver group — the entitlement alone would
        // turn approval.decide into a feed of what every other team is doing.
        Assert.Empty(await ListIdsAsync(harness, "dave"));

        // And a caller with neither sees nothing at all — 200 [], because a list is not an entity.
        var (status, body) = await harness.CallAsync("GET /api/approvals/", Principal("nosy"));
        Assert.Equal(200, status);
        Assert.Equal("[]", body);
    }

    [Fact]
    public async Task ReadingOneRequestObeysTheSameVisibilityRuleAsTheListing()
    {
        var harness = Build();
        var id = await FileAsync(harness, "alice");

        Assert.Equal(200, (await harness.CallAsync("GET /api/approvals/{id}", Principal("alice"), [("id", id)])).Status);
        Assert.Equal(200, (await harness.CallAsync("GET /api/approvals/{id}", Principal("bob"), [("id", id)])).Status);

        var (status, body) = await harness.CallAsync("GET /api/approvals/{id}", Principal("carol"), [("id", id)]);
        Assert.Equal(403, status);
        Assert.Contains("neither its requester nor an entitled approver", body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheStateFilterReachesTheStore()
    {
        var harness = Build();
        var kept = await FileAsync(harness, "alice");
        var cancelled = await FileAsync(harness, "alice", scope: "dev-enrich");
        await harness.CallAsync("POST /api/approvals/{id}/cancel", Principal("alice"), [("id", cancelled)]);

        Assert.Equal([kept], await ListIdsAsync(harness, "alice", "state=Pending"));
        Assert.Equal([cancelled], await ListIdsAsync(harness, "alice", "state=Cancelled"));
    }

    private static async Task<List<string>> ListIdsAsync(Harness harness, string who, string? query = null)
    {
        var rows = await harness.ReadAsync<List<ApprovalRequest>>("GET /api/approvals/", Principal(who), query: query);
        return [.. rows.Select(r => r.Id)];
    }

    // ---------------------------------------------------------------------------------------------
    // 4. Approvals disabled — the shipped default.
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task EveryApprovalRouteRefusesWhenApprovalsAreDisabled()
    {
        var harness = Build(approvalsEnabled: false);

        var calls = new (string Key, ClaimsPrincipal User, (string, string)[]? Route, object? Body)[]
        {
            ("POST /api/approvals/", Principal("alice"), null, new { action = Actions.PipelineWrite, scope = "*" }),
            ("GET /api/approvals/", Principal("root"), null, null),
            ("GET /api/approvals/{id}", Principal("root"), [("id", "whatever")], null),
            ("POST /api/approvals/{id}/approve", Principal("bob"), [("id", "whatever")], NoComment),
            ("POST /api/approvals/{id}/reject", Principal("bob"), [("id", "whatever")], NoComment),
            ("POST /api/approvals/{id}/cancel", Principal("alice"), [("id", "whatever")], null),
        };

        foreach (var (key, user, route, body) in calls)
        {
            var (status, text) = await harness.CallAsync(key, user, route, body);

            // 503 with the config key in it, never 200 [] — an inbox that answers "nothing to do" when
            // the feature is switched off tells the one person who needs to know that all is well.
            Assert.Equal(503, status);
            Assert.Contains(ApprovalOptions.EnabledKey, text, StringComparison.Ordinal);
        }

        Assert.Empty(harness.Approvals.Requests);
    }

    // ---------------------------------------------------------------------------------------------
    // 5. Audit.
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task TruncatedReachesTheResponseBody()
    {
        var audit = new FakeAudit
        {
            Page = new AuditPage
            {
                Entries = [Entry("alice", "pipeline.write")],
                Truncated = 4321,
                Total = 1,
            },
        };
        var harness = Build(audit: audit);

        var (status, raw) = await harness.CallAsync("GET /api/audit/{day}", Principal("root"), [("day", "20260819")]);

        Assert.Equal(200, status);
        // Asserted on the RAW JSON as well as the deserialized shape: the whole point of the counter is
        // that a client sees it, and a field that serializes under another name is a field that is not
        // there.
        Assert.Contains("\"truncated\":4321", raw, StringComparison.Ordinal);

        var page = JsonSerializer.Deserialize<AuditPageResponse>(raw, JsonOpts)!;
        Assert.Equal(4321, page.Truncated);
        Assert.Equal("20260819", page.Day);
    }

    [Fact]
    public async Task AuditIsRefusedWithoutTheReadEntitlement()
    {
        var harness = Build();

        // root holds audit.read; alice does not. The Admin policy floor is metadata applied by
        // middleware, which is not in an endpoint's RequestDelegate — this is the in-handler gate.
        Assert.Equal(200, (await harness.CallAsync("GET /api/audit/days", Principal("root"))).Status);

        var (status, body) = await harness.CallAsync("GET /api/audit/days", Principal("alice"));
        Assert.Equal(403, status);
        Assert.Contains(Actions.AuditRead, body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheDayIsValidatedBecauseItIsAStorageKey()
    {
        var harness = Build();

        var (status, body) = await harness.CallAsync("GET /api/audit/{day}", Principal("root"), [("day", "audit:../etc")]);

        Assert.Equal(400, status);
        Assert.Contains("yyyyMMdd", body, StringComparison.Ordinal);
        Assert.Null(harness.Audit.LastDay);
    }

    [Fact]
    public async Task TheFacadesFiltersAreForwarded()
    {
        var harness = Build();

        await harness.CallAsync(
            "GET /api/audit/{day}", Principal("root"), [("day", "20260819")],
            query: "actor=alice&action=pipeline.&limit=7&offset=3");

        Assert.Equal("20260819", harness.Audit.LastDay);
        Assert.Equal("alice", harness.Audit.LastActor);
        Assert.Equal("pipeline.", harness.Audit.LastActionPrefix);
        Assert.Equal(7, harness.Audit.LastLimit);
        Assert.Equal(3, harness.Audit.LastOffset);
    }

    [Fact]
    public async Task BeforeAndAfterPayloadsAreWithheldByDefaultAndTheWithholdingIsReported()
    {
        var audit = new FakeAudit
        {
            Page = new AuditPage
            {
                Entries =
                [
                    Entry("alice", "source.write", before: "{\"password\":\"hunter2\"}", after: "{\"password\":\"hunter3\"}"),
                    Entry("alice", "pipeline.read"),
                ],
                Total = 2,
            },
        };
        var harness = Build(audit: audit);

        // Default: no payloads, and the response says how many rows had one.
        var (_, raw) = await harness.CallAsync("GET /api/audit/{day}", Principal("root"), [("day", "20260819")]);
        Assert.DoesNotContain("hunter2", raw, StringComparison.Ordinal);
        var page = JsonSerializer.Deserialize<AuditPageResponse>(raw, JsonOpts)!;
        Assert.False(page.ChangesIncluded);
        Assert.Equal(1, page.ChangesWithheld);
        Assert.Equal(2, page.Entries.Count);

        // Asked for, by a caller entitled to access.read.
        var (_, withRaw) = await harness.CallAsync(
            "GET /api/audit/{day}", Principal("root"), [("day", "20260819")], query: "includeChanges=true");
        Assert.Contains("hunter2", withRaw, StringComparison.Ordinal);
        var withPage = JsonSerializer.Deserialize<AuditPageResponse>(withRaw, JsonOpts)!;
        Assert.True(withPage.ChangesIncluded);
        Assert.Equal(0, withPage.ChangesWithheld);

        // Redaction copies rather than mutates — the store's own rows still carry what they carried, or
        // reading the log would erase it.
        Assert.Equal("{\"password\":\"hunter2\"}", audit.Page.Entries[0].BeforeJson);
    }

    [Fact]
    public async Task AskingForChangesWithoutTheAccessReadEntitlementStillWithholdsThem()
    {
        var document = Document();
        // An auditor who may read the log and nothing else — the case the redaction exists for.
        document.Users.Add(User("auditor", Allow(Actions.AuditRead, "*")));

        var audit = new FakeAudit
        {
            Page = new AuditPage { Entries = [Entry("alice", "source.write", before: "{\"password\":\"hunter2\"}")], Total = 1 },
        };
        var harness = Build(document, audit: audit);

        var (status, raw) = await harness.CallAsync(
            "GET /api/audit/{day}", Principal("auditor"), [("day", "20260819")], query: "includeChanges=true");

        Assert.Equal(200, status);
        Assert.DoesNotContain("hunter2", raw, StringComparison.Ordinal);
        var page = JsonSerializer.Deserialize<AuditPageResponse>(raw, JsonOpts)!;
        Assert.False(page.ChangesIncluded);
        Assert.Equal(1, page.ChangesWithheld);
    }

    [Fact]
    public void TheAuditSurfaceHasNoWriteRoute()
    {
        var harness = Build();

        // Structural, not a promise in a comment: the only writer is the sink's drain, and a route that
        // let a caller append would let a caller forge the record of their own actions.
        var methods = AuditRouteMethods(harness);
        Assert.NotEmpty(methods);
        Assert.All(methods, m => Assert.Equal("GET", m));
    }

    private static List<string> AuditRouteMethods(Harness harness) =>
        [.. harness.Endpoints.OfType<RouteEndpoint>()
            .Where(e => e.RoutePattern.RawText?.StartsWith("/api/audit", StringComparison.Ordinal) == true)
            .SelectMany(e => e.Metadata.GetMetadata<HttpMethodMetadata>()?.HttpMethods ?? [])];

    private static AuditEntry Entry(string actor, string action, string? before = null, string? after = null) => new()
    {
        Id = Guid.NewGuid().ToString("n"),
        AtMs = 1_700_000_000_000,
        Actor = actor,
        Action = action,
        Scope = "prod-enrich",
        Outcome = "allowed",
        BeforeJson = before,
        AfterJson = after,
    };

    // ---------------------------------------------------------------------------------------------
    // Fakes
    // ---------------------------------------------------------------------------------------------

    /// <summary>The two shipped stores' shape, in twenty lines: the real
    /// <see cref="ApprovalStateMachine"/> for every transition and the real
    /// <see cref="EffectivePermissionsBuilder"/> for eligibility, over a list. Anything it decided for
    /// itself would make these tests a test of the fake.</summary>
    private sealed class FakeApprovals(AccessPolicyDocument policy) : IApprovalFacade
    {
        public List<ApprovalRequest> Requests { get; } = [];

        private static long NowMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        private ApprovalRequest? Find(string id) =>
            Requests.FirstOrDefault(r => string.Equals(r.Id, id, StringComparison.Ordinal));

        public Task<ApprovalRequest> RequestAsync(ApprovalRequest request)
        {
            var template = ApprovalStateMachine.SelectTemplate(policy.ApprovalTemplates, request.Action, request.Scope, null)
                ?? throw new InvalidOperationException(
                    $"no enabled approval template covers '{request.Action}' at scope '{request.Scope}'");

            var stored = ApprovalStateMachine.CreateRequest(
                request, template, Guid.NewGuid().ToString("n"), request.RequestedBy, NowMs());
            Requests.Add(stored);
            return Task.FromResult(stored);
        }

        public Task<ApprovalRequest?> GetAsync(string id) => Task.FromResult(Find(id));

        public Task<List<ApprovalRequest>> ListAsync(ApprovalState? state, int limit) =>
            Task.FromResult(Requests
                .Where(r => state is null || r.State == state)
                .OrderByDescending(r => r.RequestedAtMs)
                .Take(limit <= 0 ? 100 : limit)
                .ToList());

        public Task<ApprovalRequest?> VoteAsync(string id, ApprovalVote vote)
        {
            var request = Find(id);
            if (request is null)
            {
                return Task.FromResult<ApprovalRequest?>(null);
            }

            var groups = EffectivePermissionsBuilder.Build(policy, vote.Username).Groups;
            var eligibility = request.ApproverGroups.Any(g => groups.Contains(g, StringComparer.Ordinal))
                ? VoterEligibility.Eligible
                : VoterEligibility.NotAnApprover;

            var result = ApprovalStateMachine.ApplyVote(request, vote, eligibility, NowMs());
            // The Dapr convention: null means "the transition did not happen". The Orleans grain returns
            // the request either way, which is exactly why the route re-reads instead of trusting this.
            return Task.FromResult(result.Accepted ? request : null);
        }

        public Task<ApprovalRequest?> CancelAsync(string id, string username)
        {
            var request = Find(id);
            if (request is null)
            {
                return Task.FromResult<ApprovalRequest?>(null);
            }

            var result = ApprovalStateMachine.Cancel(request, username, NowMs());
            return Task.FromResult(result.Accepted ? request : null);
        }

        public Task<ApprovalRequest?> RecordOutcomeAsync(string id, bool executed, string outcome) =>
            throw new NotSupportedException();

        public Task<int> SweepAsync(long nowMs) => throw new NotSupportedException();
    }

    private sealed class FakeAudit : IAuditFacade
    {
        public AuditPage Page { get; init; } = new();
        public List<string> Days { get; init; } = ["20260819", "20260818"];

        public string? LastDay { get; private set; }
        public string? LastActor { get; private set; }
        public string? LastActionPrefix { get; private set; }
        public int LastLimit { get; private set; }
        public int LastOffset { get; private set; }

        public Task AppendAsync(AuditEntry entry) =>
            throw new InvalidOperationException("the REST surface must never write to the audit log");

        public Task<AuditPage> QueryAsync(string day, string? actor, string? actionPrefix, int limit, int offset)
        {
            LastDay = day;
            LastActor = actor;
            LastActionPrefix = actionPrefix;
            LastLimit = limit;
            LastOffset = offset;
            return Task.FromResult(Page);
        }

        public Task<List<string>> GetDaysAsync() => Task.FromResult(Days);
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
}
