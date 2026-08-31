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
using StreamsForge.Abstractions;
using StreamsForge.Api;
using StreamsForge.Api.Auth;
using StreamsForge.AppCore.Access;
using StreamsForge.AppCore.Ingest;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>
/// Plan 015 wave 8-B — <b>the execute half</b>: an approval that is approved now RUNS the action, and
/// these tests run the real route to prove it.
///
/// <para>Same in-process harness <see cref="ApprovalAuditEndpointTests"/> established (a real
/// <see cref="WebApplication"/>, real minimal-API binding, no port and no silo), with two additions it
/// deliberately did not have: an <see cref="IApprovalFacade"/> whose
/// <see cref="IApprovalFacade.RecordOutcomeAsync"/> is implemented (there it throws, which is exactly
/// how those tests still assert <see cref="ApprovalState.Approved"/> after an approve — a store that
/// cannot record the claim executes nothing, by design), and a catalog that counts what was done to
/// it.</para>
///
/// <para>What is being pinned is the security statement the wave lives on: <b>what was approved is the
/// (Action, Scope) pair the approver saw</b>. So the interesting tests are not "the delete happened" —
/// they are the four ways a payload might have made something else happen, and the two ways the same
/// approval might have been cashed in twice.</para>
/// </summary>
public class ApprovalExecutionTests
{
    // ---------------------------------------------------------------------------------------------
    // Fixture
    // ---------------------------------------------------------------------------------------------

    private const string Reviewers = "reviewers";
    private const string PipelineName = "prod-enrich";
    private const string PipelineId = "a1b2c3d4";
    private const string TableName = "prod-latest";
    private const string TableId = "d4c3b2a1";
    private const string SourceName = "prod-orders";

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

    /// <summary>alice files, bob decides — the smallest fixture that satisfies both controls on the vote
    /// route (the entitlement AND membership of the approver group) without being able to self-approve.</summary>
    private static AccessPolicyDocument Document()
    {
        var document = new AccessPolicyDocument
        {
            Roles = BuiltInRoleCatalog.Create(),
            Version = 1,
            ApprovalTemplates = [Template()],
        };

        document.Groups.Add(new GroupDefinition { Name = Reviewers, Members = ["bob"] });
        document.Users.AddRange(
            new UserAccessEntry { Username = "alice", Grants = [Allow(Actions.ApprovalRequest, "*")] },
            new UserAccessEntry
            {
                Username = "bob",
                Grants = [Allow(Actions.ApprovalRequest, "*"), Allow(Actions.ApprovalDecide, "*")],
            });

        return document;
    }

    private static PermissionGrant Allow(string action, string scope) => new() { Action = action, Scope = scope };

    private static ClaimsPrincipal Principal(string name) => PermissionResolverTests.Principal(name);

    private static readonly object NoComment = new { };

    private static readonly JsonSerializerOptions JsonOpts = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter() },
    };

    // ---------------------------------------------------------------------------------------------
    // 1. The round trip: filed, approved, and the action actually happened — traceably
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task AnApprovedPipelineDeleteIsExecutedAndTheAuditRowCarriesTheApprovalId()
    {
        var harness = Build();

        var id = await FileAsync(harness, "alice", Actions.PipelineDelete, PipelineName);
        var after = await ApproveAsync(harness, id);

        Assert.Equal(ApprovalState.Executed, after.State);
        Assert.Empty(harness.Catalog.Pipelines);
        Assert.Equal([PipelineId], harness.Catalog.DeletedPipelineIds);

        // The row that makes the execution accountable. Without ApprovalId there is a deleted pipeline
        // and no way to get from it back to the decision that authorized it.
        var row = Assert.Single(harness.Sink.Rows, r => r.Action == Actions.PipelineDelete);
        Assert.Equal(id, row.ApprovalId);
        Assert.Equal(ApprovalExecutor.ApprovalOrigin, row.Origin);
        Assert.Equal(PipelineName, row.Scope);
        // The change is the REQUESTER's — attributing it to the approver would read as an edit bob never
        // made — and the approver is named in the detail instead.
        Assert.Equal("alice", row.Actor);
        Assert.Contains("bob", row.Detail!, StringComparison.Ordinal);
        // A delete keeps the whole document: after it, this row is the only surviving copy.
        Assert.Contains(PipelineName, row.BeforeJson!, StringComparison.Ordinal);
        Assert.Null(row.AfterJson);
    }

    [Fact]
    public async Task RejectExecutesNothing()
    {
        var harness = Build();
        var id = await FileAsync(harness, "alice", Actions.PipelineDelete, PipelineName);

        var after = await harness.ReadAsync<ApprovalRequest>(
            "POST /api/approvals/{id}/reject", Principal("bob"), [("id", id)], NoComment);

        Assert.Equal(ApprovalState.Rejected, after.State);
        Assert.Empty(harness.Catalog.DeletedPipelineIds);
        Assert.Single(harness.Catalog.Pipelines);
    }

    // ---------------------------------------------------------------------------------------------
    // 2. At most once
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task ASecondApproveIsRefusedAndNothingRunsAgain()
    {
        var harness = Build();
        var id = await FileAsync(harness, "alice", Actions.PipelineDelete, PipelineName);

        await ApproveAsync(harness, id);

        // The double click. The request is Executed now, which is not a state a vote can land in, so the
        // route refuses before the executor is even consulted — 409, the same answer any other terminal
        // state gives. That is the outer of the two defences; the inner one is the claim, below.
        var (status, body) = await harness.CallAsync(
            "POST /api/approvals/{id}/approve", Principal("bob"), [("id", id)], NoComment);

        Assert.Equal(409, status);
        Assert.Contains("executed", body, StringComparison.Ordinal);
        Assert.Equal([PipelineId], harness.Catalog.DeletedPipelineIds);
        Assert.Single(harness.Sink.Rows, r => r.Action == Actions.PipelineDelete);
    }

    [Fact]
    public async Task TwoCallersHoldingTheSameApprovedSnapshotExecuteOnce()
    {
        var harness = Build();
        var id = await FileAsync(harness, "alice", Actions.PipelineDelete, PipelineName);

        // Straight at the store, so the request reaches Approved without the route having executed it.
        await harness.Approvals.VoteAsync(id, new ApprovalVote { Username = "bob", Approve = true });
        var stored = await harness.Approvals.GetAsync(id);
        Assert.Equal(ApprovalState.Approved, stored!.State);

        // Two DETACHED copies: exactly what two concurrent vote routes hold after each re-reads the
        // request and finds it Approved. Neither can tell from its own copy that the other exists, which
        // is why "I saw it become Approved" cannot be the permission to execute — the claim inside the
        // store is.
        var first = Snapshot(stored);
        var second = Snapshot(stored);

        var a = await ApprovalExecutor.ExecuteAsync(first, "bob", harness.Catalog, harness.Approvals, harness.Sink, null);
        var b = await ApprovalExecutor.ExecuteAsync(second, "carol", harness.Catalog, harness.Approvals, harness.Sink, null);

        Assert.Equal(ApprovalState.Executed, a.State);
        Assert.Equal(ApprovalState.Executed, b.State);
        Assert.Equal([PipelineId], harness.Catalog.DeletedPipelineIds);
        Assert.Single(harness.Sink.Rows, r => r.Action == Actions.PipelineDelete);

        // The STORED request, which is the one anybody else will read — and the assertion that caught a
        // real regression while wave 8 was being assembled. The loser plans against a world the winner
        // has already changed, so it concludes "the entity is gone"; when the executor planned BEFORE
        // claiming, it then wrote that conclusion over the winner's success and this read Failed.
        // Claiming first is what makes the loser silent.
        Assert.Equal(ApprovalState.Executed, (await harness.Approvals.GetAsync(id))!.State);
    }

    [Fact]
    public async Task AClaimThatCannotBeRecordedExecutesNothing()
    {
        var harness = Build();
        harness.Approvals.OutcomeThrows = true;

        var id = await FileAsync(harness, "alice", Actions.PipelineDelete, PipelineName);
        var after = await ApproveAsync(harness, id);

        // The approval is real and stays real; the action does not happen. The other ordering would be
        // an execution with nothing accountable for it.
        Assert.Equal(ApprovalState.Approved, after.State);
        Assert.Empty(harness.Catalog.DeletedPipelineIds);
    }

    // ---------------------------------------------------------------------------------------------
    // 3. The payload cannot widen what was approved
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task APayloadNamingAnotherEntityIsRefusedRatherThanReconciled()
    {
        var harness = Build();

        // The approver read "pipeline.delete on prod-enrich". The payload says otherwise; the scope wins
        // and the disagreement is fatal, because it means the approver and the executor were looking at
        // two different things.
        var id = await FileAsync(
            harness, "alice", Actions.PipelineDelete, PipelineName, payload: new { name = "some-other-pipeline" });

        var after = await ApproveAsync(harness, id);

        Assert.Equal(ApprovalState.Failed, after.State);
        Assert.Contains("some-other-pipeline", after.Outcome!, StringComparison.Ordinal);
        Assert.Empty(harness.Catalog.DeletedPipelineIds);
        Assert.Single(harness.Catalog.Pipelines);
    }

    [Fact]
    public async Task APayloadCarryingTheEntitysOwnIdAgreesWithItsNameScope()
    {
        var harness = Build();

        // The chat files payloads addressed by id while scoping by name (wave 3 settled that the scope is
        // the NAME on all three surfaces). Both denote the entity the scope resolved to, so this is
        // agreement, not a clash — and the comparison still runs scope-first.
        var id = await FileAsync(
            harness, "alice", Actions.PipelineDelete, PipelineName, payload: new { id = PipelineId });

        var after = await ApproveAsync(harness, id);

        Assert.Equal(ApprovalState.Executed, after.State);
        Assert.Equal([PipelineId], harness.Catalog.DeletedPipelineIds);
    }

    [Fact]
    public async Task AScopeThatNamesASetIsNeverCashedIn()
    {
        var harness = Build();

        // `prod-*` is a legitimate thing to hold an entitlement for and an illegitimate thing to execute:
        // the approver never looked at any particular member of that set.
        var id = await FileAsync(harness, "alice", Actions.PipelineDelete, "prod-*");
        var after = await ApproveAsync(harness, id);

        Assert.Equal(ApprovalState.Failed, after.State);
        Assert.Contains("names a set", after.Outcome!, StringComparison.Ordinal);
        Assert.Empty(harness.Catalog.DeletedPipelineIds);
    }

    [Fact]
    public async Task ALifecyclePayloadDecidesDirectionAndNothingElse()
    {
        var harness = Build();

        var id = await FileAsync(
            harness, "alice", Actions.PipelineControl, PipelineName, payload: new { status = "Running" });
        var after = await ApproveAsync(harness, id);

        Assert.Equal(ApprovalState.Executed, after.State);
        Assert.Equal([(PipelineId, PipelineStatus.Running)], harness.Catalog.PipelineStatusCalls);
    }

    [Fact]
    public async Task ALifecycleRequestWithNoDirectionIsRefusedRatherThanGuessed()
    {
        var harness = Build();

        var id = await FileAsync(harness, "alice", Actions.PipelineControl, PipelineName);
        var after = await ApproveAsync(harness, id);

        Assert.Equal(ApprovalState.Failed, after.State);
        Assert.Empty(harness.Catalog.PipelineStatusCalls);
    }

    [Fact]
    public async Task ASourceWritePayloadMustDescribeTheSourceTheScopeNames()
    {
        var harness = Build();

        var id = await FileAsync(
            harness, "alice", Actions.SourceWrite, SourceName,
            payload: new { name = "another-source", kind = "generator", eventsPerSecond = 1 });

        var after = await ApproveAsync(harness, id);

        Assert.Equal(ApprovalState.Failed, after.State);
        Assert.Empty(harness.Catalog.UpsertedSources);
    }

    [Fact]
    public async Task AnApprovedSourceWriteUpsertsThePayloadAtTheApprovedName()
    {
        var harness = Build();

        var id = await FileAsync(
            harness, "alice", Actions.SourceWrite, SourceName,
            payload: new
            {
                name = SourceName,
                kind = "generator",
                eventsPerSecond = 5,
                fields = new[] { new { name = "id", type = "string" } },
            });

        var after = await ApproveAsync(harness, id);

        Assert.Equal(ApprovalState.Executed, after.State);
        var written = Assert.Single(harness.Catalog.UpsertedSources);
        Assert.Equal(SourceName, written.Name);
        Assert.Equal(5, written.EventsPerSecond);
    }

    [Fact]
    public async Task AnInvalidSourceWritePayloadIsRefusedBeforeAnythingIsWritten()
    {
        var harness = Build();

        // No fields, no rate: the same SourceValidation the POST/PUT handlers run, so an approval cannot
        // be the way to store a definition the REST route would have 400'd.
        var id = await FileAsync(
            harness, "alice", Actions.SourceWrite, SourceName, payload: new { name = SourceName, kind = "generator" });

        var after = await ApproveAsync(harness, id);

        Assert.Equal(ApprovalState.Failed, after.State);
        Assert.Contains("not a valid source", after.Outcome!, StringComparison.Ordinal);
        Assert.Empty(harness.Catalog.UpsertedSources);
    }

    // ---------------------------------------------------------------------------------------------
    // 4. The boundary is honest: no executor is Failed with a sentence, never a silent success
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task AnApprovedActionWithNoExecutorRecordsFailedAndSaysSo()
    {
        var harness = Build();

        // pipeline.write is deliberately unsupported — see ApprovalExecutor's ponytail note on why a
        // second implementation of the PUT handler's DTO→definition translation is the wrong trade.
        var id = await FileAsync(harness, "alice", Actions.PipelineWrite, PipelineName);
        var after = await ApproveAsync(harness, id);

        Assert.Equal(ApprovalState.Failed, after.State);
        Assert.Contains("no executor", after.Outcome!, StringComparison.Ordinal);
        Assert.Contains(Actions.PipelineWrite, after.Outcome!, StringComparison.Ordinal);

        var row = Assert.Single(harness.Sink.Rows, r => r.Action == Actions.PipelineWrite);
        Assert.Equal("failed", row.Outcome);
        Assert.Equal(id, row.ApprovalId);
    }

    [Fact]
    public async Task AVanishedEntityIsFailedAndNotInvented()
    {
        var harness = Build();
        var id = await FileAsync(harness, "alice", Actions.TableDelete, "no-such-table");

        var after = await ApproveAsync(harness, id);

        Assert.Equal(ApprovalState.Failed, after.State);
        Assert.Contains("no table named", after.Outcome!, StringComparison.Ordinal);
    }

    // ---------------------------------------------------------------------------------------------
    // 5. "Approved and the action failed" is a different fact from "not approved"
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task AnExecutionThatThrowsIsRecordedAsFailedOnItsAuditRow()
    {
        var harness = Build();
        harness.Catalog.TableDeleteFailure = "table 'prod-latest' has a Running dependent";

        var id = await FileAsync(harness, "alice", Actions.TableDelete, TableName);
        var after = await ApproveAsync(harness, id);

        var row = Assert.Single(harness.Sink.Rows, r => r.Action == Actions.TableDelete);
        Assert.Equal("failed", row.Outcome);
        Assert.Equal(id, row.ApprovalId);
        Assert.Contains("Running dependent", row.Detail!, StringComparison.Ordinal);

        // DECLARED BEHAVIOUR CHANGE, same wave: this used to assert Executed, and the comment here used
        // to call that a pinned ceiling. The orchestrator took the one-line state-machine change the
        // executor's ponytail note asked for — RecordOutcome now also accepts Executed -> Failed, and
        // ONLY that — so the request's own state finally agrees with its audit row. The claim is still
        // taken before the outcome can be known (that is what makes execution at-most-once); what
        // changed is that the over-claim is now correctable, by the second RecordOutcomeAsync call the
        // executor was already making on this path.
        Assert.Equal(ApprovalState.Failed, after.State);
        Assert.Contains("Running dependent", after.Outcome!, StringComparison.Ordinal);
    }

    // ---------------------------------------------------------------------------------------------
    // Harness
    // ---------------------------------------------------------------------------------------------

    private static async Task<string> FileAsync(
        Harness harness, string who, string action, string scope, object? payload = null)
    {
        var filed = await harness.ReadAsync<ApprovalRequest>(
            "POST /api/approvals/",
            Principal(who),
            body: new
            {
                action,
                scope,
                reason = "because",
                payloadJson = payload is null ? null : JsonSerializer.Serialize(payload, JsonOpts),
            });

        return filed.Id;
    }

    private static Task<ApprovalRequest> ApproveAsync(Harness harness, string id) =>
        harness.ReadAsync<ApprovalRequest>("POST /api/approvals/{id}/approve", Principal("bob"), [("id", id)], NoComment);

    /// <summary>A detached copy, standing in for the copy a second host replica would be holding.</summary>
    private static ApprovalRequest Snapshot(ApprovalRequest request) =>
        JsonSerializer.Deserialize<ApprovalRequest>(JsonSerializer.Serialize(request, JsonOpts), JsonOpts)!;

    private sealed class Harness(IReadOnlyList<Endpoint> endpoints, IServiceProvider services)
    {
        public FakeApprovals Approvals { get; init; } = null!;
        public FakeCatalog Catalog { get; init; } = null!;
        public CapturingSink Sink { get; init; } = null!;

        public async Task<(int Status, string Body)> CallAsync(
            string key,
            ClaimsPrincipal user,
            (string Name, string Value)[]? routeValues = null,
            object? body = null)
        {
            var endpoint = endpoints.OfType<RouteEndpoint>().Single(e => KeyOf(e) == key);

            var http = new DefaultHttpContext { RequestServices = services, User = user };
            var responseBody = new MemoryStream();
            http.Response.Body = responseBody;
            http.Features.Set<IHttpRequestBodyDetectionFeature>(new BodyAllowed());

            var (method, pattern) = (key.Split(' ')[0], key.Split(' ')[1]);
            http.Request.Method = method;
            http.Request.Path = pattern;

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

        public async Task<T> ReadAsync<T>(
            string key, ClaimsPrincipal user, (string Name, string Value)[]? routeValues = null, object? body = null)
        {
            var (status, text) = await CallAsync(key, user, routeValues, body);
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

    private static Harness Build()
    {
        var document = Document();

        var builder = WebApplication.CreateBuilder();
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Jwt:Key"] = new string('k', 64),
            ["Jwt:Issuer"] = "streamsforge-test",
            ["Jwt:Audience"] = "streamsforge-test",
            ["Auth:PolicyCacheSeconds"] = "600",
        });
        builder.Services.AddStreamsForgeApi(builder.Configuration);

        // Minimal-API binding decides "service or request body?" at MAP time, so every handler dependency
        // must be registered — verbatim from ApprovalAuditEndpointTests, including the throwing factories.
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
            var type = t;
            builder.Services.AddSingleton(type, _ => throw new InvalidOperationException(
                $"{type.Name} was resolved but this test never registered a real one."));
        }

        var policyFacade = new StaticAccessPolicyFacade(document);
        var resolver = new PermissionResolver(policyFacade, NullLogger<PermissionResolver>.Instance, 600);
        var approvals = new FakeApprovals(document);
        var catalog = new FakeCatalog();
        var sink = new CapturingSink();

        builder.Services.AddSingleton<IAccessPolicyFacade>(policyFacade);
        builder.Services.AddSingleton(resolver);
        builder.Services.AddSingleton(new AccessGuard(resolver, entitlementsEnabled: true));
        builder.Services.AddSingleton<IApprovalFacade>(approvals);
        // Last registration wins over the UntouchableProxy above: the executor is the one caller in this
        // file that is SUPPOSED to reach a facade's data.
        builder.Services.AddSingleton<ICatalogFacade>(catalog);
        builder.Services.AddSingleton<IAuditSink>(sink);
        builder.Services.AddSingleton(new ApprovalOptions(true, ApprovalOptions.DefaultSweepSeconds));

        var app = builder.Build();
        app.MapStreamsForgeApi(new StreamsForgeApiOptions(
            ProtosDir: Path.Combine(Path.GetTempPath(), "sf-approval-exec-protos"),
            GrpcPort: 0,
            GrpcStaticServices: [],
            DocsFilePath: null,
            SpaDistPath: null,
            Flavor: "test"));

        return new Harness([.. ((IEndpointRouteBuilder)app).DataSources.SelectMany(d => d.Endpoints)], app.Services)
        {
            Approvals = approvals,
            Catalog = catalog,
            Sink = sink,
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

    // ---------------------------------------------------------------------------------------------
    // Fakes
    // ---------------------------------------------------------------------------------------------

    /// <summary>The two shipped stores' shape: the real <see cref="ApprovalStateMachine"/> for every
    /// transition and the real <see cref="EffectivePermissionsBuilder"/> for eligibility, over a list.
    /// <see cref="RecordOutcomeAsync"/> is implemented here (unlike
    /// <see cref="ApprovalAuditEndpointTests"/>' fake, which throws) because the execution path is what
    /// this file exists to test, and it follows the Orleans convention of returning the request whether
    /// or not the transition was accepted — the harder of the two for the executor's claim to survive.</summary>
    private sealed class FakeApprovals(AccessPolicyDocument policy) : IApprovalFacade
    {
        public List<ApprovalRequest> Requests { get; } = [];

        /// <summary>A store that cannot record the claim. Nothing may execute.</summary>
        public bool OutcomeThrows { get; set; }

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
            Task.FromResult(Requests.Where(r => state is null || r.State == state).Take(limit <= 0 ? 100 : limit).ToList());

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

        public Task<ApprovalRequest?> RecordOutcomeAsync(string id, bool executed, string outcome)
        {
            if (OutcomeThrows)
            {
                throw new InvalidOperationException("the approval store is unreachable");
            }

            var request = Find(id);
            if (request is null)
            {
                return Task.FromResult<ApprovalRequest?>(null);
            }

            ApprovalStateMachine.RecordOutcome(request, executed, outcome, NowMs());
            // The Orleans convention: the request comes back whether or not the transition was accepted.
            return Task.FromResult<ApprovalRequest?>(request);
        }

        public Task<int> SweepAsync(long nowMs) => throw new NotSupportedException();
    }

    /// <summary>A catalog that remembers what was done to it, and refuses everything the executor is not
    /// supposed to reach.</summary>
    private sealed class FakeCatalog : ICatalogFacade
    {
        public List<SourceDefinition> Sources { get; } =
            [new SourceDefinition { Name = SourceName, Kind = "generator", EventsPerSecond = 1 }];

        public List<PipelineDefinition> Pipelines { get; } =
            [new PipelineDefinition { Id = PipelineId, Name = PipelineName, Sql = "select 1" }];

        public List<TableDefinition> Tables { get; } =
            [new TableDefinition { Id = TableId, Name = TableName, Sql = "select 1" }];

        public List<string> DeletedPipelineIds { get; } = [];
        public List<string> DeletedSourceNames { get; } = [];
        public List<SourceDefinition> UpsertedSources { get; } = [];
        public List<(string Id, PipelineStatus Status)> PipelineStatusCalls { get; } = [];

        /// <summary>Non-null makes DeleteTableAsync throw, the way the real registry does when a Running
        /// table depends on the one being deleted.</summary>
        public string? TableDeleteFailure { get; set; }

        public Task<List<SourceDefinition>> GetSourcesAsync() => Task.FromResult(Sources);
        public Task<SourceDefinition?> GetSourceAsync(string name) =>
            Task.FromResult(Sources.FirstOrDefault(s => s.Name == name));

        public Task UpsertSourceAsync(SourceDefinition def)
        {
            UpsertedSources.Add(def);
            return Task.CompletedTask;
        }

        public Task<bool> DeleteSourceAsync(string name)
        {
            DeletedSourceNames.Add(name);
            return Task.FromResult(Sources.RemoveAll(s => s.Name == name) > 0);
        }

        public Task<List<PipelineDefinition>> GetPipelinesAsync() => Task.FromResult(Pipelines);
        public Task<PipelineDefinition?> GetPipelineAsync(string id) =>
            Task.FromResult(Pipelines.FirstOrDefault(p => p.Id == id));

        public Task<bool> DeletePipelineAsync(string id)
        {
            DeletedPipelineIds.Add(id);
            return Task.FromResult(Pipelines.RemoveAll(p => p.Id == id) > 0);
        }

        public Task<PipelineDefinition?> SetPipelineStatusAsync(string id, PipelineStatus status)
        {
            PipelineStatusCalls.Add((id, status));
            var pipeline = Pipelines.FirstOrDefault(p => p.Id == id);
            if (pipeline is not null)
            {
                pipeline.Status = status;
            }

            return Task.FromResult(pipeline);
        }

        public Task<List<TableDefinition>> GetTablesAsync() => Task.FromResult(Tables);
        public Task<TableDefinition?> GetTableAsync(string id) => Task.FromResult(Tables.FirstOrDefault(t => t.Id == id));

        public Task<bool> DeleteTableAsync(string id)
        {
            if (TableDeleteFailure is not null)
            {
                throw new InvalidOperationException(TableDeleteFailure);
            }

            return Task.FromResult(Tables.RemoveAll(t => t.Id == id) > 0);
        }

        public Task<TableDefinition?> SetTableStatusAsync(string id, PipelineStatus status)
        {
            var table = Tables.FirstOrDefault(t => t.Id == id);
            if (table is not null)
            {
                table.Status = status;
            }

            return Task.FromResult(table);
        }

        public Task<PipelineDefinition> CreatePipelineAsync(PipelineDefinition def) => throw new NotSupportedException();
        public Task<PipelineDefinition?> UpdatePipelineAsync(PipelineDefinition def) => throw new NotSupportedException();
        public Task<TableDefinition> CreateTableAsync(TableDefinition def) => throw new NotSupportedException();
        public Task<TableDefinition?> UpdateTableAsync(TableDefinition def) => throw new NotSupportedException();
        public Task<string> EnsureFieldNumbersAsync(string entityKey, List<FieldDef> fields) => throw new NotSupportedException();
        public Task<ScenarioRunResult> RunSourceAsync(string name, ScenarioRunRequest request) => throw new NotSupportedException();
    }

    private sealed class CapturingSink : IAuditSink
    {
        public List<AuditEntry> Rows { get; } = [];

        public void Record(AuditEntry entry) => Rows.Add(entry);
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
