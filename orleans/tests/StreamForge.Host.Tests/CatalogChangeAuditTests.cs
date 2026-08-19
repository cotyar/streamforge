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
using StreamForge.Abstractions;
using StreamForge.Api;
using StreamForge.Api.Auth;
using StreamForge.AppCore.Access;
using StreamForge.AppCore.Ingest;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// Plan 015 wave 5-B — <see cref="AuditEntry.BeforeJson"/> / <see cref="AuditEntry.AfterJson"/> at the
/// nine catalog mutation sites, tested by RUNNING the handlers, the way
/// <see cref="CatalogEntitlementEndpointTests"/> established: a real <see cref="WebApplication"/> built
/// and mapped in-process, a real <see cref="DefaultHttpContext"/>, real minimal-API binding, real
/// handlers — no <c>Run()</c>, no port, no silo.
///
/// <para><b>The first test in this file is the one that matters</b>, and it is why the wave was not
/// trivial: a source definition carries credentials, and an audit log is an append-only store readable
/// by anyone holding <c>audit.read</c> that nobody thinks of as a secret store. If a plaintext password
/// can reach a row, this feature is worse than the bug <c>[Secret]</c> exists to prevent. So the
/// assertion is literal and blunt: the exact secret STRING appears in no field of any row produced by
/// the request.</para>
/// </summary>
public class CatalogChangeAuditTests
{
    // Distinctive enough that a substring assertion cannot pass by accident, and each one lives in a
    // different masking mechanism: SecretWalk's [Secret]-attributed properties, and the two hand-written
    // collection shapes SecretsMasker still owns.
    private const string GrpcPassword = "hunter2-CORRECT-HORSE-pw";
    private const string GrpcToken = "eyJhbGciOi-TOKEN-do-not-log";
    private const string HeaderSecret = "Bearer HEADER-SECRET-do-not-log";
    private const string RotatedPassword = "rotated-2NDpassword-do-not-log";
    private const string NewPassword = "bobs-NEW-login-password-do-not-log";

    // ---------------------------------------------------------------------------------------------
    // 1. The hazard. A secret placed in a source config never appears in plaintext in an audit row.
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task ASecretInASourceConfigNeverReachesAnAuditRowOnCreate()
    {
        var harness = Build();

        var (status, _) = await harness.CallAsync("POST /api/sources/", Admin, body: SourceBody(GrpcPassword));
        Assert.Equal(201, status);

        var rows = harness.Rows();
        Assert.NotEmpty(rows);
        AssertNoPlaintextSecret(rows);

        // …and the masking really ran, rather than the fields simply being absent: the mask is present
        // where the secret was. Without this, a bug that dropped `connector` entirely would pass above.
        var created = Assert.Single(rows, r => r.Outcome == CatalogChangeAudit.ExecutedOutcome);
        Assert.Contains(SourceKinds.SecretMask, created.AfterJson!);
    }

    [Fact]
    public async Task ASecretInASourceConfigNeverReachesAnAuditRowOnUpdate()
    {
        var harness = Build();
        Assert.Equal(201, (await harness.CallAsync("POST /api/sources/", Admin, body: SourceBody(GrpcPassword))).Status);
        harness.Clear();

        // A rotation: the same source, a different password. The most audit-relevant edit anyone makes
        // to a source, and the one a naive implementation leaks.
        var (status, _) = await harness.CallAsync(
            "PUT /api/sources/{name}", Admin, [("name", "prod-feed")], SourceBody(RotatedPassword, "rotated"));
        Assert.Equal(200, status);

        var rows = harness.Rows();
        Assert.NotEmpty(rows);
        AssertNoPlaintextSecret(rows);
    }

    /// <summary>
    /// The subtlety that makes the diff non-obvious: masking collapses both sides of a rotated
    /// credential to <c>***</c>, so a diff computed AFTER masking would report the single most
    /// audit-relevant edit as "nothing changed". <see cref="CatalogChangeAudit"/> decides which
    /// properties moved on the unmasked pair and emits only the masked ones, so the row says
    /// <c>connector</c> changed and shows <c>***</c> either side.
    /// </summary>
    [Fact]
    public async Task ACredentialRotationIsReportedAsAChangeEvenThoughBothSidesAreMasked()
    {
        var harness = Build();
        Assert.Equal(201, (await harness.CallAsync("POST /api/sources/", Admin, body: SourceBody(GrpcPassword))).Status);
        harness.Clear();

        // Description deliberately unchanged: `connector` is the ONLY thing that moved, and it moved
        // only inside a masked slot.
        Assert.Equal(200, (await harness.CallAsync(
            "PUT /api/sources/{name}", Admin, [("name", "prod-feed")], SourceBody(RotatedPassword))).Status);

        var executed = Assert.Single(harness.Rows(), r => r.Outcome == CatalogChangeAudit.ExecutedOutcome);
        Assert.Contains("connector", executed.BeforeJson!);
        Assert.Contains("connector", executed.AfterJson!);
        Assert.Contains(SourceKinds.SecretMask, executed.AfterJson!);
        AssertNoPlaintextSecret([executed]);
    }

    // ---------------------------------------------------------------------------------------------
    // 2. The shape of the three operations.
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task ACreateHasNoBeforeJson()
    {
        var harness = Build();

        Assert.Equal(201, (await harness.CallAsync("POST /api/sources/", Admin, body: SourceBody(GrpcPassword))).Status);

        var executed = Assert.Single(harness.Rows(), r => r.Outcome == CatalogChangeAudit.ExecutedOutcome);
        Assert.Null(executed.BeforeJson);
        Assert.NotNull(executed.AfterJson);
        Assert.Equal("created", executed.Detail);
        Assert.Equal("prod-feed", executed.Scope);
    }

    [Fact]
    public async Task ADeleteHasNoAfterJsonAndKeepsTheWholeDocument()
    {
        var harness = Build();

        Assert.Equal(204, (await harness.CallAsync("DELETE /api/tables/{id}", Admin, [("id", "aaaa")])).Status);

        var executed = Assert.Single(harness.Rows(), r => r.Outcome == CatalogChangeAudit.ExecutedOutcome);
        Assert.Null(executed.AfterJson);
        Assert.NotNull(executed.BeforeJson);
        Assert.Equal("deleted", executed.Detail);
        // The whole document, not a diff — after a delete this row is the last surviving copy.
        Assert.Contains("SELECT * FROM dev", executed.BeforeJson!);
    }

    /// <summary>The size decision, made visible: an update carries the properties that MOVED and not the
    /// ones that did not. A table's SQL body is the big field, and an edit that does not touch it must
    /// not pay for it twice on every save.</summary>
    [Fact]
    public async Task AnUpdateRecordsOnlyThePropertiesThatMoved()
    {
        var harness = Build();

        var (status, _) = await harness.CallAsync(
            "PUT /api/tables/{id}", Admin, [("id", "aaaa")],
            new { name = "dev-positions", description = "now with a description", sql = "SELECT * FROM dev" });
        Assert.Equal(200, status);

        var executed = Assert.Single(harness.Rows(), r => r.Outcome == CatalogChangeAudit.ExecutedOutcome);
        Assert.Equal("updated", executed.Detail);
        Assert.Contains("description", executed.AfterJson!);
        Assert.Contains("now with a description", executed.AfterJson!);
        // The unchanged SQL body is in neither side — that is the entire size argument.
        Assert.DoesNotContain("SELECT * FROM dev", executed.AfterJson!);
        Assert.DoesNotContain("SELECT * FROM dev", executed.BeforeJson!);
    }

    /// <summary>The pipeline PUT takes the same in-place-update snapshot the table PUT does, and its
    /// definition is a different shape — so the round trip is pinned on both rather than on one.</summary>
    [Fact]
    public async Task APipelineUpdateRecordsItsDiffToo()
    {
        var harness = Build();

        var (status, _) = await harness.CallAsync(
            "PUT /api/pipelines/{id}", Admin, [("id", "1111")],
            new { name = "dev-enrich", description = "renamed reason", sql = "SELECT * FROM dev" });
        Assert.Equal(200, status);

        var executed = Assert.Single(harness.Rows(), r => r.Outcome == CatalogChangeAudit.ExecutedOutcome);
        Assert.Equal("updated", executed.Detail);
        Assert.Equal("dev-enrich", executed.Scope);
        Assert.Contains("renamed reason", executed.AfterJson!);
        Assert.DoesNotContain("SELECT * FROM dev", executed.AfterJson!);
    }

    /// <summary>Past the cap the row keeps the changed field NAMES and drops the values. A blob cut off
    /// mid-JSON is unparseable and answers nothing; the names still answer "what changed".</summary>
    [Fact]
    public async Task AnOversizeDocumentDegradesToItsFieldNamesRatherThanToATruncatedBlob()
    {
        var giant = "SELECT * FROM dev WHERE note = '" + new string('x', CatalogChangeAudit.MaxJsonChars * 2) + "'";
        var harness = Build();
        harness.Catalog.Tables.Single(t => t.Id == "aaaa").Sql = giant;

        Assert.Equal(204, (await harness.CallAsync("DELETE /api/tables/{id}", Admin, [("id", "aaaa")])).Status);

        var executed = Assert.Single(harness.Rows(), r => r.Outcome == CatalogChangeAudit.ExecutedOutcome);
        Assert.Contains("_truncated", executed.BeforeJson!);
        Assert.Contains("\"sql\"", executed.BeforeJson!);
        Assert.DoesNotContain("xxxxxxxxxx", executed.BeforeJson!);
        Assert.True(executed.BeforeJson!.Length < CatalogChangeAudit.MaxJsonChars);
    }

    /// <summary><see cref="AccessGuard"/>'s <c>allowed</c> row is the DECISION and this wave's row is the
    /// EFFECT — a request can be allowed and then 400 on validation. Both exist, with the same action and
    /// scope so they correlate, and only one of them carries the change.</summary>
    [Fact]
    public async Task TheDecisionRowAndTheExecutionRowAreDistinctAndCorrelate()
    {
        var harness = Build();

        Assert.Equal(201, (await harness.CallAsync("POST /api/sources/", Admin, body: SourceBody(GrpcPassword))).Status);

        var rows = harness.Rows();
        var allowed = Assert.Single(rows, r => r.Outcome == "allowed");
        var executed = Assert.Single(rows, r => r.Outcome == CatalogChangeAudit.ExecutedOutcome);

        Assert.Equal(allowed.Action, executed.Action);
        Assert.Equal(allowed.Scope, executed.Scope);
        Assert.Null(allowed.AfterJson);
        Assert.NotNull(executed.AfterJson);
    }

    /// <summary>A request the guard allowed but validation then refused writes a decision row and NO
    /// execution row — nothing happened, and the audit log must not say otherwise.</summary>
    [Fact]
    public async Task ARefusedMutationWritesNoExecutionRow()
    {
        var harness = Build();

        // No fields, so SourceValidation answers 400 after the guard has already said yes.
        var (status, _) = await harness.CallAsync(
            "POST /api/sources/", Admin, body: new { name = "half-baked", kind = "generator", eventsPerSecond = 1 });

        Assert.Equal(400, status);
        Assert.DoesNotContain(harness.Rows(), r => r.Outcome == CatalogChangeAudit.ExecutedOutcome);
    }

    // ---------------------------------------------------------------------------------------------
    // 3. Attribution. The chat's Actor/OnBehalfOf/Origin must not become "rest" passing through here.
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task ARestMutationIsAttributedToTheAuthenticatedHuman()
    {
        var harness = Build();

        Assert.Equal(201, (await harness.CallAsync("POST /api/sources/", Admin, body: SourceBody(GrpcPassword))).Status);

        var executed = Assert.Single(harness.Rows(), r => r.Outcome == CatalogChangeAudit.ExecutedOutcome);
        Assert.Equal("admin", executed.Actor);
        Assert.Equal("rest", executed.Origin);
        // Nobody is acting on anyone's behalf on this path, and a self-referential value here would make
        // the field useless for the one case it exists for.
        Assert.Null(executed.OnBehalfOf);
    }

    /// <summary>
    /// The chat's tools call <c>ICatalogFacade</c> directly today and never reach these handlers, so
    /// nothing is attributed to a model through them yet. What this pins is that there is no line in
    /// <see cref="CatalogChangeAudit"/> that can quietly overwrite an attribution it was handed: a row
    /// built by <see cref="ChatAttribution.Row"/> keeps its model Actor, its human OnBehalfOf and its
    /// <c>chat</c> Origin, and gains only the before/after it came for.
    /// </summary>
    [Fact]
    public void ChatAttributionSurvivesAMutationRow()
    {
        var sink = new RecordingSink();
        var attribution = ChatAttribution.For("gemini-3.6-flash", Principal("alice"));

        CatalogChangeAudit.RecordSource(
            sink,
            attribution.Row(Actions.SourceWrite, "prod-feed", CatalogChangeAudit.ExecutedOutcome),
            before: Source(GrpcPassword),
            after: Source(RotatedPassword, "rotated"));

        var row = Assert.Single(sink.Entries);
        Assert.Equal("model:gemini-3.6-flash", row.Actor);
        Assert.Equal("alice", row.OnBehalfOf);
        Assert.Equal(ChatAttribution.ChatOrigin, row.Origin);
        Assert.NotNull(row.BeforeJson);
        Assert.NotNull(row.AfterJson);
        AssertNoPlaintextSecret([row]);
    }

    /// <summary>The rule the whole audit design rests on: audit never makes a request fail. A sink that
    /// throws on every row must not turn a successful mutation into a 500.</summary>
    [Fact]
    public void AThrowingSinkDoesNotPropagate()
    {
        CatalogChangeAudit.RecordSource(
            new ThrowingSink(), CatalogChangeAudit.RestRow(Principal("admin"), Actions.SourceWrite, "prod-feed"),
            before: null, after: Source(GrpcPassword));
    }

    // ---------------------------------------------------------------------------------------------
    // Assertions, bodies and fakes
    // ---------------------------------------------------------------------------------------------

    /// <summary>The blunt one. Every string field of every row, against every secret literal this file
    /// plants — a leak anywhere in the entry, not only in the two fields this wave added.</summary>
    private static void AssertNoPlaintextSecret(IReadOnlyList<AuditEntry> rows)
    {
        var secrets = new[] { GrpcPassword, GrpcToken, HeaderSecret, RotatedPassword };
        foreach (var row in rows)
        {
            var haystack = string.Join(' ', row.BeforeJson, row.AfterJson, row.Detail, row.Actor, row.Scope, row.Action);
            foreach (var secret in secrets)
            {
                Assert.DoesNotContain(secret, haystack, StringComparison.Ordinal);
            }
        }
    }

    /// <summary>A grpc-kind source carrying a credential in each of the three masking mechanisms:
    /// <c>[Secret]</c>-attributed properties found by <c>SecretWalk</c> (grpc password and token), and
    /// the hand-written URL-header dictionary. The stray <c>url</c> block on a grpc-kind source is
    /// deliberate — <c>SourceValidation</c> only validates the block its kind names, and masking must
    /// not be kind-aware.</summary>
    private static object SourceBody(string password, string description = "the upstream feed") => new
    {
        name = "prod-feed",
        description,
        kind = "grpc",
        fields = new[] { new { name = "symbol", type = "String" } },
        connector = new
        {
            grpc = new
            {
                address = "http://upstream:5299",
                entityKey = "source:trades",
                username = "svc",
                password,
                token = GrpcToken,
                restAddress = "http://upstream:5199",
                schemaSource = "reflection",
            },
            url = new
            {
                url = "https://upstream.example.com/feed",
                headers = new Dictionary<string, string> { ["Authorization"] = HeaderSecret },
            },
        },
    };

    /// <summary>The same shape as <see cref="SourceBody"/>, as a model — for the unit-level tests that
    /// call <see cref="CatalogChangeAudit"/> directly.</summary>
    private static SourceDefinition Source(string password, string description = "the upstream feed") => new()
    {
        Name = "prod-feed",
        Description = description,
        Kind = SourceKinds.Grpc,
        Fields = [new FieldDef("symbol", FieldType.String)],
        Connector = new ConnectorConfig
        {
            Grpc = new GrpcSubConfig
            {
                Address = "http://upstream:5299",
                EntityKey = "source:trades",
                Username = "svc",
                Password = password,
                Token = GrpcToken,
                RestAddress = "http://upstream:5199",
            },
            Url = new UrlPollConfig
            {
                Url = "https://upstream.example.com/feed",
                Headers = { ["Authorization"] = HeaderSecret },
            },
        },
    };

    private static ClaimsPrincipal Admin => Principal("admin");

    private static ClaimsPrincipal Principal(string name) => PermissionResolverTests.Principal(name);

    private sealed class RecordingSink : IAuditSink
    {
        private readonly List<AuditEntry> _entries = [];

        public IReadOnlyList<AuditEntry> Entries
        {
            get { lock (_entries) { return [.. _entries]; } }
        }

        public void Record(AuditEntry entry)
        {
            lock (_entries)
            {
                _entries.Add(entry);
            }
        }

        public void Clear()
        {
            lock (_entries)
            {
                _entries.Clear();
            }
        }
    }

    private sealed class ThrowingSink : IAuditSink
    {
        public void Record(AuditEntry entry) => throw new InvalidOperationException("this sink always throws");
    }

    // ---------------------------------------------------------------------------------------------
    // The harness — the same one CatalogEntitlementEndpointTests uses, plus a recording audit sink
    // registered LAST so it wins over AddStreamForgeApi's real one and over the throwing placeholders.
    // ---------------------------------------------------------------------------------------------

    private sealed class Harness(IReadOnlyList<Endpoint> endpoints, IServiceProvider services, RecordingSink sink)
    {
        public FakeCatalog Catalog { get; init; } = null!;

        public IReadOnlyList<AuditEntry> Rows() => sink.Entries;

        public void Clear() => sink.Clear();

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
                var json = JsonSerializer.SerializeToUtf8Bytes(body, new JsonSerializerOptions(JsonSerializerDefaults.Web));
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

    // ------------------------------------------------------------------------------------------
    // Plan 015 wave 6: /api/users. Added after the audit console's first live run showed these three
    // routes writing NO row at all — neither the guard's decision nor the mutation — which made
    // creating an account and changing somebody's role the only privileged mutations invisible in the
    // log. The credential hazard here is sharper than a source's: the secret is not a config field the
    // masker walks, it IS the record.
    // ------------------------------------------------------------------------------------------

    [Fact]
    public async Task APasswordResetIsRecordedWithoutThePasswordOrItsStoredHash()
    {
        var harness = Build();

        var (status, _) = await harness.CallAsync(
            "PUT /api/users/{username}", Admin, [("username", "bob")],
            new UpdateUserRequest(null, null, NewPassword));
        Assert.Equal(200, status);

        var rows = harness.Rows();
        Assert.NotEmpty(rows);
        foreach (var row in rows)
        {
            var blob = string.Join('\n', row.BeforeJson, row.AfterJson, row.Detail);
            Assert.DoesNotContain(NewPassword, blob, StringComparison.Ordinal);
            Assert.DoesNotContain(FakeUserStore.StoredHashPrefix, blob, StringComparison.Ordinal);
        }

        // …and the reset is still VISIBLE. Both sides mask to "***", so the diff is carried by the key
        // being present at all — which only works because the change is detected on the unmasked pair.
        // Drop the fields instead of masking them and a password reset renders as nothing happening.
        var executed = Assert.Single(rows, r => r.Outcome == CatalogChangeAudit.ExecutedOutcome);
        Assert.Contains("passwordHash", executed.BeforeJson!, StringComparison.Ordinal);
        Assert.Contains("passwordHash", executed.AfterJson!, StringComparison.Ordinal);
        Assert.Contains(SourceKinds.SecretMask, executed.AfterJson!, StringComparison.Ordinal);
        Assert.Equal("bob", executed.Scope);
    }

    [Fact]
    public async Task ARoleChangeSaysWhichRoleItWasAndWhichItBecame()
    {
        var harness = Build();

        Assert.Equal(200, (await harness.CallAsync(
            "PUT /api/users/{username}", Admin, [("username", "bob")],
            new UpdateUserRequest(null, "Admin", null))).Status);

        var executed = Assert.Single(harness.Rows(), r => r.Outcome == CatalogChangeAudit.ExecutedOutcome);
        Assert.Contains("Viewer", executed.BeforeJson!, StringComparison.Ordinal);
        Assert.Contains("Admin", executed.AfterJson!, StringComparison.Ordinal);
        Assert.Equal(Actions.UserWrite, executed.Action);
    }

    [Fact]
    public async Task DeletingAUserKeepsTheLastCopyOfTheRecordAndStillNoCredential()
    {
        var harness = Build();

        Assert.Equal(204, (await harness.CallAsync(
            "DELETE /api/users/{username}", Admin, [("username", "bob")])).Status);

        var executed = Assert.Single(harness.Rows(), r => r.Outcome == CatalogChangeAudit.ExecutedOutcome);
        Assert.NotNull(executed.BeforeJson);
        Assert.Null(executed.AfterJson);
        Assert.Equal("deleted", executed.Detail);
        Assert.Contains("bob", executed.BeforeJson!, StringComparison.Ordinal);
        Assert.DoesNotContain(FakeUserStore.StoredHashPrefix, executed.BeforeJson!, StringComparison.Ordinal);
    }

    private static Harness Build()
    {
        var catalog = new FakeCatalog
        {
            Tables =
            {
                new TableDefinition { Id = "aaaa", Name = "dev-positions", Sql = "SELECT * FROM dev" },
            },
            Pipelines =
            {
                new PipelineDefinition { Id = "1111", Name = "dev-enrich", Sql = "SELECT * FROM dev" },
            },
        };
        var sink = new RecordingSink();

        var builder = WebApplication.CreateBuilder();
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Jwt:Key"] = new string('k', 64),
            ["Jwt:Issuer"] = "streamforge-test",
            ["Jwt:Audience"] = "streamforge-test",
            ["Auth:PolicyCacheSeconds"] = "600",
        });
        builder.Services.AddStreamForgeApi(builder.Configuration);

        // Same reasoning as CatalogEntitlementEndpointTests: minimal-API binding asks the container at
        // MAP time whether a parameter's type is a service, so everything has to be registered; records
        // are skipped (they are request bodies) and facade interfaces get a proxy that throws on first
        // USE rather than on resolution.
        foreach (var t in typeof(ICatalogFacade).Assembly.GetTypes()
                     .Where(t => t.IsInterface && t.IsPublic && t.Name.EndsWith("Facade", StringComparison.Ordinal)))
        {
            var iface = t;
            builder.Services.AddSingleton(iface, _ => DispatchProxy.Create(
                iface, typeof(CatalogEntitlementEndpointTests.UntouchedFacade)));
        }

        foreach (var t in typeof(StreamForgeApiExtensions).Assembly.GetTypes()
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

        var document = new AccessPolicyDocument { Roles = BuiltInRoleCatalog.Create(), Version = 1 };
        document.Users.Add(new UserAccessEntry
        {
            Username = "admin",
            Grants = [new PermissionGrant { Action = "*", Scope = "*" }],
        });

        var policyFacade = new StaticPolicy(document);
        var resolver = new PermissionResolver(policyFacade, NullLogger<PermissionResolver>.Instance, 600);

        builder.Services.AddSingleton<IAccessPolicyFacade>(policyFacade);
        builder.Services.AddSingleton(resolver);
        // The guard gets the SAME sink, so "the secret appears in no audit row" covers the decision rows
        // as well as the ones this wave writes.
        builder.Services.AddSingleton(new AccessGuard(resolver, entitlementsEnabled: true, audit: sink));
        builder.Services.AddSingleton<ICatalogFacade>(catalog);
        builder.Services.AddSingleton<IUserStoreFacade>(new FakeUserStore());
        builder.Services.AddSingleton(new IngestKeyUsageTracker());
        builder.Services.AddSingleton<IAuditSink>(sink);

        var app = builder.Build();
        app.MapStreamForgeApi(new StreamForgeApiOptions(
            ProtosDir: Path.Combine(Path.GetTempPath(), "sf-change-audit-protos"),
            GrpcPort: 0,
            GrpcStaticServices: [],
            DocsFilePath: null,
            SpaDistPath: null,
            Flavor: "test"));

        return new Harness(
            [.. ((IEndpointRouteBuilder)app).DataSources.SelectMany(d => d.Endpoints)],
            app.Services,
            sink)
        {
            Catalog = catalog,
        };
    }

    private static bool IsRecord(Type t) =>
        t.GetMethod("<Clone>$", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic) is not null;

    /// <summary>Stores what it is given, unlike <see cref="CatalogEntitlementEndpointTests"/>' fake:
    /// these tests are about what a mutation RECORDED, so the mutation has to actually land.</summary>
    private sealed class FakeCatalog : ICatalogFacade
    {
        public List<SourceDefinition> Sources { get; } = [];
        public List<PipelineDefinition> Pipelines { get; } = [];
        public List<TableDefinition> Tables { get; } = [];

        public Task<List<SourceDefinition>> GetSourcesAsync() => Task.FromResult(Sources);

        public Task<SourceDefinition?> GetSourceAsync(string name) =>
            Task.FromResult(Sources.FirstOrDefault(s => s.Name == name));

        public Task UpsertSourceAsync(SourceDefinition def)
        {
            Sources.RemoveAll(s => s.Name == def.Name);
            Sources.Add(def);
            return Task.CompletedTask;
        }

        public Task<bool> DeleteSourceAsync(string name) => Task.FromResult(Sources.RemoveAll(s => s.Name == name) > 0);

        public Task<List<PipelineDefinition>> GetPipelinesAsync() => Task.FromResult(Pipelines);

        public Task<PipelineDefinition?> GetPipelineAsync(string id) =>
            Task.FromResult(Pipelines.FirstOrDefault(p => p.Id == id));

        public Task<PipelineDefinition> CreatePipelineAsync(PipelineDefinition def)
        {
            def.Id = Guid.NewGuid().ToString("n");
            Pipelines.Add(def);
            return Task.FromResult(def);
        }

        public Task<PipelineDefinition?> UpdatePipelineAsync(PipelineDefinition def) =>
            Task.FromResult<PipelineDefinition?>(def);

        public Task<bool> DeletePipelineAsync(string id) => Task.FromResult(Pipelines.RemoveAll(p => p.Id == id) > 0);

        public Task<PipelineDefinition?> SetPipelineStatusAsync(string id, PipelineStatus status) =>
            Task.FromResult(Pipelines.FirstOrDefault(p => p.Id == id));

        public Task<List<TableDefinition>> GetTablesAsync() => Task.FromResult(Tables);

        public Task<TableDefinition?> GetTableAsync(string id) =>
            Task.FromResult(Tables.FirstOrDefault(t => t.Id == id));

        public Task<TableDefinition> CreateTableAsync(TableDefinition def)
        {
            def.Id = Guid.NewGuid().ToString("n");
            Tables.Add(def);
            return Task.FromResult(def);
        }

        public Task<TableDefinition?> UpdateTableAsync(TableDefinition def) => Task.FromResult<TableDefinition?>(def);

        public Task<bool> DeleteTableAsync(string id) => Task.FromResult(Tables.RemoveAll(t => t.Id == id) > 0);

        public Task<TableDefinition?> SetTableStatusAsync(string id, PipelineStatus status) =>
            Task.FromResult(Tables.FirstOrDefault(t => t.Id == id));

        public Task<string> EnsureFieldNumbersAsync(string entityKey, List<FieldDef> fields) => Task.FromResult("{}");

        public Task<ScenarioRunResult> RunSourceAsync(string name, ScenarioRunRequest request) =>
            throw new NotSupportedException();
    }

    private sealed class StaticPolicy(AccessPolicyDocument document) : IAccessPolicyFacade
    {
        public Task<long> GetVersionAsync() => Task.FromResult(document.Version);
        public Task<AccessPolicyDocument> GetPolicyAsync() => Task.FromResult(document);
        public Task<RoleDefinition?> UpsertRoleAsync(RoleDefinition role, string actor) => throw new NotSupportedException();
        public Task<bool> DeleteRoleAsync(string name) => throw new NotSupportedException();
        public Task<GroupDefinition?> UpsertGroupAsync(GroupDefinition group, string actor) => throw new NotSupportedException();
        public Task<bool> DeleteGroupAsync(string name) => throw new NotSupportedException();
        public Task<UserAccessEntry?> UpsertUserAccessAsync(UserAccessEntry entry, string actor)
        {
            document.Users.RemoveAll(u => u.Username == entry.Username);
            document.Users.Add(entry);
            return Task.FromResult<UserAccessEntry?>(entry);
        }

        public Task<bool> DeleteUserAccessAsync(string username) =>
            Task.FromResult(document.Users.RemoveAll(u => u.Username == username) > 0);
        public Task<ApprovalTemplate?> UpsertApprovalTemplateAsync(ApprovalTemplate template, string actor) => throw new NotSupportedException();
        public Task<bool> DeleteApprovalTemplateAsync(string name) => throw new NotSupportedException();
    }

    /// <summary>The credential store, in memory. <see cref="HashOf"/> is deliberately a readable
    /// function of the password rather than a real KDF: the point of the users tests is that NEITHER
    /// the plaintext NOR the stored hash reaches a row, and a fake whose hash cannot be predicted could
    /// not assert the second half.</summary>
    private sealed class FakeUserStore : IUserStoreFacade
    {
        public const string StoredHashPrefix = "STORED-HASH-OF-";

        private readonly List<UserRecord> _users =
        [
            new() { Username = "admin", DisplayName = "Admin", Role = "Admin", PasswordHash = HashOf("admin123!"), PasswordSalt = "salt-admin" },
            new() { Username = "bob", DisplayName = "Bob", Role = "Viewer", PasswordHash = HashOf("bob-old-password"), PasswordSalt = "salt-bob" },
        ];

        public static string HashOf(string password) => StoredHashPrefix + password;

        public Task<UserRecord?> ValidateCredentialsAsync(string username, string password) =>
            Task.FromResult(_users.FirstOrDefault(u => u.Username == username && u.PasswordHash == HashOf(password)));

        public Task<List<UserRecord>> GetUsersAsync() => Task.FromResult(_users);

        public Task<bool> CreateUserAsync(string username, string displayName, string role, string password)
        {
            if (_users.Any(u => u.Username == username))
            {
                return Task.FromResult(false);
            }

            _users.Add(new UserRecord
            {
                Username = username,
                DisplayName = displayName,
                Role = role,
                PasswordHash = HashOf(password),
                PasswordSalt = "salt-" + username,
            });
            return Task.FromResult(true);
        }

        public Task<bool> UpdateUserAsync(string username, string? displayName, string? role, string? password)
        {
            var user = _users.FirstOrDefault(u => u.Username == username);
            if (user is null)
            {
                return Task.FromResult(false);
            }

            if (displayName is not null) user.DisplayName = displayName;
            if (role is not null) user.Role = role;
            if (password is not null) user.PasswordHash = HashOf(password);
            return Task.FromResult(true);
        }

        public Task<bool> DeleteUserAsync(string username) =>
            Task.FromResult(_users.RemoveAll(u => u.Username == username) > 0);
    }

    private sealed class BodyAllowed : IHttpRequestBodyDetectionFeature
    {
        public bool CanHaveBody => true;
    }
}
