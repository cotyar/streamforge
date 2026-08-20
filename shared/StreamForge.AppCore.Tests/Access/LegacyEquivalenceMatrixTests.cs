using StreamForge.Abstractions;
using StreamForge.AppCore.Access;
using Xunit;

namespace StreamForge.AppCore.Tests.Access;

/// <summary>
/// Plan 015 wave 1 — the acceptance test for <see cref="BuiltInRoleCatalog"/>: for every authorization
/// site the platform has today, the entitlement evaluator must answer exactly what the ASP.NET policy
/// answers.
///
/// <para><b>How the table was built.</b> Not from memory. Every row below was read off a map site:
/// the <c>RequireAuthorization("…")</c> / <c>AllowAnonymous()</c> call attached to each
/// <c>Map{Get,Post,Put,Delete}</c> under <c>shared/StreamForge.Api/Endpoints/**</c> plus
/// <c>Chat/ChatEndpoints.cs</c> and the two <c>MapGet</c>/<c>MapScalarApiReference</c> sites in
/// <c>StreamForgeApiExtensions.cs</c>, and the <c>[Authorize(Policy = …)]</c> attributes on the gRPC
/// services under <c>orleans/src/StreamForge.Host/Grpc/**</c> and on <c>Hubs/StreamHub.cs</c>. Where a
/// route is <c>MapGroup</c>-gated (<c>/api/users</c> is <c>RequireAuthorization("Admin")</c> on the
/// group) the group's policy is the row's policy.</para>
///
/// <para><b>What it proves and what it does not.</b> It proves the built-in role grant sets are a
/// faithful translation of <c>Viewer = authenticated</c>, <c>Editor = role Editor|Admin</c>,
/// <c>Admin = role Admin</c> — so an untouched catalog behaves identically before and after the
/// upgrade. It does NOT prove that each route ends up asking for the action named here: that is the
/// endpoint-metadata test wave 2 owns. Until then this table is also the specification wave 3 migrates
/// the routes against, which is why it carries the paths and not only the actions.</para>
/// </summary>
public class LegacyEquivalenceMatrixTests
{
    /// <summary>One authorization site. <paramref name="Action"/> is null for the sites that are not
    /// permission-gated at all: the anonymous ones, and <c>GET /api/auth/me</c>, which is the Viewer
    /// policy's degenerate case — it returns the caller's own identity, so "authenticated" is the whole
    /// requirement and there is no resource to name.</summary>
    public sealed record Site(string Surface, string Route, string Policy, string? Action, string Scope);

    private static readonly Site[] All =
    [
        // ---- anonymous / self ------------------------------------------------------------------
        new("rest", "POST /api/auth/login", "anonymous", null, "*"),
        new("rest", "POST /api/auth/logout", "anonymous", null, "*"),
        new("rest", "GET /api/auth/me", "Viewer", null, "*"),
        new("rest", "GET /healthz", "anonymous", null, "*"),
        new("rest", "GET /api/healthz", "anonymous", null, "*"),
        // Plan 016 wave 5: anonymous like /healthz, on purpose — the endpoint a peer probes and an
        // operator curls before they have any credential. See MetaEndpoints' own doc comment on this
        // route for why that is safe (counts, not names; kind names, not connector configuration).
        new("rest", "GET /api/meta/instance", "anonymous", null, "*"),

        // ---- sources ---------------------------------------------------------------------------
        new("rest", "GET /api/sources", "Viewer", Actions.SourceRead, "*"),
        new("rest", "GET /api/sources/{name}", "Viewer", Actions.SourceRead, "trades"),
        new("rest", "GET /api/sources/{name}/proto", "Viewer", Actions.SourceRead, "trades"),
        new("rest", "GET /api/sources/{name}/status", "Viewer", Actions.SourceRead, "trades"),
        new("rest", "GET /api/sources/{name}/ingest", "Viewer", Actions.SourceRead, "trades"),
        new("rest", "GET /api/sources/{name}/openapi.json", "Viewer", Actions.SourceRead, "trades"),
        new("rest", "GET /scalar/sources/{name}", "Viewer", Actions.SourceRead, "trades"),
        new("rest", "POST /api/sources", "Editor", Actions.SourceWrite, "trades"),
        new("rest", "PUT /api/sources/{name}", "Editor", Actions.SourceWrite, "trades"),
        new("rest", "DELETE /api/sources/{name}", "Editor", Actions.SourceDelete, "trades"),
        new("rest", "POST /api/sources/{name}/ingest/keys", "Editor", Actions.SourceWrite, "trades"),
        new("rest", "GET /api/sources/{name}/ingest/keys", "Editor", Actions.SourceWrite, "trades"),
        new("rest", "DELETE /api/sources/{name}/ingest/keys/{id}", "Editor", Actions.SourceWrite, "trades"),
        new("rest", "POST /api/sources/schema/mapping-validate", "Editor", Actions.SourceWrite, "*"),
        new("rest", "POST /api/sources/schema/derive-openapi", "Editor", Actions.SourceWrite, "*"),
        new("rest", "POST /api/sources/schema/from-remote", "Editor", Actions.SourceWrite, "*"),
        new("rest", "POST /api/sources/{name}/run", "Editor", Actions.SourceRun, "trades"),
        // AllowAnonymous at the route, then a MANUAL dual check inside: an Editor JWT OR a valid
        // X-SF-Ingest-Key (plan 009 A1.2). The JWT half is what this row pins.
        new("rest", "POST /api/sources/{name}/events", "Editor", Actions.SourceIngest, "trades"),

        // ---- pipelines -------------------------------------------------------------------------
        new("rest", "GET /api/pipelines", "Viewer", Actions.PipelineRead, "*"),
        new("rest", "GET /api/pipelines/{id}", "Viewer", Actions.PipelineRead, "p1"),
        new("rest", "GET /api/pipelines/{id}/proto", "Viewer", Actions.PipelineRead, "p1"),
        new("rest", "GET /api/pipelines/{id}/plan", "Viewer", Actions.PipelineRead, "p1"),
        new("rest", "GET /api/pipelines/{id}/results", "Viewer", Actions.PipelineRead, "p1"),
        new("rest", "GET /api/pipelines/{id}/results.csv", "Viewer", Actions.PipelineRead, "p1"),
        new("rest", "GET /api/pipelines/{id}/metrics", "Viewer", Actions.PipelineRead, "p1"),
        new("rest", "GET /api/pipelines/{id}/openapi.json", "Viewer", Actions.PipelineRead, "p1"),
        new("rest", "GET /scalar/pipelines/{id}", "Viewer", Actions.PipelineRead, "p1"),
        new("rest", "POST /api/pipelines", "Editor", Actions.PipelineWrite, "p1"),
        new("rest", "PUT /api/pipelines/{id}", "Editor", Actions.PipelineWrite, "p1"),
        new("rest", "POST /api/pipelines/validate", "Editor", Actions.PipelineWrite, "*"),
        new("rest", "DELETE /api/pipelines/{id}", "Editor", Actions.PipelineDelete, "p1"),
        new("rest", "POST /api/pipelines/{id}/start", "Editor", Actions.PipelineControl, "p1"),
        new("rest", "POST /api/pipelines/{id}/stop", "Editor", Actions.PipelineControl, "p1"),

        // ---- tables ----------------------------------------------------------------------------
        new("rest", "GET /api/tables", "Viewer", Actions.TableRead, "*"),
        new("rest", "GET /api/tables/{id}", "Viewer", Actions.TableRead, "t1"),
        new("rest", "GET /api/tables/{id}/plan", "Viewer", Actions.TableRead, "t1"),
        new("rest", "GET /api/tables/{id}/rows", "Viewer", Actions.TableRead, "t1"),
        new("rest", "GET /api/tables/{id}/rows.csv", "Viewer", Actions.TableRead, "t1"),
        new("rest", "GET /api/tables/{id}/metrics", "Viewer", Actions.TableRead, "t1"),
        new("rest", "GET /api/tables/{id}/proto", "Viewer", Actions.TableRead, "t1"),
        new("rest", "GET /api/tables/{id}/search", "Viewer", Actions.TableRead, "t1"),
        new("rest", "POST /api/tables/{id}/history/lookup", "Viewer", Actions.TableRead, "t1"),
        new("rest", "GET /api/tables/{id}/history/stats", "Viewer", Actions.TableRead, "t1"),
        new("rest", "POST /api/tables/{id}/shard/lookup", "Viewer", Actions.TableRead, "t1"),
        new("rest", "GET /api/tables/{id}/shards", "Viewer", Actions.TableRead, "t1"),
        new("rest", "GET /api/tables/{id}/shards/scan", "Viewer", Actions.TableRead, "t1"),
        new("rest", "GET /api/tables/{id}/openapi.json", "Viewer", Actions.TableRead, "t1"),
        new("rest", "GET /scalar/tables/{id}", "Viewer", Actions.TableRead, "t1"),
        new("rest", "POST /api/tables", "Editor", Actions.TableWrite, "t1"),
        new("rest", "PUT /api/tables/{id}", "Editor", Actions.TableWrite, "t1"),
        new("rest", "POST /api/tables/validate", "Editor", Actions.TableWrite, "*"),
        new("rest", "DELETE /api/tables/{id}", "Editor", Actions.TableDelete, "t1"),
        new("rest", "POST /api/tables/{id}/start", "Editor", Actions.TableControl, "t1"),
        new("rest", "POST /api/tables/{id}/stop", "Editor", Actions.TableControl, "t1"),

        // ---- config ----------------------------------------------------------------------------
        new("rest", "GET /api/config/export", "Viewer", Actions.ConfigExport, "*"),
        new("rest", "POST /api/config/import", "Editor", Actions.ConfigReplace, "*"),

        // ---- platform metadata (no dedicated action; catalog.read / catalog.write cover them) ----
        new("rest", "GET /api/meta/protos/static", "Viewer", Actions.CatalogRead, "*"),
        new("rest", "GET /api/meta/grpc", "Viewer", Actions.CatalogRead, "*"),
        new("rest", "GET /api/meta/arrangements", "Viewer", Actions.CatalogRead, "*"),
        // Plan 016 wave 5: a directory listing/probe is read-only catalog metadata about THIS instance's
        // own configuration, not about any one entity — catalog.read at * fits it the same way it fits
        // the three routes above. See MetaEndpoints' doc comment on the probe route for why a nominally
        // mutating POST is gated no more strictly than the read it augments.
        new("rest", "GET /api/meta/peers", "Viewer", Actions.CatalogRead, "*"),
        new("rest", "POST /api/meta/peers/{name}/probe", "Viewer", Actions.CatalogRead, "*"),
        new("rest", "GET /api/sql/functions", "Viewer", Actions.CatalogRead, "*"),
        new("rest", "GET /api/transports", "Viewer", Actions.CatalogRead, "*"),
        new("rest", "POST /api/transports/{kind}/probe", "Editor", Actions.CatalogWrite, "*"),

        // ---- users, chat -----------------------------------------------------------------------
        new("rest", "GET /api/users", "Admin", Actions.UserRead, "*"),
        new("rest", "POST /api/users", "Admin", Actions.UserWrite, "*"),
        new("rest", "PUT /api/users/{username}", "Admin", Actions.UserWrite, "bob"),
        new("rest", "DELETE /api/users/{username}", "Admin", Actions.UserWrite, "bob"),
        new("rest", "POST /api/chat", "Editor", Actions.ChatUse, "*"),

        // ---- SignalR ---------------------------------------------------------------------------
        new("signalr", "StreamHub /hubs/stream", "Viewer", Actions.CatalogRead, "*"),

        // ---- gRPC (orleans/src/StreamForge.Host/Grpc/**) ----------------------------------------
        new("grpc", "SourceGrpcService.List", "Viewer", Actions.SourceRead, "*"),
        new("grpc", "SourceGrpcService.Get", "Viewer", Actions.SourceRead, "trades"),
        new("grpc", "SourceGrpcService.Create", "Editor", Actions.SourceWrite, "trades"),
        new("grpc", "SourceGrpcService.Update", "Editor", Actions.SourceWrite, "trades"),
        new("grpc", "SourceGrpcService.Delete", "Editor", Actions.SourceDelete, "trades"),
        new("grpc", "PipelineGrpcService.List", "Viewer", Actions.PipelineRead, "*"),
        new("grpc", "PipelineGrpcService.Get", "Viewer", Actions.PipelineRead, "p1"),
        new("grpc", "PipelineGrpcService.Create", "Editor", Actions.PipelineWrite, "p1"),
        new("grpc", "PipelineGrpcService.Update", "Editor", Actions.PipelineWrite, "p1"),
        new("grpc", "PipelineGrpcService.Delete", "Editor", Actions.PipelineDelete, "p1"),
        new("grpc", "PipelineGrpcService.Start", "Editor", Actions.PipelineControl, "p1"),
        new("grpc", "PipelineGrpcService.Stop", "Editor", Actions.PipelineControl, "p1"),
        new("grpc", "TableGrpcService.List", "Viewer", Actions.TableRead, "*"),
        new("grpc", "TableGrpcService.Get", "Viewer", Actions.TableRead, "t1"),
        new("grpc", "TableGrpcService.Rows", "Viewer", Actions.TableRead, "t1"),
        new("grpc", "TableGrpcService.Create", "Editor", Actions.TableWrite, "t1"),
        new("grpc", "TableGrpcService.Update", "Editor", Actions.TableWrite, "t1"),
        new("grpc", "TableGrpcService.Delete", "Editor", Actions.TableDelete, "t1"),
        new("grpc", "TableGrpcService.Start", "Editor", Actions.TableControl, "t1"),
        new("grpc", "TableGrpcService.Stop", "Editor", Actions.TableControl, "t1"),
        new("grpc", "StreamGrpcService.Subscribe", "Viewer", Actions.PipelineRead, "p1"),
        new("grpc", "DynamicStreamService.Subscribe", "Viewer", Actions.TableRead, "t1"),
        // IngestGrpcService carries NO method-level [Authorize] on purpose — per-message dual auth,
        // the gRPC twin of POST /api/sources/{name}/events.
        new("grpc", "IngestGrpcService.Push", "Editor", Actions.SourceIngest, "trades"),
    ];

    /// <summary>Flattened to strings so each row gets a readable, individually-runnable test name —
    /// xUnit can only serialize primitives, and "which route regressed" is the whole value of a
    /// table-driven matrix.</summary>
    public static TheoryData<string, string, string, string, string> GatedSites()
    {
        var data = new TheoryData<string, string, string, string, string>();
        foreach (var s in All.Where(s => s.Action is not null))
        {
            data.Add(s.Surface, s.Route, s.Policy, s.Action!, s.Scope);
        }

        return data;
    }

    /// <summary>The matrix itself: every gated site × the three built-in roles, against what the ASP.NET
    /// policy at that site grants today.</summary>
    [Theory]
    [MemberData(nameof(GatedSites))]
    public void EachBuiltInRoleAnswersExactlyWhatItsLegacyPolicyAnswers(
        string surface, string route, string policy, string action, string scope)
    {
        foreach (var role in BuiltInRoles.All)
        {
            var expected = LegacyPolicyGrants(policy, role) ? AccessDecision.Allowed : AccessDecision.Denied;
            var actual = PermissionEvaluator.Evaluate(PermissionsFor(role), action, scope);

            Assert.True(
                expected == actual.Decision,
                $"{role} @ {surface} {route} [{policy}] asking {action} on {scope}: " +
                $"expected {expected}, got {actual.Decision} — {actual.Reason}");
        }
    }

    /// <summary>Today's three policies, verbatim from
    /// <c>StreamForgeApiExtensions.AddStreamForgeApi</c>: Viewer = any authenticated user, Editor =
    /// role Editor or Admin, Admin = role Admin.</summary>
    private static bool LegacyPolicyGrants(string policy, string role) => policy switch
    {
        "Viewer" => true,
        "Editor" => role is BuiltInRoles.Editor or BuiltInRoles.Admin,
        "Admin" => role is BuiltInRoles.Admin,
        _ => throw new ArgumentOutOfRangeException(nameof(policy), policy, "unknown legacy policy"),
    };

    private static EffectivePermissions PermissionsFor(string role)
    {
        // The full flattening path, not a hand-built grant list: this must break if the built-in seeds
        // and the builder ever stop agreeing.
        var document = new AccessPolicyDocument { Roles = BuiltInRoleCatalog.Create(), Version = 1 };
        document.Users.Add(new UserAccessEntry { Username = "u", Roles = [role] });
        return EffectivePermissionsBuilder.Build(document, "u");
    }

    [Fact]
    public void NoBuiltInRoleCanReachTheNewPrivilegedSurfaceExceptAdmin()
    {
        // Not legacy equivalence — these actions have no route today — but the other half of "an
        // untouched catalog behaves identically": the new surface must not arrive pre-granted to
        // everybody. approval.request is the deliberate exception (see BuiltInRoleCatalog).
        string[] privileged =
        [
            Actions.AccessRead, Actions.AccessWrite, Actions.AuditRead,
            Actions.ApprovalDecide, Actions.ApprovalBypass,
        ];

        foreach (var action in privileged)
        {
            Assert.Equal(AccessDecision.Allowed, PermissionEvaluator.Evaluate(PermissionsFor(BuiltInRoles.Admin), action, "*").Decision);
            Assert.Equal(AccessDecision.Denied, PermissionEvaluator.Evaluate(PermissionsFor(BuiltInRoles.Editor), action, "*").Decision);
            Assert.Equal(AccessDecision.Denied, PermissionEvaluator.Evaluate(PermissionsFor(BuiltInRoles.Viewer), action, "*").Decision);
        }

        foreach (var role in BuiltInRoles.All)
        {
            Assert.Equal(AccessDecision.Allowed, PermissionEvaluator.Evaluate(PermissionsFor(role), Actions.ApprovalRequest, "*").Decision);
        }
    }

    [Fact]
    public void NoBuiltInRoleGrantIsScopedNarrowerThanEverything()
    {
        // Legacy roles are global — none of the three policies looks at the resource. A built-in whose
        // grant carried a scope would take capability away on upgrade for every entity that did not
        // match it, and would do it silently.
        foreach (var role in BuiltInRoleCatalog.Create())
        {
            Assert.All(role.Grants, g =>
            {
                Assert.Equal("*", g.Scope);
                Assert.Equal(PermissionEffect.Allow, g.Effect);
                Assert.False(g.RequiresApproval);
            });
        }
    }

    [Fact]
    public void EveryBuiltInIsMarkedBuiltInAndTheCatalogHandsOutFreshInstances()
    {
        var first = BuiltInRoleCatalog.Create();
        var second = BuiltInRoleCatalog.Create();

        Assert.Equal(BuiltInRoles.All.Order(), first.Select(r => r.Name).Order());
        Assert.All(first, r => Assert.True(r.BuiltIn));
        // Seeds are written into a document an administrator then edits — sharing instances would make
        // one seed's edit show up in another's.
        Assert.NotSame(first[0], second[0]);
        Assert.NotSame(first[0].Grants[0], second[0].Grants[0]);
    }
}
