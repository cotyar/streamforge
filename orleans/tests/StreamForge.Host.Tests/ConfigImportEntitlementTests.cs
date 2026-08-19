using System.Security.Claims;
using Microsoft.Extensions.Logging.Abstractions;
using StreamForge.Abstractions;
using StreamForge.Api;
using StreamForge.Api.Auth;
using StreamForge.AppCore.Access;
using StreamForge.AppCore.Config;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// Plan 015 wave 3-C — <c>POST /api/config/import</c> could rewrite the whole catalog behind one coarse
/// <c>Editor</c> gate, including entities the caller has no entitlement to touch.
///
/// <para><b>The rule this pins: refuse the whole import, naming the entities. Never apply the entitled
/// subset.</b> The argument is written out on
/// <see cref="ConfigImportService.FindUnentitledChangesAsync"/>; the short version is that a config
/// document's parts reference each other, so half of one is a document nobody wrote — and in
/// <c>replace</c> mode, half of one deletes the entities the caller <i>can</i> touch while leaving the
/// ones they cannot.</para>
///
/// <para>Every decision below comes from a real <see cref="AccessGuard"/> over a real
/// <see cref="PermissionResolver"/> over a fake store, so a guard wired to the wrong evaluator would
/// fail these tests rather than pass them.</para>
/// </summary>
public class ConfigImportEntitlementTests
{
    // =============================================================================================
    // The core rule.
    // =============================================================================================

    [Fact]
    public async Task An_import_touching_an_unentitled_entity_is_refused_and_the_entity_is_named()
    {
        var catalog = new ReadOnlyCatalog();
        var doc = new ConfigDocument
        {
            Sources =
            [
                Source("dev-feed"),
                Source("prod-feed"),
            ],
        };

        // Entitled to write dev-* and nothing else — the shape of a real "may change the dev catalog"
        // grant, and exactly the shape that a single coarse Editor gate could not express.
        var refusals = await FindAsync(doc, "merge", catalog, Grant(Actions.SourceWrite, "dev-*"));

        var line = Assert.Single(refusals);
        Assert.Contains("prod-feed", line);
        Assert.Contains("created", line);
        Assert.Contains(Actions.SourceWrite, line);
        // The reason from the evaluator travels all the way into the 403 body, so an operator knows
        // whether they are missing a grant or tripping over a Deny.
        Assert.Contains("no grant matches", line);

        // …and the message says, in words, that the entitled half was not applied either.
        var message = ConfigImportService.UnentitledImportMessage(refusals);
        Assert.Contains("nothing was applied", message);
        Assert.Contains("prod-feed", message);
    }

    [Fact]
    public async Task A_fully_entitled_document_produces_no_refusals()
    {
        var catalog = new ReadOnlyCatalog();
        var doc = new ConfigDocument { Sources = [Source("dev-feed"), Source("dev-book")] };

        Assert.Empty(await FindAsync(doc, "merge", catalog, Grant(Actions.SourceWrite, "dev-*")));
    }

    /// <summary>The property that makes the obvious workflow work: export the whole catalog, edit one
    /// pipeline, import it back, and be judged on the one pipeline you actually changed. A restated
    /// entity is planned <c>skipped</c>, and a change that is not a change needs no entitlement.</summary>
    [Fact]
    public async Task An_unchanged_entity_needs_no_entitlement()
    {
        var catalog = new ReadOnlyCatalog();
        catalog.Sources.Add(Source("prod-feed"));

        var doc = new ConfigDocument { Sources = [Source("prod-feed")] };

        // No grant at all, and still no refusal: the document asks for nothing.
        Assert.Empty(await FindAsync(doc, "merge", catalog));
    }

    // =============================================================================================
    // The right action for the right change.
    // =============================================================================================

    /// <summary>A replace-mode deletion costs <c>source.delete</c>, not <c>source.write</c>. Getting
    /// this wrong is how "may edit the catalog" quietly becomes "may empty the catalog".</summary>
    [Fact]
    public async Task A_replace_mode_deletion_costs_the_delete_action_and_not_the_write_action()
    {
        var catalog = new ReadOnlyCatalog();
        catalog.Sources.Add(Source("legacy-feed"));

        var doc = new ConfigDocument { Sources = [Source("dev-feed")] };

        var refusals = await FindAsync(doc, "replace", catalog, Grant(Actions.SourceWrite, "*"));

        var line = Assert.Single(refusals);
        Assert.Contains("legacy-feed", line);
        Assert.Contains("deleted", line);
        Assert.Contains(Actions.SourceDelete, line);

        // Grant the delete and the same document goes through.
        Assert.Empty(await FindAsync(doc, "replace", catalog, Grant(Actions.SourceWrite, "*"), Grant(Actions.SourceDelete, "*")));
    }

    /// <summary>Pipelines and tables are scoped by ID when they exist, because <c>/api/pipelines/{id}</c>
    /// is what the wave 2-B matrix scopes <c>pipeline.write</c> by — a grant must mean the same thing
    /// whichever surface asks. A pipeline being created has no id yet, so the proposed NAME is the only
    /// scope that exists at decision time.</summary>
    [Fact]
    public async Task An_existing_pipeline_is_scoped_by_id_and_a_new_one_by_name()
    {
        var catalog = new ReadOnlyCatalog();
        catalog.Pipelines.Add(new PipelineDefinition { Id = "pl-7", Name = "vwap", Sql = "SELECT 1" });

        var doc = new ConfigDocument
        {
            Pipelines =
            [
                new ConfigPipeline { Name = "vwap", Sql = "SELECT 2" },
                new ConfigPipeline { Name = "spread", Sql = "SELECT 3" },
            ],
        };

        // A grant written against the stored pipeline's ID covers the update and nothing else.
        var refusals = await FindAsync(doc, "merge", catalog, Grant(Actions.PipelineWrite, "pl-7"));
        var line = Assert.Single(refusals);
        Assert.Contains("spread", line);
        Assert.Contains("on spread", line); // the new one is scoped by its proposed name

        // A grant written against the new pipeline's NAME covers the creation and nothing else.
        var other = await FindAsync(doc, "merge", catalog, Grant(Actions.PipelineWrite, "spread"));
        Assert.Contains("on pl-7", Assert.Single(other));
    }

    /// <summary>Tag-scoped entitlements are decided against the STORED entity's tags — the resource
    /// being changed. (The retag ceiling is documented on <c>TagsFor</c>: it is <c>PUT
    /// /api/sources/{name}</c>'s ceiling too, and closing it only here would make importing stricter
    /// than editing.)</summary>
    [Fact]
    public async Task A_tag_scoped_grant_is_decided_against_the_stored_entitys_tags()
    {
        var catalog = new ReadOnlyCatalog();
        var stored = Source("feed");
        stored.Tags = ["finance"];
        catalog.Sources.Add(stored);

        var changed = Source("feed");
        changed.Tags = ["finance"];
        changed.Description = "edited";

        var doc = new ConfigDocument { Sources = [changed] };

        Assert.Empty(await FindAsync(doc, "merge", catalog, Grant(Actions.SourceWrite, "tag:finance")));
        Assert.Single(await FindAsync(doc, "merge", catalog, Grant(Actions.SourceWrite, "tag:marketing")));
    }

    // =============================================================================================
    // Fail closed.
    // =============================================================================================

    /// <summary>A <see cref="AccessDecision.RequiresApproval"/> grant is a refusal here, not a pass.
    /// Parking a whole catalog rewrite behind one approval is wave 4's design question; until it has an
    /// answer, "needs a second pair of eyes" must not be read as yes.</summary>
    [Fact]
    public async Task A_grant_that_requires_approval_refuses_the_import()
    {
        var catalog = new ReadOnlyCatalog();
        var doc = new ConfigDocument { Sources = [Source("dev-feed")] };

        var refusals = await FindAsync(doc, "merge", catalog,
            new PermissionGrant { Action = Actions.SourceWrite, Scope = "*", RequiresApproval = true });

        Assert.Contains("requires approval", Assert.Single(refusals));
    }

    /// <summary>Validate is the dry run of exactly this import, so it answers the same yes/no. A dry
    /// run that passes where the real run will 403 is worse than no dry run at all.</summary>
    [Fact]
    public async Task Validate_mode_answers_the_same_question_as_the_apply_it_is_a_dry_run_of()
    {
        var catalog = new ReadOnlyCatalog();
        var doc = new ConfigDocument { Sources = [Source("prod-feed")] };

        Assert.Single(await FindAsync(doc, "validate", catalog, Grant(Actions.SourceWrite, "dev-*")));
        Assert.Single(await FindAsync(doc, "merge", catalog, Grant(Actions.SourceWrite, "dev-*")));
    }

    /// <summary>Every entity is named, not just the first — a caller who has to run the import three
    /// times to discover three refusals will get a grant three times too wide the first time they ask.</summary>
    [Fact]
    public async Task Every_unentitled_entity_is_named_not_only_the_first()
    {
        var catalog = new ReadOnlyCatalog();
        var doc = new ConfigDocument { Sources = [Source("prod-a"), Source("prod-b"), Source("prod-c")] };

        var refusals = await FindAsync(doc, "merge", catalog, Grant(Actions.SourceWrite, "dev-*"));

        Assert.Equal(3, refusals.Count);
        var message = ConfigImportService.UnentitledImportMessage(refusals);
        Assert.Contains("prod-a", message);
        Assert.Contains("prod-b", message);
        Assert.Contains("prod-c", message);
    }

    // =============================================================================================
    // Helpers
    // =============================================================================================

    private static PermissionGrant Grant(string action, string scope) => new() { Action = action, Scope = scope };

    private static SourceDefinition Source(string name) => new()
    {
        Name = name,
        Fields = [new FieldDef("price", FieldType.Double)],
        GeneratorProfile = "generic",
        EventsPerSecond = 5,
        Enabled = true,
    };

    private static Task<IReadOnlyList<string>> FindAsync(
        ConfigDocument doc, string mode, ICatalogFacade catalog, params PermissionGrant[] grants)
    {
        var document = new AccessPolicyDocument
        {
            Version = 1,
            Users = [new UserAccessEntry { Username = "alice", Grants = [.. grants] }],
        };
        var resolver = new PermissionResolver(
            new CountingAccessPolicyFacade(document), NullLogger<PermissionResolver>.Instance, policyCacheSeconds: 600);
        var guard = new AccessGuard(resolver, entitlementsEnabled: true);
        var principal = new ClaimsPrincipal(new ClaimsIdentity([new Claim(ClaimTypes.Name, "alice")], "test"));

        return ConfigImportService.FindUnentitledChangesAsync(
            doc, mode, catalog, (action, scope, tags) => guard.CheckAsync(principal, action, scope, tags));
    }

    /// <summary>The three reads <see cref="ConfigImportService.FindUnentitledChangesAsync"/> makes, and
    /// nothing else — every write member throws, so a pre-check that accidentally grew a write would
    /// fail loudly instead of quietly mutating the catalog it is supposed to be deciding about.</summary>
    private sealed class ReadOnlyCatalog : ICatalogFacade
    {
        public List<SourceDefinition> Sources { get; } = [];
        public List<PipelineDefinition> Pipelines { get; } = [];
        public List<TableDefinition> Tables { get; } = [];

        public Task<List<SourceDefinition>> GetSourcesAsync() => Task.FromResult(new List<SourceDefinition>(Sources));
        public Task<List<PipelineDefinition>> GetPipelinesAsync() => Task.FromResult(new List<PipelineDefinition>(Pipelines));
        public Task<List<TableDefinition>> GetTablesAsync() => Task.FromResult(new List<TableDefinition>(Tables));

        public Task<SourceDefinition?> GetSourceAsync(string name) => throw new NotImplementedException();
        public Task UpsertSourceAsync(SourceDefinition def) => throw new NotImplementedException();
        public Task<bool> DeleteSourceAsync(string name) => throw new NotImplementedException();
        public Task<ScenarioRunResult> RunSourceAsync(string name, ScenarioRunRequest request) => throw new NotImplementedException();
        public Task<PipelineDefinition?> GetPipelineAsync(string id) => throw new NotImplementedException();
        public Task<PipelineDefinition> CreatePipelineAsync(PipelineDefinition def) => throw new NotImplementedException();
        public Task<PipelineDefinition?> UpdatePipelineAsync(PipelineDefinition def) => throw new NotImplementedException();
        public Task<bool> DeletePipelineAsync(string id) => throw new NotImplementedException();
        public Task<PipelineDefinition?> SetPipelineStatusAsync(string id, PipelineStatus status) => throw new NotImplementedException();
        public Task<TableDefinition?> GetTableAsync(string id) => throw new NotImplementedException();
        public Task<TableDefinition> CreateTableAsync(TableDefinition def) => throw new NotImplementedException();
        public Task<TableDefinition?> UpdateTableAsync(TableDefinition def) => throw new NotImplementedException();
        public Task<bool> DeleteTableAsync(string id) => throw new NotImplementedException();
        public Task<TableDefinition?> SetTableStatusAsync(string id, PipelineStatus status) => throw new NotImplementedException();
        public Task<string> EnsureFieldNumbersAsync(string entityKey, List<FieldDef> fields) => throw new NotImplementedException();
    }
}
