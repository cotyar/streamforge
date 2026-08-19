using StreamForge.Abstractions;
using StreamForge.AppCore.Config;
using Xunit;

namespace StreamForge.AppCore.Tests;

/// <summary>
/// Plan 016 wave 3-B — <see cref="ImportPlanner"/>'s dependency scan for entities that do not exist yet
/// (now <c>SqlCompiler.ExtractReferences</c>, not the old FROM-only regex), the ordinal-vs-catalog
/// missing-dependency diagnostic it now produces, and proof that EXISTING entities still order by their
/// persisted <c>TableInputs</c>, never by a scan of the document's SQL. <c>ImportPlannerTests</c> (not
/// mine to edit — orleans/tests/StreamForge.Host.Tests) already pins the regex-era behaviour for its own
/// fixtures; this file is where the wave 3-B additions get their own coverage.
/// </summary>
public class ImportPlannerDependencyScanTests
{
    // ------------------------------------------------------------------
    // The regex's two defects: JOINs were invisible, and a CTE invented a phantom dependency.
    // ------------------------------------------------------------------

    [Fact]
    public void JoinIntroducedDependencyIsFoundForANewTable()
    {
        // The old FROM-only regex would see "leaf" (via FROM) on both tables and nothing else — it never
        // looked at a JOIN clause, so "downstream" JOINing "upstream" was invisible to it.
        var doc = new ConfigDocument
        {
            Tables =
            [
                new ConfigTable { Name = "downstream", Sql = "SELECT a.x FROM leaf a JOIN upstream b ON a.x = b.x" },
                new ConfigTable { Name = "upstream", Sql = "SELECT x FROM leaf" },
            ],
        };

        var actions = ImportPlanner.Plan(doc, [], [], [], "merge");
        var order = actions.Where(a => a.Kind == "table").Select(a => a.Name).ToList();

        Assert.Equal(["upstream", "downstream"], order);
    }

    [Fact]
    public void SubqueryIntroducedDependencyIsFoundForANewTable()
    {
        // The old regex also never looked inside a WHERE-position subquery.
        var doc = new ConfigDocument
        {
            Tables =
            [
                new ConfigTable { Name = "downstream", Sql = "SELECT x FROM leaf WHERE x IN ( SELECT x FROM upstream )" },
                new ConfigTable { Name = "upstream", Sql = "SELECT x FROM leaf" },
            ],
        };

        var actions = ImportPlanner.Plan(doc, [], [], [], "merge");
        var order = actions.Where(a => a.Kind == "table").Select(a => a.Name).ToList();

        Assert.Equal(["upstream", "downstream"], order);
    }

    [Fact]
    public void CteNameIsNeverAPhantomDependencyForANewTable()
    {
        // The regex bug this wave exists to fix: "WITH recent AS (…) SELECT * FROM recent" made the
        // regex emit "recent" — a name no catalog will ever hold — instead of the CTE body's real
        // relation. The compiler resolves the CTE away, so the real dependency ("upstream") is what
        // orders the tables, and "recent" never appears anywhere, including in a diagnostic.
        var doc = new ConfigDocument
        {
            Tables =
            [
                new ConfigTable { Name = "upstream", Sql = "SELECT x FROM leaf" },
                new ConfigTable { Name = "downstream", Sql = "WITH recent AS ( SELECT x FROM upstream ) SELECT * FROM recent" },
            ],
        };

        var actions = ImportPlanner.Plan(doc, [], [], [], "merge");
        var order = actions.Where(a => a.Kind == "table").Select(a => a.Name).ToList();
        Assert.Equal(["upstream", "downstream"], order);

        var downstreamAction = actions.Single(a => a.Name == "downstream");
        Assert.DoesNotContain(downstreamAction.Diagnostics, d => d.Contains("recent"));
    }

    // ------------------------------------------------------------------
    // The split: EXISTING entities order by persisted TableInputs, never by a scan of the document SQL.
    // ------------------------------------------------------------------

    [Fact]
    public void ExistingTableOrderingIgnoresANewlyAuthoredJoinInTheDocument()
    {
        // Both tables already exist with EMPTY persisted TableInputs (their last successful compile read
        // nothing — e.g. authored before this dependency existed). The document now edits "downstream"'s
        // SQL to JOIN "upstream", but recompiling that SQL (and refreshing TableInputs) is
        // ConfigImportService's job at APPLY time, never the planner's at PLAN time — so the persisted
        // (stale) facts, not a scan, are what decide apply order here.
        var storedDownstream = new TableDefinition { Id = "d1", Name = "downstream", Sql = "SELECT 1", TableInputs = [] };
        var storedUpstream = new TableDefinition { Id = "u1", Name = "upstream", Sql = "SELECT 1", TableInputs = [] };

        var doc = new ConfigDocument
        {
            Tables =
            [
                new ConfigTable { Name = "downstream", Sql = "SELECT a.x FROM leaf a JOIN upstream b ON a.x = b.x" },
                new ConfigTable { Name = "upstream", Sql = "SELECT x FROM leaf" },
            ],
        };

        var actions = ImportPlanner.Plan(doc, [], [], [storedUpstream, storedDownstream], "merge");
        var order = actions.Where(a => a.Kind == "table").Select(a => a.Name).ToList();

        // No forced ordering (both existing, both persisted-empty) -> alphabetical: "downstream" first.
        // If the planner scanned the document SQL for existing entities too, "upstream" would come first.
        Assert.Equal(["downstream", "upstream"], order);
    }

    [Fact]
    public void ExistingTableStillOrdersByItsPersistedInputEvenWhenTheDocumentSqlDisagrees()
    {
        // The mirror image: the STORED TableInputs say "downstream" depends on "upstream", but the
        // document's SQL for "downstream" no longer even mentions it. Ordering still follows the
        // persisted fact.
        var storedUpstream = new TableDefinition { Id = "u1", Name = "upstream", Sql = "SELECT 1", TableInputs = [] };
        var storedDownstream = new TableDefinition { Id = "d1", Name = "downstream", Sql = "SELECT 1", TableInputs = ["upstream"] };

        var doc = new ConfigDocument
        {
            Tables =
            [
                new ConfigTable { Name = "upstream", Sql = "SELECT 2" },
                new ConfigTable { Name = "downstream", Sql = "SELECT 2" }, // no longer mentions "upstream"
            ],
        };

        var actions = ImportPlanner.Plan(doc, [], [], [storedUpstream, storedDownstream], "merge");
        var order = actions.Where(a => a.Kind == "table").Select(a => a.Name).ToList();

        Assert.Equal(["upstream", "downstream"], order);
    }

    // ------------------------------------------------------------------
    // Missing-dependency diagnostics: informational only, never a different Action.
    // ------------------------------------------------------------------

    [Fact]
    public void MissingDependencyIsDiagnosedForANewTableButDoesNotChangeTheOutcome()
    {
        var doc = new ConfigDocument
        {
            Tables = [new ConfigTable { Name = "t", Sql = "SELECT x FROM nonexistent" }],
        };

        var action = Assert.Single(ImportPlanner.Plan(doc, [], [], [], "merge"));

        Assert.Equal("created", action.Action);
        Assert.Contains(action.Diagnostics, d => d.Contains("nonexistent") && d.Contains("does not exist"));
    }

    [Fact]
    public void MissingDependencyIsDiagnosedForANewPipelineToo()
    {
        var doc = new ConfigDocument
        {
            Pipelines = [new ConfigPipeline { Name = "p", Sql = "SELECT x FROM nonexistent" }],
        };

        var action = Assert.Single(ImportPlanner.Plan(doc, [], [], [], "merge"));

        Assert.Equal("created", action.Action);
        Assert.Contains(action.Diagnostics, d => d.Contains("nonexistent") && d.Contains("does not exist"));
    }

    [Fact]
    public void NoMissingDependencyDiagnosticWhenTheReferenceResolvesAgainstAnExistingSource()
    {
        var source = new SourceDefinition { Name = "trades", Fields = [new FieldDef("symbol", FieldType.String)] };
        var doc = new ConfigDocument
        {
            Tables = [new ConfigTable { Name = "t", Sql = "SELECT symbol FROM trades" }],
        };

        var action = Assert.Single(ImportPlanner.Plan(doc, [source], [], [], "merge"), a => a.Kind == "table");

        Assert.DoesNotContain(action.Diagnostics, d => d.Contains("does not exist"));
    }

    [Fact]
    public void NoMissingDependencyDiagnosticWhenTheReferenceResolvesAgainstAnotherDocumentEntity()
    {
        var doc = new ConfigDocument
        {
            Tables =
            [
                new ConfigTable { Name = "upstream", Sql = "SELECT x FROM leaf" },
                new ConfigTable { Name = "downstream", Sql = "SELECT x FROM upstream" },
            ],
        };

        var downstreamAction = ImportPlanner.Plan(doc, [], [], [], "merge").Single(a => a.Name == "downstream");

        Assert.DoesNotContain(downstreamAction.Diagnostics, d => d.Contains("does not exist"));
    }

    [Fact]
    public void MissingDependencyCheckIsOrdinalNotCaseInsensitive()
    {
        // Plan 016's resolution rule is ordinal everywhere (RegistryGrain's SQL namespace, EntityRef
        // resolution) — a name that only matches case-insensitively would still fail to compile for
        // real, so reporting it as "exists" here would be a false negative on the one diagnostic this
        // exists to give.
        var source = new SourceDefinition { Name = "Trades", Fields = [new FieldDef("symbol", FieldType.String)] };
        var doc = new ConfigDocument
        {
            Tables = [new ConfigTable { Name = "t", Sql = "SELECT symbol FROM trades" }], // lowercase; catalog has "Trades"
        };

        var action = Assert.Single(ImportPlanner.Plan(doc, [source], [], [], "merge"), a => a.Kind == "table");

        Assert.Contains(action.Diagnostics, d => d.Contains("'trades'") && d.Contains("does not exist"));
    }
}
