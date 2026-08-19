using StreamForge.Abstractions;
using StreamForge.AppCore;
using Xunit;

namespace StreamForge.AppCore.Tests;

/// <summary>Plan 016 wave 1 — the resolution rule every later wave of that plan depends on. These tests
/// are the rule's only executable statement, so they pin the parts that are decisions rather than
/// implementation: id beats a competing name, a duplicate name is Ambiguous (not "first wins" and not
/// "not found" — the two things the four hand-rolled sites did instead), and nothing matches
/// case-insensitively, by prefix or fuzzily, because the SQL namespace is ordinal.</summary>
public class EntityRefTests
{
    private static TableDefinition Table(string id, string name) => new() { Id = id, Name = name };
    private static PipelineDefinition Pipeline(string id, string name) => new() { Id = id, Name = name };
    private static SourceDefinition Source(string name) => new() { Name = name };

    // ---- Id wins outright -------------------------------------------------

    [Fact]
    public void AnExactIdResolves()
    {
        var result = EntityRef.Resolve([Table("t1", "trades")], "t1");

        Assert.Equal(EntityRefOutcome.Found, result.Outcome);
        Assert.True(result.IsFound);
        Assert.Equal("trades", result.Value!.Name);
        Assert.Empty(result.Message);
    }

    [Fact]
    public void AnIdMatchBeatsADifferentEntityWhoseNameIsTheSameString()
    {
        // Pathological but legal: someone named a table after another table's id.
        var tables = new List<TableDefinition> { Table("t1", "trades"), Table("t2", "t1") };

        var result = EntityRef.Resolve(tables, "t1");

        Assert.Equal(EntityRefOutcome.Found, result.Outcome);
        Assert.Equal("t1", result.Value!.Id);
        Assert.Equal("trades", result.Value.Name);
    }

    [Fact]
    public void AnIdMatchIsNotAmbiguousEvenWhenTheNameSideCollides()
    {
        var tables = new List<TableDefinition> { Table("t1", "dup"), Table("t2", "dup"), Table("dup", "other") };

        var result = EntityRef.Resolve(tables, "dup");

        Assert.Equal(EntityRefOutcome.Found, result.Outcome);
        Assert.Equal("dup", result.Value!.Id);
    }

    // ---- Name: 1 / 0 / >=2 ------------------------------------------------

    [Fact]
    public void AUniqueNameResolves()
    {
        var result = EntityRef.Resolve([Table("t1", "trades"), Table("t2", "quotes")], "quotes");

        Assert.Equal(EntityRefOutcome.Found, result.Outcome);
        Assert.Equal("t2", result.Value!.Id);
    }

    [Fact]
    public void NothingMatchingIsNotFoundAndSaysSo()
    {
        var result = EntityRef.Resolve([Table("t1", "trades")], "nope");

        Assert.Equal(EntityRefOutcome.NotFound, result.Outcome);
        Assert.Null(result.Value);
        Assert.Empty(result.CandidateIds);
        Assert.Equal("table 'nope' not found", result.Message);
    }

    [Fact]
    public void TwoEntitiesSharingANameAreAmbiguousAndCarryBothIds()
    {
        var pipelines = new List<PipelineDefinition> { Pipeline("p1", "pnl"), Pipeline("p2", "pnl") };

        var result = EntityRef.Resolve(pipelines, "pnl");

        Assert.Equal(EntityRefOutcome.Ambiguous, result.Outcome);
        Assert.Null(result.Value);                       // never guess: not first-wins
        Assert.Equal(["p1", "p2"], result.CandidateIds); // catalog order, so the message is deterministic
        Assert.Equal("2 pipelines are named 'pnl' — address one by id: p1, p2", result.Message);
    }

    [Fact]
    public void AnAmbiguousQueryStaysResolvableByEitherCandidateId()
    {
        // The escape hatch the 409 message promises has to actually work.
        var pipelines = new List<PipelineDefinition> { Pipeline("p1", "pnl"), Pipeline("p2", "pnl") };

        Assert.Equal("p2", EntityRef.Resolve(pipelines, "p2").Value!.Id);
    }

    [Fact]
    public void AnEmptyCatalogIsNotFoundNotAmbiguous()
    {
        Assert.Equal(EntityRefOutcome.NotFound, EntityRef.Resolve(new List<TableDefinition>(), "trades").Outcome);
    }

    // ---- No case-insensitive, prefix or fuzzy matching --------------------

    [Theory]
    [InlineData("Trades")]  // case
    [InlineData("TRADES")]
    [InlineData("trade")]   // prefix
    [InlineData("trades ")] // trailing space
    [InlineData(" trades")]
    [InlineData("trades2")]
    public void MatchingIsOrdinalAndExactBecauseTheSqlNamespaceIs(string query)
    {
        // RegistryGrain builds the SQL namespace with ordinal dictionaries. A looser resolver here would
        // let GET /api/tables/Trades and FROM Trades disagree, which is unexplainable to a user.
        var result = EntityRef.Resolve([Table("t1", "trades")], query);

        Assert.Equal(EntityRefOutcome.NotFound, result.Outcome);
    }

    [Fact]
    public void AnEmptyQueryIsNotFound()
    {
        Assert.Equal(EntityRefOutcome.NotFound, EntityRef.Resolve([Table("t1", "trades")], "").Outcome);
    }

    // ---- Sources are name-only -------------------------------------------

    [Fact]
    public void ASourceResolvesByNameOnly()
    {
        var sources = new List<SourceDefinition> { Source("trades"), Source("quotes") };

        var found = EntityRef.Resolve(sources, "quotes");
        Assert.Equal(EntityRefOutcome.Found, found.Outcome);
        Assert.Equal("quotes", found.Value!.Name);

        // There is no id to fall back on — a GUID-shaped query is simply not a source.
        var missing = EntityRef.Resolve(sources, Guid.NewGuid().ToString("n"));
        Assert.Equal(EntityRefOutcome.NotFound, missing.Outcome);
        Assert.Equal(EntityRef.SourceKind, missing.Kind);
    }

    // ---- The result shape both transports read ----------------------------

    [Fact]
    public void TheResultCarriesKindAndQuerySoNeitherTransportRebuildsTheMessage()
    {
        var result = EntityRef.Resolve(new List<TableDefinition> { Table("t1", "dup"), Table("t2", "dup") }, "dup");

        Assert.Equal(EntityRef.TableKind, result.Kind);
        Assert.Equal("dup", result.Query);
        // 404 vs 409 (and NotFound vs FailedPrecondition on gRPC) is a switch on this one value.
        Assert.Equal(EntityRefOutcome.Ambiguous, result.Outcome);
    }
}
