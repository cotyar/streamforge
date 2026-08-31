using StreamsForge.Abstractions;
using StreamsForge.Host.Search;
using Xunit;

namespace StreamsForge.Engine.Tests;

public class TableSearchIndexTests
{
    private static Dictionary<string, object?> Row(params (string Field, object? Value)[] fields) =>
        fields.ToDictionary(f => f.Field, f => f.Value);

    private static TableSearchIndex ExactIndexWithDemoRows()
    {
        var idx = new TableSearchIndex(TableSearchMode.Exact);
        idx.Add("row1", Row(("symbol", "AAPL"), ("note", "Apple Inc trending up"), ("tag", "tech")));
        idx.Add("row2", Row(("symbol", "GOOG"), ("note", "Google search giant"), ("tag", "tech")));
        idx.Add("row3", Row(("symbol", "MSFT"), ("note", "Microsoft software"), ("tag", "tech")));
        return idx;
    }

    // ------------------------------------------------------------------
    // Exact mode
    // ------------------------------------------------------------------

    [Fact]
    public void ExactModeReportsItsOwnConfiguredMode()
    {
        var idx = new TableSearchIndex(TableSearchMode.Exact);
        Assert.Equal(TableSearchMode.Exact, idx.Mode);
    }

    [Fact]
    public void ExactTokenMatchFindsRow()
    {
        var idx = ExactIndexWithDemoRows();
        var hits = idx.Search("AAPL", 10);
        Assert.Single(hits);
        Assert.Equal("row1", hits[0].RowKey);
    }

    [Fact]
    public void ExactTokenMatchIsCaseInsensitive()
    {
        var idx = ExactIndexWithDemoRows();
        var hits = idx.Search("aapl", 10);
        Assert.Single(hits);
        Assert.Equal("row1", hits[0].RowKey);
    }

    [Fact]
    public void ExactPrefixMatchFindsRow()
    {
        var idx = ExactIndexWithDemoRows();
        // "goo" is a strict prefix of both the "GOOG" symbol token and the "Google" note token — both live
        // on row2 only.
        var hits = idx.Search("goo", 10);
        Assert.Single(hits);
        Assert.Equal("row2", hits[0].RowKey);
    }

    [Fact]
    public void ExactSubstringWithinTokenFallsBackAndFindsRow()
    {
        var idx = ExactIndexWithDemoRows();
        // "rend" is a substring of "trending" (row1's note) but is not a prefix of any indexed token, so
        // the token/prefix path finds nothing and the substring fallback must catch it.
        var hits = idx.Search("rend", 10);
        Assert.Single(hits);
        Assert.Equal("row1", hits[0].RowKey);
    }

    [Fact]
    public void ExactMultiWordRequiresAllWordsPresent()
    {
        var idx = ExactIndexWithDemoRows();
        // Both "apple" and "trending" are tokens on row1 only.
        var hits = idx.Search("apple trending", 10);
        Assert.Single(hits);
        Assert.Equal("row1", hits[0].RowKey);
    }

    [Fact]
    public void ExactMultiWordAcrossDifferentRowsIsMiss()
    {
        var idx = ExactIndexWithDemoRows();
        // "apple" only lives on row1, "google" only on row2 — no row has both, and the literal phrase
        // "apple google" isn't a substring of any row's text either.
        var hits = idx.Search("apple google", 10);
        Assert.Empty(hits);
    }

    [Fact]
    public void ExactModeMissReturnsEmpty()
    {
        var idx = ExactIndexWithDemoRows();
        var hits = idx.Search("nonexistentterm", 10);
        Assert.Empty(hits);
    }

    [Fact]
    public void ExactModeEmptyQueryReturnsEmpty()
    {
        var idx = ExactIndexWithDemoRows();
        Assert.Empty(idx.Search("", 10));
        Assert.Empty(idx.Search("   ", 10));
    }

    [Fact]
    public void ExactModeRespectsLimit()
    {
        var idx = new TableSearchIndex(TableSearchMode.Exact);
        for (int i = 0; i < 5; i++)
        {
            idx.Add($"row{i}", Row(("tag", "tech")));
        }
        var hits = idx.Search("tech", 2);
        Assert.Equal(2, hits.Count);
    }

    [Fact]
    public void NumbersAndBoolsAreStringifiedInvariantAndSearchable()
    {
        var idx = new TableSearchIndex(TableSearchMode.Exact);
        idx.Add("row1", Row(("price", 1234.5), ("qty", 42L), ("active", true)));

        Assert.Single(idx.Search("1234.5", 10));
        Assert.Single(idx.Search("42", 10));
        Assert.Single(idx.Search("true", 10));
    }

    [Fact]
    public void JsonObjectValuesAreSerializedCompactlyAndSearchable()
    {
        var idx = new TableSearchIndex(TableSearchMode.Exact);
        idx.Add("row1", Row(("payload", new Dictionary<string, object?> { ["tier"] = "gold" })));

        var hits = idx.Search("gold", 10);
        Assert.Single(hits);
    }

    // ------------------------------------------------------------------
    // Incremental Add/Remove/Rebuild
    // ------------------------------------------------------------------

    [Fact]
    public void RemoveMakesRowUnsearchable()
    {
        var idx = ExactIndexWithDemoRows();
        idx.Remove("row1");

        Assert.Empty(idx.Search("AAPL", 10));
        Assert.Equal(2, idx.RowCount);
    }

    [Fact]
    public void RemoveOfUnknownKeyIsNoOp()
    {
        var idx = ExactIndexWithDemoRows();
        idx.Remove("does-not-exist");
        Assert.Equal(3, idx.RowCount);
    }

    [Fact]
    public void AddIsUpsertReplacingPriorContentForSameKey()
    {
        var idx = new TableSearchIndex(TableSearchMode.Exact);
        idx.Add("row1", Row(("symbol", "AAPL")));
        Assert.Single(idx.Search("AAPL", 10));

        idx.Add("row1", Row(("symbol", "MSFT")));
        Assert.Empty(idx.Search("AAPL", 10)); // old content gone
        Assert.Single(idx.Search("MSFT", 10)); // new content indexed
        Assert.Equal(1, idx.RowCount); // still one row, not two
    }

    [Fact]
    public void ClearRemovesEverything()
    {
        var idx = ExactIndexWithDemoRows();
        idx.Clear();
        Assert.Equal(0, idx.RowCount);
        Assert.Empty(idx.Search("AAPL", 10));
    }

    [Fact]
    public void RebuildReplacesEntireContentsFromSnapshot()
    {
        var idx = ExactIndexWithDemoRows();

        var snapshot = new Dictionary<string, IReadOnlyDictionary<string, object?>>
        {
            ["r1"] = Row(("symbol", "TSLA")),
        };
        idx.Rebuild(snapshot);

        Assert.Equal(1, idx.RowCount);
        Assert.Empty(idx.Search("AAPL", 10)); // old demo rows gone
        Assert.Single(idx.Search("TSLA", 10));
    }

    // ------------------------------------------------------------------
    // Fuzzy mode
    // ------------------------------------------------------------------

    [Fact]
    public void FuzzyModeReportsItsOwnConfiguredMode()
    {
        var idx = new TableSearchIndex(TableSearchMode.Fuzzy);
        Assert.Equal(TableSearchMode.Fuzzy, idx.Mode);
    }

    [Fact]
    public void FuzzyModeToleratesATypoAndFindsTheRow()
    {
        var idx = new TableSearchIndex(TableSearchMode.Fuzzy);
        idx.Add("row1", Row(("company", "Google")));
        idx.Add("row2", Row(("company", "Microsoft")));

        var hits = idx.Search("gogle", 10); // typo for "google"
        Assert.Contains(hits, h => h.RowKey == "row1");
        Assert.DoesNotContain(hits, h => h.RowKey == "row2");
    }

    [Fact]
    public void FuzzyModeToleratesATrailingTypoOnASymbol()
    {
        var idx = new TableSearchIndex(TableSearchMode.Fuzzy);
        idx.Add("row1", Row(("symbol", "AAPL")));
        idx.Add("row2", Row(("symbol", "MSFT")));

        var hits = idx.Search("AAPLl", 10); // typo'd extra trailing letter
        Assert.Contains(hits, h => h.RowKey == "row1");
        Assert.DoesNotContain(hits, h => h.RowKey == "row2");
    }

    [Fact]
    public void FuzzyModeRanksTheCloserMatchFirst()
    {
        var idx = new TableSearchIndex(TableSearchMode.Fuzzy);
        idx.Add("close", Row(("name", "google")));   // one edit away from "gogle"
        idx.Add("far", Row(("name", "gorgeous")));    // shares a prefix but much further away

        var hits = idx.Search("gogle", 10);
        Assert.NotEmpty(hits);
        Assert.Equal("close", hits[0].RowKey);
        // Every returned hit must be at least as good as anything ranked after it.
        for (int i = 1; i < hits.Count; i++)
        {
            Assert.True(hits[i - 1].Score >= hits[i].Score);
        }
    }

    [Fact]
    public void FuzzyModeCompletelyUnrelatedQueryReturnsEmpty()
    {
        var idx = new TableSearchIndex(TableSearchMode.Fuzzy);
        idx.Add("row1", Row(("symbol", "AAPL")));

        Assert.Empty(idx.Search("zzzzxqqqq", 10));
    }

    [Fact]
    public void FuzzyModeExactMatchScoresAtOrNearOne()
    {
        var idx = new TableSearchIndex(TableSearchMode.Fuzzy);
        idx.Add("row1", Row(("symbol", "AAPL")));

        var hits = idx.Search("aapl", 10);
        var hit = Assert.Single(hits);
        Assert.Equal(1.0, hit.Score, precision: 6);
    }

    [Fact]
    public void FuzzyModeIncrementalRemoveDropsRowFromResults()
    {
        var idx = new TableSearchIndex(TableSearchMode.Fuzzy);
        idx.Add("row1", Row(("company", "Google")));
        Assert.NotEmpty(idx.Search("gogle", 10));

        idx.Remove("row1");
        Assert.Empty(idx.Search("gogle", 10));
    }
}
