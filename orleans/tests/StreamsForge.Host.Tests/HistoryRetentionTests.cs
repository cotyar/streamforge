using StreamsForge.Abstractions;
using StreamsForge.Host.Grains;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>Pure, Orleans-free tests for TableRowHistoryRetention — the retention math TableHistoryGrain
/// applies per HistoryMode. No grain, no cluster: constructs RowHistoryEntry directly and calls
/// TableRowHistoryRetention.Append/PruneWindow.</summary>
public sealed class HistoryRetentionTests
{
    private static HistoryVersion V(long value, long tsMs, long seq, string field = "v") =>
        new(new Dictionary<string, object?> { [field] = value, ["seq_marker"] = seq }, tsMs, seq);

    [Fact]
    public void All_KeepsEveryVersion_UpToSafetyCap()
    {
        var entry = new RowHistoryEntry();
        for (var i = 0; i < 5; i++)
        {
            TableRowHistoryRetention.Append(entry, V(i, i, i), TableHistoryMode.All, limit: 0, byField: null, windowMs: 0);
        }
        Assert.Equal(5, entry.Versions.Count);
        Assert.Equal([0L, 1L, 2L, 3L, 4L], entry.Versions.Select(v => v.Seq));
    }

    [Fact]
    public void All_ExceedingSafetyCap_DropsOldestFirst()
    {
        var entry = new RowHistoryEntry();
        var total = TableRowHistoryRetention.AllModeCap + 5;
        for (var i = 0; i < total; i++)
        {
            TableRowHistoryRetention.Append(entry, V(i, i, i), TableHistoryMode.All, limit: 0, byField: null, windowMs: 0);
        }
        Assert.Equal(TableRowHistoryRetention.AllModeCap, entry.Versions.Count);
        // Oldest 5 (seq 0..4) were dropped; the window now starts at seq 5.
        Assert.Equal(5, entry.Versions[0].Seq);
        Assert.Equal(total - 1, entry.Versions[^1].Seq);
    }

    [Fact]
    public void LastN_KeepsOnlyMostRecentN_RingBuffer()
    {
        var entry = new RowHistoryEntry();
        for (var i = 0; i < 5; i++)
        {
            TableRowHistoryRetention.Append(entry, V(i, i, i), TableHistoryMode.LastN, limit: 3, byField: null, windowMs: 0);
        }
        Assert.Equal(3, entry.Versions.Count);
        Assert.Equal([2L, 3L, 4L], entry.Versions.Select(v => v.Seq));
    }

    [Fact]
    public void FirstN_KeepsOnlyEarliestN_StopsAppending()
    {
        var entry = new RowHistoryEntry();
        for (var i = 0; i < 5; i++)
        {
            TableRowHistoryRetention.Append(entry, V(i, i, i), TableHistoryMode.FirstN, limit: 3, byField: null, windowMs: 0);
        }
        Assert.Equal(3, entry.Versions.Count);
        Assert.Equal([0L, 1L, 2L], entry.Versions.Select(v => v.Seq));
    }

    [Fact]
    public void MinBy_KeepsMinimumExtremePlusLatest_TwoEntriesMax()
    {
        var entry = new RowHistoryEntry();
        long[] values = [10, 5, 8, 3, 7];
        for (var i = 0; i < values.Length; i++)
        {
            TableRowHistoryRetention.Append(entry, V(values[i], i, i, "byField"), TableHistoryMode.MinBy, limit: 0, byField: "byField", windowMs: 0);
        }

        Assert.Equal(2, entry.Versions.Count);
        Assert.Equal(3L, entry.Versions[0].Row["byField"]); // extreme (min = 3, seq 3)
        Assert.Equal(3L, entry.Versions[0].Seq);
        Assert.Equal(7L, entry.Versions[1].Row["byField"]); // latest (seq 4, value 7)
        Assert.Equal(4L, entry.Versions[1].Seq);
    }

    [Fact]
    public void MaxBy_KeepsMaximumExtremePlusLatest_TwoEntriesMax()
    {
        var entry = new RowHistoryEntry();
        long[] values = [10, 5, 8, 3, 7];
        for (var i = 0; i < values.Length; i++)
        {
            TableRowHistoryRetention.Append(entry, V(values[i], i, i, "byField"), TableHistoryMode.MaxBy, limit: 0, byField: "byField", windowMs: 0);
        }

        Assert.Equal(2, entry.Versions.Count);
        Assert.Equal(10L, entry.Versions[0].Row["byField"]); // extreme (max = 10, seq 0)
        Assert.Equal(0L, entry.Versions[0].Seq);
        Assert.Equal(7L, entry.Versions[1].Row["byField"]); // latest (seq 4, value 7)
        Assert.Equal(4L, entry.Versions[1].Seq);
    }

    [Fact]
    public void MinBy_FirstVersionEver_CollapsesToSingleEntry()
    {
        var entry = new RowHistoryEntry();
        TableRowHistoryRetention.Append(entry, V(42, 0, 0, "byField"), TableHistoryMode.MinBy, limit: 0, byField: "byField", windowMs: 0);
        var single = Assert.Single(entry.Versions);
        Assert.Equal(42L, single.Row["byField"]);
    }

    [Fact]
    public void MinBy_WhenNewVersionIsTheNewExtreme_CollapsesBackToSingleEntry()
    {
        var entry = new RowHistoryEntry();
        TableRowHistoryRetention.Append(entry, V(10, 0, 0, "byField"), TableHistoryMode.MinBy, limit: 0, byField: "byField", windowMs: 0);
        TableRowHistoryRetention.Append(entry, V(20, 1, 1, "byField"), TableHistoryMode.MinBy, limit: 0, byField: "byField", windowMs: 0);
        Assert.Equal(2, entry.Versions.Count); // extreme=10, latest=20

        // A new, more extreme (smaller) version IS both the extreme and the latest -> collapses to 1.
        TableRowHistoryRetention.Append(entry, V(1, 2, 2, "byField"), TableHistoryMode.MinBy, limit: 0, byField: "byField", windowMs: 0);
        var single = Assert.Single(entry.Versions);
        Assert.Equal(1L, single.Row["byField"]);
    }

    [Fact]
    public void MinBy_MissingByFieldValue_NeverDisplacesExtreme_ButStillBecomesLatest()
    {
        var entry = new RowHistoryEntry();
        TableRowHistoryRetention.Append(entry, V(5, 0, 0, "byField"), TableHistoryMode.MinBy, limit: 0, byField: "byField", windowMs: 0);

        // Second version doesn't even carry "byField" -> NaN -> comparisons are always false.
        var missing = new HistoryVersion(new Dictionary<string, object?> { ["other"] = "x" }, 1, 1);
        TableRowHistoryRetention.Append(entry, missing, TableHistoryMode.MinBy, limit: 0, byField: "byField", windowMs: 0);

        Assert.Equal(2, entry.Versions.Count);
        Assert.Equal(5L, entry.Versions[0].Row["byField"]); // extreme unchanged
        Assert.Equal(1L, entry.Versions[1].Seq); // latest is still the new (missing-field) version
    }

    [Fact]
    public void PruneWindow_DropsVersionsOlderThanWindow()
    {
        var entry = new RowHistoryEntry();
        entry.Versions.Add(V(1, tsMs: 1000, seq: 0));
        entry.Versions.Add(V(2, tsMs: 5000, seq: 1));
        entry.Versions.Add(V(3, tsMs: 9000, seq: 2));

        TableRowHistoryRetention.PruneWindow(entry, nowMs: 10_000, windowMs: 3000); // cutoff = 7000

        Assert.Single(entry.Versions);
        Assert.Equal(2L, entry.Versions[0].Seq);
    }

    [Fact]
    public void PruneWindow_ZeroOrNegativeWindow_IsUnbounded_NoOp()
    {
        var entry = new RowHistoryEntry();
        entry.Versions.Add(V(1, tsMs: 0, seq: 0));
        TableRowHistoryRetention.PruneWindow(entry, nowMs: 1_000_000, windowMs: 0);
        Assert.Single(entry.Versions);
    }

    [Fact]
    public void Append_WithWindow_PrunesStaleVersionsBeforeApplyingModeRetention()
    {
        var entry = new RowHistoryEntry();
        TableRowHistoryRetention.Append(entry, V(1, tsMs: 0, seq: 0), TableHistoryMode.All, limit: 0, byField: null, windowMs: 2000);
        TableRowHistoryRetention.Append(entry, V(2, tsMs: 1500, seq: 1), TableHistoryMode.All, limit: 0, byField: null, windowMs: 2000);
        // This append's tsMs=2500 means the window (cutoff=500) now excludes the first version (ts=0) but
        // keeps the second (ts=1500 >= 500).
        TableRowHistoryRetention.Append(entry, V(3, tsMs: 2500, seq: 2), TableHistoryMode.All, limit: 0, byField: null, windowMs: 2000);

        Assert.Equal([1L, 2L], entry.Versions.Select(v => v.Seq));
    }
}
