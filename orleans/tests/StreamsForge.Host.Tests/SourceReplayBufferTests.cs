using StreamsForge.AppCore.Connectors;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary><see cref="SourceReplayBuffer"/> — the bounded ring a connector source keeps of what it has
/// already published, so a table/pipeline created AFTER the source started polling still gets those rows.
/// Pure and framework-free, so these are plain unit tests; the end-to-end behaviour it enables is pinned by
/// <see cref="SourceLateConsumerClusterTests"/>. What matters here is the honesty of the two numbers a late
/// consumer is handed: the rows it CAN have, and the total the source has published — because their
/// difference is exactly "rows you will never see", which the drivers turn into a warning rather than
/// silence.</summary>
public class SourceReplayBufferTests
{
    private static Dictionary<string, object?> Row(int id) => new() { ["id"] = id };

    [Fact]
    public void An_empty_buffer_snapshots_as_no_rows_and_nothing_seen()
    {
        var (rows, totalSeen) = new SourceReplayBuffer().Snapshot();

        Assert.Empty(rows);
        Assert.Equal(0, totalSeen);
    }

    [Fact]
    public void Appended_rows_come_back_in_order_with_TotalSeen_matching()
    {
        var buffer = new SourceReplayBuffer();
        for (var i = 0; i < 3; i++) buffer.Append(Row(i));

        var (rows, totalSeen) = buffer.Snapshot();

        Assert.Equal(3, rows.Count);
        Assert.Equal(3, totalSeen);
        Assert.Equal([0, 1, 2], rows.Select(r => (int)r["id"]!).ToArray());
    }

    [Fact]
    public void The_ring_holds_exactly_Capacity_rows_and_evicts_the_oldest_past_it()
    {
        var buffer = new SourceReplayBuffer();
        for (var i = 0; i < SourceReplayBuffer.Capacity; i++) buffer.Append(Row(i));

        Assert.Equal(SourceReplayBuffer.Capacity, buffer.Count);
        Assert.Equal(0, (int)buffer.Snapshot().Rows[0]["id"]!);

        // One past capacity: the count stays pinned and the OLDEST row is the one that goes.
        buffer.Append(Row(SourceReplayBuffer.Capacity));

        var (rows, _) = buffer.Snapshot();
        Assert.Equal(SourceReplayBuffer.Capacity, rows.Count);
        Assert.Equal(1, (int)rows[0]["id"]!);
        Assert.Equal(SourceReplayBuffer.Capacity, (int)rows[^1]["id"]!);
    }

    [Fact]
    public void TotalSeen_keeps_counting_past_capacity_so_the_gap_is_reportable()
    {
        var buffer = new SourceReplayBuffer();
        const int appended = SourceReplayBuffer.Capacity + 250;
        for (var i = 0; i < appended; i++) buffer.Append(Row(i));

        var (rows, totalSeen) = buffer.Snapshot();

        Assert.Equal(appended, totalSeen);
        Assert.Equal(SourceReplayBuffer.Capacity, rows.Count);
        // This subtraction IS the warning a late consumer logs — it must be truthful, not clamped.
        Assert.Equal(250, totalSeen - rows.Count);
    }

    [Fact]
    public void A_snapshot_is_a_copy_all_the_way_down()
    {
        var buffer = new SourceReplayBuffer();
        buffer.Append(Row(1));

        var (first, _) = buffer.Snapshot();
        first[0]["id"] = 999;          // mutate the row a consumer was handed
        first.Add(Row(2));             // and the list itself

        var (second, totalSeen) = buffer.Snapshot();

        Assert.Single(second);
        Assert.Equal(1, (int)second[0]["id"]!);
        Assert.Equal(1, totalSeen);
    }

    [Fact]
    public void The_row_a_caller_keeps_mutating_after_Append_is_the_one_it_handed_over()
    {
        // Documents the ownership rule rather than asserting a defensive copy that does not exist: Append
        // stores the reference it is given (the driver hands over a freshly-built dictionary and never
        // touches it again), and Snapshot is where the copy happens. Anyone who changes that contract
        // should have to change this test on purpose.
        var buffer = new SourceReplayBuffer();
        var row = Row(1);
        buffer.Append(row);
        row["id"] = 42;

        Assert.Equal(42, (int)buffer.Snapshot().Rows[0]["id"]!);
    }
}
