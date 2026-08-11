using System.Diagnostics;
using StreamForge.Abstractions;
using StreamForge.AppCore.Ingest;
using Xunit;

namespace StreamForge.Engine.Tests;

/// <summary>Plan 008 W4: SourceIngressBuffer — explicit row-count accounting under a lock, plus the
/// drain pump. Concurrent push/drain, Block waking on space, Block timing out (deterministically, no
/// real sleep), DropOldest eviction order, and counters reconciling with what drain returns.</summary>
public class SourceIngressBufferTests
{
    private static Dictionary<string, object?> Row(string id) => new() { ["id"] = id };

    private static SourceIngressBuffer MakeBuffer(
        IngressOverflowPolicy policy, int capacity, int maxBatch,
        Func<IReadOnlyList<Dictionary<string, object?>>, CancellationToken, Task> drain,
        int maxWaitMs = 5000, Func<long>? clock = null, Func<TimeSpan, CancellationToken, Task>? delay = null)
    {
        var config = new IngestConfig { Policy = policy, CapacityRows = capacity, MaxBatchRows = maxBatch, MaxWaitMs = maxWaitMs };
        return new SourceIngressBuffer("test-source", config, "fp", drain, clock, delay);
    }

    [Fact]
    public async Task Push_under_capacity_is_accepted_and_counted()
    {
        var buffer = MakeBuffer(IngressOverflowPolicy.Reject, capacity: 10, maxBatch: 10, drain: (_, _) => Task.CompletedTask);

        var result = await buffer.PushAsync([Row("a"), Row("b")]);

        Assert.Equal(IngestOutcome.Accepted, result.Outcome);
        Assert.Equal(2, result.Accepted);
        Assert.Equal(0, result.Dropped);
        Assert.Equal(2, buffer.DepthRows);
        Assert.Equal(2, buffer.GetStatus().TotalAccepted);
    }

    [Fact]
    public async Task Push_over_capacity_under_Reject_is_overloaded_and_leaves_the_buffer_unchanged()
    {
        var buffer = MakeBuffer(IngressOverflowPolicy.Reject, capacity: 5, maxBatch: 100, drain: (_, _) => Task.CompletedTask);
        await buffer.PushAsync([Row("a"), Row("b"), Row("c"), Row("d"), Row("e")]);

        var result = await buffer.PushAsync([Row("f"), Row("g"), Row("h")]);

        Assert.Equal(IngestOutcome.Overloaded, result.Outcome);
        Assert.Equal(0, result.Accepted);
        Assert.True(result.RetryAfterMs > 0);
        Assert.Equal(5, buffer.DepthRows);
        Assert.Equal(3, buffer.GetStatus().TotalRejected);
    }

    [Fact]
    public async Task TooLarge_batch_is_rejected_whole_and_never_touches_the_queue()
    {
        var buffer = MakeBuffer(IngressOverflowPolicy.Reject, capacity: 10, maxBatch: 5, drain: (_, _) => Task.CompletedTask);

        var result = await buffer.PushAsync([Row("a"), Row("b"), Row("c"), Row("d"), Row("e"), Row("f")]);

        Assert.Equal(IngestOutcome.TooLarge, result.Outcome);
        Assert.Equal(0, result.Accepted);
        Assert.Equal(0, buffer.DepthRows);
        Assert.Equal(6, buffer.GetStatus().TotalRejected);
    }

    [Fact]
    public async Task DropNewest_admits_what_fits_and_reports_the_drop()
    {
        var buffer = MakeBuffer(IngressOverflowPolicy.DropNewest, capacity: 5, maxBatch: 100, drain: (_, _) => Task.CompletedTask);
        await buffer.PushAsync([Row("a"), Row("b"), Row("c")]);

        var result = await buffer.PushAsync([Row("d"), Row("e"), Row("f"), Row("g")]); // only 2 free

        Assert.Equal(IngestOutcome.Accepted, result.Outcome);
        Assert.Equal(2, result.Accepted);
        Assert.Equal(2, result.Dropped);
        Assert.Equal(5, buffer.DepthRows);
        Assert.Equal(2, buffer.GetStatus().TotalDropped);
    }

    [Fact]
    public async Task DropOldest_evicts_from_the_head_in_fifo_order()
    {
        var drained = new List<Dictionary<string, object?>>();
        var buffer = MakeBuffer(IngressOverflowPolicy.DropOldest, capacity: 3, maxBatch: 100, drain: (batch, _) =>
        {
            drained.AddRange(batch);
            return Task.CompletedTask;
        });

        await buffer.PushAsync([Row("1"), Row("2"), Row("3")]); // fills the buffer
        var result = await buffer.PushAsync([Row("4")]); // evicts "1"

        Assert.Equal(IngestOutcome.Accepted, result.Outcome);
        Assert.Equal(1, result.Accepted);
        Assert.Equal(1, result.Dropped);
        Assert.Equal(3, buffer.DepthRows);
        Assert.Equal(1, buffer.GetStatus().TotalDropped);

        await buffer.DrainAsync();

        Assert.Equal(["2", "3", "4"], drained.Select(r => (string)r["id"]!));
    }

    [Fact]
    public async Task Inline_publishes_directly_without_ever_buffering()
    {
        var drained = new List<Dictionary<string, object?>>();
        var buffer = MakeBuffer(IngressOverflowPolicy.Inline, capacity: 999, maxBatch: 100, drain: (batch, _) =>
        {
            drained.AddRange(batch);
            return Task.CompletedTask;
        });

        var result = await buffer.PushAsync([Row("a"), Row("b")]);

        Assert.Equal(IngestOutcome.Accepted, result.Outcome);
        Assert.Equal(2, result.Accepted);
        Assert.Equal(0, buffer.DepthRows); // never queued
        Assert.Equal(2, drained.Count); // already published, no separate DrainAsync needed
        var status = buffer.GetStatus();
        Assert.Equal(2, status.TotalAccepted);
        Assert.Equal(2, status.TotalPublished);
    }

    [Fact]
    public async Task DrainAsync_counters_reconcile_with_what_it_returns()
    {
        var drained = new List<Dictionary<string, object?>>();
        var buffer = MakeBuffer(IngressOverflowPolicy.Reject, capacity: 100, maxBatch: 100, drain: (batch, _) =>
        {
            drained.AddRange(batch);
            return Task.CompletedTask;
        });

        await buffer.PushAsync([Row("a"), Row("b"), Row("c"), Row("d"), Row("e"), Row("f"), Row("g")]);

        var drainedCount = await buffer.DrainAsync();

        Assert.Equal(7, drainedCount);
        Assert.Equal(7, drained.Count);
        Assert.Equal(0, buffer.DepthRows);
        var status = buffer.GetStatus();
        Assert.Equal(7, status.TotalAccepted);
        Assert.Equal(7, status.TotalPublished);
    }

    [Fact]
    public async Task DrainAsync_on_an_empty_buffer_is_a_noop()
    {
        var buffer = MakeBuffer(IngressOverflowPolicy.Reject, capacity: 10, maxBatch: 10, drain: (_, _) => Task.CompletedTask);

        var drainedCount = await buffer.DrainAsync();

        Assert.Equal(0, drainedCount);
    }

    [Fact]
    public async Task DrainAsync_respects_maxRows_leaving_the_remainder_queued()
    {
        var drained = new List<Dictionary<string, object?>>();
        var buffer = MakeBuffer(IngressOverflowPolicy.Reject, capacity: 10, maxBatch: 10, drain: (batch, _) =>
        {
            drained.AddRange(batch);
            return Task.CompletedTask;
        });
        await buffer.PushAsync([Row("a"), Row("b"), Row("c")]);

        var drainedCount = await buffer.DrainAsync(2);

        Assert.Equal(2, drainedCount);
        Assert.Equal(1, buffer.DepthRows);
    }

    [Fact]
    public async Task Block_wakes_when_space_frees_up()
    {
        var buffer = MakeBuffer(IngressOverflowPolicy.Block, capacity: 2, maxBatch: 10, drain: (_, _) => Task.CompletedTask, maxWaitMs: 5000);
        await buffer.PushAsync([Row("a"), Row("b")]); // fills the buffer
        Assert.Equal(2, buffer.DepthRows);

        var blockedPush = buffer.PushAsync([Row("c")]);
        await Task.Delay(30); // let it enter the Wait state
        Assert.False(blockedPush.IsCompleted);

        var drainedCount = await buffer.DrainAsync(1); // frees exactly one slot
        Assert.Equal(1, drainedCount);

        var completed = await Task.WhenAny(blockedPush, Task.Delay(2000));
        Assert.Same(blockedPush, completed);
        var result = await blockedPush;
        Assert.Equal(IngestOutcome.Accepted, result.Outcome);
        Assert.Equal(1, result.Accepted);
    }

    [Fact]
    public async Task Block_times_out_without_sleeping_for_real()
    {
        long simulatedNow = 0;
        long Clock() => simulatedNow;
        Task Delay(TimeSpan ts, CancellationToken ct)
        {
            simulatedNow += (long)Math.Ceiling(ts.TotalMilliseconds);
            return Task.CompletedTask;
        }

        var buffer = MakeBuffer(
            IngressOverflowPolicy.Block, capacity: 1, maxBatch: 10, drain: (_, _) => Task.CompletedTask,
            maxWaitMs: 100, clock: Clock, delay: Delay);

        await buffer.PushAsync([Row("a")]); // fills the only slot; nothing ever drains it

        var sw = Stopwatch.StartNew();
        var result = await buffer.PushAsync([Row("b")]);
        sw.Stop();

        Assert.Equal(IngestOutcome.Overloaded, result.Outcome);
        Assert.True(sw.ElapsedMilliseconds < 1000, $"should not have actually slept for the timeout (took {sw.ElapsedMilliseconds}ms)");
        Assert.True(simulatedNow >= 100); // the simulated clock DID advance past the deadline
    }

    [Fact]
    public void RecordInvalid_and_RecordDownstreamDropped_update_status_counters()
    {
        var buffer = MakeBuffer(IngressOverflowPolicy.Reject, capacity: 10, maxBatch: 10, drain: (_, _) => Task.CompletedTask);

        buffer.RecordInvalid(3);
        buffer.RecordDownstreamDropped(2);

        var status = buffer.GetStatus();
        Assert.Equal(3, status.TotalInvalid);
        Assert.Equal(2, status.DownstreamDropped);
    }

    [Fact]
    public async Task Concurrent_pushes_reconcile_with_a_final_drain()
    {
        var drained = new List<Dictionary<string, object?>>();
        var buffer = MakeBuffer(IngressOverflowPolicy.Reject, capacity: 100_000, maxBatch: 1000, drain: (batch, _) =>
        {
            lock (drained) drained.AddRange(batch);
            return Task.CompletedTask;
        });

        var pushTasks = Enumerable.Range(0, 50)
            .Select(i => buffer.PushAsync(Enumerable.Range(0, 10).Select(j => Row($"{i}-{j}")).ToList()))
            .ToArray();

        var results = await Task.WhenAll(pushTasks);
        Assert.All(results, r => Assert.Equal(IngestOutcome.Accepted, r.Outcome));

        var afterPush = buffer.GetStatus();
        Assert.Equal(500, afterPush.TotalAccepted);
        Assert.Equal(500, afterPush.DepthRows);

        var drainedCount = await buffer.DrainAsync();

        Assert.Equal(500, drainedCount);
        Assert.Equal(500, drained.Count);
        Assert.Equal(500, drained.Select(r => (string)r["id"]!).Distinct().Count()); // no duplicate/lost rows

        var final = buffer.GetStatus();
        Assert.Equal(500, final.TotalPublished);
        Assert.Equal(0, final.DepthRows);
    }

    [Fact]
    public async Task Empty_batch_push_is_a_noop_accept()
    {
        var buffer = MakeBuffer(IngressOverflowPolicy.Reject, capacity: 10, maxBatch: 10, drain: (_, _) => Task.CompletedTask);

        var result = await buffer.PushAsync([]);

        Assert.Equal(IngestOutcome.Accepted, result.Outcome);
        Assert.Equal(0, result.Accepted);
        Assert.Equal(0, buffer.DepthRows);
    }
}
