using System.Diagnostics;
using Microsoft.Extensions.Logging.Abstractions;
using StreamForge.Client.Tests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace StreamForge.Client.Tests;

/// <summary>
/// Unit tests for <see cref="LiveTable"/>'s <c>Changed</c> coalescing window and its
/// <see cref="LiveTable.WatchAsync"/> view -- no engine, no network, driven entirely by
/// <see cref="FakeTransport"/> so timing assertions are not at the mercy of a real connection.
/// </summary>
public sealed class LiveTableTests
{
    private readonly ITestOutputHelper _output;

    public LiveTableTests(ITestOutputHelper output) => _output = output;

    private static Dictionary<string, object?> Row(string id, long n) => new() { ["id"] = id, ["n"] = n };

    private static async Task<LiveTable> StartAsync(FakeTransport transport, TimeSpan? flush)
    {
        transport.SetSnapshot(Array.Empty<RowDelta>());
        var table = new LiveTable(transport, "t", ["id"], NullLogger.Instance, flush);
        await table.StartAsync(TimeSpan.FromSeconds(5), CancellationToken.None);
        return table;
    }

    // ---- Changed: leading edge / trailing coalesce ----

    [Fact]
    public async Task LoneBatchOnQuietTableEmitsWithoutArtificialDelay()
    {
        var transport = new FakeTransport();
        await using var table = await StartAsync(transport, LiveTable.DefaultFlushWindow); // 16ms

        var tcs = new TaskCompletionSource<Stopwatch>(TaskCreationOptions.RunContinuationsAsynchronously);
        var sw = Stopwatch.StartNew();
        table.Changed += (_, _) => tcs.TrySetResult(sw);

        transport.Push([new RowDelta(Row("a", 1), 1)], 1);

        var finished = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(2));
        var elapsedMs = finished.ElapsedMilliseconds;
        _output.WriteLine($"lone batch -> Changed after {elapsedMs}ms");

        // The old code did an UNCONDITIONAL 120ms Task.Delay before every emit. A quiet table's
        // first (and only) batch is always a "since the last emit, the window has long elapsed"
        // leading-edge case -- it must fire essentially immediately, well under even the new 16ms
        // default window, and nowhere near the old 120ms floor.
        Assert.True(elapsedMs < 100, $"expected well under the old 120ms floor, took {elapsedMs}ms");
    }

    [Fact]
    public async Task BurstInsideOneWindowYieldsExactlyOneChanged()
    {
        var transport = new FakeTransport();
        var window = TimeSpan.FromMilliseconds(200); // generous, so CI scheduling jitter can't cause a spurious 3rd/4th emit
        await using var table = await StartAsync(transport, window);

        var count = 0;
        table.Changed += (_, _) => Interlocked.Increment(ref count);

        // First batch on a quiet table: leading edge, emits immediately (count -> 1) and resets
        // the window's clock.
        transport.Push([new RowDelta(Row("a", 1), 1)], 1);
        await WaitUntilAsync(() => Volatile.Read(ref count) == 1, TimeSpan.FromSeconds(2));

        // A burst of further batches, all well inside the just-started window -- every one of
        // these must merge into the SAME pending trailing emit rather than firing its own.
        for (var i = 2; i <= 6; i++)
            transport.Push([new RowDelta(Row("a", i), i)], i);

        // Give the trailing emit time to fire (window + slack), then confirm it fired exactly once.
        await Task.Delay(window + TimeSpan.FromMilliseconds(300));
        _output.WriteLine($"Changed fired {Volatile.Read(ref count)} times for 1 leading + 5 coalesced batches");
        Assert.Equal(2, Volatile.Read(ref count));
    }

    [Fact]
    public async Task ZeroWindowEmitsPerAppliedBatch()
    {
        var transport = new FakeTransport();
        await using var table = await StartAsync(transport, TimeSpan.Zero);

        var count = 0;
        table.Changed += (_, _) => Interlocked.Increment(ref count);

        for (var i = 1; i <= 4; i++)
            transport.Push([new RowDelta(Row("a", i), i)], i);

        await WaitUntilAsync(() => Volatile.Read(ref count) == 4, TimeSpan.FromSeconds(2));
        Assert.Equal(4, Volatile.Read(ref count));
    }

    // ---- WatchAsync ----

    [Fact]
    public async Task WatchAsyncReceivesUpdatesAndASlowConsumerSeesOnlyTheLatestRows()
    {
        var transport = new FakeTransport();
        await using var table = await StartAsync(transport, TimeSpan.Zero); // one Changed per batch -> one channel write per batch

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var enumerator = table.WatchAsync(cts.Token).GetAsyncEnumerator();

        // WatchAsync is a normal C# async-iterator method: like any IAsyncEnumerable, its body
        // (including the `Changed += ...` subscribe) does not run at all until the first
        // MoveNextAsync -- GetAsyncEnumerator() alone is inert. So the FIRST MoveNextAsync both
        // establishes the subscription (synchronously, before the call can return an incomplete
        // task) and consumes the first batch, leaving the consumer genuinely idle afterwards --
        // exactly what "slow consumer" needs to test against next.
        var move1 = enumerator.MoveNextAsync().AsTask();
        transport.Push([new RowDelta(Row("a", 1), 1)], 1);
        Assert.True(await move1);
        Assert.Equal(1L, enumerator.Current.Single(r => Equals(r["id"], "a"))["n"]);

        // Now genuinely slow: nobody calls MoveNextAsync while several more batches land.
        for (var i = 2; i <= 6; i++)
            transport.Push([new RowDelta(Row("a", i), i)], i);
        await Task.Delay(200); // let the reader loop apply + Emit() all of them before we read anything

        var got = await enumerator.MoveNextAsync();
        Assert.True(got);
        var rows = enumerator.Current;
        _output.WriteLine($"slow consumer's next read after the burst: n={rows.Single(r => Equals(r["id"], "a"))["n"]}");

        // DropOldest + capacity 1: the slow consumer must see the LATEST snapshot (n=6), never a
        // backlog and never a stale early one from the burst (n=2..5).
        Assert.Equal(6L, rows.Single(r => Equals(r["id"], "a"))["n"]);

        await enumerator.DisposeAsync();
    }

    [Fact]
    public async Task TwoConcurrentEnumerationsEachGetTheirOwnItems()
    {
        var transport = new FakeTransport();
        await using var table = await StartAsync(transport, TimeSpan.Zero);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var e1 = table.WatchAsync(cts.Token).GetAsyncEnumerator();
        var e2 = table.WatchAsync(cts.Token).GetAsyncEnumerator();

        var move1 = e1.MoveNextAsync().AsTask();
        var move2 = e2.MoveNextAsync().AsTask();

        transport.Push([new RowDelta(Row("a", 42), 42)], 1);

        Assert.True(await move1);
        Assert.True(await move2);

        // Neither enumerator stole the other's item -- both independently observed it, because
        // each WatchAsync call owns its own channel.
        Assert.Equal(42L, e1.Current.Single(r => Equals(r["id"], "a"))["n"]);
        Assert.Equal(42L, e2.Current.Single(r => Equals(r["id"], "a"))["n"]);

        await e1.DisposeAsync();
        await e2.DisposeAsync();
    }

    [Fact]
    public async Task CancellingAnEnumerationUnsubscribesAndReleasesItsHandler()
    {
        var transport = new FakeTransport();
        await using var table = await StartAsync(transport, TimeSpan.Zero);

        WeakReference sentinel;
        using (var cts = new CancellationTokenSource())
        {
            var enumerator = table.WatchAsync(cts.Token).GetAsyncEnumerator();

            // Prove the enumeration is genuinely live (its Changed handler really attached) before
            // tearing it down -- otherwise "it got collected" would be true for the trivial reason
            // that it was never wired up.
            var move = enumerator.MoveNextAsync().AsTask();
            transport.Push([new RowDelta(Row("a", 1), 1)], 1);
            Assert.True(await move);

            sentinel = new WeakReference(enumerator);

            cts.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await enumerator.MoveNextAsync());
            await enumerator.DisposeAsync();
            enumerator = null!;
        }

        // Observable, reflection-free proof of unsubscription: if LiveTable's Changed event still
        // held a reference to the cancelled enumeration's handler (and, transitively, its channel
        // and this object), it would never become collectible -- LiveTable itself is still alive
        // and rooted (the `await using table` above), so anything IT still references would survive
        // GC too. A few full collections give any pending finalization/compaction a chance to run.
        var collected = false;
        for (var i = 0; i < 10 && !collected; i++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            collected = !sentinel.IsAlive;
            if (!collected) await Task.Delay(20);
        }

        Assert.True(collected, "the cancelled WatchAsync enumeration was not collected -- Changed still references it");

        // And the table itself is unaffected: a plain Changed subscriber, attached after the
        // cancelled enumeration was torn down, still sees fresh pushes normally.
        var laterCount = 0;
        table.Changed += (_, _) => Interlocked.Increment(ref laterCount);
        transport.Push([new RowDelta(Row("a", 2), 2)], 2);
        await WaitUntilAsync(() => Volatile.Read(ref laterCount) == 1, TimeSpan.FromSeconds(2));
        Assert.Equal(1, laterCount);
    }

    private static async Task WaitUntilAsync(Func<bool> predicate, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!predicate())
        {
            if (DateTime.UtcNow >= deadline) throw new TimeoutException("condition never became true");
            await Task.Delay(10);
        }
    }
}
