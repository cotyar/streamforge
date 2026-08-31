using StreamsForge.Abstractions;
using StreamsForge.AppCore.Ingest;
using Xunit;

namespace StreamsForge.Engine.Tests;

/// <summary>Plan 009 A1.1: <see cref="IngestIdempotencyCache"/> — the batch-level idempotency cache
/// both runtime flavors' PushAsync wraps every call in via <see cref="IngestIdempotencyCache.RunAsync"/>.
/// Pure, no I/O, so this is exercised directly rather than only through a facade.</summary>
public class IngestIdempotencyCacheTests
{
    private static IngestResult Accepted(int n) => new() { Outcome = IngestOutcome.Accepted, Accepted = n };

    [Fact]
    public async Task RunAsync_with_no_key_always_recomputes_and_never_touches_the_cache()
    {
        var cache = new IngestIdempotencyCache();
        var calls = 0;

        Task<IngestResult> Core() { calls++; return Task.FromResult(Accepted(1)); }

        await IngestIdempotencyCache.RunAsync(cache, "s1", null, Core);
        await IngestIdempotencyCache.RunAsync(cache, "s1", "", Core);

        Assert.Equal(2, calls);
        Assert.Equal(0, cache.Count);
    }

    [Fact]
    public async Task RunAsync_a_repeat_key_replays_the_original_result_and_never_recomputes()
    {
        var cache = new IngestIdempotencyCache();
        var calls = 0;

        Task<IngestResult> Core() { calls++; return Task.FromResult(Accepted(5)); }

        var first = await IngestIdempotencyCache.RunAsync(cache, "s1", "key-1", Core);
        var second = await IngestIdempotencyCache.RunAsync(cache, "s1", "key-1", Core);

        Assert.Equal(1, calls); // core only ran once
        Assert.False(first.Replayed);
        Assert.True(second.Replayed);
        Assert.Equal(first.Accepted, second.Accepted);
        Assert.Equal(5, second.Accepted); // the REPLAYED counts are the ORIGINAL push's counts
    }

    [Fact]
    public async Task RunAsync_replays_a_non_accepted_outcome_too()
    {
        var cache = new IngestIdempotencyCache();

        Task<IngestResult> Core() => Task.FromResult(new IngestResult { Outcome = IngestOutcome.Invalid, Invalid = 3, Error = "bad batch" });

        var first = await IngestIdempotencyCache.RunAsync(cache, "s1", "key-1", Core);
        var second = await IngestIdempotencyCache.RunAsync(cache, "s1", "key-1", Core);

        Assert.Equal(IngestOutcome.Invalid, first.Outcome);
        Assert.Equal(IngestOutcome.Invalid, second.Outcome);
        Assert.True(second.Replayed);
        Assert.Equal(3, second.Invalid);
    }

    [Fact]
    public async Task RunAsync_the_same_key_on_two_different_sources_is_tracked_independently()
    {
        var cache = new IngestIdempotencyCache();
        var calls = 0;

        Task<IngestResult> Core() { calls++; return Task.FromResult(Accepted(calls)); }

        var a = await IngestIdempotencyCache.RunAsync(cache, "source-a", "same-key", Core);
        var b = await IngestIdempotencyCache.RunAsync(cache, "source-b", "same-key", Core);

        Assert.Equal(2, calls);
        Assert.False(a.Replayed);
        Assert.False(b.Replayed);
    }

    [Fact]
    public void Remember_is_a_noop_when_the_pair_is_already_remembered()
    {
        var cache = new IngestIdempotencyCache();

        cache.Remember("s1", "k1", Accepted(1));
        cache.Remember("s1", "k1", Accepted(999)); // must not overwrite

        Assert.Equal(1, cache.TryGet("s1", "k1")!.Accepted);
    }

    [Fact]
    public void Cache_evicts_oldest_first_once_bounded()
    {
        var cache = new IngestIdempotencyCache();

        for (var i = 0; i < IngestIdempotencyCache.MaxEntries + 5; i++)
        {
            cache.Remember("s1", $"k{i}", Accepted(i));
        }

        Assert.Equal(IngestIdempotencyCache.MaxEntries, cache.Count);
        Assert.Null(cache.TryGet("s1", "k0")); // the oldest 5 were evicted
        Assert.Null(cache.TryGet("s1", "k4"));
        Assert.NotNull(cache.TryGet("s1", "k5")); // the rest survive
        Assert.NotNull(cache.TryGet("s1", $"k{IngestIdempotencyCache.MaxEntries + 4}"));
    }

    [Fact]
    public void AsReplay_returns_a_distinct_instance_so_the_cached_result_is_never_mutated()
    {
        var original = Accepted(2);
        var replay = IngestIdempotencyCache.AsReplay(original);

        Assert.NotSame(original, replay);
        Assert.False(original.Replayed);
        Assert.True(replay.Replayed);
        Assert.Equal(original.Accepted, replay.Accepted);
    }
}
