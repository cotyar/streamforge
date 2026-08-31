using StreamsForge.Engine.Dataflow;
using Xunit;

namespace StreamsForge.Engine.Tests;

/// <summary>EpochBuffer holds the core replay guarantee: the flush sequence for a fixed set of
/// batches depends only on (Epoch, EdgeId, FromPartition), never on Add() order. These tests
/// exercise out-of-order arrival, shuffled-permutation determinism, empty-epoch markers, and the
/// bounded-memory / no-silent-drop guarantees.</summary>
public class DataflowEpochBufferTests
{
    private static readonly EdgeId Edge0 = new(0);
    private static readonly EdgeId Edge1 = new(1);

    private static TableDelta Delta(string id) => new(new EventRecord { ["id"] = id }, 1);

    private static DeltaBatch Batch(EdgeId edge, int partition, long epoch, params string[] rowIds) =>
        new(edge, partition, new Epoch(epoch), rowIds.Select(Delta).ToList());

    [Fact]
    public void OutOfOrderArrivalFlushesInEpochOrder()
    {
        var buffer = new EpochBuffer();
        buffer.Add(Batch(Edge0, 0, 3, "e3"));
        buffer.Add(Batch(Edge0, 0, 1, "e1"));
        buffer.Add(Batch(Edge0, 0, 2, "e2"));

        var ready = buffer.OnFrontier(new Epoch(3));

        Assert.Equal(3, ready.Count);
        Assert.Equal(new Epoch(1), ready[0].Epoch);
        Assert.Equal(new Epoch(2), ready[1].Epoch);
        Assert.Equal(new Epoch(3), ready[2].Epoch);
    }

    [Fact]
    public void OnlyBatchesAtOrBelowFrontierAreFlushed_RestStayBuffered()
    {
        var buffer = new EpochBuffer();
        buffer.Add(Batch(Edge0, 0, 1, "a"));
        buffer.Add(Batch(Edge0, 0, 5, "b"));

        var ready = buffer.OnFrontier(new Epoch(2));

        var only = Assert.Single(ready);
        Assert.Equal(new Epoch(1), only.Epoch);
        Assert.Equal(1, buffer.BatchCount); // epoch 5 batch still buffered
        Assert.Equal(1, buffer.DeltaCount);
    }

    [Fact]
    public void WithinAnEpochOrderIsDeterministicByEdgeThenPartition()
    {
        var buffer = new EpochBuffer();
        buffer.Add(Batch(Edge1, 1, 1, "z"));
        buffer.Add(Batch(Edge0, 2, 1, "y"));
        buffer.Add(Batch(Edge0, 0, 1, "x"));
        buffer.Add(Batch(Edge1, 0, 1, "w"));

        var ready = buffer.OnFrontier(new Epoch(1));

        Assert.Equal(4, ready.Count);
        var expectedOrder = new (EdgeId, int)[] { (Edge0, 0), (Edge0, 2), (Edge1, 0), (Edge1, 1) };
        Assert.Equal(expectedOrder, ready.Select(b => (b.EdgeId, b.FromPartition)).ToArray());
    }

    [Fact]
    public void EmptyEpochMarkerIsFlushedLikeAnyOtherBatch()
    {
        var buffer = new EpochBuffer();
        var marker = new DeltaBatch(Edge0, 0, new Epoch(1), []);
        buffer.Add(marker);

        var ready = buffer.OnFrontier(new Epoch(1));

        var only = Assert.Single(ready);
        Assert.True(only.IsMarker);
        Assert.Equal(new Epoch(1), only.Epoch);
    }

    [Fact]
    public void RepeatedFlushesNeverReturnTheSameBatchTwice_AndNothingIsSilentlyDropped()
    {
        var buffer = new EpochBuffer();
        for (var e = 0; e < 10; e++)
        {
            buffer.Add(Batch(Edge0, 0, e, $"row{e}"));
        }

        var firstFlush = buffer.OnFrontier(new Epoch(4));
        Assert.Equal(5, firstFlush.Count); // epochs 0..4
        Assert.Equal(5, buffer.BatchCount); // epochs 5..9 remain

        var secondFlush = buffer.OnFrontier(new Epoch(9));
        Assert.Equal(5, secondFlush.Count); // epochs 5..9
        Assert.Equal(0, buffer.BatchCount);

        var allFlushed = firstFlush.Concat(secondFlush).Select(b => b.Epoch.Value).ToArray();
        Assert.Equal(Enumerable.Range(0, 10).Select(i => (long)i), allFlushed);
    }

    [Fact]
    public void FlushingWithNoReadyBatchesReturnsEmptyAndLeavesBufferIntact()
    {
        var buffer = new EpochBuffer();
        buffer.Add(Batch(Edge0, 0, 5, "a"));

        var ready = buffer.OnFrontier(new Epoch(1));

        Assert.Empty(ready);
        Assert.Equal(1, buffer.BatchCount);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    public void DeterministicReplay_SameBatchesInAnyArrivalOrderProduceIdenticalFlushSequence(int seed)
    {
        var rng = new Random(seed);
        var canonical = new List<DeltaBatch>();
        for (var e = 0; e < 12; e++)
        {
            for (var p = 0; p < 3; p++)
            {
                var edge = p % 2 == 0 ? Edge0 : Edge1;
                canonical.Add(Batch(edge, p, e, $"e{e}-p{p}"));
            }
        }

        // The reference flush sequence, added in canonical order.
        var reference = new EpochBuffer();
        foreach (var b in canonical) reference.Add(b);
        var referenceFlush = reference.OnFrontier(new Epoch(11));

        // Try several random shuffles (permutations) of arrival order — every one must produce
        // the exact same flush sequence. This is the property that makes replay debugging work.
        for (var trial = 0; trial < 20; trial++)
        {
            var shuffled = canonical.OrderBy(_ => rng.Next()).ToList();
            var buffer = new EpochBuffer();
            foreach (var b in shuffled) buffer.Add(b);
            var flush = buffer.OnFrontier(new Epoch(11));

            Assert.Equal(referenceFlush.Count, flush.Count);
            for (var i = 0; i < referenceFlush.Count; i++)
            {
                Assert.Equal(referenceFlush[i].Epoch, flush[i].Epoch);
                Assert.Equal(referenceFlush[i].EdgeId, flush[i].EdgeId);
                Assert.Equal(referenceFlush[i].FromPartition, flush[i].FromPartition);
                Assert.Equal(
                    referenceFlush[i].Deltas.Select(d => d.Row["id"]),
                    flush[i].Deltas.Select(d => d.Row["id"]));
            }
        }
    }

    [Fact]
    public void DeterministicReplay_IncrementalFrontierAdvancesInAnyArrivalOrderMatchReference()
    {
        // Same idea but flushing incrementally at several frontier checkpoints instead of once at
        // the end — the concatenation of incremental flushes must equal the single big flush.
        var rng = new Random(999);
        var canonical = new List<DeltaBatch>();
        for (var e = 0; e < 20; e++)
        {
            canonical.Add(Batch(Edge0, e % 4, e, $"row{e}"));
        }

        var reference = new EpochBuffer();
        foreach (var b in canonical) reference.Add(b);
        var referenceFlush = new List<DeltaBatch>();
        foreach (var checkpoint in new long[] { 3, 3, 10, 15, 19 })
        {
            referenceFlush.AddRange(reference.OnFrontier(new Epoch(checkpoint)));
        }

        for (var trial = 0; trial < 10; trial++)
        {
            var shuffled = canonical.OrderBy(_ => rng.Next()).ToList();
            var buffer = new EpochBuffer();
            foreach (var b in shuffled) buffer.Add(b);

            var flush = new List<DeltaBatch>();
            foreach (var checkpoint in new long[] { 3, 3, 10, 15, 19 })
            {
                flush.AddRange(buffer.OnFrontier(new Epoch(checkpoint)));
            }

            Assert.Equal(referenceFlush.Select(b => b.Epoch.Value), flush.Select(b => b.Epoch.Value));
            Assert.Equal(
                referenceFlush.Select(b => (b.EdgeId, b.FromPartition)),
                flush.Select(b => (b.EdgeId, b.FromPartition)));
        }
    }
}
