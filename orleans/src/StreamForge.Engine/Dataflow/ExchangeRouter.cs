using System.Text;

namespace StreamForge.Engine.Dataflow;

/// <summary>
/// Where a stage's output is exchanged to. Key expressions are a plan-time concern (the
/// TablePlanner decides join/group-by key expressions) — that's explicitly out of scope for M0
/// (plans/003-materialize-territory.md: "Exchange spec is computed at plan time: (stageId,
/// keyExprs)"). By the time a delta reaches <see cref="ExchangeRouter"/> it has already been
/// reduced to canonical key bytes via the engine's existing key encoding
/// (Runtime/TableKeyEncoding.cs — the same encoding joins/reduces already use for in-process
/// grouping), so <see cref="ExchangeSpec"/> here only carries the routing target.
/// </summary>
public sealed record ExchangeSpec
{
    public ExchangeSpec(EdgeId edgeId, int partitionCount)
    {
        if (partitionCount < 1)
            throw new ArgumentOutOfRangeException(nameof(partitionCount), partitionCount, "partitionCount must be >= 1.");
        EdgeId = edgeId;
        PartitionCount = partitionCount;
    }

    public EdgeId EdgeId { get; }
    public int PartitionCount { get; }
}

/// <summary>
/// Routes a canonical key (TableKeyEncoding output — see <see cref="ExchangeSpec"/> docs) to a
/// stable partition index via FNV-1a. FNV-1a rather than <see cref="object.GetHashCode"/> because
/// the router must be deterministic ACROSS PROCESSES: the same key must land on the same
/// partition on every node and after every restart, which rules out .NET's randomized string
/// hash and any hash whose algorithm can change between runtimes.
/// </summary>
public static class ExchangeRouter
{
    private const ulong FnvOffsetBasis = 14695981039346656037UL;
    private const ulong FnvPrime = 1099511628211UL;

    /// <summary>Routes pre-extracted canonical key bytes to a partition in [0, partitionCount).
    /// Deterministic: same bytes + same partitionCount always yields the same index, on any
    /// process, on any run.</summary>
    public static int PartitionOf(ReadOnlySpan<byte> keyBytes, int partitionCount)
    {
        if (partitionCount < 1)
            throw new ArgumentOutOfRangeException(nameof(partitionCount), partitionCount, "partitionCount must be >= 1.");
        if (partitionCount == 1) return 0;

        var hash = Fnv1a32(keyBytes);
        return (int)(hash % (uint)partitionCount);
    }

    /// <summary>Convenience overload for a canonical key already produced as a string (e.g. via
    /// TableKeyEncoding.EncodeScalar/EncodeGroupKey) — encodes it to UTF-8 bytes first, so the
    /// same string always maps to the same bytes and therefore the same partition.</summary>
    public static int PartitionOf(string canonicalKey, int partitionCount) =>
        PartitionOf(Encoding.UTF8.GetBytes(canonicalKey), partitionCount);

    /// <summary>64-bit FNV-1a folded to 32 bits (xor-fold) for a well-mixed partition hash.</summary>
    private static uint Fnv1a32(ReadOnlySpan<byte> data)
    {
        var hash = FnvOffsetBasis;
        foreach (var b in data)
        {
            hash ^= b;
            hash *= FnvPrime;
        }
        return (uint)(hash ^ (hash >> 32));
    }
}
