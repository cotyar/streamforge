using System.Collections.Concurrent;

namespace StreamsForge.Host.Grains;

/// <summary>
/// Plan 011 wave D1: how many of a table's shards are ACTIVATED right now, and how many times shards have
/// been activated and deactivated since this process started.
///
/// WHY THIS EXISTS AT ALL. The claim wave D1 makes is "an idle shard deactivates, and its state waits on
/// disk until the next lookup". A claim like that is worthless unless it can be checked, and every
/// obvious way to check it destroys what it measures: calling a shard to ask whether it is activated
/// activates it. Orleans' own <c>IManagementGrain.GetDetailedGrainStatistics</c> would answer without
/// waking anything, but it answers per grain TYPE across the cluster and would still have to be filtered
/// by grain id — so this counter, maintained by the shard grain's own OnActivate/OnDeactivate, is both
/// cheaper and exactly scoped to one table.
///
/// WHAT THE NUMBERS MEAN, precisely:
///   * <see cref="ResidentCount"/> is a live gauge — shards currently in memory in THIS process.
///   * <see cref="Activations"/> is cumulative. Activations FAR exceeding ResidentCount is the direct
///     evidence that shards are being collected and faithfully reloaded, rather than merely never having
///     been created in the first place. A run where the two numbers track each other means nothing is
///     being swapped out.
///
/// PER-PROCESS BY CONSTRUCTION. A static in this host's process, so in a multi-silo cluster this is one
/// replica's view rather than a cluster total. That is accurate for every deployment this repo actually
/// runs (single silo — see DESIGN.md's "Known ceilings": single-node topology) and is reported as such
/// on <see cref="StreamsForge.Abstractions.TableShardingInfo"/>. Thread-safe because Orleans activates and
/// deactivates grains on arbitrary scheduler threads.
/// </summary>
public static class ShardResidency
{
    private sealed class TableCounters
    {
        public readonly ConcurrentDictionary<string, byte> Live = new(StringComparer.Ordinal);
        public long Activations;
        public long Deactivations;
    }

    private static readonly ConcurrentDictionary<string, TableCounters> Tables = new(StringComparer.Ordinal);

    public static void OnActivated(string tableName, string grainKey)
    {
        var c = Tables.GetOrAdd(tableName, _ => new TableCounters());
        c.Live[grainKey] = 0;
        Interlocked.Increment(ref c.Activations);
    }

    public static void OnDeactivated(string tableName, string grainKey)
    {
        if (!Tables.TryGetValue(tableName, out var c)) return;
        c.Live.TryRemove(grainKey, out _);
        Interlocked.Increment(ref c.Deactivations);
    }

    public static int ResidentCount(string tableName) =>
        Tables.TryGetValue(tableName, out var c) ? c.Live.Count : 0;

    public static long Activations(string tableName) =>
        Tables.TryGetValue(tableName, out var c) ? Interlocked.Read(ref c.Activations) : 0;

    public static long Deactivations(string tableName) =>
        Tables.TryGetValue(tableName, out var c) ? Interlocked.Read(ref c.Deactivations) : 0;

    /// <summary>Forgets a table's counters entirely — the table-delete path. Not called on tier
    /// reconfiguration: the cumulative counters are process-lifetime diagnostics, and resetting them on
    /// every config change would erase the evidence they exist to provide.</summary>
    public static void Forget(string tableName) => Tables.TryRemove(tableName, out _);
}
