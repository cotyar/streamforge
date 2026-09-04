namespace StreamsForge.AppCore.Connectors;

/// <summary>A connector source's bounded, in-memory memory of what it has already published — the thing
/// that makes a LATE consumer possible at all.
///
/// <para>WHY IT EXISTS: Orleans memory streams have no replay, so a table/pipeline only ever receives rows
/// published after its own subscription existed. The natural console flow is "create the source (enabled),
/// then write the table SQL" — by which time a `url`/`file` source's first poll has long since fired, and
/// with a dedup key configured those rows never come round again. The source had no memory of what it
/// emitted, so the rows were simply gone. This ring is that memory: a consumer attaching late replays the
/// recent rows exactly once (the attach gate in the driver is what makes "exactly once" true — see
/// <c>ConnectorGrain.BeginAttachAsync</c>), with no `_seq` column leaking into the row shape and no double
/// delivery to consumers that were already subscribed.</para>
///
/// <para>Pure and framework-free — it holds plain row dictionaries, knows nothing about streams, grains or
/// actors, and is unit-tested on its own. In-memory only: a restart empties it, which is deliberate (a
/// restart's replay window is a different problem, solved by boot ordering, not by this).</para>
///
/// <para>Not thread-safe: it is owned by one turn-based driver (a grain/actor activation), the same
/// ownership rule <c>DedupTracker</c>/<c>FileLedger</c> already rely on.</para>
///
/// <para>// ponytail: fixed 10k-row ring per source (~10 MB worst case); per-kind policy (whole last cycle
/// for polled kinds) if a late consumer ever needs more.</para></summary>
public sealed class SourceReplayBuffer
{
    /// <summary>Rows retained. Past this the OLDEST is evicted — <see cref="TotalSeen"/> keeps counting, so
    /// a caller can always tell "you got everything" from "you got the last N of M"; the driver turns that
    /// difference into an operator-visible warning rather than letting it pass silently.</summary>
    public const int Capacity = 10_000;

    private readonly Queue<Dictionary<string, object?>> _ring = new();

    /// <summary>Every row ever appended since this buffer was created (i.e. since the activation came up),
    /// including the ones already evicted. Never decreases.</summary>
    public long TotalSeen { get; private set; }

    /// <summary>Rows currently retained (&lt;= <see cref="Capacity"/>).</summary>
    public int Count => _ring.Count;

    public void Append(Dictionary<string, object?> row)
    {
        TotalSeen++;
        _ring.Enqueue(row);
        if (_ring.Count > Capacity)
        {
            _ring.Dequeue();
        }
    }

    /// <summary>The retained rows, oldest first, as a COPY (both the list and each row dictionary) — a late
    /// consumer feeds these through its own admission path and is free to mutate them; nothing it does can
    /// reach back into what the next consumer will be handed.</summary>
    public (List<Dictionary<string, object?>> Rows, long TotalSeen) Snapshot()
    {
        var rows = new List<Dictionary<string, object?>>(_ring.Count);
        foreach (var row in _ring)
        {
            rows.Add(new Dictionary<string, object?>(row));
        }
        return (rows, TotalSeen);
    }
}
