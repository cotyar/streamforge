namespace StreamForge.AppCore.Connectors.Polling;

/// <summary>Bounded dedup key set for polling connectors (plan 006, D-D). Remembers up to
/// <see cref="MaxKeys"/> keys, FIFO — beyond the bound the oldest key is forgotten and could in
/// principle be re-emitted (documented ceiling, not a bug: at-least-once, not exactly-once).
/// O(1) <see cref="Seen"/> via a HashSet mirrored by an insertion-order Queue for eviction.</summary>
public sealed class DedupTracker
{
    /// <summary>Bound on the number of remembered keys (D-D).</summary>
    public const int MaxKeys = 10_000;

    private readonly HashSet<string> _keys;
    private readonly Queue<string> _order;

    /// <param name="persisted">Previously-persisted keys in insertion (oldest-first) order, as
    /// returned by <see cref="ToPersistable"/>. Null starts empty.</param>
    public DedupTracker(List<string>? persisted = null)
    {
        _order = new Queue<string>(persisted ?? []);
        _keys = new HashSet<string>(_order);
    }

    /// <summary>True if <paramref name="key"/> was already seen (no state change). False if it is
    /// new — it is recorded as seen before returning.</summary>
    public bool Seen(string key)
    {
        if (!_keys.Add(key))
            return true;

        _order.Enqueue(key);
        if (_order.Count > MaxKeys)
            _keys.Remove(_order.Dequeue());

        return false;
    }

    /// <summary>Insertion order, oldest first — persist this and hand it back to the constructor
    /// to restore.</summary>
    public List<string> ToPersistable() => [.. _order];

    /// <summary>Number of keys currently remembered (never exceeds <see cref="MaxKeys"/>).</summary>
    public int Count => _order.Count;
}
