namespace StreamsForge.AppCore.Connectors.Polling;

/// <summary>File-change tracking for file and folder connector sources (plan 006, D-D): name →
/// last-recorded modified-time (epoch ms). Bounded at <see cref="MaxEntries"/>, FIFO by
/// first-record order — beyond the bound the oldest file is forgotten and its next change may be
/// missed once (documented ceiling, same as <see cref="DedupTracker"/>). O(1) lookups via a
/// Dictionary mirrored by an insertion-order Queue for eviction.</summary>
public sealed class FileLedger
{
    /// <summary>Bound on the number of remembered files (D-D).</summary>
    public const int MaxEntries = 10_000;

    private readonly Dictionary<string, long> _mtimes;
    private readonly Queue<string> _order;

    /// <param name="persisted">Previously-persisted name → mtime map, as returned by
    /// <see cref="ToPersistable"/>. Null starts empty. FIFO order after restore follows the
    /// dictionary's own enumeration order (stable insertion order in practice for the CLR's
    /// Dictionary&lt;TKey,TValue&gt;, consistent with how this map was itself built).</param>
    public FileLedger(Dictionary<string, long>? persisted = null)
    {
        _mtimes = persisted is null ? new Dictionary<string, long>() : new Dictionary<string, long>(persisted);
        _order = new Queue<string>(_mtimes.Keys);
    }

    /// <summary>True if <paramref name="name"/> is unknown, or its recorded mtime differs from
    /// <paramref name="mtimeMs"/>. Does NOT record — call <see cref="Record"/> once the file has
    /// actually been processed.</summary>
    public bool IsNewOrChanged(string name, long mtimeMs)
        => !_mtimes.TryGetValue(name, out var known) || known != mtimeMs;

    /// <summary>Records/updates <paramref name="name"/>'s mtime. A brand-new name is appended to
    /// FIFO order (and may evict the oldest entry if that pushes past <see cref="MaxEntries"/>); an
    /// existing name is simply updated in place — it does NOT move within the FIFO order.</summary>
    public void Record(string name, long mtimeMs)
    {
        var isNew = !_mtimes.ContainsKey(name);
        _mtimes[name] = mtimeMs;

        if (!isNew)
            return;

        _order.Enqueue(name);
        if (_order.Count > MaxEntries)
            _mtimes.Remove(_order.Dequeue());
    }

    /// <summary>Snapshot suitable for persistence and restore via the constructor.</summary>
    public Dictionary<string, long> ToPersistable() => new(_mtimes);
}
