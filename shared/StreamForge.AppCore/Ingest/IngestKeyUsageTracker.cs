namespace StreamForge.AppCore.Ingest;

/// <summary>
/// Plan 009 A1.2 — host-process, in-memory-only "last used" tracker for per-source push keys
/// (<c>IngestKey.LastUsedMs</c>, whose own doc comment calls it "0 = never used. Best-effort: updated
/// on successful pushes, per replica"). Deliberately NOT round-tripped through
/// <c>ICatalogFacade.UpsertSourceAsync</c> on every authenticated push: that call rewrites the WHOLE
/// catalog state (every source/pipeline/table), which would make key validation — the very thing
/// gating the ingress hot path — the single most expensive step in the request. Lost on restart, same
/// ceiling <see cref="SourceIngressBuffer"/>'s own counters and
/// <see cref="StreamForge.AppCore.Connectors.Polling.DedupTracker"/> already accept for other
/// process-local ingress state.
/// </summary>
public sealed class IngestKeyUsageTracker
{
    private readonly object _gate = new();
    private readonly Dictionary<string, long> _lastUsedMs = new(StringComparer.Ordinal);

    public void RecordUse(string sourceName, string keyId, long nowMs)
    {
        lock (_gate) { _lastUsedMs[CacheKey(sourceName, keyId)] = nowMs; }
    }

    /// <summary>0 when this replica has never recorded a use for this key (it may still have been
    /// used on a different replica, or before this process last restarted).</summary>
    public long GetLastUsedMs(string sourceName, string keyId)
    {
        lock (_gate) { return _lastUsedMs.TryGetValue(CacheKey(sourceName, keyId), out var v) ? v : 0; }
    }

    private static string CacheKey(string sourceName, string keyId) => sourceName + '\0' + keyId;
}
