using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Runtime;
using StreamsForge.Abstractions;

namespace StreamsForge.Host.Grains;

public sealed class TableShardDirectoryGrainState
{
    /// <summary>Live RAW shard keys (not grain-key tokens). A list rather than a set because that is what
    /// the JSON storage provider round-trips; the live copy is a HashSet.</summary>
    public List<string> Keys { get; set; } = [];
}

/// <summary>
/// Plan 011 wave D1: the shard directory. Key = table name. Holds the live shard-key set so a sharded
/// table can be enumerated and so deleting the table can delete its shards.
///
/// THE HONEST LIMIT, first rather than last, because it is the one thing this design does not fix: this
/// grain is O(distinct shard keys) of strings and it is RESIDENT. It is deliberately kept alive (a
/// per-batch registration would keep it alive anyway) and nothing evicts from it except a shard that has
/// genuinely emptied. What it holds is keys — one string per instrument — not rows and not version
/// trails, so on the shape this wave was built for it is kilobytes against the shards' megabytes. But
/// "the shard tier bounds resident memory" is true of the shards and not of this, and a table with tens
/// of millions of distinct keys would feel it. The honest answer for that shape is an external index, not
/// a bigger grain; it is out of scope here and written down rather than discovered.
///
/// It is NOT on any hot read path. A per-key lookup goes straight to the shard grain and never consults
/// the directory; only enumeration (<c>GET /api/tables/{id}/shards</c>) and deletion do. That matters,
/// because it means the directory can never become the thing that wakes shards.
/// </summary>
public sealed class TableShardDirectoryGrain(
    [PersistentState("tableShardDir", StreamConstants.StorageName)] IPersistentState<TableShardDirectoryGrainState> state,
    ILogger<TableShardDirectoryGrain> logger)
    : Grain, ITableShardDirectoryGrain
{
    private readonly HashSet<string> _live = new(StringComparer.Ordinal);
    private bool _dirty;
    private IGrainTimer? _flushTimer;

    public override Task OnActivateAsync(CancellationToken cancellationToken)
    {
        foreach (var key in state.State.Keys)
        {
            _live.Add(key);
        }
        // Write-behind on a fixed cadence: the directory is not the table's durability contract (a key
        // missing from a restored directory reappears the moment its shard is written to again, and its
        // shard's own state file is the real record), so it deliberately does not follow the table's
        // configured FlushMs — one cadence, one less thing to reconfigure.
        _flushTimer = this.RegisterGrainTimer(OnFlushTickAsync, TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(2));
        return Task.CompletedTask;
    }

    public Task RegisterAsync(List<string> shardKeys)
    {
        foreach (var key in shardKeys)
        {
            if (_live.Add(key)) _dirty = true;
        }
        return Task.CompletedTask;
    }

    public Task RemoveAsync(List<string> shardKeys)
    {
        foreach (var key in shardKeys)
        {
            if (_live.Remove(key)) _dirty = true;
        }
        return Task.CompletedTask;
    }

    public Task<List<string>> GetKeysAsync(int limit, int offset)
    {
        // Ordinal-ordered so paging is stable across calls — a HashSet's own enumeration order is not.
        var ordered = _live.OrderBy(k => k, StringComparer.Ordinal).Skip(Math.Max(0, offset));
        return Task.FromResult(limit > 0 ? ordered.Take(limit).ToList() : ordered.ToList());
    }

    public Task<int> GetCountAsync() => Task.FromResult(_live.Count);

    public async Task<List<string>> DrainAllAsync()
    {
        var all = _live.ToList();
        _live.Clear();
        state.State = new TableShardDirectoryGrainState();
        _dirty = false;
        try { await state.ClearStateAsync(); } catch (Exception ex) { logger.LogWarning(ex, "Shard directory '{Table}': clear failed", this.GetPrimaryKeyString()); }
        return all;
    }

    private async Task OnFlushTickAsync()
    {
        if (!_dirty) return;
        state.State.Keys = [.. _live];
        _dirty = false;
        try { await state.WriteStateAsync(); } catch (Exception ex) { logger.LogWarning(ex, "Shard directory '{Table}': flush failed", this.GetPrimaryKeyString()); }
    }

    public override async Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        if (_dirty)
        {
            try { await OnFlushTickAsync(); } catch { /* best-effort */ }
        }
        await base.OnDeactivateAsync(reason, cancellationToken);
    }
}
