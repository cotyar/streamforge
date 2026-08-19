using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Runtime;
using StreamForge.Abstractions;

namespace StreamForge.Host.Grains;

/// <summary>One day's audit. <see cref="Truncated"/> is persisted alongside the entries because a
/// counter that reset when the day was evicted would count the same thing the silence already says
/// nothing about.</summary>
public sealed class AuditLogGrainState
{
    /// <summary>Append order, oldest first. Drop-oldest removes from the front.</summary>
    public List<AuditEntry> Entries { get; set; } = [];

    /// <summary>How many entries this day has dropped to stay under the cap. Cumulative, never reset —
    /// see <see cref="AuditPage.Truncated"/>.</summary>
    public long Truncated { get; set; }
}

/// <summary>The index's state: the days that have ever been written to.</summary>
public sealed class AuditIndexGrainState
{
    /// <summary><c>yyyyMMdd</c> strings. A list rather than a set because that is what the JSON storage
    /// provider round-trips; the live copy is a HashSet, exactly as TableShardDirectoryGrain does it.</summary>
    public List<string> Days { get; set; } = [];
}

/// <summary>
/// Plan 015 W4-B — ONE DAY of audit (key <c>audit:yyyyMMdd</c>, UTC).
///
/// <para><b>Day-sharded so that a day activates only when written to or read, and is collected when
/// idle</b> — the mechanism plan 011-D1 established for <c>TableShardGrain</c>, and copied from it
/// deliberately, down to the one rule that makes it work: <b>this grain never calls
/// <c>DelayDeactivation</c></b>. Every grain in the table path does, which is exactly why nothing in the
/// table path has ever been swapped out; a self-pinning audit log would be a memory leak with a
/// retention policy. Yesterday is on disk and stays there until somebody reads it. The collection age is
/// Orleans' silo-wide default here (no <c>ClassSpecificCollectionAge</c> entry is added for it —
/// <c>Program.cs</c> belongs to another session this wave), which is the right default: an audit day is
/// tiny compared with a table shard and nothing about it wants a special residency rule.</para>
///
/// <para><b>The write path is append-only.</b> There is no update and no delete on
/// <see cref="IAuditFacade"/> and none here — the only way an entry leaves is the cap below, which drops
/// the OLDEST and counts what it dropped. Nothing else can remove a row, which is the property that
/// makes the log worth reading.</para>
///
/// <para><b>Bounded, and honest about it.</b> <c>Audit:MaxEntriesPerDay</c> (default 20 000) is
/// drop-oldest with a persisted <see cref="AuditLogGrainState.Truncated"/> counter surfaced on every
/// page: silence must never be mistaken for absence. Note the direction is the OPPOSITE of the sink's
/// bounded channel (drop-write), and deliberately so — the sink's competing rows are milliseconds apart
/// during a burst, so the onset is the valuable one; here they are a whole day apart, so recent is the
/// valuable one. Both count what they dropped, which is the part that has to be the same.</para>
///
/// <para><b>Write-behind, not write-through.</b> The drain (<c>AuditWriterService</c>) calls
/// <see cref="AppendAsync"/> one entry at a time, so a write per entry would rewrite the whole day's
/// document per row — O(n²) bytes over a day, megabytes per row once the day is full. A 2s flush timer
/// (plus a flush on deactivation) is the same shape <c>TableShardDirectoryGrain</c> already uses, for
/// the same reason. What it costs is honest and small: a hard process kill loses up to 2s of audit, on
/// top of the queue the sink was already holding — audit is best-effort by construction (the sink drops
/// on overflow and the writer drops on a store failure), and buying durability here would not buy it
/// anywhere else on the path.</para>
/// </summary>
public sealed class AuditLogGrain(
    [PersistentState("audit", StreamConstants.StorageName)] IPersistentState<AuditLogGrainState> state,
    IConfiguration configuration,
    ILogger<AuditLogGrain> logger)
    : Grain, IAuditLogGrain
{
    public const string MaxEntriesPerDayKey = "Audit:MaxEntriesPerDay";
    public const int DefaultMaxEntriesPerDay = 20_000;

    private int _max = DefaultMaxEntriesPerDay;
    private bool _dirty;
    private bool _registered;
    private IGrainTimer? _flushTimer;

    /// <summary>Values below 1 fall back to the default rather than to "keep nothing": a cap of 0 would
    /// turn every append into a drop, and there is no reading of "audit, but discard it all" that is not
    /// better spelled <c>Audit:Enabled=false</c>.</summary>
    public override Task OnActivateAsync(CancellationToken cancellationToken)
    {
        var configured = configuration.GetValue<int?>(MaxEntriesPerDayKey);
        _max = configured is > 0 ? configured.Value : DefaultMaxEntriesPerDay;

        _flushTimer = this.RegisterGrainTimer(FlushAsync, TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(2));
        return Task.CompletedTask;
    }

    private string Day => this.GetPrimaryKeyString().StartsWith(StreamConstants.AuditKeyPrefix, StringComparison.Ordinal)
        ? this.GetPrimaryKeyString()[StreamConstants.AuditKeyPrefix.Length..]
        : this.GetPrimaryKeyString();

    /// <summary>Append, then trim. The trim is a loop rather than a single RemoveRange because the cap
    /// can be lowered by configuration between restarts and a day restored from disk may already be over
    /// it — in which case the excess is dropped (and counted) on the next append rather than silently
    /// kept forever.</summary>
    public async Task AppendAsync(AuditEntry entry)
    {
        ArgumentNullException.ThrowIfNull(entry);

        state.State.Entries.Add(entry);

        while (state.State.Entries.Count > _max)
        {
            state.State.Entries.RemoveAt(0);
            state.State.Truncated++;
        }

        _dirty = true;

        // First write of this activation: tell the index this day exists. One grain call per activation,
        // not per entry — and only ever on the write path, so a read of a day that was never written
        // cannot invent it.
        if (!_registered)
        {
            _registered = true;
            try
            {
                await GrainFactory.GetGrain<IAuditIndexGrain>(AuditKeys.IndexKey).RegisterDayAsync(Day);
            }
            catch (Exception ex)
            {
                // The entry is already recorded; only the "which days exist" listing would miss it, and
                // the next activation retries. Failing the append here would trade a real audit row for
                // an index row.
                _registered = false;
                logger.LogWarning(ex, "Audit day '{Day}': registering with the day index failed; the entry is stored regardless.", Day);
            }
        }
    }

    /// <summary>
    /// Exact-match on actor, prefix-match on action, newest first, paged.
    ///
    /// <para><paramref name="day"/> is ignored on purpose: this grain IS one day, its key says which,
    /// and honouring a mismatched parameter would mean either lying (returning another day's rows from
    /// this activation) or waking a second grain from inside the first. The facade routes on the day and
    /// is the only caller.</para>
    ///
    /// <para><see cref="AuditPage.Total"/> counts the FILTERED rows before paging, so a UI can page; the
    /// entries are the page. <see cref="AuditPage.Truncated"/> is the day's cumulative drop count, and it
    /// is reported on every page rather than only the last one — a caller reading page 1 must not have to
    /// page to the end to learn that there is a hole.</para>
    /// </summary>
    public Task<AuditPage> QueryAsync(string day, string? actor, string? actionPrefix, int limit, int offset)
    {
        IEnumerable<AuditEntry> rows = state.State.Entries;

        if (!string.IsNullOrEmpty(actor))
        {
            rows = rows.Where(e => string.Equals(e.Actor, actor, StringComparison.Ordinal));
        }

        if (!string.IsNullOrEmpty(actionPrefix))
        {
            rows = rows.Where(e => e.Action.StartsWith(actionPrefix, StringComparison.Ordinal));
        }

        // Materialized once: Total and the page have to agree, and re-enumerating a filtered sequence
        // twice would evaluate the predicates twice for the privilege of possibly disagreeing.
        var matched = rows.ToList();
        matched.Reverse();

        IEnumerable<AuditEntry> page = matched.Skip(Math.Max(0, offset));
        if (limit > 0)
        {
            page = page.Take(limit);
        }

        return Task.FromResult(new AuditPage
        {
            Entries = [.. page],
            Total = matched.Count,
            Truncated = state.State.Truncated,
        });
    }

    /// <summary>Delegated to the index, because a day grain cannot enumerate its siblings — see
    /// <see cref="IAuditIndexGrain"/>. It is on this interface only because
    /// <see cref="IAuditFacade"/> is one interface; the facade adapter calls the index directly and
    /// never comes through here, so asking a day for the day list costs one extra hop and wakes one day
    /// that did not need waking. Kept honest rather than throwing: an implementation of a frozen
    /// interface that throws on a member is a landmine for whoever calls it next.</summary>
    public Task<List<string>> GetDaysAsync() =>
        GrainFactory.GetGrain<IAuditIndexGrain>(AuditKeys.IndexKey).GetDaysAsync();

    private async Task FlushAsync()
    {
        if (!_dirty)
        {
            return;
        }

        _dirty = false;
        try
        {
            await state.WriteStateAsync();
        }
        catch (Exception ex)
        {
            _dirty = true;
            logger.LogWarning(ex, "Audit day '{Day}': flush failed; {Count} entr(ies) are still only in memory.", Day, state.State.Entries.Count);
        }
    }

    public override async Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        _flushTimer?.Dispose();
        if (_dirty)
        {
            try { await FlushAsync(); } catch { /* best-effort: see the class remarks on write-behind */ }
        }

        await base.OnDeactivateAsync(reason, cancellationToken);
    }
}

/// <summary>Plan 015 W4-B — the day index (key <see cref="AuditKeys.IndexKey"/>). Small, singular, and
/// written once per day per activation; see <see cref="IAuditIndexGrain"/> for why it exists at all.
///
/// <para>Written through rather than write-behind, unlike the day grains: a register is at most one
/// write per day per activation, so there is nothing to batch, and the whole value of the index is that
/// it survives to answer for a day that has been evicted.</para></summary>
public sealed class AuditIndexGrain(
    [PersistentState("auditIndex", StreamConstants.StorageName)] IPersistentState<AuditIndexGrainState> state)
    : Grain, IAuditIndexGrain
{
    private readonly HashSet<string> _days = new(StringComparer.Ordinal);

    public override Task OnActivateAsync(CancellationToken cancellationToken)
    {
        foreach (var day in state.State.Days)
        {
            _days.Add(day);
        }

        return Task.CompletedTask;
    }

    public async Task RegisterDayAsync(string day)
    {
        if (string.IsNullOrWhiteSpace(day) || !_days.Add(day))
        {
            return;
        }

        state.State.Days = [.. _days];
        await state.WriteStateAsync();
    }

    /// <summary>Newest first — <c>yyyyMMdd</c> sorts lexicographically exactly as it sorts
    /// chronologically, which is the entire reason the key format is that one.</summary>
    public Task<List<string>> GetDaysAsync() =>
        Task.FromResult(_days.OrderByDescending(d => d, StringComparer.Ordinal).ToList());
}

/// <summary>
/// The runtime-neutral <see cref="IAuditFacade"/>, routed onto day grains.
///
/// <para>This is the only place the day-sharding is a fact rather than a convention: an append is routed
/// by the ENTRY's own timestamp (so a row that sat in the sink's queue across midnight lands in the day
/// it happened, not the day it was drained), a query is routed by the day the caller asked for, and the
/// day listing does not touch a day grain at all.</para>
///
/// <para>Public, unlike its sibling adapters in OrleansFacades.cs, only so that the routing rule above
/// is directly testable — it is the one behaviour of this class, and asserting it through DI would mean
/// standing up the whole host.</para>
/// </summary>
public sealed class OrleansAuditFacade(IClusterClient client) : IAuditFacade
{
    /// <summary>An entry with no timestamp is stamped with now rather than filed under 1970-01-01 —
    /// which is what <c>AuditKeyFor(0)</c> would do, creating a permanent junk day in the index that no
    /// operator could explain. Every real caller stamps <c>AtMs</c>; this is the forgotten path, and it
    /// should land on today.</summary>
    public Task AppendAsync(AuditEntry entry)
    {
        ArgumentNullException.ThrowIfNull(entry);

        if (entry.AtMs <= 0)
        {
            entry.AtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        }

        return client.GetGrain<IAuditLogGrain>(StreamConstants.AuditKeyFor(entry.AtMs)).AppendAsync(entry);
    }

    public Task<AuditPage> QueryAsync(string day, string? actor, string? actionPrefix, int limit, int offset) =>
        client.GetGrain<IAuditLogGrain>(AuditKeys.DayKey(day)).QueryAsync(day, actor, actionPrefix, limit, offset);

    public Task<List<string>> GetDaysAsync() =>
        client.GetGrain<IAuditIndexGrain>(AuditKeys.IndexKey).GetDaysAsync();
}
