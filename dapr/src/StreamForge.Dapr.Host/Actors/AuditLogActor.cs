using Dapr.Actors;
using Dapr.Actors.Client;
using Dapr.Actors.Runtime;
using StreamForge.Abstractions;
using StreamForge.Dapr.Host.Access;

namespace StreamForge.Dapr.Host.Actors;

/// <summary>
/// Plan 015 W4-C: the day-sharded audit log — Dapr counterpart of Orleans' <c>AuditLogGrain</c>. See
/// <see cref="IAuditLogActor"/> for the sharding and for why the day index is one instance of this same
/// type at <see cref="AuditLogStore.IndexActorId"/> rather than a fourth actor type.
///
/// <para><b>Thin, like its two siblings.</b> The cap, the drop-oldest, the persisted
/// <c>Truncated</c> counter and the filtering all live in <see cref="AuditLogStore"/>, a plain class over
/// an in-memory <see cref="AuditDayState"/>; this actor loads, saves, and — once per day — tells the
/// index that the day exists.</para>
///
/// <para><b>A failing index write never fails an append.</b> The entry is already persisted in its shard
/// by then, so the worst case is a day missing from <see cref="IAuditFacade.GetDaysAsync"/> while its
/// rows are perfectly readable by <see cref="IAuditFacade.QueryAsync"/>. Letting the append throw
/// instead would trade a listing gap for a lost audit row, which is the wrong way round.
/// <br/>ponytail: nothing repairs such a gap. Ceiling: a day whose index write failed stays invisible in
/// the day picker for ever. Upgrade path: have the index re-register on any query for a day it does not
/// know, which is two lines in <see cref="Facades.DaprAuditFacade"/> — not built, because the failure
/// needs the state store to be down at exactly the first write of a day.</para>
/// </summary>
public sealed class AuditLogActor(ActorHost host, IConfiguration configuration, ILogger<AuditLogActor> logger)
    : Actor(host), IAuditLogActor
{
    private const string DayStateName = "day";
    private const string DaysStateName = "days";

    private bool _isIndex;
    private AuditDayState _state = new();
    private AuditLogStore _store = null!;
    private List<string> _days = [];

    protected override async Task OnActivateAsync()
    {
        _isIndex = string.Equals(Id.GetId(), AuditLogStore.IndexActorId, StringComparison.Ordinal);

        if (_isIndex)
        {
            var days = await StateManager.TryGetStateAsync<List<string>>(DaysStateName);
            _days = days.HasValue ? days.Value : [];
            return;
        }

        var existing = await StateManager.TryGetStateAsync<AuditDayState>(DayStateName);
        _state = existing.HasValue ? existing.Value : new AuditDayState();
        _store = new AuditLogStore(
            _state,
            configuration.GetValue(AuditLogStore.MaxEntriesPerDayKey, AuditLogStore.DefaultMaxEntriesPerDay));
    }

    public async Task AppendAsync(AuditEntry entry)
    {
        RefuseIndexRole();

        var first = _store.Append(entry);
        await StateManager.SetStateAsync(DayStateName, _state);

        if (!first)
        {
            return;
        }

        var day = AuditLogStore.DayOf(Id.GetId());
        try
        {
            await IndexProxy().RegisterDayAsync(day);
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "Audit day {Day} was written but could not be registered in the day index; its entries are readable, "
                + "but the day may be missing from the day list.",
                day);
        }
    }

    public Task<AuditPage> QueryAsync(AuditQueryActorRequest request)
    {
        RefuseIndexRole();
        return Task.FromResult(_store.Query(request.Actor, request.ActionPrefix, request.Limit, request.Offset));
    }

    public async Task RegisterDayAsync(string day)
    {
        RefuseDayRole();

        if (string.IsNullOrWhiteSpace(day) || _days.Contains(day, StringComparer.Ordinal))
        {
            return;
        }

        _days.Add(day);
        _days.Sort(StringComparer.Ordinal);   // yyyyMMdd sorts lexicographically = chronologically
        await StateManager.SetStateAsync(DaysStateName, _days);
    }

    public Task<List<string>> GetDaysAsync()
    {
        RefuseDayRole();
        return Task.FromResult(new List<string>(_days));
    }

    private static IAuditLogActor IndexProxy() =>
        ActorProxy.Create<IAuditLogActor>(
            new ActorId(AuditLogStore.IndexActorId), nameof(AuditLogActor), ActorProxyDefaults.Options);

    /// <summary>The index instance holds no entries and has no store; an entry routed to it would be a
    /// routing bug, and a routing bug that silently succeeded would put audit rows where nothing looks
    /// for them.</summary>
    private void RefuseIndexRole()
    {
        if (_isIndex)
        {
            throw new InvalidOperationException(
                $"'{AuditLogStore.IndexActorId}' is the audit day index and holds no entries");
        }
    }

    private void RefuseDayRole()
    {
        if (!_isIndex)
        {
            throw new InvalidOperationException(
                $"'{Id.GetId()}' is an audit day shard; the day index is '{AuditLogStore.IndexActorId}'");
        }
    }
}
