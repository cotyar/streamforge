using StreamsForge.Abstractions;

namespace StreamsForge.Dapr.Host.Access;

/// <summary>One day's audit shard: the entries, and how many were dropped to make room for them.
/// <see cref="Truncated"/> is persisted and cumulative — it is the whole reason drop-oldest is
/// acceptable at all.</summary>
public sealed class AuditDayState
{
    public List<AuditEntry> Entries { get; set; } = [];
    public long Truncated { get; set; }
}

/// <summary>
/// Plan 015 W4-C: actor-framework-free audit storage behind <see cref="Actors.AuditLogActor"/>, one
/// instance per day shard. Same pure-store-behind-a-thin-actor split as <see cref="AccessPolicyStore"/>
/// and <see cref="ApprovalStore"/>, and unit-tested the same way — no sidecar, no Redis
/// (dapr/tests/StreamsForge.Dapr.Tests/AuditLogStoreTests.cs).
///
/// <para><b>Append-only, and structurally so.</b> There is no update method and no delete method, so
/// there is no way to write one from outside this class — the closest thing to a delete is drop-oldest,
/// which cannot be aimed at a particular entry and which counts what it removed. An audit log a caller
/// can edit is not an audit log.</para>
///
/// <para><b>Bounded, and honest about it.</b> <c>Audit:MaxEntriesPerDay</c> (default
/// <see cref="DefaultMaxEntriesPerDay"/>) caps a day; past that the oldest go and
/// <see cref="AuditDayState.Truncated"/> counts them, for ever — it never resets, and every
/// <see cref="AuditPage"/> carries it. Silence must never be mistaken for absence: a page of 20 000
/// entries that says <c>Truncated = 0</c> and one that says <c>Truncated = 4 117</c> are different
/// answers to "is this everything?", and without the counter they would look identical.</para>
/// </summary>
public sealed class AuditLogStore(AuditDayState state, int maxEntriesPerDay)
{
    /// <summary><c>Audit:MaxEntriesPerDay</c>, plan 015's stated default.</summary>
    public const int DefaultMaxEntriesPerDay = 20_000;

    public const string MaxEntriesPerDayKey = "Audit:MaxEntriesPerDay";

    /// <summary>Page size for a <see cref="Query"/> that asks for no limit. A day can hold 20 000
    /// entries; answering "everything" by default would put all of them on one wire.</summary>
    public const int DefaultPageSize = 100;

    /// <summary>The actor id of the day INDEX — the one <see cref="Actors.AuditLogActor"/> that holds no
    /// entries and instead lists which days have any. It shares the day shards' <c>audit:</c> prefix and
    /// cannot collide with one: no day formats as "index".</summary>
    public const string IndexActorId = StreamConstants.AuditKeyPrefix + "index";

    // A non-positive cap falls back to the default rather than clamping to 1. "Audit but discard almost
    // all of it" is not a thing anyone means; "audit nothing" is spelled Audit:Enabled=false. The Orleans
    // twin reads the setting the same way, and it has to: a misconfigured host that kept 20 000 rows on
    // one flavour and 1 on the other would be a security log that disagrees with itself.
    private readonly int _max = maxEntriesPerDay > 0 ? maxEntriesPerDay : DefaultMaxEntriesPerDay;

    public AuditDayState State => state;

    public int MaxEntriesPerDay => _max;

    /// <summary>The day shard an entry with this timestamp belongs to —
    /// <see cref="StreamConstants.AuditKeyFor"/>, UTC, <c>audit:yyyyMMdd</c>. Public and static so the
    /// facade's routing is the same function the tests assert on rather than a second copy of it.</summary>
    public static string ActorIdFor(long atMs) => StreamConstants.AuditKeyFor(atMs);

    /// <summary>The day shard for a <c>yyyyMMdd</c> string, which is what
    /// <see cref="IAuditFacade.QueryAsync"/> is given.</summary>
    public static string ActorIdForDay(string day) => StreamConstants.AuditKeyPrefix + day;

    /// <summary>The <c>yyyyMMdd</c> a shard actor id names.</summary>
    public static string DayOf(string actorId) =>
        actorId.StartsWith(StreamConstants.AuditKeyPrefix, StringComparison.Ordinal)
            ? actorId[StreamConstants.AuditKeyPrefix.Length..]
            : actorId;

    /// <summary>Append one entry, dropping the oldest if the day is full.</summary>
    /// <returns>True when this was the day's FIRST entry — the signal the actor uses to register the day
    /// with the index exactly once, instead of on every write.</returns>
    public bool Append(AuditEntry entry)
    {
        ArgumentNullException.ThrowIfNull(entry);

        var first = state.Entries.Count == 0;
        state.Entries.Add(entry);

        var over = state.Entries.Count - _max;
        if (over > 0)
        {
            // One RemoveRange rather than a RemoveAt per entry: the normal case removes exactly one, and
            // the abnormal case (the cap was lowered under an existing shard) is then also one shift
            // instead of thousands.
            state.Entries.RemoveRange(0, over);
            state.Truncated += over;
        }

        return first;
    }

    /// <summary>
    /// One page. Exact-match on <paramref name="actor"/>, prefix-match on
    /// <paramref name="actionPrefix"/> — <see cref="IAuditFacade.QueryAsync"/> is explicit that "anything
    /// richer is a query engine this platform already is, one layer up".
    ///
    /// <para><b>Newest first.</b> An audit page is read to answer "what just happened", and a truncated
    /// page whose useful half is the tail would be the wrong half. <see cref="AuditPage.Total"/> counts
    /// everything that matched the filters before <paramref name="limit"/>/<paramref name="offset"/>, so
    /// a caller can page without guessing, and <see cref="AuditPage.Truncated"/> is the day's cumulative
    /// drop count — a property of the shard, never of the page, so it does not move as you page.</para>
    /// </summary>
    public AuditPage Query(string? actor, string? actionPrefix, int limit, int offset)
    {
        IEnumerable<AuditEntry> matched = state.Entries;

        if (!string.IsNullOrWhiteSpace(actor))
        {
            matched = matched.Where(e => string.Equals(e.Actor, actor, StringComparison.Ordinal));
        }

        if (!string.IsNullOrWhiteSpace(actionPrefix))
        {
            matched = matched.Where(e => e.Action.StartsWith(actionPrefix, StringComparison.Ordinal));
        }

        var all = matched.ToList();

        return new AuditPage
        {
            Total = all.Count,
            Truncated = state.Truncated,
            Entries = Enumerable.Reverse(all)
                .Skip(Math.Max(0, offset))
                .Take(limit <= 0 ? DefaultPageSize : limit)
                .ToList(),
        };
    }
}
