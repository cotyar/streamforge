using Dapr.Actors;
using StreamsForge.Abstractions;

namespace StreamsForge.Dapr.Host.Actors;

// Plan 015 W4-C. One request payload, for the same reason as everywhere else on this flavour: a Dapr
// actor method takes at most one parameter.

public sealed record AuditQueryActorRequest(string? Actor, string? ActionPrefix, int Limit, int Offset);

/// <summary>
/// Actor-invocation surface for the day-sharded audit log — the Dapr counterpart of Orleans'
/// <c>IAuditLogGrain</c>, adapted onto <see cref="IAuditFacade"/> by
/// <see cref="Facades.DaprAuditFacade"/>.
///
/// <para><b>One actor per day</b>, id = <see cref="StreamConstants.AuditKeyFor"/> →
/// <c>audit:yyyyMMdd</c>, so a day is activated only when it is written to or read and is evicted when
/// idle — the mechanism plan 011-D1 established for <c>TableShardGrain</c>.</para>
///
/// <para><b>And one extra instance that holds no entries: the day index</b>, at
/// <see cref="Access.AuditLogStore.IndexActorId"/> ("audit:index").
/// <see cref="IAuditFacade.GetDaysAsync"/> promises to be cheap because "it reads an index, not the
/// shards", and an actor cannot enumerate its siblings — Dapr has no actor-id listing at all, and even
/// Orleans' does not survive eviction — so the list has to be WRITTEN somewhere as days appear. It lives
/// in this same actor type at a reserved id, rather than in a fourth actor type, for two reasons: the
/// registration surface stays at exactly two new actors (this one and
/// <see cref="ApprovalActor"/>), and "the audit log knows which days it has" is one concept, not two
/// components. The cost is that four methods share one interface where each id only answers two of them;
/// the unmatched pairs throw rather than silently doing nothing.</para>
///
/// <para><b>The index is written once per day, by the day itself.</b>
/// <see cref="Access.AuditLogStore.Append"/> reports whether an entry was the day's first, and only then
/// does the shard call <see cref="RegisterDayAsync"/> — so the index costs one extra actor call per day,
/// not one per audit row. Registering from the facade instead would have meant a call on EVERY append.</para>
/// </summary>
public interface IAuditLogActor : IActor
{
    /// <summary>Append-only: there is no update and no delete on this interface, and none in the store
    /// behind it. Called from a bounded in-process channel with drop-on-overflow, off the request path —
    /// audit must never make a request fail or slow.</summary>
    Task AppendAsync(AuditEntry entry);

    /// <summary>One page of this day. Exact-match actor, prefix-match action, newest first.</summary>
    Task<AuditPage> QueryAsync(AuditQueryActorRequest request);

    /// <summary>Index only. Idempotent — a day already listed is not listed twice and costs no write.</summary>
    Task RegisterDayAsync(string day);

    /// <summary>Index only. The <c>yyyyMMdd</c> days that have at least one entry, ascending.</summary>
    Task<List<string>> GetDaysAsync();
}
