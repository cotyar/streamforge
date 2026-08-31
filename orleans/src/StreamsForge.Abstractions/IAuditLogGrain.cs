namespace StreamsForge.Abstractions;

// ============================================================================
// Plan 015 (RBAC → entitlements) W4-B: the Orleans half of the audit log.
//
// Two interfaces, because "one day's entries" and "which days exist" are two different residency
// stories and the second one must never wake the first (see IAuditIndexGrain).
// ============================================================================

/// <summary>ONE DAY of audit, keyed <see cref="StreamConstants.AuditKeyFor"/> (<c>audit:yyyyMMdd</c>),
/// so a day activates only when written to or read and is collected when idle — the mechanism plan
/// 011-D1 established for <c>TableShardGrain</c>, and for the same reason: an audit log that pinned
/// every day it had ever seen into memory would be a slow memory leak with a retention policy.
///
/// <para>Like <c>TableShardGrain</c>, the implementation must never call <c>DelayDeactivation</c> — that
/// is the whole feature, and a grain that pinned itself would deliver nothing while appearing to
/// work.</para>
///
/// <para>Plan 005's seam rule: every member lives on the runtime-neutral <see cref="IAuditFacade"/>. Note
/// that this puts <see cref="IAuditFacade.GetDaysAsync"/> on a grain that is ONE day — it answers by
/// delegating to <see cref="IAuditIndexGrain"/>, because a day grain cannot enumerate its siblings and
/// must not try.</para></summary>
public interface IAuditLogGrain : IAuditFacade, IGrainWithStringKey
{
}

/// <summary>The day index — the answer to "where does <see cref="IAuditFacade.GetDaysAsync"/> read
/// from", whose doc comment promises it "reads an index, not the shards".
///
/// <para><b>Why a separate singleton and not the day grains themselves.</b> A day grain cannot enumerate
/// its siblings: Orleans has no "list activations of this type with this key prefix" that does not
/// either scan storage or wake grains, and waking every day that has ever existed to ask each one
/// whether it exists is precisely the cost day-sharding was introduced to avoid. Nor can the index live
/// on the access-policy singleton, which is read on every request and cached by every replica — an
/// audit write must never bump a version the whole cluster polls.</para>
///
/// <para>One tiny grain (key <see cref="AuditKeys.IndexKey"/>) holding a set of <c>yyyyMMdd</c> strings,
/// written by a day grain the first time that day is written to in this activation, and by nobody else.
/// It is O(days) forever — a decade of daily operation is ~3 650 short strings — so it is deliberately
/// not bounded.</para></summary>
public interface IAuditIndexGrain : IGrainWithStringKey
{
    /// <summary>Idempotent. Called by a day grain on its first write, so the steady-state cost is one
    /// grain call per day per activation, not one per entry.</summary>
    Task RegisterDayAsync(string day);

    /// <summary>Days with at least one recorded entry, <c>yyyyMMdd</c>, newest first.</summary>
    Task<List<string>> GetDaysAsync();
}

/// <summary>The audit keys this flavour needs and <see cref="StreamConstants"/> does not carry.
///
/// <para>Here rather than in the shared contracts because the index is an ORLEANS implementation
/// detail: the Dapr twin can answer "which days" however its store makes cheapest (a Redis
/// <c>SCAN</c> over a key prefix, for instance) and owes nothing to this grain's existence. Only the
/// two things both flavours must agree on — the <c>audit:</c> prefix and the <c>yyyyMMdd</c> key
/// derivation — live in <see cref="StreamConstants"/>, and they already did.</para></summary>
public static class AuditKeys
{
    /// <summary>Under the <c>audit:</c> prefix so everything audit sorts together in a state directory,
    /// and unmistakable for a day: <c>StreamConstants.AuditKeyFor</c> only ever produces eight digits,
    /// so <c>audit:index</c> can never collide with one. (It is a different grain TYPE in any case, so a
    /// collision would need both a name clash and a type clash.)</summary>
    public const string IndexKey = StreamConstants.AuditKeyPrefix + "index";

    /// <summary>The grain key one <c>yyyyMMdd</c> day lives under. Accepts a day that already carries
    /// the prefix, because "day" arrives from a REST query string and a caller that pasted a grain key
    /// meant the same day.</summary>
    public static string DayKey(string day) =>
        day.StartsWith(StreamConstants.AuditKeyPrefix, StringComparison.Ordinal)
            ? day
            : StreamConstants.AuditKeyPrefix + day;
}
