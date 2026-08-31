using StreamsForge.Abstractions;
using StreamsForge.Dapr.Host.Access;
using Xunit;

namespace StreamsForge.Dapr.Tests;

/// <summary>
/// Plan 015 W4-C: unit tests for the day-sharded audit store. No sidecar, no actor runtime, no Redis —
/// <see cref="AuditLogStore"/> is a plain class over an in-memory <see cref="AuditDayState"/>, the same
/// split the access-policy and approval stores use.
///
/// <para>Two properties carry the weight. <b>Truncated</b>: drop-oldest is only acceptable because the
/// count of what it dropped is persisted and surfaced, so silence is never mistaken for absence — a page
/// that says nothing happened and a page that dropped four thousand rows must not look alike. And
/// <b>the day key</b>: entries a day apart must land in different shards, because the whole eviction
/// story (and <c>GetDaysAsync</c>) is built on one actor per day.</para>
/// </summary>
public class AuditLogStoreTests
{
    // 2026-08-19T12:00:00Z and 2026-08-20T12:00:00Z.
    private const long Day1Noon = 1_787_140_800_000;
    private const long Day2Noon = Day1Noon + 86_400_000;

    private static (AuditDayState State, AuditLogStore Store) NewStore(int max = 100)
    {
        var state = new AuditDayState();
        return (state, new AuditLogStore(state, max));
    }

    private static AuditEntry Entry(string action = "pipeline.update", string actor = "alice", long atMs = Day1Noon) => new()
    {
        Id = Guid.NewGuid().ToString("n"),
        AtMs = atMs,
        Actor = actor,
        Action = action,
        Scope = "prod-orders",
        Outcome = "allowed",
    };

    // -------------------------------------------------------------------------------- sharding

    [Fact]
    public void TwoDifferentDays_LandInTwoDifferentKeys()
    {
        var first = AuditLogStore.ActorIdFor(Day1Noon);
        var second = AuditLogStore.ActorIdFor(Day2Noon);

        Assert.Equal("audit:20260819", first);
        Assert.Equal("audit:20260820", second);
        Assert.NotEqual(first, second);

        // Same day, twelve hours apart: one shard. The boundary is UTC, so it cannot move with the host's
        // locale on a redeploy.
        Assert.Equal(first, AuditLogStore.ActorIdFor(Day1Noon - 43_200_000));
        Assert.Equal(first, AuditLogStore.ActorIdFor(Day1Noon + 43_199_999));
    }

    [Fact]
    public void DayKeyRoundTrips_AndTheIndexIsNotADay()
    {
        Assert.Equal("audit:20260819", AuditLogStore.ActorIdForDay("20260819"));
        Assert.Equal("20260819", AuditLogStore.DayOf("audit:20260819"));

        // The index shares the prefix and can never collide with a yyyyMMdd shard.
        Assert.Equal("audit:index", AuditLogStore.IndexActorId);
        Assert.NotEqual(AuditLogStore.IndexActorId, AuditLogStore.ActorIdFor(Day1Noon));
    }

    // -------------------------------------------------------------------------------- appending

    [Fact]
    public void Append_ReportsOnlyTheFirstEntryOfADay()
    {
        // The actor registers the day with the index exactly on this signal — once per day, not once per
        // audit row.
        var (_, store) = NewStore();

        Assert.True(store.Append(Entry()));
        Assert.False(store.Append(Entry()));
        Assert.False(store.Append(Entry()));
    }

    [Fact]
    public void Append_DropsOldestAndCountsWhatItDropped()
    {
        var (state, store) = NewStore(max: 3);

        store.Append(Entry(action: "a1"));
        store.Append(Entry(action: "a2"));
        store.Append(Entry(action: "a3"));

        Assert.Equal(0, state.Truncated);

        store.Append(Entry(action: "a4"));

        Assert.Equal(3, state.Entries.Count);
        Assert.Equal(["a2", "a3", "a4"], state.Entries.Select(e => e.Action));
        Assert.Equal(1, state.Truncated);

        store.Append(Entry(action: "a5"));
        store.Append(Entry(action: "a6"));

        // Cumulative, and it never resets: this is the number that says "there is a gap here".
        Assert.Equal(3, state.Truncated);
        Assert.Equal(["a4", "a5", "a6"], state.Entries.Select(e => e.Action));
    }

    [Fact]
    public void Query_SurfacesTheTruncatedCounter()
    {
        var (_, store) = NewStore(max: 2);
        store.Append(Entry(action: "a1"));
        store.Append(Entry(action: "a2"));
        store.Append(Entry(action: "a3"));

        var page = store.Query(null, null, 10, 0);

        Assert.Equal(1, page.Truncated);
        Assert.Equal(2, page.Total);
    }

    [Fact]
    public void ALoweredCap_TrimsInOneStepAndCountsEveryRowItDropped()
    {
        // The abnormal case: a shard written with the default cap, reloaded under a smaller one.
        var state = new AuditDayState();
        var wide = new AuditLogStore(state, 10);
        for (var i = 0; i < 10; i++)
        {
            wide.Append(Entry(action: $"a{i}"));
        }

        var narrow = new AuditLogStore(state, 3);
        narrow.Append(Entry(action: "a10"));

        Assert.Equal(3, state.Entries.Count);
        Assert.Equal(8, state.Truncated);
    }

    /// <summary>A non-positive cap falls back to the DEFAULT, not to 1. Clamping to 1 was the first
    /// reading and it is worse in both directions: it keeps almost nothing from a misconfigured host,
    /// and — the reason it was changed — the Orleans twin reads the same setting as "fall back to the
    /// default", so a `Audit:MaxEntriesPerDay=0` deployment would have kept 20 000 rows on one flavour
    /// and 1 on the other. A security log that disagrees with itself across flavours is worse than
    /// either answer. "Audit nothing" is spelled Audit:Enabled=false.</summary>
    [Theory]
    [InlineData(0)]
    [InlineData(-5)]
    public void ANonPositiveCap_FallsBackToTheDefault_TheSameWayOrleansReadsIt(int configured)
    {
        var (state, store) = NewStore(max: configured);

        store.Append(Entry(action: "a1"));

        Assert.Equal(AuditLogStore.DefaultMaxEntriesPerDay, store.MaxEntriesPerDay);
        Assert.Single(state.Entries);
    }

    // -------------------------------------------------------------------------------- querying

    [Fact]
    public void Query_FiltersExactOnActorAndPrefixOnAction()
    {
        var (_, store) = NewStore();
        store.Append(Entry(action: "pipeline.update", actor: "alice"));
        store.Append(Entry(action: "pipeline.delete", actor: "bob"));
        store.Append(Entry(action: "source.delete", actor: "alice"));

        Assert.Equal(2, store.Query("alice", null, 10, 0).Total);
        Assert.Equal(0, store.Query("ALICE", null, 10, 0).Total);      // exact, ordinal
        Assert.Equal(2, store.Query(null, "pipeline.", 10, 0).Total);
        Assert.Equal(1, store.Query("alice", "pipeline.", 10, 0).Total);
        Assert.Equal(3, store.Query(null, null, 10, 0).Total);
    }

    [Fact]
    public void Query_IsNewestFirstAndPagesWithLimitAndOffset()
    {
        var (_, store) = NewStore();
        for (var i = 0; i < 5; i++)
        {
            store.Append(Entry(action: $"a{i}"));
        }

        var first = store.Query(null, null, 2, 0);
        Assert.Equal(["a4", "a3"], first.Entries.Select(e => e.Action));
        Assert.Equal(5, first.Total);

        var second = store.Query(null, null, 2, 2);
        Assert.Equal(["a2", "a1"], second.Entries.Select(e => e.Action));

        // Total counts everything that matched the filters, not the page.
        Assert.Equal(5, second.Total);

        Assert.Empty(store.Query(null, null, 2, 99).Entries);
    }

    [Fact]
    public void Query_WithNoLimit_FallsBackToAPageRatherThanTheWholeDay()
    {
        var (_, store) = NewStore(max: 1000);
        for (var i = 0; i < AuditLogStore.DefaultPageSize + 5; i++)
        {
            store.Append(Entry(action: $"a{i}"));
        }

        Assert.Equal(AuditLogStore.DefaultPageSize, store.Query(null, null, 0, 0).Entries.Count);
        Assert.Equal(AuditLogStore.DefaultPageSize + 5, store.Query(null, null, 0, 0).Total);
    }

    [Fact]
    public void Query_OnAnEmptyDay_IsAnEmptyPageNotAFailure()
    {
        var (_, store) = NewStore();

        var page = store.Query("alice", "pipeline.", 10, 0);

        Assert.Empty(page.Entries);
        Assert.Equal(0, page.Total);
        Assert.Equal(0, page.Truncated);
    }
}
