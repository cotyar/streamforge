using StreamForge.Engine.Runtime;
using Xunit;
using static StreamForge.Engine.Tests.TestHelpers;

namespace StreamForge.Engine.Tests;

/// <summary>
/// What happens when a GROUP BY table is attached to an upstream that ALREADY HOLDS ROWS and those rows
/// are never replayed to it.
///
/// <para>LATEST BY emits a bare assert on first sight of a key and retract(−1)+assert(+1) thereafter, so
/// a consumer that attached late receives a RETRACTION for a row it has never seen. That is not an
/// internal inconsistency — the deltas are individually well-formed — it is missing history, and no
/// arithmetic downstream can invent it. The only real fix is to replay the upstream's current contents
/// on attach; these tests pin what happens until that exists, which is the difference between an
/// obviously-empty answer and a convincingly wrong one.</para>
/// </summary>
public class AggregateOverWarmTableTests
{
    private static readonly SourceSchema Ticks = Schema("ticks", ("g", FieldKind.String), ("x", FieldKind.Double));

    /// <summary>Builds `rolled` over a LATEST BY table WITHOUT replaying what the LATEST BY already holds
    /// — exactly the state a table created mid-session lands in.</summary>
    private static (TableExecutor Latest, TableExecutor Rolled, SourceSchema RolledInput) BuildLate(string rolledSql)
    {
        var latest = CompileTable("SELECT g, x FROM ticks LATEST BY (g)", Ticks);
        Assert.True(latest.Ok, string.Join(";", latest.Diagnostics));
        var input = new SourceSchema("latest_x", latest.OutputSchema!.Fields);

        var rolled = CompileTable(rolledSql, [], [input]);
        Assert.True(rolled.Ok, string.Join(";", rolled.Diagnostics));
        return (latest.Plan!.CreateExecutor(), rolled.Plan!.CreateExecutor(), input);
    }

    /// <summary>The reported symptom: the aggregate used to report ONE contributing row per group,
    /// however many the upstream actually held, because the unmatched retraction destroyed the empty
    /// group and the assert that followed rebuilt it from that single row. A count of 1 out of 3 is the
    /// kind of wrong answer nobody questions. It now reports nothing at all until the missing asserts
    /// are supplied — the same information, impossible to mistake for an answer.</summary>
    [Fact]
    public void An_aggregate_attached_to_a_populated_table_reports_nothing_rather_than_a_plausible_wrong_count()
    {
        var (latest, rolled, _) = BuildLate("SELECT g, COUNT(*) AS n FROM latest_x GROUP BY g");

        long ts = 1000;
        // Three keys land in the LATEST BY while `rolled` is NOT yet listening.
        foreach (var key in new[] { "a", "b", "c" })
        {
            latest.OnStreamEvent("ticks", Evt(ts++, "ticks", ("g", key), ("x", 1.0)));
        }

        // `rolled` attaches here and now sees every subsequent delta — for an existing key that is
        // retract(old) then assert(new).
        foreach (var delta in latest.OnStreamEvent("ticks", Evt(ts++, "ticks", ("g", "a"), ("x", 2.0))))
        {
            rolled.OnTableDelta("latest_x", delta);
        }

        Assert.Empty(rolled.Snapshot());
        Assert.Equal(1, rolled.UnmatchedRetractions);

        // And it stays stable rather than flapping between one row and none on every later change.
        for (int i = 0; i < 3; i++)
        {
            foreach (var delta in latest.OnStreamEvent("ticks", Evt(ts++, "ticks", ("g", "a"), ("x", 3.0 + i))))
            {
                rolled.OnTableDelta("latest_x", delta);
            }
            Assert.Empty(rolled.Snapshot());
        }
        Assert.Equal(4, rolled.UnmatchedRetractions);
    }

    /// <summary>The property that makes a future backfill safe: the group's arithmetic is only PAUSED,
    /// not corrupted. Feeding the asserts the consumer missed brings it to the right answer, which it
    /// could not do if the unmatched retraction had thrown the group away.</summary>
    [Fact]
    public void Supplying_the_missed_asserts_afterwards_converges_to_the_right_answer()
    {
        var (latest, rolled, _) = BuildLate("SELECT g, COUNT(*) AS n FROM latest_x GROUP BY g");

        long ts = 1000;
        var missed = new List<TableDelta>();
        foreach (var key in new[] { "a", "a", "a" })
        {
            // Same group three times over: the LATEST BY collapses them to one key, but the aggregate
            // below groups on a constant, so what matters is that `rolled` never saw these asserts.
            missed.AddRange(latest.OnStreamEvent("ticks", Evt(ts++, "ticks", ("g", key), ("x", 1.0))));
        }

        foreach (var delta in latest.OnStreamEvent("ticks", Evt(ts++, "ticks", ("g", "a"), ("x", 9.0))))
        {
            rolled.OnTableDelta("latest_x", delta);
        }
        Assert.Empty(rolled.Snapshot());

        // The backfill that does not exist yet, done by hand: replay every delta the consumer missed,
        // retractions included. Replaying only the asserts would over-count — the three pushes above are
        // one assert followed by two retract+assert pairs, so the history nets to a single row.
        foreach (var delta in missed)
        {
            rolled.OnTableDelta("latest_x", delta);
        }

        var row = Assert.Single(rolled.Snapshot());
        Assert.Equal(1L, row.Value.Row["n"]);
    }

    /// <summary>The counter must stay at zero for a table built the ordinary way, or it is noise rather
    /// than a signal.</summary>
    [Fact]
    public void A_table_attached_before_its_upstream_had_rows_reports_no_unmatched_retractions()
    {
        var (latest, rolled, _) = BuildLate("SELECT g, COUNT(*) AS n FROM latest_x GROUP BY g");

        long ts = 1000;
        foreach (var key in new[] { "a", "b", "a", "c", "b" })
        {
            foreach (var delta in latest.OnStreamEvent("ticks", Evt(ts++, "ticks", ("g", key), ("x", 1.0))))
            {
                rolled.OnTableDelta("latest_x", delta);
            }
        }

        Assert.Equal(0, rolled.UnmatchedRetractions);
        Assert.Equal(3, rolled.Snapshot().Count);
    }

    [Fact]
    public void A_table_with_no_group_by_reports_minus_one_rather_than_a_misleading_zero()
    {
        var (_, rolled, _) = BuildLate("SELECT g, x FROM latest_x");
        Assert.Equal(-1, rolled.UnmatchedRetractions);
    }
}
