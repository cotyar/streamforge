using Xunit;
using static StreamForge.Engine.Tests.TestHelpers;

namespace StreamForge.Engine.Tests;

/// <summary>
/// VAR_SAMP / VAR_POP / STDDEV_SAMP / STDDEV_POP / MEDIAN. Every one of them is built to subtract,
/// because the table path maintains a Z-set — a row superseded by LATEST BY comes back with weight −1 —
/// and an aggregate that only adds drifts silently rather than failing.
/// </summary>
public class StatAggregateTests
{
    private static readonly SourceSchema Samples = Schema(
        "samples", ("g", FieldKind.String), ("x", FieldKind.Double), ("n", FieldKind.Long));

    /// <summary>Folds a set of values through a windowed pipeline aggregate and returns the last result.</summary>
    private static object? Fold(string expr, params double[] values)
    {
        var exec = CompileAndCreate(
            $"SELECT g, {expr} AS y FROM samples GROUP BY g WINDOW TUMBLING(SIZE 1 HOURS) EMIT CHANGES", Samples);
        object? last = null;
        long ts = 1000;
        foreach (var v in values)
        {
            var rows = exec.OnEvent("samples", Evt(ts++, "samples", ("g", "A"), ("x", v), ("n", 1L)));
            if (rows.Count > 0) last = rows[^1]["y"];
        }
        return last;
    }

    // The textbook sample: mean 5, population variance 4, sample variance 32/7.
    private static readonly double[] Textbook = [2, 4, 4, 4, 5, 5, 7, 9];

    [Fact]
    public void Variance_and_stddev_match_their_textbook_definitions()
    {
        Assert.Equal(4.0, Assert.IsType<double>(Fold("VAR_POP(x)", Textbook)), 10);
        Assert.Equal(2.0, Assert.IsType<double>(Fold("STDDEV_POP(x)", Textbook)), 10);
        Assert.Equal(32.0 / 7.0, Assert.IsType<double>(Fold("VAR_SAMP(x)", Textbook)), 10);
        Assert.Equal(Math.Sqrt(32.0 / 7.0), Assert.IsType<double>(Fold("STDDEV_SAMP(x)", Textbook)), 10);
    }

    /// <summary>The bare spellings mean the sample forms, which is what SQL means by them.</summary>
    [Fact]
    public void STDDEV_and_VARIANCE_alias_the_sample_forms()
    {
        Assert.Equal(32.0 / 7.0, Assert.IsType<double>(Fold("VARIANCE(x)", Textbook)), 10);
        Assert.Equal(Math.Sqrt(32.0 / 7.0), Assert.IsType<double>(Fold("STDDEV(x)", Textbook)), 10);
    }

    /// <summary>A sample variance of one observation is undefined, not zero.</summary>
    [Fact]
    public void Sample_variance_of_a_single_observation_is_null_while_the_population_form_is_zero()
    {
        Assert.Null(Fold("VAR_SAMP(x)", 7.0));
        Assert.Equal(0.0, Assert.IsType<double>(Fold("VAR_POP(x)", 7.0)), 10);
    }

    /// <summary>The reason the moments carry an offset. Σx² over values near 1e8 throws away most of a
    /// double's mantissa before the subtraction, so the naive form returns garbage (often a negative)
    /// for a spread this small. Shifting by a constant keeps every term a plain sum — still subtractable,
    /// which Welford's stable update is not — while keeping the sums small.</summary>
    [Fact]
    public void Variance_survives_large_values_with_a_small_spread()
    {
        Assert.Equal(2.0 / 3.0, Assert.IsType<double>(Fold("VAR_POP(x)", 1e8, 1e8 + 1, 1e8 + 2)), 9);
        Assert.Equal(1.0, Assert.IsType<double>(Fold("VAR_SAMP(x)", 1e8, 1e8 + 1, 1e8 + 2)), 9);
        // Identical values must give exactly zero, never a rounding-negative that NaNs the square root.
        Assert.Equal(0.0, Assert.IsType<double>(Fold("STDDEV_POP(x)", 1e8, 1e8, 1e8)), 12);
    }

    [Fact]
    public void Median_interpolates_between_the_two_middle_observations()
    {
        Assert.Equal(2.0, Assert.IsType<double>(Fold("MEDIAN(x)", 1, 2, 3)), 10);
        Assert.Equal(2.5, Assert.IsType<double>(Fold("MEDIAN(x)", 1, 2, 3, 4)), 10);
        Assert.Equal(5.0, Assert.IsType<double>(Fold("MEDIAN(x)", 9, 1, 5)), 10); // order of arrival is irrelevant
        Assert.Equal(4.0, Assert.IsType<double>(Fold("MEDIAN(x)", 4, 4, 4)), 10); // a run wider than the rank
    }

    [Fact]
    public void A_statistical_aggregate_is_a_double_even_over_integer_input()
    {
        var r = Compile("SELECT g, STDDEV_SAMP(n) AS y FROM samples GROUP BY g WINDOW TUMBLING(SIZE 1 HOURS)", Samples);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics.Select(d => d.Message)));
        Assert.Equal(FieldKind.Double, r.OutputSchema!.Fields["y"]);
    }

    // ------------------------------------------------------------------
    // The half that actually matters: retraction
    // ------------------------------------------------------------------

    /// <summary>Drives values through a LATEST BY table into a rolling aggregate, so a repeated key
    /// retracts its previous value with weight −1 before asserting the new one.</summary>
    [Theory]
    [InlineData("VAR_POP(x)")]
    [InlineData("STDDEV_SAMP(x)")]
    [InlineData("MEDIAN(x)")]
    public void A_superseded_row_is_subtracted_back_out(string expr)
    {
        var latest = CompileTable("SELECT g, x FROM samples LATEST BY (g)", Samples);
        Assert.True(latest.Ok, string.Join(";", latest.Diagnostics));
        var latestExec = latest.Plan!.CreateExecutor();

        var rolled = CompileTable($"SELECT {expr} AS y FROM latest_x", [], [new SourceSchema("latest_x", latest.OutputSchema!.Fields)]);
        Assert.True(rolled.Ok, string.Join(";", rolled.Diagnostics));
        var rolledExec = rolled.Plan!.CreateExecutor();

        long ts = 1000;
        void Push(string key, double value)
        {
            foreach (var delta in latestExec.OnStreamEvent("samples", Evt(ts++, "samples", ("g", key), ("x", value), ("n", 1L))))
            {
                rolledExec.OnTableDelta("latest_x", delta);
            }
        }

        // Three keys carrying a wrong value first, then corrected to 1/2/3 one at a time.
        Push("a", 100); Push("b", 200); Push("c", 300);
        Push("a", 1); Push("b", 2); Push("c", 3);

        var actual = Assert.Single(rolledExec.Snapshot()).Value.Row["y"];

        // What a from-scratch aggregate over exactly {1,2,3} produces — if any retraction leaked, the
        // 100/200/300 contributions would still be in here and this would be wildly off.
        var expected = Fold(expr, 1, 2, 3);
        Assert.Equal(Assert.IsType<double>(expected), Assert.IsType<double>(actual), 9);
    }

    /// <summary>Retracting everything must leave the aggregate empty rather than at a stale last value —
    /// the multiset has to prune entries at zero, not merely stop counting them.</summary>
    [Fact]
    public void Retracting_every_contribution_empties_the_aggregate()
    {
        var median = new StreamForge.Engine.Runtime.MedianZAggregator();
        median.Apply(1.0, 1);
        median.Apply(3.0, 1);
        Assert.Equal(2.0, Assert.IsType<double>(median.Result), 10);
        median.Apply(1.0, -1);
        Assert.Equal(3.0, Assert.IsType<double>(median.Result), 10);
        median.Apply(3.0, -1);
        Assert.Null(median.Result);
    }
}
