using StreamForge.Engine.Runtime;
using StreamForge.Engine.Sql;
using Xunit;
using static StreamForge.Engine.Tests.TestHelpers;

namespace StreamForge.Engine.Tests;

/// <summary>
/// The extension seam: an assembly outside the Engine adds a scalar function or an aggregate without
/// editing it. These tests ARE the seam's consumer proof — they register through the same public API
/// StreamForge.Quant uses and then drive the result through real SQL, both ways the dialect runs it
/// (a pipeline folding forward over a stream, and a table maintaining a Z-set where a superseded row
/// comes back with weight -1).
/// </summary>
public class SqlFunctionRegistryTests : IDisposable
{
    /// <summary>Deliberately not a plausible future built-in — the registry is process-global, so a
    /// name another test might one day want would make these tests interfere with it.</summary>
    private const string ScalarName = "TEST_HYPOT";
    private const string AggregateName = "TEST_RANGE";

    public SqlFunctionRegistryTests()
    {
        SqlFunctions.Register(new HypotFunction());
        SqlFunctions.Register(new RangeAggregate());
    }

    public void Dispose() => SqlFunctions.Clear();

    private static readonly SourceSchema Mixed = Schema(
        "mixed", ("s", FieldKind.String), ("l", FieldKind.Long), ("d", FieldKind.Double), ("e", FieldKind.Double));

    // ------------------------------------------------------------------
    // Scalars
    // ------------------------------------------------------------------

    [Fact]
    public void A_registered_scalar_compiles_types_and_evaluates()
    {
        var r = Compile($"SELECT {ScalarName}(d, e) AS x FROM mixed", Mixed);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics.Select(diag => diag.Message)));
        Assert.Equal(FieldKind.Double, r.OutputSchema!.Fields["x"]);

        var exec = CompileAndCreate($"SELECT {ScalarName}(d, e) AS x FROM mixed", Mixed);
        var results = exec.OnEvent("mixed", Evt(1000, "mixed", ("s", "a"), ("l", 1L), ("d", 3.0), ("e", 4.0)));
        Assert.Equal(5.0, Assert.Single(results)["x"]);
    }

    [Fact]
    public void A_registered_scalar_is_arity_checked_like_a_built_in()
    {
        var r = Compile($"SELECT {ScalarName}(d) AS x FROM mixed", Mixed);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("wrong number of arguments"));
    }

    [Fact]
    public void An_unregistered_name_is_still_an_unknown_function()
    {
        var r = Compile("SELECT NO_SUCH_FN(d) AS x FROM mixed", Mixed);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("Unknown function"));
    }

    /// <summary>The registry is consulted only after the built-in switches, and registration refuses a
    /// built-in's name outright — so a third party cannot change what SUM or ABS means in SQL that is
    /// already deployed. That failure would surface as wrong numbers, not as an error, which is why it
    /// is rejected at registration rather than resolved by precedence alone.</summary>
    [Theory]
    [InlineData("SUM")]
    [InlineData("ABS")]
    [InlineData("if")]
    public void A_registration_may_not_shadow_a_built_in(string name)
        => Assert.Throws<ArgumentException>(() => SqlFunctions.Register(new HypotFunction(name)));

    [Fact]
    public void Registering_the_same_name_twice_replaces_rather_than_duplicates()
    {
        SqlFunctions.Register(new HypotFunction());
        Assert.Single(SqlFunctions.RegisteredScalarNames(), n => n == ScalarName);
    }

    // ------------------------------------------------------------------
    // Aggregates — both accumulators, because the dialect runs SQL both ways
    // ------------------------------------------------------------------

    [Fact]
    public void A_registered_aggregate_folds_forward_in_a_pipeline()
    {
        // A pipeline aggregate needs a WINDOW in this dialect; all three events fall in the same one.
        var exec = CompileAndCreate(
            $"SELECT s, {AggregateName}(d) AS spread FROM mixed GROUP BY s WINDOW TUMBLING(SIZE 1 SECONDS) EMIT CHANGES", Mixed);
        exec.OnEvent("mixed", Evt(1000, "mixed", ("s", "A"), ("l", 1L), ("d", 2.0), ("e", 0.0)));
        exec.OnEvent("mixed", Evt(1001, "mixed", ("s", "A"), ("l", 1L), ("d", 9.0), ("e", 0.0)));
        var last = exec.OnEvent("mixed", Evt(1002, "mixed", ("s", "A"), ("l", 1L), ("d", 4.0), ("e", 0.0)));
        Assert.Equal(7.0, Assert.Single(last)["spread"]); // 9 - 2
    }

    /// <summary>The half that actually matters: in a table the aggregate must survive retraction. The
    /// LATEST BY below supersedes a key, which reaches the aggregate as weight -1 — an aggregate that
    /// only knew how to add would keep the old value's contribution forever.</summary>
    [Fact]
    public void A_registered_aggregate_survives_retraction_in_a_table()
    {
        var latest = CompileTable("SELECT s, d FROM mixed LATEST BY (s)", Mixed);
        Assert.True(latest.Ok, string.Join(";", latest.Diagnostics));
        var latestExec = latest.Plan!.CreateExecutor();

        var rolled = CompileTable(
            $"SELECT {AggregateName}(d) AS spread FROM latest_d",
            [],
            [new SourceSchema("latest_d", latest.OutputSchema!.Fields)]);
        Assert.True(rolled.Ok, string.Join(";", rolled.Diagnostics));
        var rolledExec = rolled.Plan!.CreateExecutor();

        void Push(long ts, string key, double value)
        {
            foreach (var delta in latestExec.OnStreamEvent("mixed", Evt(ts, "mixed", ("s", key), ("l", 1L), ("d", value), ("e", 0.0))))
            {
                rolledExec.OnTableDelta("latest_d", delta);
            }
        }

        Push(1000, "A", 2.0);
        Push(1001, "B", 9.0);
        Assert.Equal(7.0, Assert.Single(rolledExec.Snapshot()).Value.Row["spread"]);

        // B is superseded: 9.0 is retracted and 3.0 asserted, so the range must shrink to 3 - 2.
        Push(1002, "B", 3.0);
        Assert.Equal(1.0, Assert.Single(rolledExec.Snapshot()).Value.Row["spread"]);
    }

    // ------------------------------------------------------------------
    // Fixtures
    // ------------------------------------------------------------------

    private sealed class HypotFunction(string? name = null) : IScalarFunction
    {
        public string Name { get; } = name ?? ScalarName;
        public bool IsValidArity(int argCount) => argCount == 2;
        public FieldKind? ResultKind(IReadOnlyList<FieldKind?> argKinds) => FieldKind.Double;

        public object? Eval(IReadOnlyList<object?> args)
        {
            if (args.Count != 2 || args[0] is not double a || args[1] is not double b) return null;
            return Math.Sqrt(a * a + b * b);
        }
    }

    /// <summary>max - min. Chosen because a naive implementation cannot subtract: it forces the Z half
    /// to keep a real multiset, which is exactly what the retraction test above exercises.</summary>
    private sealed class RangeAggregate : IAggregateFunction
    {
        public string Name => AggregateName;
        public FieldKind? ResultKind(FieldKind? argKind) => FieldKind.Double;
        public Aggregator CreateStream() => new RangeStream();
        public IZAggregator CreateZ() => new RangeZ();
    }

    private sealed class RangeStream : Aggregator
    {
        private double _min = double.MaxValue, _max = double.MinValue;
        private bool _any;

        public override void Add(object? value)
        {
            if (value is not double d) return;
            _any = true;
            _min = Math.Min(_min, d);
            _max = Math.Max(_max, d);
        }

        public override object? Result => _any ? _max - _min : null;
    }

    private sealed class RangeZ : IZAggregator
    {
        private readonly SortedDictionary<double, long> _counts = [];

        public void Apply(object? value, long weight)
        {
            if (value is not double d) return;
            long next = _counts.TryGetValue(d, out var existing) ? existing + weight : weight;
            if (next <= 0) _counts.Remove(d);
            else _counts[d] = next;
        }

        public object? Result => _counts.Count == 0 ? null : _counts.Keys.Last() - _counts.Keys.First();
    }
}
