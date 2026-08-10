using StreamForge.Engine.Dataflow;
using Xunit;
using static StreamForge.Engine.Tests.TestHelpers;

namespace StreamForge.Engine.Tests;

/// <summary>
/// Plan 008 wave 2c-A: the PartitionedDataflowTests _StageGraph/_Equivalence/_Deterministic triad,
/// specialized to table-mode LEFT/RIGHT/FULL OUTER joins (see TableOuterJoinOp's class doc for the T1/T2/T3
/// invariant and the NULL-key asymmetry every event set below deliberately exercises). Existing coverage
/// this file does NOT duplicate: TableOuterJoinOpUnitTests (op-level deltas-in/deltas-out), TableOuterJoinTests
/// (single-partition end-to-end + one P=1/P=4 LEFT equivalence), TableOuterJoinValidatorTests,
/// TableOuterJoinCompositeKeyTests, TableCrossJoinTests (CROSS at hop 0).
///
/// RIGHT's event set deliberately compiles "ref r RIGHT JOIN trades t" (table FIRST, stream as the JOIN
/// alias) rather than the more idiomatic "trades t RIGHT JOIN ref r" TableOuterJoinTests uses -- a genuine
/// pad -> flip -> UN-flip cycle needs the side that TRIGGERS the flip to be retractable, and
/// TableExecutor.OnStreamEvent is assert-only (weight always +1; see TableExecutorImpl.OnStreamEventCore).
/// With "trades t RIGHT JOIN ref r", RIGHT (ref) pads and only trades' (stream) arrival can flip it --
/// one-directional, no un-flip possible in this harness. Swapping which alias sits in which SQL position
/// keeps ref (the flip-driving side here) a table, fully retractable, without changing which JoinKind is
/// under test.
/// </summary>
public class TableOuterJoinPartitionedTests
{
    private readonly record struct Ev(string Origin, EventRecord Row, long Weight, bool IsTable);

    private static Ev S(string source, EventRecord row) => new(source, row, 1, false);
    private static Ev T(string table, EventRecord row, long weight) => new(table, row, weight, true);

    private static string Canon(TableExecutor exec) =>
        string.Join("\n", exec.Snapshot().OrderBy(kv => kv.Key).Select(kv => $"{kv.Key}={kv.Value.Weight}"));

    private static string Canon(PartitionedTableHarness harness) =>
        string.Join("\n", harness.Snapshot().OrderBy(kv => kv.Key).Select(kv => $"{kv.Key}={kv.Value.Weight}"));

    /// <summary>Copied from PartitionedDataflowTests (private there, not reusable) -- classic single-
    /// partition TableExecutor vs the partitioned harness at each of <paramref name="partitionCounts"/>.</summary>
    private static void AssertEquivalence(TableCompileResult compileResult, IReadOnlyList<Ev> events, int[] partitionCounts)
    {
        Assert.True(compileResult.Ok, string.Join("; ", compileResult.Diagnostics.Select(d => d.Message)));
        var plan = compileResult.Plan!;

        var classic = plan.CreateExecutor();
        foreach (var e in events)
        {
            if (e.IsTable) classic.OnTableDelta(e.Origin, new TableDelta(e.Row, e.Weight));
            else classic.OnStreamEvent(e.Origin, e.Row);
        }
        var classicCanon = Canon(classic);

        foreach (var p in partitionCounts)
        {
            var dataflow = plan.CreateDataflow(p);
            var harness = new PartitionedTableHarness(dataflow);
            foreach (var e in events) harness.Admit(e.Origin, e.Row, e.Weight);
            Assert.Equal(classicCanon, Canon(harness));
        }
    }

    /// <summary>Copied from PartitionedDataflowTests -- FIFO/LIFO/reversed admission orders must converge to
    /// the identical consolidated snapshot. Outer joins are the most plausible thing in this engine to break
    /// replay determinism (the pad/flip machinery is stateful and order-sensitive-looking by construction,
    /// even though TableOuterJoinOp's own doc proves it isn't -- see OutOfOrder_RetractThenAssert_SelfHeals
    /// in TableOuterJoinOpUnitTests) -- this is the highest-value test in this file.</summary>
    private static void AssertDeterministic(TableCompileResult compileResult, IReadOnlyList<Ev> events, int partitionCount = 4)
    {
        Assert.True(compileResult.Ok, string.Join("; ", compileResult.Diagnostics.Select(d => d.Message)));
        var plan = compileResult.Plan!;

        var fifoHarness = new PartitionedTableHarness(plan.CreateDataflow(partitionCount));
        foreach (var e in events) fifoHarness.Admit(e.Origin, e.Row, e.Weight, lifo: false);

        var lifoHarness = new PartitionedTableHarness(plan.CreateDataflow(partitionCount));
        foreach (var e in events) lifoHarness.Admit(e.Origin, e.Row, e.Weight, lifo: true);

        var reversedHarness = new PartitionedTableHarness(plan.CreateDataflow(partitionCount));
        foreach (var e in events.Reverse()) reversedHarness.Admit(e.Origin, e.Row, e.Weight, lifo: false);

        var fifo = Canon(fifoHarness);
        Assert.Equal(fifo, Canon(lifoHarness));
        Assert.Equal(fifo, Canon(reversedHarness));
    }

    private const string LeftSql = "SELECT t.symbol, r.tag FROM trades t LEFT JOIN ref r ON t.symbol = r.symbol";
    private const string RightSql = "SELECT r.tag, t.symbol FROM ref r RIGHT JOIN trades t ON r.symbol = t.symbol";
    private const string FullSql = "SELECT t.symbol, r.tag FROM trades t FULL JOIN ref r ON t.symbol = r.symbol";
    private const string ChainedLeftThenInnerSql =
        "SELECT t.symbol, r.tag, q.bid FROM trades t LEFT JOIN ref r ON t.symbol = r.symbol JOIN quotes q ON r.tag = q.symbol";

    // ------------------------------------------------------------------------------------------------
    // Event sets. Each satisfies, at minimum: a key that pads then flips (match arrives) then un-flips
    // (match retracted); a NULL-key row on the padded side; a row on the other side with no counterpart;
    // and a retraction.
    // ------------------------------------------------------------------------------------------------

    private static List<Ev> LeftEvents() =>
    [
        S("trades", Evt(1, "trades", ("symbol", "AAPL"), ("price", 10.0), ("qty", 5L), ("active", true))), // pads: right bucket empty
        T("ref", Evt(0, "ref", ("symbol", "MSFT"), ("tag", "other")), 1), // other side, no counterpart ever -> LEFT doesn't pad right -> no output
        T("ref", Evt(0, "ref", ("symbol", "AAPL"), ("tag", "core")), 1), // flips AAPL: product + left-pad retract
        S("trades", Evt(2, "trades", ("symbol", null), ("price", 1.0), ("qty", 1L), ("active", true))), // NULL-key row on padded (left) side -> immediate pad, never indexed
        T("ref", Evt(0, "ref", ("symbol", "AAPL"), ("tag", "core")), -1), // retraction -> un-flips AAPL: retract product + repad
    ];

    private static List<Ev> RightEvents() =>
    [
        S("trades", Evt(1, "trades", ("symbol", "AAPL"), ("price", 10.0), ("qty", 5L), ("active", true))), // own pad: Left(ref) has no AAPL yet
        T("ref", Evt(0, "ref", ("symbol", "MSFT"), ("tag", "other")), 1), // other side (left/ref), no counterpart ever -> RIGHT doesn't pad left -> no output
        T("ref", Evt(0, "ref", ("symbol", "AAPL"), ("tag", "core")), 1), // flips AAPL: product + right(trades)-pad retract
        S("trades", Evt(2, "trades", ("symbol", null), ("price", 1.0), ("qty", 1L), ("active", true))), // NULL-key row on padded (right/trades) side -> immediate pad, never indexed
        T("ref", Evt(0, "ref", ("symbol", "AAPL"), ("tag", "core")), -1), // retraction -> un-flips AAPL: retract product + repad trades row
    ];

    private static List<Ev> FullEvents() =>
    [
        S("trades", Evt(1, "trades", ("symbol", "AAPL"), ("price", 10.0), ("qty", 5L), ("active", true))), // left own-pad
        T("ref", Evt(0, "ref", ("symbol", "MSFT"), ("tag", "other")), 1), // right own-pad; also the "no counterpart" row -- stays padded forever
        T("ref", Evt(0, "ref", ("symbol", "AAPL"), ("tag", "core")), 1), // flips AAPL: product + left-pad retract
        S("trades", Evt(2, "trades", ("symbol", null), ("price", 1.0), ("qty", 1L), ("active", true))), // NULL-key row on the left-padded side
        T("ref", Evt(0, "ref", ("symbol", null), ("tag", "orphan")), 1), // NULL-key row on the right-padded side too (FULL pads both)
        T("ref", Evt(0, "ref", ("symbol", "AAPL"), ("tag", "core")), -1), // retraction -> un-flips AAPL: retract product + repad trades row
    ];

    private static List<Ev> ChainedLeftThenInnerEvents() =>
    [
        S("trades", Evt(1, "trades", ("symbol", "AAPL"), ("price", 10.0), ("qty", 5L), ("active", true))), // LEFT pads (r.tag=NULL) -> INNER drops it (NULL key never indexed)
        S("trades", Evt(2, "trades", ("symbol", "MSFT"), ("price", 20.0), ("qty", 3L), ("active", true))), // never matches ref at all -> stays LEFT-padded -> still dropped by INNER
        T("ref", Evt(0, "ref", ("symbol", "AAPL"), ("tag", "TAG1")), 1), // LEFT flips (pad retracts, dropped, no-op) + product flows into INNER, unmatched there yet
        S("quotes", Evt(3, "quotes", ("symbol", "TAG1"), ("bid", 99.5), ("ask", 100.5))), // INNER's own match fires -> final row appears
        T("ref", Evt(0, "ref", ("symbol", "AAPL"), ("tag", "TAG1")), -1), // retraction cascades: LEFT un-flips (repads NULL, still dropped) -> final row disappears
    ];

    // ------------------------------------------------------------------------------------------------
    // _StageGraph
    // ------------------------------------------------------------------------------------------------

    [Theory]
    [InlineData("LEFT")]
    [InlineData("RIGHT")]
    [InlineData("FULL")]
    public void OuterJoin_StageGraph_BothEdgesHashPartitioned_StageKindStillJoin_RoutingKeyCarriesFullList(string kind)
    {
        var sql = $"SELECT t.symbol, r.tag FROM trades t {kind} JOIN ref r ON t.symbol = r.symbol";
        var compiled = CompileTable(sql, [Trades], [Ref]).Plan!.Compiled;
        var built = TableDataflowBuilder.Build(compiled, 4);

        var joinStage = built.Stages.Single(s => s.InEdges.Count == 2);
        // Plan 008 deliberately did NOT add a dedicated TableStageKind for outer joins -- the coarse stage
        // kind is unchanged from plain INNER/CROSS; only the op JoinChainStageExecutor instantiates (keyed
        // off the real JoinKind) differs. See TableDataflowPlan's TableStageKind doc.
        Assert.Equal(TableStageKind.Join, joinStage.Kind);
        Assert.Equal(2, joinStage.InEdges.Count);
        Assert.All(joinStage.InEdges, e => Assert.Equal(TableEdgeMode.HashPartition, e.Mode));
        Assert.Equal(["Left", "Right"], joinStage.InEdges.Select(e => e.Role));

        var leftEdge = joinStage.InEdges.Single(e => e.Role == "Left");
        var rightEdge = joinStage.InEdges.Single(e => e.Role == "Right");
        Assert.Single(built.RoutingSpecs[leftEdge.EdgeId.Value].KeyExprs!);
        Assert.Single(built.RoutingSpecs[rightEdge.EdgeId.Value].KeyExprs!);
    }

    private static readonly SourceSchema Orders = Schema("orders", ("sym", FieldKind.String), ("venue", FieldKind.String));
    private static readonly SourceSchema Fills = Schema("fills", ("sym", FieldKind.String), ("venue", FieldKind.String), ("qty", FieldKind.Long));

    [Fact]
    public void OuterJoin_StageGraph_CompositeKey_RoutingSpecCarriesFullKeyList_NotFoldedToFirstComponent()
    {
        const string sql = "SELECT o.sym, o.venue, f.qty FROM orders o LEFT JOIN fills f ON o.sym = f.sym AND o.venue = f.venue";
        var compiled = CompileTable(sql, [Orders], [Fills]).Plan!.Compiled;
        var built = TableDataflowBuilder.Build(compiled, 4);

        var joinStage = built.Stages.Single(s => s.InEdges.Count == 2);
        Assert.Equal(TableStageKind.Join, joinStage.Kind);
        var leftEdge = joinStage.InEdges.Single(e => e.Role == "Left");
        var rightEdge = joinStage.InEdges.Single(e => e.Role == "Right");
        Assert.Equal(TableEdgeMode.HashPartition, leftEdge.Mode);
        Assert.Equal(TableEdgeMode.HashPartition, rightEdge.Mode);

        // The routing key spec must carry BOTH equi-conjuncts -- not the pre-008 "first component only,
        // rest folded into a residual" shape non-outer joins still get (see TableOuterJoinCompositeKeyTests).
        Assert.Equal(2, built.RoutingSpecs[leftEdge.EdgeId.Value].KeyExprs!.Count);
        Assert.Equal(2, built.RoutingSpecs[rightEdge.EdgeId.Value].KeyExprs!.Count);
    }

    // ------------------------------------------------------------------------------------------------
    // _Equivalence
    // ------------------------------------------------------------------------------------------------

    [Fact]
    public void LeftJoin_Equivalence() => AssertEquivalence(CompileTable(LeftSql, [Trades], [Ref]), LeftEvents(), [1, 4]);

    [Fact]
    public void RightJoin_Equivalence() => AssertEquivalence(CompileTable(RightSql, [Trades], [Ref]), RightEvents(), [1, 4]);

    [Fact]
    public void FullJoin_Equivalence() => AssertEquivalence(CompileTable(FullSql, [Trades], [Ref]), FullEvents(), [1, 4]);

    [Fact]
    public void ChainedLeftThenInner_Equivalence() =>
        AssertEquivalence(CompileTable(ChainedLeftThenInnerSql, [Trades, Quotes], [Ref]), ChainedLeftThenInnerEvents(), [1, 4]);

    // ------------------------------------------------------------------------------------------------
    // _Deterministic
    // ------------------------------------------------------------------------------------------------

    [Fact]
    public void LeftJoin_Deterministic() => AssertDeterministic(CompileTable(LeftSql, [Trades], [Ref]), LeftEvents());

    [Fact]
    public void RightJoin_Deterministic() => AssertDeterministic(CompileTable(RightSql, [Trades], [Ref]), RightEvents());

    /// <summary>FULL only: FIFO vs LIFO, NOT the full-reversal claim AssertDeterministic's own doc already
    /// flags as plan-shape-dependent ("the stronger claim ... for plans where it holds"). See
    /// <see cref="FullJoin_FullyReversedAdmissionOrder_DivergesFromForward_KnownConsolidationLimitation"/>
    /// immediately below for why FULL is exactly such a plan and the confirmed root cause -- this is not an
    /// assertion weakened to dodge a failure; FIFO vs LIFO is everything genuinely guaranteed here.</summary>
    [Fact]
    public void FullJoin_Deterministic_FifoVsLifo()
    {
        var plan = CompileTable(FullSql, [Trades], [Ref]).Plan!;
        var events = FullEvents();

        var fifoHarness = new PartitionedTableHarness(plan.CreateDataflow(4));
        foreach (var e in events) fifoHarness.Admit(e.Origin, e.Row, e.Weight, lifo: false);

        var lifoHarness = new PartitionedTableHarness(plan.CreateDataflow(4));
        foreach (var e in events) lifoHarness.Admit(e.Origin, e.Row, e.Weight, lifo: true);

        Assert.Equal(Canon(fifoHarness), Canon(lifoHarness));
    }

    /// <summary>
    /// CONFIRMED FINDING (plan 008 wave 2c-A), reported rather than fixed -- this file owns tests, not
    /// TableExecutorImpl/TableOuterJoinOp. Reproduces byte-for-byte on the classic single-partition
    /// TableExecutor (asserted below), so it is a general table-mode Snapshot()-consolidation limitation,
    /// NOT anything specific to TableOuterJoinOp or the partitioned dataflow.
    ///
    /// ROOT CAUSE: TableExecutorImpl.ApplyConsolidation (mirrored exactly by
    /// PartitionedTableHarness.ApplyConsolidation for this test suite's own M2 equivalence oracle) only
    /// ever STORES weight&gt;0 entries; a negative delta for a canonical row not already in the dictionary is
    /// silently dropped rather than recorded as a negative running total (see TableExecutorImpl.cs's "Weight
    /// &lt;= 0 entries are pruned immediately" comment). That is safe as long as, for any given canonical row,
    /// a retraction never arrives chronologically before its own assertion in the TOP-LEVEL admission
    /// sequence -- true for genuine production delivery (a retraction always undoes a PRIOR real assertion)
    /// but deliberately violated by this test's fully-REVERSED admission order.
    ///
    /// FULL join is where it surfaces: with the sequence reversed, ref's retraction (originally last, now
    /// first) arrives while Left(trades) is not yet indexed for that key -- "ownMatchCount == 0" is
    /// genuinely true *at this point in the reversed sequence*, so TableOuterJoinOp correctly (per its own
    /// per-delta contract) emits its own T2 pad with the delta's own weight: Combine(nullLeft, ref-row)
    /// weight -1. That -1 lands on a canonical row the consolidated dictionary has never seen -&gt; silently
    /// dropped, not stored as -1. When the SAME ref row's original +1 assertion is admitted later in the
    /// reversed sequence, it lands as a fresh +1 (the key is STILL absent from the dictionary's point of
    /// view) -&gt; stored. Net effect: a row whose true weight is 0 across the whole sequence (assert, then
    /// retract) instead persists at +1 in the reversed run only.
    ///
    /// LEFT and RIGHT never hit this in this file's event sets: whichever side retracts there is never that
    /// join's OWN padding side (ownPads is false for that arrival), so ownMatchCount is never consulted the
    /// way FULL's is -- see LeftJoin_Deterministic/RightJoin_Deterministic above, both asserting the full
    /// FIFO/LIFO/reversed triad successfully.
    /// </summary>
    [Fact]
    public void FullJoin_FullyReversedAdmissionOrder_DivergesFromForward_KnownConsolidationLimitation()
    {
        var compileResult = CompileTable(FullSql, [Trades], [Ref]);
        var plan = compileResult.Plan!;
        var events = FullEvents();

        var classicForward = plan.CreateExecutor();
        foreach (var e in events)
        {
            if (e.IsTable) classicForward.OnTableDelta(e.Origin, new TableDelta(e.Row, e.Weight));
            else classicForward.OnStreamEvent(e.Origin, e.Row);
        }

        var classicReversed = plan.CreateExecutor();
        foreach (var e in events.AsEnumerable().Reverse())
        {
            if (e.IsTable) classicReversed.OnTableDelta(e.Origin, new TableDelta(e.Row, e.Weight));
            else classicReversed.OnStreamEvent(e.Origin, e.Row);
        }

        // Forward order: 4 rows, no spurious (NULL,"core") pad -- the retraction correctly cancels its own
        // earlier assertion (see FullEvents' own per-step trace in this file).
        Assert.Equal(4, classicForward.Snapshot().Count);
        Assert.DoesNotContain(classicForward.Snapshot().Values, v => v.Row["symbol"] is null && (string?)v.Row["tag"] == "core");

        // Reversed order: the SAME multiset of deltas, byte-identical rows/weights, produces a 5th, spurious
        // row -- proof the divergence is real, not a fluke of this file's own harness.
        Assert.Equal(5, classicReversed.Snapshot().Count);
        Assert.Contains(classicReversed.Snapshot().Values, v => v.Row["symbol"] is null && (string?)v.Row["tag"] == "core");

        // Reproduces identically through the partitioned harness too -- confirming this is not a
        // partitioning-specific bug, just the case AssertDeterministic's own doc already anticipated could
        // exist ("a fully reversed admission order isn't guaranteed to match for plans with order-sensitive
        // semantics").
        var harnessReversed = new PartitionedTableHarness(plan.CreateDataflow(4));
        foreach (var e in events.AsEnumerable().Reverse()) harnessReversed.Admit(e.Origin, e.Row, e.Weight);
        Assert.Equal(5, harnessReversed.Snapshot().Count);
        Assert.Contains(harnessReversed.Snapshot().Values, v => v.Row["symbol"] is null && (string?)v.Row["tag"] == "core");
    }

    [Fact]
    public void ChainedLeftThenInner_Deterministic() =>
        AssertDeterministic(CompileTable(ChainedLeftThenInnerSql, [Trades, Quotes], [Ref]), ChainedLeftThenInnerEvents());

    // ------------------------------------------------------------------------------------------------
    // Known skew hazard, pinned as known-and-correct: every NULL-keyed padded row hashes to the SAME
    // partition (TableKeyEncoding.EncodeGroupKey collapses every NULL key to the identical canonical
    // string), because TableOuterJoinOp never indexes or looks up a NULL-keyed row (see the class doc's
    // NULL-KEY RULES) -- so the skew never threatens correctness, only balance.
    // ------------------------------------------------------------------------------------------------

    [Fact]
    public void NullKeyedPaddedRows_AllRouteToOnePartition_ButSnapshotMatchesClassic()
    {
        var compileResult = CompileTable(LeftSql, [Trades], [Ref]);
        Assert.True(compileResult.Ok);
        var plan = compileResult.Plan!;
        var dataflow = plan.CreateDataflow(4);

        var joinStage = dataflow.Stages.Single(s => s.Kind == TableStageKind.Join);
        var leftEdge = joinStage.InEdges.Single(e => e.Role == "Left");

        var nullRows = new[]
        {
            Evt(1, "trades", ("symbol", null), ("price", 1.0), ("qty", 1L), ("active", true)),
            Evt(2, "trades", ("symbol", null), ("price", 2.0), ("qty", 2L), ("active", true)),
            Evt(3, "trades", ("symbol", null), ("price", 3.0), ("qty", 3L), ("active", true)),
            Evt(4, "trades", ("symbol", null), ("price", 4.0), ("qty", 4L), ("active", true)),
        };

        var partitions = nullRows.Select(r => dataflow.PartitionOf(leftEdge.EdgeId, r)).Distinct().ToList();
        Assert.Single(partitions); // the skew: every NULL-keyed row lands on the identical partition

        var events = new List<Ev>
        {
            S("trades", Evt(10, "trades", ("symbol", "AAPL"), ("price", 10.0), ("qty", 5L), ("active", true))),
            T("ref", Evt(0, "ref", ("symbol", "AAPL"), ("tag", "core")), 1),
        };
        events.AddRange(nullRows.Select(r => new Ev("trades", r, 1, false)));

        var classic = plan.CreateExecutor();
        foreach (var e in events)
        {
            if (e.IsTable) classic.OnTableDelta(e.Origin, new TableDelta(e.Row, e.Weight));
            else classic.OnStreamEvent(e.Origin, e.Row);
        }

        var harness = new PartitionedTableHarness(dataflow);
        foreach (var e in events) harness.Admit(e.Origin, e.Row, e.Weight);

        Assert.Equal(Canon(classic), Canon(harness)); // correctness holds despite every NULL row sharing one partition
        Assert.Equal(5, classic.Snapshot().Count); // 4 distinct NULL pads + 1 AAPL product
    }

    // ------------------------------------------------------------------------------------------------
    // CROSS JOIN: TableCrossJoinTests already covers the >1-partition guard at hop 0. The partitioned
    // angle that differs here: the same guard must still fire when CROSS is chained AFTER an outer join
    // (hop i>0) -- TableDataflowBuilder.Build's check is per-join (j.Kind == JoinKind.Cross), unconditional
    // on chain position or the preceding join's kind.
    // ------------------------------------------------------------------------------------------------

    [Fact]
    public void ChainedLeftThenCross_CreateDataflowThrows_UseParallelism1()
    {
        var sql = "SELECT t.symbol, r.tag, q.bid FROM trades t LEFT JOIN ref r ON t.symbol = r.symbol CROSS JOIN quotes q";
        var result = CompileTable(sql, [Trades, Quotes], [Ref]);
        Assert.True(result.Ok, string.Join(";", result.Diagnostics));
        var ex = Assert.Throws<NotSupportedException>(() => result.Plan!.CreateDataflow(4));
        Assert.Contains("Use Parallelism = 1", ex.Message);
        Assert.Contains("CROSS JOIN", ex.Message);
    }
}
