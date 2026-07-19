using System.Text.Json;
using StreamForge.Abstractions;
using StreamForge.Abstractions.Streaming;
using StreamForge.Dapr.Host.Actors;
using Xunit;

namespace StreamForge.Dapr.Tests;

/// <summary>
/// Plan 005 (Dapr sibling runtime) W7-B: unit tests for <see cref="TableHistoryApplication"/> — the pure
/// state-transition/query logic extracted from <see cref="TableHistoryActor"/> specifically so it can be
/// tested without any actor/timer/Dapr-sidecar machinery (mirrors how
/// <see cref="PipelineCompilationTests"/> exercises <see cref="PipelineCompilation"/> rather than
/// <see cref="PipelineActor"/> directly).
///
/// <para><b>The wire-round-trip tests below are this file's centerpiece</b> — proving the same finding
/// <see cref="PipelineActorWireNormalizationTests"/> proves for <c>sf-sources</c>: a
/// <see cref="TableDeltaEnvelope"/> that already holds plain CLR values when
/// <c>Streaming.TableHistoryDeltaSink</c> forwards it comes back out with every <c>Row</c> value re-boxed
/// as a <see cref="JsonElement"/> once it crosses the Dapr actor-invocation wire into
/// <c>TableHistoryActor.ApplyDeltasAsync</c> — so <see cref="TableHistoryApplication.ApplyDeltas"/>'s own
/// <c>JsonValueNormalizer.NormalizeInPlace</c> call is not redundant, it is what makes
/// <c>RowKeyCodec.EncodeIdentity</c> and the stored <see cref="HistoryVersion.Row"/> come out as plain
/// CLR values instead of JsonElements.</para>
/// </summary>
public class TableHistoryApplicationTests
{
    /// <summary>Default (no options) — mirrors the Dapr .NET SDK's actor-invocation JSON serializer,
    /// exactly like <c>PipelineActorWireNormalizationTests.ActorWireOptions</c>.</summary>
    private static readonly JsonSerializerOptions ActorWireOptions = new();

    private static TableDefinition GroupByTable(string sql = "SELECT symbol, SUM(qty) AS total_qty FROM trades GROUP BY symbol") => new()
    {
        Name = "positions",
        Sql = sql,
        HistoryEnabled = true,
        HistoryMode = TableHistoryMode.All,
        HistoryLimit = 10,
    };

    /// <summary>Round-trips <paramref name="envelope"/> through JSON exactly like the actor-invocation
    /// wire does (client proxy serialize -> server actor-method deserialize), so every <c>Row</c> value
    /// comes back out as a <see cref="JsonElement"/> — simulating what
    /// <c>TableHistoryDeltaSink.OnTableDeltaAsync</c>'s <c>ActorProxy.Create&lt;ITableHistoryActor&gt;(...)
    /// .ApplyDeltasAsync(envelope)</c> call actually produces on the actor side, without needing a live
    /// sidecar.</summary>
    private static TableDeltaEnvelope RoundTripAcrossActorWire(TableDeltaEnvelope envelope)
    {
        var json = JsonSerializer.Serialize(envelope, ActorWireOptions);
        return JsonSerializer.Deserialize<TableDeltaEnvelope>(json, ActorWireOptions)!;
    }

    // ------------------------------------------------------------------
    // Reset
    // ------------------------------------------------------------------

    [Fact]
    public void Reset_DerivesIdentityColumnsFromGroupBySql()
    {
        var state = TableHistoryApplication.Reset(GroupByTable());

        Assert.Equal(["symbol"], state.IdentityColumns);
        Assert.True(state.HistoryEnabled);
        Assert.Empty(state.Entries);
        Assert.Equal(0, state.Seq);
    }

    [Fact]
    public void Reset_HistoryDisabledDef_StillReturnsUsableConfigWithEnabledFalse()
    {
        // Mirrors TableHistoryGrain.ResetAsync: a Reset can turn history OFF just as easily as ON — the
        // caller (DaprLifecycleOrchestrator.ResetTableHistoryAsync) always calls this on create/update
        // regardless of def.HistoryEnabled's value.
        var def = GroupByTable();
        def.HistoryEnabled = false;

        var state = TableHistoryApplication.Reset(def);

        Assert.False(state.HistoryEnabled);
    }

    [Fact]
    public void Reset_NoGroupByOrLatestBy_IdentityColumnsIsNull()
    {
        var state = TableHistoryApplication.Reset(GroupByTable("SELECT symbol, price FROM trades"));

        Assert.Null(state.IdentityColumns);
    }

    // ------------------------------------------------------------------
    // ApplyDeltas — cheap no-ops
    // ------------------------------------------------------------------

    [Fact]
    public void ApplyDeltas_HistoryDisabled_ReturnsFalseAndTouchesNoState()
    {
        var state = TableHistoryApplication.Reset(GroupByTable());
        state.HistoryEnabled = false;
        var envelope = new TableDeltaEnvelope
        {
            Table = "positions",
            Deltas = [new TableDeltaDto { Row = new Dictionary<string, object?> { ["symbol"] = "AAPL", ["total_qty"] = 10L }, Weight = 1 }],
        };

        var dirty = TableHistoryApplication.ApplyDeltas(state, envelope);

        Assert.False(dirty);
        Assert.Empty(state.Entries);
        Assert.Equal(0, state.Seq);
    }

    [Fact]
    public void ApplyDeltas_EmptyBatch_ReturnsFalse()
    {
        var state = TableHistoryApplication.Reset(GroupByTable());

        var dirty = TableHistoryApplication.ApplyDeltas(state, new TableDeltaEnvelope { Table = "positions", Deltas = [] });

        Assert.False(dirty);
    }

    // ------------------------------------------------------------------
    // ApplyDeltas — wire round-trip + retention math (the centerpiece)
    // ------------------------------------------------------------------

    [Fact]
    public void ApplyDeltas_WireRoundTrippedAssertion_StoresPlainClrValuesUnderTheCorrectIdentityKey()
    {
        var state = TableHistoryApplication.Reset(GroupByTable());
        var original = new TableDeltaEnvelope
        {
            Table = "positions",
            Seq = 1,
            Deltas = [new TableDeltaDto { Row = new Dictionary<string, object?> { ["symbol"] = "AAPL", ["total_qty"] = 42L }, Weight = 1 }],
        };
        // Sanity: genuinely plain CLR before the wire round trip.
        Assert.IsType<string>(original.Deltas[0].Row["symbol"]);

        var envelope = RoundTripAcrossActorWire(original);
        // Sanity: the wire round trip really does re-box every Row value as a JsonElement — proving the
        // normalization inside ApplyDeltas below is doing real work, not a no-op.
        Assert.IsType<JsonElement>(envelope.Deltas[0].Row["symbol"]);

        var dirty = TableHistoryApplication.ApplyDeltas(state, envelope);

        Assert.True(dirty);
        var key = StreamForge.Host.Grains.RowKeyCodec.EncodeIdentity(
            new Dictionary<string, object?> { ["symbol"] = "AAPL" }, state.IdentityColumns);
        Assert.True(state.Entries.ContainsKey(key));
        var version = Assert.Single(state.Entries[key].Versions);
        Assert.IsType<string>(version.Row["symbol"]);
        Assert.Equal("AAPL", version.Row["symbol"]);
        Assert.IsType<long>(version.Row["total_qty"]);
        Assert.Equal(42L, version.Row["total_qty"]);
        Assert.Equal(1, state.Seq);
    }

    [Fact]
    public void ApplyDeltas_RetractionWeight_IncrementsRetractionCountWithoutAddingAVersion()
    {
        var state = TableHistoryApplication.Reset(GroupByTable());
        var assertion = new TableDeltaEnvelope
        {
            Table = "positions",
            Deltas = [new TableDeltaDto { Row = new Dictionary<string, object?> { ["symbol"] = "AAPL", ["total_qty"] = 10L }, Weight = 1 }],
        };
        TableHistoryApplication.ApplyDeltas(state, assertion);

        var retraction = new TableDeltaEnvelope
        {
            Table = "positions",
            Deltas = [new TableDeltaDto { Row = new Dictionary<string, object?> { ["symbol"] = "AAPL", ["total_qty"] = 10L }, Weight = -1 }],
        };
        TableHistoryApplication.ApplyDeltas(state, retraction);

        var key = StreamForge.Host.Grains.RowKeyCodec.EncodeIdentity(
            new Dictionary<string, object?> { ["symbol"] = "AAPL" }, state.IdentityColumns);
        var entry = state.Entries[key];
        Assert.Single(entry.Versions); // retraction does not append a version
        Assert.Equal(1, entry.RetractionCount);
        Assert.Equal(2, state.Seq); // Seq increments for every observed delta, assertion or retraction
    }

    [Fact]
    public void ApplyDeltas_LastNMode_CapsAtConfiguredLimitKeepingNewest()
    {
        var def = GroupByTable();
        def.HistoryMode = TableHistoryMode.LastN;
        def.HistoryLimit = 2;
        var state = TableHistoryApplication.Reset(def);

        for (var i = 1; i <= 3; i++)
        {
            var envelope = new TableDeltaEnvelope
            {
                Table = "positions",
                Deltas = [new TableDeltaDto { Row = new Dictionary<string, object?> { ["symbol"] = "AAPL", ["total_qty"] = (long)i }, Weight = 1 }],
            };
            TableHistoryApplication.ApplyDeltas(state, envelope);
        }

        var key = StreamForge.Host.Grains.RowKeyCodec.EncodeIdentity(
            new Dictionary<string, object?> { ["symbol"] = "AAPL" }, state.IdentityColumns);
        var versions = state.Entries[key].Versions;
        Assert.Equal(2, versions.Count); // capped at HistoryLimit, oldest evicted
        Assert.Equal(2L, versions[0].Row["total_qty"]);
        Assert.Equal(3L, versions[1].Row["total_qty"]);
    }

    [Fact]
    public void ApplyDeltas_DistinctIdentityKeys_TrackedIndependently()
    {
        var state = TableHistoryApplication.Reset(GroupByTable());
        var envelope = new TableDeltaEnvelope
        {
            Table = "positions",
            Deltas =
            [
                new TableDeltaDto { Row = new Dictionary<string, object?> { ["symbol"] = "AAPL", ["total_qty"] = 1L }, Weight = 1 },
                new TableDeltaDto { Row = new Dictionary<string, object?> { ["symbol"] = "MSFT", ["total_qty"] = 2L }, Weight = 1 },
            ],
        };

        TableHistoryApplication.ApplyDeltas(state, envelope);

        Assert.Equal(2, state.Entries.Count);
    }

    // ------------------------------------------------------------------
    // Query
    // ------------------------------------------------------------------

    [Fact]
    public void Query_UnknownKey_ReturnsKeyFoundFalse()
    {
        var state = TableHistoryApplication.Reset(GroupByTable());

        var result = TableHistoryApplication.Query(state, "never-seen", 0);

        Assert.False(result.KeyFound);
        Assert.Equal(TableHistoryMode.All, result.Mode);
    }

    [Fact]
    public void Query_KnownKey_ReturnsNewestFirstAndRespectsLimit()
    {
        var def = GroupByTable();
        def.HistoryMode = TableHistoryMode.All;
        var state = TableHistoryApplication.Reset(def);
        for (var i = 1; i <= 3; i++)
        {
            var envelope = new TableDeltaEnvelope
            {
                Table = "positions",
                Deltas = [new TableDeltaDto { Row = new Dictionary<string, object?> { ["symbol"] = "AAPL", ["total_qty"] = (long)i }, Weight = 1 }],
            };
            TableHistoryApplication.ApplyDeltas(state, envelope);
        }
        var key = StreamForge.Host.Grains.RowKeyCodec.EncodeIdentity(
            new Dictionary<string, object?> { ["symbol"] = "AAPL" }, state.IdentityColumns);

        var all = TableHistoryApplication.Query(state, key, 0);
        Assert.True(all.KeyFound);
        Assert.Equal(3, all.TotalVersions);
        Assert.Equal([3L, 2L, 1L], all.Versions.Select(v => (long)v.Row["total_qty"]!));

        var limited = TableHistoryApplication.Query(state, key, 2);
        Assert.Equal(2, limited.Versions.Count);
        Assert.Equal([3L, 2L], limited.Versions.Select(v => (long)v.Row["total_qty"]!));
        Assert.Equal(3, limited.TotalVersions); // TotalVersions reflects the full retained set, not the limited page
    }

    // ------------------------------------------------------------------
    // Stats
    // ------------------------------------------------------------------

    [Fact]
    public void Stats_ReflectsEnabledModeAndCounts()
    {
        var def = GroupByTable();
        def.HistoryMode = TableHistoryMode.LastN;
        var state = TableHistoryApplication.Reset(def);
        var envelope = new TableDeltaEnvelope
        {
            Table = "positions",
            Deltas =
            [
                new TableDeltaDto { Row = new Dictionary<string, object?> { ["symbol"] = "AAPL", ["total_qty"] = 1L }, Weight = 1 },
                new TableDeltaDto { Row = new Dictionary<string, object?> { ["symbol"] = "MSFT", ["total_qty"] = 2L }, Weight = 1 },
            ],
        };
        TableHistoryApplication.ApplyDeltas(state, envelope);

        var stats = TableHistoryApplication.Stats(state);

        Assert.True(stats.Enabled);
        Assert.Equal(TableHistoryMode.LastN, stats.Mode);
        Assert.Equal(2, stats.KeyCount);
        Assert.Equal(2, stats.TotalVersions);
    }

    [Fact]
    public void Stats_NeverReset_ReportsDisabledZeroed()
    {
        var stats = TableHistoryApplication.Stats(new TableHistoryActorState());

        Assert.False(stats.Enabled);
        Assert.Equal(0, stats.KeyCount);
        Assert.Equal(0, stats.TotalVersions);
    }
}
