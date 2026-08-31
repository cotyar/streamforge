using StreamsForge.Abstractions;
using StreamsForge.Abstractions.Streaming;
using StreamsForge.Dapr.Host.Actors;
using StreamsForge.Host.Grains;
using Xunit;

namespace StreamsForge.Dapr.Tests;

/// <summary>
/// Plan 005 (Dapr sibling runtime) W7-B: proves the key-codec parity the wave brief calls out explicitly —
/// "check how the endpoint derives `key` — endpoint or grain? mirror exactly".
///
/// <para><b>The finding (see <see cref="Facades.DaprTableHistoryFacade"/>'s doc comment for the full
/// writeup):</b> <c>shared/StreamsForge.Api/Endpoints/TablesEndpoints.cs</c>'s
/// <c>POST /{id}/history/lookup</c> handler derives the row-identity key ITSELF, from the request's raw
/// row, via <c>TableGroupKeyExtractor.ExtractIdentityColumns(def.Sql)</c> +
/// <c>RowKeyCodec.EncodeIdentity(req.Row, identityColumns)</c> — BEFORE ever calling
/// <c>ITableHistoryFacade.GetHistoryAsync(tableName, key, limit)</c>. So the key
/// <see cref="Facades.DaprTableHistoryFacade.GetHistoryAsync"/> receives is ALREADY ENCODED; it is not
/// derived a second time inside the facade, nor inside <see cref="TableHistoryActor"/> — that actor only
/// derives identity columns (not a full row key) once, at <see cref="TableHistoryActor.ResetAsync"/> time,
/// to key its OWN live-delta bookkeeping the identical way.</para>
///
/// <para><b>What this test proves:</b> simulating the endpoint's derivation (independently, using the
/// exact same <c>TableGroupKeyExtractor</c>/<c>RowKeyCodec</c> calls the endpoint makes) against a row,
/// and simulating the actor's live-delta derivation (via <see cref="TableHistoryApplication.ApplyDeltas"/>,
/// fed the identical row shape a real delta would carry) against the SAME <see cref="TableDefinition.Sql"/>,
/// produces the IDENTICAL key string — so a <see cref="TableHistoryApplication.Query"/> using the
/// endpoint-derived key finds the entry the actor accumulated from live deltas. No live actor/facade/HTTP
/// endpoint is involved; this is a pure-function parity proof, exactly the "no live actors" constraint the
/// wave brief sets for this wave's tests.</para>
/// </summary>
public class TableHistoryKeyCodecParityTests
{
    private static readonly TableDefinition PositionsTable = new()
    {
        Name = "positions",
        Sql = "SELECT symbol, SUM(qty) AS total_qty FROM trades GROUP BY symbol",
        HistoryEnabled = true,
        HistoryMode = TableHistoryMode.All,
    };

    /// <summary>Exactly what TablesEndpoints' <c>/history/lookup</c> handler does with
    /// <c>HistoryLookupRequest.Row</c> — see that endpoint's own comment: "The server derives the
    /// row-identity key from req.Row via TableGroupKeyExtractor/RowKeyCodec ... so the client never needs
    /// to know the table's GROUP BY identity columns or the key encoding."</summary>
    private static string EndpointDerivedKey(TableDefinition def, Dictionary<string, object?> row)
    {
        var identityColumns = TableGroupKeyExtractor.ExtractIdentityColumns(def.Sql);
        return RowKeyCodec.EncodeIdentity(row, identityColumns);
    }

    [Fact]
    public void EndpointDerivedKey_MatchesTheKeyTheActorAccumulatedFromLiveDeltas()
    {
        var state = TableHistoryApplication.Reset(PositionsTable);
        var liveDelta = new TableDeltaEnvelope
        {
            Table = "positions",
            Deltas = [new TableDeltaDto { Row = new Dictionary<string, object?> { ["symbol"] = "AAPL", ["total_qty"] = 100L }, Weight = 1 }],
        };
        TableHistoryApplication.ApplyDeltas(state, liveDelta);

        // A client submits only the identity-relevant column(s) — exactly what the SPA's row-history
        // sheet does, and exactly what HistoryLookupRequest.Row carries (see RowKeyCodec.EncodeValue's own
        // "BUGFIX" comment on that dictionary's JsonElement-boxing history for why this only needs the
        // identity columns, not the full row).
        var lookupRow = new Dictionary<string, object?> { ["symbol"] = "AAPL" };
        var key = EndpointDerivedKey(PositionsTable, lookupRow);

        var result = TableHistoryApplication.Query(state, key, 0);

        Assert.True(result.KeyFound);
        Assert.Single(result.Versions);
        Assert.Equal(100L, result.Versions[0].Row["total_qty"]);
    }

    [Fact]
    public void EndpointDerivedKey_DifferentIdentityValue_DoesNotMatchAnUnrelatedEntry()
    {
        var state = TableHistoryApplication.Reset(PositionsTable);
        TableHistoryApplication.ApplyDeltas(state, new TableDeltaEnvelope
        {
            Table = "positions",
            Deltas = [new TableDeltaDto { Row = new Dictionary<string, object?> { ["symbol"] = "AAPL", ["total_qty"] = 100L }, Weight = 1 }],
        });

        var key = EndpointDerivedKey(PositionsTable, new Dictionary<string, object?> { ["symbol"] = "MSFT" });

        var result = TableHistoryApplication.Query(state, key, 0);

        Assert.False(result.KeyFound);
    }

    [Fact]
    public void EndpointDerivedKey_NoGroupByIdentity_FallsBackToWholeRowOnBothSides()
    {
        // No GROUP BY -> TableGroupKeyExtractor.ExtractIdentityColumns returns null on BOTH sides (the
        // endpoint's derivation and TableHistoryActor.ResetAsync's own), so RowKeyCodec.EncodeIdentity
        // falls back to whole-row canonical encoding identically for both — see RowKeyCodec's class doc:
        // "each distinct combination of output values gets its own key".
        var def = new TableDefinition { Name = "raw_trades", Sql = "SELECT symbol, price FROM trades", HistoryEnabled = true };
        var state = TableHistoryApplication.Reset(def);
        Assert.Null(state.IdentityColumns);

        var row = new Dictionary<string, object?> { ["symbol"] = "AAPL", ["price"] = 101.5 };
        TableHistoryApplication.ApplyDeltas(state, new TableDeltaEnvelope
        {
            Table = "raw_trades",
            Deltas = [new TableDeltaDto { Row = new Dictionary<string, object?>(row), Weight = 1 }],
        });

        var key = EndpointDerivedKey(def, row);
        var result = TableHistoryApplication.Query(state, key, 0);

        Assert.True(result.KeyFound);
    }
}
