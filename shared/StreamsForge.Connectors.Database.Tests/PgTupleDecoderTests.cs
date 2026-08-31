using StreamsForge.Connectors.Database;
using Xunit;

namespace StreamsForge.Connectors.Database.Tests;

/// <summary>
/// <see cref="PgRelationCache"/> and <see cref="PgTupleDecoder"/> — the pure half of the CDC reader, driven
/// with no server and no Npgsql replication type in sight (per <see cref="PgTupleField"/>'s own doc, this
/// is the entire reason it exists as a small record of its own).
/// </summary>
public class PgTupleDecoderTests
{
    private static readonly PgRelation Orders = new(
        RelationId: 1,
        Namespace: "public",
        RelationName: "orders",
        ColumnNames: ["id", "customer", "amount"],
        ReplicaIdentity: "Default");

    [Fact]
    public void NullBecomesNull()
    {
        var result = PgTupleDecoder.Decode(Orders, [new PgTupleField("id", PgTupleValueKind.Null, null)]);

        Assert.Null(result.Row["id"]);
    }

    [Fact]
    public void AnOrdinaryValuePassesThroughUnchanged()
    {
        var result = PgTupleDecoder.Decode(Orders, [new PgTupleField("amount", PgTupleValueKind.Value, 42.5m)]);

        Assert.Equal(42.5m, result.Row["amount"]);
    }

    [Fact]
    public void AnUnchangedToastedValueBecomesDebeziumsOwnSentinel()
    {
        var result = PgTupleDecoder.Decode(Orders, [new PgTupleField("customer", PgTupleValueKind.UnchangedToast, null)]);

        // The literal, not a StreamsForge invention — see CdcStamp.UnavailableValue's own doc for why.
        Assert.Equal("__debezium_unavailable_value", result.Row["customer"]);
        Assert.Equal(CdcStamp.UnavailableValue, result.Row["customer"]);
    }

    [Fact]
    public void ColumnNamesComeFromTheFieldsThemselvesNotFromPosition()
    {
        // Deliberately out of the relation's declared column order — decoding by name must not care.
        var result = PgTupleDecoder.Decode(Orders,
        [
            new PgTupleField("amount", PgTupleValueKind.Value, 9.99m),
            new PgTupleField("id", PgTupleValueKind.Value, 7L),
            new PgTupleField("customer", PgTupleValueKind.Value, "acme"),
        ]);

        Assert.Equal(9.99m, result.Row["amount"]);
        Assert.Equal(7L, result.Row["id"]);
        Assert.Equal("acme", result.Row["customer"]);
    }

    [Fact]
    public void AWellFormedTupleReportsNoDiagnostic()
    {
        var result = PgTupleDecoder.Decode(Orders,
        [
            new PgTupleField("id", PgTupleValueKind.Value, 1L),
            new PgTupleField("customer", PgTupleValueKind.Value, "acme"),
            new PgTupleField("amount", PgTupleValueKind.Value, 1.0m),
        ]);

        Assert.Null(result.Diagnostic);
    }

    [Fact]
    public void AnUnknownRelationIdFailsLoudlyRatherThanGuessingAShape()
    {
        var cache = new PgRelationCache();

        var ex = Assert.Throws<InvalidOperationException>(() => cache.Get(relationId: 99));

        Assert.Contains("99", ex.Message, StringComparison.Ordinal);
        Assert.Contains("RelationMessage", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AKnownRelationIdRoundTripsWhatWasSet()
    {
        var cache = new PgRelationCache();
        cache.Set(1, "public", "orders", ["id", "customer", "amount"], "Default");

        var relation = cache.Get(1);

        Assert.Equal("public.orders", relation.QualifiedName);
        Assert.Equal(["id", "customer", "amount"], relation.ColumnNames);
        Assert.Equal("Default", relation.ReplicaIdentity);
    }

    [Fact]
    public void ATupleShorterThanItsRelationIsReportedRatherThanSilentlyZippedShort()
    {
        // Two values for a three-column relation. Decoding is by field name, so nothing is misaligned —
        // the diagnostic exists purely because a mismatch this size is worth an operator's attention.
        var result = PgTupleDecoder.Decode(Orders,
        [
            new PgTupleField("id", PgTupleValueKind.Value, 1L),
            new PgTupleField("customer", PgTupleValueKind.Value, "acme"),
        ]);

        Assert.NotNull(result.Diagnostic);
        Assert.Contains("2", result.Diagnostic, StringComparison.Ordinal);
        Assert.Contains("3", result.Diagnostic, StringComparison.Ordinal);
        // And the two values that WERE present decoded correctly under their own names, not shifted.
        Assert.Equal(1L, result.Row["id"]);
        Assert.Equal("acme", result.Row["customer"]);
        Assert.False(result.Row.ContainsKey("amount"));
    }

    [Fact]
    public void ATupleLongerThanItsRelationIsAlsoReported()
    {
        var result = PgTupleDecoder.Decode(Orders,
        [
            new PgTupleField("id", PgTupleValueKind.Value, 1L),
            new PgTupleField("customer", PgTupleValueKind.Value, "acme"),
            new PgTupleField("amount", PgTupleValueKind.Value, 1.0m),
            new PgTupleField("extra_column", PgTupleValueKind.Value, "surprise"),
        ]);

        Assert.NotNull(result.Diagnostic);
        Assert.Equal("surprise", result.Row["extra_column"]);
    }
}
