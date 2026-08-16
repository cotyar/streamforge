using StreamForge.AppCore.Ingest;
using Xunit;

namespace StreamForge.Engine.Tests;

/// <summary>
/// Gap 1 of wishlist item 13 ("gRPC ingest bypasses the retraction validate gate"): the fix wires
/// <see cref="IngestGrpcService"/> (StreamForge.Host.Grpc) through the SAME
/// <see cref="RetractConsumerValidation.FindNonLatestByConsumer"/> the REST path
/// (SourcesEndpoints.cs) already ran, rather than a second copy. The one piece of new logic that fix
/// needed — turning a batch of already-converted rows into "which indexes ask for a retraction" — is
/// <see cref="RetractConsumerValidation.CollectRetractRowIndexes"/>, added specifically so gRPC (which
/// has no JSON request body to scan the way SourcesEndpoints.cs's own private CollectRetractRowIndexes
/// does) can reuse one implementation instead of hand-rolling a second scan. This file is that method's
/// own unit coverage — pure POCOs, no gRPC/HTTP/cluster infrastructure, mirroring
/// RetractConsumerValidationTests.cs's own zero-setup style for the identical reason stated there.
/// </summary>
public class RetractConsumerValidationCollectRowIndexesTests
{
    private static Dictionary<string, object?> Row(bool? retract = null, string? id = null)
    {
        var row = new Dictionary<string, object?>();
        if (id is not null)
        {
            row["order_id"] = id;
        }
        if (retract is not null)
        {
            row[IngressRowAcceptance.RetractField] = retract;
        }
        return row;
    }

    [Fact]
    public void NoRowsCarryTheFlag_ReturnsEmpty()
    {
        var rows = new List<Dictionary<string, object?>> { Row(id: "a"), Row(id: "b") };

        var indexes = RetractConsumerValidation.CollectRetractRowIndexes(rows);

        Assert.Empty(indexes);
    }

    [Fact]
    public void ARowFlaggedTrue_IsCollectedByIndex()
    {
        var rows = new List<Dictionary<string, object?>> { Row(id: "a"), Row(retract: true, id: "b"), Row(id: "c") };

        var indexes = RetractConsumerValidation.CollectRetractRowIndexes(rows);

        Assert.Equal([1], indexes);
    }

    [Fact]
    public void MultipleFlaggedRows_AreAllCollectedInOrder()
    {
        var rows = new List<Dictionary<string, object?>>
        {
            Row(retract: true, id: "a"),
            Row(id: "b"),
            Row(retract: true, id: "c"),
        };

        var indexes = RetractConsumerValidation.CollectRetractRowIndexes(rows);

        Assert.Equal([0, 2], indexes);
    }

    [Fact]
    public void ARowFlaggedFalse_IsNotCollected()
    {
        // A row that explicitly says "_retract": false is an ordinary assert, not a retraction — only
        // `true` means anything (IngressRowAcceptance's own coercion treats it identically: only a
        // true-coerced value flips the weight).
        var rows = new List<Dictionary<string, object?>> { Row(retract: false, id: "a") };

        var indexes = RetractConsumerValidation.CollectRetractRowIndexes(rows);

        Assert.Empty(indexes);
    }

    [Fact]
    public void AMissingFlag_IsNotCollected()
    {
        var rows = new List<Dictionary<string, object?>> { Row(id: "a") };

        var indexes = RetractConsumerValidation.CollectRetractRowIndexes(rows);

        Assert.Empty(indexes);
    }

    [Fact]
    public void ANonBoolValue_IsNotCollected()
    {
        // A malformed/mistyped "_retract" (a string "true", a number) does not normalize to the boolean
        // `true` JsonValueNormalizer.Normalize would produce from real JSON — same "only true counts"
        // rule as the false case above, just via a different input shape. This mirrors what a gRPC
        // Value.BoolValue always already is (a real bool) vs. what a hand-crafted or buggy client could
        // still send as a Struct field of the wrong kind.
        var rows = new List<Dictionary<string, object?>> { new() { ["_retract"] = "true" } };

        var indexes = RetractConsumerValidation.CollectRetractRowIndexes(rows);

        Assert.Empty(indexes);
    }

    [Fact]
    public void EmptyBatch_ReturnsEmpty()
    {
        var indexes = RetractConsumerValidation.CollectRetractRowIndexes([]);

        Assert.Empty(indexes);
    }
}
