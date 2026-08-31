using Xunit;

namespace StreamsForge.Connectors.Fix.Tests;

/// <summary>Plan 019 wave F (D6): <see cref="FixRequiredFields"/> in isolation — no session, no socket, a
/// pure function over a hand-built row, the same "fake seam (no socket)" plan 018-C established and
/// <see cref="FixRowMapperTests"/> already uses for <see cref="FixRowMapper"/> itself.</summary>
public class FixRequiredFieldsTests
{
    private static Dictionary<string, object?> ValidNewOrderSingle() => new()
    {
        ["ClOrdID"] = "ORD1",
        ["Symbol"] = "EUR/USD",
        ["Side"] = "1",
        ["OrdType"] = "2",
        ["OrderQty"] = 1000000L,
    };

    // ------------------------------------------------------------------
    // NewOrderSingle (D)
    // ------------------------------------------------------------------

    [Fact]
    public void AValidNewOrderSinglePassesValidation()
    {
        Assert.True(FixRequiredFields.TryValidate(FixRequiredFields.NewOrderSingle, ValidNewOrderSingle(), out var failure));
        Assert.Null(failure);
    }

    [Theory]
    [InlineData("ClOrdID", "11")]
    [InlineData("Symbol", "55")]
    [InlineData("Side", "54")]
    [InlineData("OrdType", "40")]
    [InlineData("OrderQty", "38")]
    public void ANewOrderSingleMissingEachRequiredTagIsRefusedNamingIt(string missingField, string expectedTag)
    {
        var row = ValidNewOrderSingle();
        row.Remove(missingField);

        Assert.False(FixRequiredFields.TryValidate(FixRequiredFields.NewOrderSingle, row, out var failure));
        Assert.Contains(FixRequiredFields.NewOrderSingle, failure);
        Assert.Contains(missingField, failure);
        Assert.Contains($"tag {expectedTag}", failure);
    }

    [Fact]
    public void ANewOrderSingleWithABlankRequiredFieldIsRefusedJustLikeAnAbsentOne()
    {
        var row = ValidNewOrderSingle();
        row["Symbol"] = "";

        Assert.False(FixRequiredFields.TryValidate(FixRequiredFields.NewOrderSingle, row, out var failure));
        Assert.Contains("Symbol", failure);
    }

    [Fact]
    public void ANewOrderSingleWithANullRequiredFieldIsRefused()
    {
        var row = ValidNewOrderSingle();
        row["Side"] = null;

        Assert.False(FixRequiredFields.TryValidate(FixRequiredFields.NewOrderSingle, row, out var failure));
        Assert.Contains("Side", failure);
    }

    [Fact]
    public void ANonStringRequiredFieldCountsAsPresent()
    {
        // OrderQty/Price are ordinary numeric row values (see FixRowMapper.TryFormatValue) -- a required
        // field that happens to hold a long or a double must not be treated as "missing" just because it
        // is not a string.
        var row = ValidNewOrderSingle();
        row["OrderQty"] = 1000000L;

        Assert.True(FixRequiredFields.TryValidate(FixRequiredFields.NewOrderSingle, row, out var failure));
        Assert.Null(failure);
    }

    // ------------------------------------------------------------------
    // OrderCancelRequest (F) / OrderCancelReplaceRequest (G) -- the OrigClOrdID chain
    // ------------------------------------------------------------------

    [Fact]
    public void ACancelRequestWithoutOrigClOrdIdIsRefused()
    {
        var row = new Dictionary<string, object?> { ["ClOrdID"] = "ORD2", ["Symbol"] = "EUR/USD", ["Side"] = "1" };

        Assert.False(FixRequiredFields.TryValidate(FixRequiredFields.OrderCancelRequest, row, out var failure));
        Assert.Contains("OrigClOrdID", failure);
        Assert.Contains("tag 41", failure);
    }

    [Fact]
    public void ACancelRequestCarryingOrigClOrdIdAndTheOtherRequiredFieldsPasses()
    {
        var row = new Dictionary<string, object?>
        {
            ["OrigClOrdID"] = "ORD1",
            ["ClOrdID"] = "ORD2",
            ["Symbol"] = "EUR/USD",
            ["Side"] = "1",
        };

        Assert.True(FixRequiredFields.TryValidate(FixRequiredFields.OrderCancelRequest, row, out var failure));
        Assert.Null(failure);
    }

    [Fact]
    public void ACancelReplaceWithoutOrigClOrdIdIsRefused()
    {
        var row = ValidNewOrderSingle(); // has ClOrdID/Symbol/Side/OrdType/OrderQty, but no OrigClOrdID

        Assert.False(FixRequiredFields.TryValidate(FixRequiredFields.OrderCancelReplaceRequest, row, out var failure));
        Assert.Contains("OrigClOrdID", failure);
    }

    [Fact]
    public void ACancelReplaceCarryingEveryRequiredFieldPasses()
    {
        var row = ValidNewOrderSingle();
        row["OrigClOrdID"] = "ORD1";

        Assert.True(FixRequiredFields.TryValidate(FixRequiredFields.OrderCancelReplaceRequest, row, out var failure));
        Assert.Null(failure);
    }

    // ------------------------------------------------------------------
    // MsgTypes this table does not curate
    // ------------------------------------------------------------------

    [Fact]
    public void AnUncuratedMsgTypeIsNotGatedAtAll()
    {
        // "8" is ExecutionReport -- not in RequiredByMsgType. An empty row must not be refused: this table
        // only ever ADDS a refusal for the three MsgTypes it curates, never a new failure mode elsewhere.
        Assert.True(FixRequiredFields.TryValidate("8", new Dictionary<string, object?>(), out var failure));
        Assert.Null(failure);
    }
}
