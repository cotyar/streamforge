using StreamsForge.Abstractions;
using Xunit;

namespace StreamsForge.Connectors.Fix.Tests;

/// <summary>Plan 019 wave F (D6/D7): <see cref="FixDuplexSession.SendAsync"/> before any socket has ever
/// been opened — the same "not logged on" fake seam <see cref="FixDuplexSessionTests"/> already uses (no
/// <see cref="FixDuplexSession.SubscribeAsync"/> enumeration means <c>IsReady</c> is false), which is
/// enough to exercise <c>ClOrdID</c> generation/passthrough (it runs BEFORE the readiness check, see
/// <see cref="FixDuplexSession.SendAsync"/>'s own doc comment) and the never-throw contract. Proving the
/// required-field GATE actually blocks a send end-to-end needs a session that reaches
/// <c>IsReady == true</c>, which needs a real socket -- that lives in
/// <see cref="FixOrderEntryValidationAcceptanceTests"/> instead.</summary>
public class FixDuplexOrderEntryTests
{
    private static FixSourceConfig ConfigWithGeneration(bool generate)
    {
        var config = FixTestSupport.ValidConfig();
        config.GenerateClOrdId = generate;
        return config;
    }

    // ------------------------------------------------------------------
    // ClOrdID passthrough / generation (D7)
    // ------------------------------------------------------------------

    [Fact]
    public async Task ACallerSuppliedClOrdIdIsNeverOverwrittenEvenWhenGenerationIsOn()
    {
        var session = new FixDuplexSession("fx-passthrough", ConfigWithGeneration(true));
        var rows = new List<Dictionary<string, object?>> { new() { ["MsgType"] = "D", ["ClOrdID"] = "CALLER-1" } };

        var outcome = await session.SendAsync(rows, CancellationToken.None);

        Assert.Equal("CALLER-1", Assert.Single(outcome.Failures).CorrelationId);
    }

    [Fact]
    public async Task ClOrdIdIsNotGeneratedByDefault()
    {
        // FixSourceConfig.GenerateClOrdId defaults to false -- a row with no ClOrdID stays without one.
        var session = new FixDuplexSession("fx-no-generation", FixTestSupport.ValidConfig());
        var rows = new List<Dictionary<string, object?>> { new() { ["MsgType"] = "D" } };

        var outcome = await session.SendAsync(rows, CancellationToken.None);

        Assert.Null(Assert.Single(outcome.Failures).CorrelationId);
    }

    [Fact]
    public async Task ClOrdIdIsGeneratedWhenMissingAndOptedIn()
    {
        var session = new FixDuplexSession("fx-generation", ConfigWithGeneration(true));
        var rows = new List<Dictionary<string, object?>> { new() { ["MsgType"] = "D" } };

        var outcome = await session.SendAsync(rows, CancellationToken.None);

        var generated = Assert.Single(outcome.Failures).CorrelationId;
        Assert.False(string.IsNullOrEmpty(generated));
    }

    [Fact]
    public async Task GeneratedClOrdIdsAreUniqueAcrossRowsInTheSameBatch()
    {
        var session = new FixDuplexSession("fx-generation-unique", ConfigWithGeneration(true));
        var rows = new List<Dictionary<string, object?>>
        {
            new() { ["MsgType"] = "D" },
            new() { ["MsgType"] = "D" },
            new() { ["MsgType"] = "D" },
        };

        var outcome = await session.SendAsync(rows, CancellationToken.None);

        var ids = outcome.Failures.Select(f => f.CorrelationId).ToList();
        Assert.Equal(3, ids.Count);
        Assert.Equal(ids.Count, ids.Distinct().Count());
    }

    [Fact]
    public async Task GenerationAcrossTwoSeparateCallsAlsoStaysUnique()
    {
        var session = new FixDuplexSession("fx-generation-unique-2", ConfigWithGeneration(true));

        var first = await session.SendAsync([new() { ["MsgType"] = "D" }], CancellationToken.None);
        var second = await session.SendAsync([new() { ["MsgType"] = "D" }], CancellationToken.None);

        Assert.NotEqual(first.Failures[0].CorrelationId, second.Failures[0].CorrelationId);
    }

    [Fact]
    public async Task GenerationDoesNotMutateTheCallersOriginalRow()
    {
        var session = new FixDuplexSession("fx-no-mutation", ConfigWithGeneration(true));
        var row = new Dictionary<string, object?> { ["MsgType"] = "D" };

        await session.SendAsync([row], CancellationToken.None);

        Assert.False(row.ContainsKey("ClOrdID")); // the caller's own dictionary is untouched
    }

    // ------------------------------------------------------------------
    // Ordering: "not logged on" wins over a required-field gap, by design (see SendAsync's own doc)
    // ------------------------------------------------------------------

    [Fact]
    public async Task ANotLoggedOnFailureIsReportedEvenForARowMissingRequiredFields()
    {
        // No socket was ever opened, so IsReady is false. This row is missing Side/OrdType/OrderQty (all
        // required for NewOrderSingle per FixRequiredFields) -- but readiness is checked first, so the
        // reported reason is "not logged on", not a required-field complaint. See FixDuplexSession.
        // SendAsync's own doc comment for why required-field validation is the one check NOT ordered
        // "worst reason first".
        var session = new FixDuplexSession("fx-not-ready-incomplete", FixTestSupport.ValidConfig());
        var rows = new List<Dictionary<string, object?>> { new() { ["MsgType"] = "D", ["ClOrdID"] = "ORD1" } };

        var outcome = await session.SendAsync(rows, CancellationToken.None);

        var failure = Assert.Single(outcome.Failures);
        Assert.Contains("not logged on", failure.Reason, StringComparison.OrdinalIgnoreCase);
    }

    // ------------------------------------------------------------------
    // Never-throw, for every refusal path this wave adds
    // ------------------------------------------------------------------

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SendAsyncNeverThrowsRegardlessOfGenerationOrRowShape(bool generate)
    {
        var session = new FixDuplexSession("fx-never-throws", ConfigWithGeneration(generate));
        var rows = new List<Dictionary<string, object?>>
        {
            new() { ["MsgType"] = "D" },                                   // missing everything
            new() { ["MsgType"] = "D", ["ClOrdID"] = "" },                  // blank ClOrdID
            new() { ["MsgType"] = "F", ["ClOrdID"] = "ORD1" },              // cancel, no OrigClOrdID
            new() { ["MsgType"] = "G", ["ClOrdID"] = "ORD1", ["OrigClOrdID"] = "ORD0" }, // replace, incomplete
            new(),                                                          // no MsgType at all
        };

        var outcome = await session.SendAsync(rows, CancellationToken.None); // must not throw

        Assert.Equal(0, outcome.Sent);
        Assert.Equal(rows.Count, outcome.Failed);
        Assert.All(outcome.Failures, f => Assert.False(string.IsNullOrEmpty(f.Reason)));
    }
}
