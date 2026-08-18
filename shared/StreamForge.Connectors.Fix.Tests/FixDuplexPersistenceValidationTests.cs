using StreamForge.Abstractions;
using StreamForge.AppCore.Transports;
using Xunit;

namespace StreamForge.Connectors.Fix.Tests;

/// <summary>
/// Plan 019 wave G, deliverable 1 (D5): "sequence-number persistence stops being optional" — for
/// <c>fix-duplex</c>, and ONLY for it. On an order session the sequence-number store IS the record of what
/// this platform has sent and received; losing it is a resend request the platform cannot answer, not the
/// re-sent-quote inconvenience a lost market-data store costs (plan 018's <c>fix</c> kind, unaffected by
/// this wave — see <see cref="FixKindIsUnaffected"/> below and TRANSPORTS.md's FIX section for the wording
/// this class's messages point back to).
///
/// A NEW file, not an edit to <c>FixDuplexTransportTests.cs</c> — this wave's instruction is to touch no
/// pre-existing test file where avoidable. The two assertions in that file that directly contradicted this
/// wave's own job (<c>ValidateAcceptsAGoodConfig</c>'s premise, and the test that is now named
/// <c>ValidateNowRequiresAStorePath</c>) were the unavoidable exception — both are self-documented in that
/// file as anticipating exactly this change.
/// </summary>
public class FixDuplexPersistenceValidationTests
{
    private static FixSourceConfig DurableConfig() => new()
    {
        Host = "fix.venue.example.com",
        Port = 9880,
        SenderCompId = "CLIENT",
        TargetCompId = "VENUE",
        BeginString = "FIX.4.4",
        HeartBtIntSeconds = 30,
        QueueCapacity = 100,
        StorePath = "/data/fix-duplex/venue-orders.store",
        ResetOnLogon = false,
    };

    // ------------------------------------------------------------------
    // StorePath is required
    // ------------------------------------------------------------------

    [Fact]
    public void EmptyStorePathIsRefusedWithAReasonNotJustAWhat()
    {
        var config = DurableConfig();
        config.StorePath = "";

        var errors = new List<string>();
        new FixDuplexTransport().Validate(FixDuplexTestSupport.FixDuplexSource(config), errors);

        var message = Assert.Single(errors, e => e.StartsWith("connector.fix.storePath is required", StringComparison.Ordinal));
        // The message must say WHY, not just WHAT (plan 019 D5's own wording) -- an operator reading only
        // "storePath is required" and pointing it at a path that does not survive a restart has satisfied
        // the validator and not the requirement, so the reasoning has to be in the message itself.
        Assert.Contains("record of what this platform has sent and received", message, StringComparison.Ordinal);
        Assert.Contains("resend request", message, StringComparison.Ordinal);
    }

    [Fact]
    public void WhitespaceOnlyStorePathIsRefusedTheSameAsEmpty()
    {
        var config = DurableConfig();
        config.StorePath = "   ";

        var errors = new List<string>();
        new FixDuplexTransport().Validate(FixDuplexTestSupport.FixDuplexSource(config), errors);

        Assert.Contains(errors, e => e.StartsWith("connector.fix.storePath is required", StringComparison.Ordinal));
    }

    // ------------------------------------------------------------------
    // ResetOnLogon must be false -- even with a StorePath set
    // ------------------------------------------------------------------

    [Fact]
    public void StorePathSetButResetOnLogonTrueIsStillRefused()
    {
        var config = DurableConfig();
        config.ResetOnLogon = true;

        var errors = new List<string>();
        new FixDuplexTransport().Validate(FixDuplexTestSupport.FixDuplexSource(config), errors);

        var message = Assert.Single(errors, e => e.StartsWith("connector.fix.resetOnLogon must be false", StringComparison.Ordinal));
        Assert.Contains("throws away", message, StringComparison.Ordinal);
        // A durable path was supplied -- this must be the ONLY error, not a StorePath-required complaint
        // riding along with it.
        Assert.DoesNotContain(errors, e => e.StartsWith("connector.fix.storePath is required", StringComparison.Ordinal));
    }

    [Fact]
    public void EmptyStorePathAndResetOnLogonTrueBothSurface()
    {
        // The FixSourceConfig default shape (market-data-shaped: StorePath empty, ResetOnLogon true) hits
        // both rules at once -- both messages must be present, not just the first one checked.
        var config = DurableConfig();
        config.StorePath = "";
        config.ResetOnLogon = true;

        var errors = new List<string>();
        new FixDuplexTransport().Validate(FixDuplexTestSupport.FixDuplexSource(config), errors);

        Assert.Contains(errors, e => e.StartsWith("connector.fix.storePath is required", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.StartsWith("connector.fix.resetOnLogon must be false", StringComparison.Ordinal));
    }

    // ------------------------------------------------------------------
    // Cheap, honest path checks: absolute, and obviously ephemeral
    // ------------------------------------------------------------------

    [Fact]
    public void ARelativeStorePathIsRefused()
    {
        var config = DurableConfig();
        config.StorePath = "fix-store/venue.store";

        var errors = new List<string>();
        new FixDuplexTransport().Validate(FixDuplexTestSupport.FixDuplexSource(config), errors);

        Assert.Contains(errors, e => e.StartsWith("connector.fix.storePath 'fix-store/venue.store' must be an absolute path", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData("/tmp/fix-store/venue.store")]
    [InlineData("/tmp")]
    [InlineData("/var/tmp/fix-store/venue.store")]
    [InlineData("/var/tmp")]
    public void AStorePathUnderAPosixTempDirectoryIsRefused(string path)
    {
        var config = DurableConfig();
        config.StorePath = path;

        var errors = new List<string>();
        new FixDuplexTransport().Validate(FixDuplexTestSupport.FixDuplexSource(config), errors);

        var message = Assert.Single(errors, e => e.Contains("POSIX temp directory", StringComparison.Ordinal));
        // Stated as a hint, not a guarantee -- this process cannot see the volume behind ANY path (plan
        // 019-G's brief: "do not invent a guarantee"), so the message must not claim certainty about paths
        // it did NOT flag either.
        Assert.Contains("cannot see the actual volume behind ANY path", message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("/tmpfs-data/fix-store")] // starts with "/tmp" as a STRING but is a different directory
    [InlineData("/data/not-var-tmp-either/fix-store")]
    public void APathThatMerelyStartsWithTheSameLettersAsTmpIsNotFlagged(string path)
    {
        var config = DurableConfig();
        config.StorePath = path;

        var errors = new List<string>();
        new FixDuplexTransport().Validate(FixDuplexTestSupport.FixDuplexSource(config), errors);

        Assert.DoesNotContain(errors, e => e.Contains("POSIX temp directory", StringComparison.Ordinal));
    }

    // ------------------------------------------------------------------
    // The accepted configuration
    // ------------------------------------------------------------------

    [Fact]
    public void AnAbsoluteDurableNonEphemeralStorePathWithResetOnLogonFalseIsAccepted()
    {
        var errors = new List<string>();
        new FixDuplexTransport().Validate(FixDuplexTestSupport.FixDuplexSource(DurableConfig()), errors);

        Assert.Empty(errors);
    }

    // ------------------------------------------------------------------
    // Regression: the plain 'fix' kind (plan 018) is completely unaffected
    // ------------------------------------------------------------------

    [Fact]
    public void FixKindIsUnaffected()
    {
        // Plan 018's fix kind keeps ResetOnLogon=true + an in-memory store as its own, still-current
        // default -- FixTestSupport.ValidConfig() (StorePath empty, ResetOnLogon true, the market-data
        // shape) must still validate clean through FixInboundTransport, unmoved by this wave.
        var config = FixTestSupport.ValidConfig();
        Assert.Equal("", config.StorePath);
        Assert.True(config.ResetOnLogon);

        var errors = new List<string>();
        new FixInboundTransport().Validate(FixTestSupport.FixSource(config), errors);

        Assert.Empty(errors);
    }

    [Fact]
    public void FixKindAcceptsAnEphemeralTempStorePathToo()
    {
        // The /tmp //var/tmp refusal is fix-duplex-only -- a market-data session choosing an in-memory
        // (or even a /tmp-backed) store is exactly plan 018's own sanctioned shape, not a mistake to flag.
        var config = FixTestSupport.ValidConfig();
        config.StorePath = "/tmp/fix-market-data.store";

        var errors = new List<string>();
        new FixInboundTransport().Validate(FixTestSupport.FixSource(config), errors);

        Assert.Empty(errors);
    }
}
