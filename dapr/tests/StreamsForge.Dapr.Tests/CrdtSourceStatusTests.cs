using StreamsForge.Abstractions;
using StreamsForge.Dapr.Host.Facades;
using Xunit;

namespace StreamsForge.Dapr.Tests;

/// <summary>
/// PARITY.md D5 (plan 025 wave C3): <see cref="CrdtSourceStatus"/> is the pure decision behind
/// <see cref="DaprConnectorStatusFacade"/>'s crdt-kind branch, extracted so it is testable against a bare
/// <see cref="SourceDefinition"/> — no <see cref="ICatalogFacadeFactory"/>, no actor proxy, no Dapr
/// sidecar. <see cref="DaprConnectorStatusFacade"/> itself is exercised only through
/// this pure helper — the gating condition (Crdt kind AND Enabled) lives in
/// <see cref="DaprConnectorStatusFacade.GetStatusAsync"/>, which this test suite does not re-derive; it
/// only proves what <see cref="CrdtSourceStatus.Synthesize"/> builds once that gate has passed.
/// </summary>
public class CrdtSourceStatusTests
{
    [Fact]
    public void Synthesize_ReturnsErrorStatus_WithTheOrleansOnlyMessage()
    {
        var def = new SourceDefinition { Name = "doc1", Kind = "crdt", Enabled = true };

        var status = CrdtSourceStatus.Synthesize(def);

        Assert.Equal("doc1", status.SourceName);
        Assert.Equal("error", status.LastStatus);
        Assert.Equal(
            "Source 'doc1' has kind 'crdt', which is Orleans-only (plan 020 D9) — this flavor stores the " +
            "definition but will never run it, so this source emits nothing. Run it on an Orleans instance " +
            "and subscribe to it here with a 'grpc' source.",
            status.LastError);
    }

    [Fact]
    public void Synthesize_ReportsNotRunning_ViaNullNextRunMs_AndZeroedCounters()
    {
        // There is no actor behind this status, so every counter/schedule field a real connector would
        // populate stays at its zero/null default — NextRunMs = null in particular is this status's
        // stand-in for "not running": every real connector kind sets it to its next scheduled poll.
        var def = new SourceDefinition { Name = "doc1", Kind = "crdt", Enabled = true };

        var status = CrdtSourceStatus.Synthesize(def);

        Assert.Null(status.NextRunMs);
        Assert.Null(status.LastRunMs);
        Assert.Equal(0, status.ConsecutiveFailures);
        Assert.Equal(0, status.EventsEmittedTotal);
        Assert.Equal(0, status.LastBatchCount);
    }

    [Fact]
    public void MessageFor_NamesTheActualSourceAndKind()
    {
        var def = new SourceDefinition { Name = "orders-doc", Kind = "crdt", Enabled = true };

        var message = CrdtSourceStatus.MessageFor(def);

        Assert.Contains("'orders-doc'", message);
        Assert.Contains("'crdt'", message);
        Assert.Contains("grpc", message);
    }
}
