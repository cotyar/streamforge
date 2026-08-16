using StreamForge.Abstractions;
using StreamForge.Host.Grpc;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// Gap 1 of wishlist item 13 ("gRPC ingest bypasses the retraction validate gate"): before this fix,
/// <see cref="IngestGrpcService"/> called <c>IIngressFacade.PushAsync</c> straight off the wire, so a
/// "_retract" row pushed over gRPC was silently accepted instead of refused — only the REST path
/// (SourcesEndpoints.cs) ran <c>RetractConsumerValidation.FindNonLatestByConsumer</c> before admission.
///
/// This repo has no gRPC/HTTP test harness (see SourcesEndpointsLogicTests.cs's own class doc: "There is
/// no HTTP-level test harness in this repo"), so <see cref="IngestGrpcService.Ingest"/>'s bidi-stream loop
/// itself is not exercised end-to-end here — the same limitation the REST retract-validation branch in
/// SourcesEndpoints.cs already lives with today. What IS pinned here, pure and infrastructure-free: the
/// two static seams <see cref="IngestGrpcService.Ingest"/> extracted from that loop —
/// <see cref="IngestGrpcService.BuildRetractErrors"/> (proves gRPC's per-row error text is byte-identical
/// to SourcesEndpoints.cs's own <c>"_retract" is only valid when …</c> message) and
/// <see cref="IngestGrpcService.RejectedResult"/> (proves the whole-batch-rejection shape sent back on a
/// non-partial invalid batch matches IngestOutcome.Invalid's own documented contract: Invalid count,
/// RowErrors, and a non-null Error naming how many rows failed).
/// </summary>
public class IngestGrpcServiceRetractGateTests
{
    [Fact]
    public void BuildRetractErrors_MatchesSourcesEndpointsWordingExactly()
    {
        var errors = IngestGrpcService.BuildRetractErrors("order_events", "order_mirror", [0, 2]);

        Assert.Equal(2, errors.Count);
        Assert.Equal(
            "row 0: \"_retract\" is only valid when every running table reading source 'order_events' directly is a LATEST BY table; 'order_mirror' is not",
            errors[0]);
        Assert.Equal(
            "row 2: \"_retract\" is only valid when every running table reading source 'order_events' directly is a LATEST BY table; 'order_mirror' is not",
            errors[1]);
    }

    [Fact]
    public void BuildRetractErrors_OneMessagePerOffendingIndex_InOrder()
    {
        var errors = IngestGrpcService.BuildRetractErrors("s", "t", [4, 1, 9]);

        Assert.Equal(3, errors.Count);
        Assert.StartsWith("row 4:", errors[0]);
        Assert.StartsWith("row 1:", errors[1]);
        Assert.StartsWith("row 9:", errors[2]);
    }

    [Fact]
    public void RejectedResult_IsInvalidWithNoAcceptedRows()
    {
        var errors = IngestGrpcService.BuildRetractErrors("order_events", "order_mirror", [0]);

        var result = IngestGrpcService.RejectedResult(errors);

        Assert.Equal(IngestOutcome.Invalid, result.Outcome);
        Assert.Equal(0, result.Accepted);
        Assert.Equal(0, result.Dropped);
        Assert.Equal(0, result.Duplicate);
        Assert.Equal(1, result.Invalid);
        Assert.Equal("1 row(s) failed retract validation", result.Error);
        Assert.Single(result.RowErrors);
    }

    [Fact]
    public void RejectedResult_CountsEveryOffendingRow()
    {
        var errors = IngestGrpcService.BuildRetractErrors("s", "t", [0, 1, 2]);

        var result = IngestGrpcService.RejectedResult(errors);

        Assert.Equal(3, result.Invalid);
        Assert.Equal("3 row(s) failed retract validation", result.Error);
        Assert.Equal(3, result.RowErrors.Count);
    }
}
