using StreamsForge.Abstractions;
using StreamsForge.Dapr.Host.Actors;
using StreamsForge.Engine;
using Xunit;

namespace StreamsForge.Dapr.Tests;

/// <summary>
/// Plan 025 (table-over-pipeline, PARITY.md D6): unit tests for <see cref="PipelineResultMapping.
/// ToEventRecord"/> — the pure row-mapper <see cref="TableActor.ProcessPipelineResultsAsync"/> extracts
/// specifically so it's testable without any actor/timer/Dapr-sidecar machinery (mirrors
/// <see cref="TableAttachPolicy"/>'s own extraction rationale). Verbatim port of Orleans'
/// <c>PipelineInputs.ToEventRecord</c> (orleans/src/StreamsForge.Host/Grains/PipelineInputs.cs) — same
/// back-fill-only-if-absent rule for <c>_ts</c>/<c>_source</c>.
/// </summary>
public class PipelineResultMappingTests
{
    [Fact]
    public void ToEventRecord_BackfillsTimestampAndSourceWhenAbsent()
    {
        var envelope = new ResultEnvelope
        {
            PipelineId = "p1",
            TimestampMs = 12345,
            Row = new Dictionary<string, object?> { ["symbol"] = "AAPL", ["vwap"] = 100.5 },
        };

        var record = PipelineResultMapping.ToEventRecord(envelope, "vwap");

        Assert.Equal(12345L, record[EventRecord.TimestampField]);
        Assert.Equal("vwap", record[EventRecord.SourceField]);
        Assert.Equal("AAPL", record["symbol"]);
        Assert.Equal(100.5, record["vwap"]);
    }

    [Fact]
    public void ToEventRecord_DoesNotOverwriteExplicitlyProjectedTimestampOrSource()
    {
        // A pipeline that explicitly SELECTs _ts/_source has already said what they should be —
        // overwriting a value the query asked for would be a silent rewrite of the user's own output.
        var envelope = new ResultEnvelope
        {
            PipelineId = "p1",
            TimestampMs = 12345,
            Row = new Dictionary<string, object?>
            {
                [EventRecord.TimestampField] = 999L,
                [EventRecord.SourceField] = "custom",
                ["symbol"] = "AAPL",
            },
        };

        var record = PipelineResultMapping.ToEventRecord(envelope, "vwap");

        Assert.Equal(999L, record[EventRecord.TimestampField]);
        Assert.Equal("custom", record[EventRecord.SourceField]);
    }
}
