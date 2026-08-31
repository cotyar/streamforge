using System.Text.Json;
using StreamsForge.AppCore.Sinks;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>
/// Plan 009 B2: pins the exact wire shape a NATS sink message serializes to — camelCase, matching every
/// other JSON contract in this codebase (ConfigJsonMapper.ModelOptions, the REST/SignalR JSON options).
/// <see cref="NatsSinkClient.PublishAsync{T}"/> uses these same <c>JsonSerializerOptions</c> internally;
/// these tests exercise the shape directly rather than through a network call.
/// </summary>
public class SinkMessagesTests
{
    private static readonly JsonSerializerOptions CamelCase = new() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

    [Fact]
    public void PipelineRowMessage_SerializesAsCamelCase()
    {
        var message = new NatsPipelineRowMessage
        {
            PipelineId = "p1",
            Seq = 42,
            TimestampMs = 1_700_000_000_000,
            Row = new Dictionary<string, object?> { ["price"] = 101.5, ["symbol"] = "ABC" },
        };

        var json = JsonSerializer.Serialize(message, CamelCase);

        Assert.Contains("\"pipelineId\":\"p1\"", json);
        Assert.Contains("\"seq\":42", json);
        Assert.Contains("\"timestampMs\":1700000000000", json);
        Assert.Contains("\"row\":{\"price\":101.5,\"symbol\":\"ABC\"}", json);
    }

    [Fact]
    public void TableDeltaMessage_SerializesAsCamelCase_AndCarriesTableAndSeq()
    {
        // Unlike a pipeline row, TableDeltaDto alone has no table name or batch seq — this message
        // wraps both in, which is the whole reason NatsTableDeltaMessage exists (see its own doc).
        var message = new NatsTableDeltaMessage
        {
            Table = "positions",
            Seq = 7,
            Row = new Dictionary<string, object?> { ["qty"] = 10L },
            Weight = 1,
        };

        var json = JsonSerializer.Serialize(message, CamelCase);

        Assert.Contains("\"table\":\"positions\"", json);
        Assert.Contains("\"seq\":7", json);
        Assert.Contains("\"weight\":1", json);
        Assert.Contains("\"row\":{\"qty\":10}", json);
    }
}
