using StreamForge.Abstractions;
using StreamForge.AppCore.Ingest;
using Xunit;

namespace StreamForge.Engine.Tests;

/// <summary>Wishlist "explicit key retraction through ingest": <see cref="RetractConsumerValidation"/>
/// — the validate-time rule SourcesEndpoints.cs runs before admitting a "_retract" row. Pure (plain
/// POCOs, no catalog/facade/host), mirroring IngressAdmissionTests.cs's own zero-setup style for the
/// same reason IngressAdmission.cs states in its class doc: exhaustive per-branch coverage with no
/// infrastructure to stand up.</summary>
public class RetractConsumerValidationTests
{
    private static readonly SourceDefinition OrderEvents = new()
    {
        Name = "order_events",
        Kind = SourceKinds.Ingest,
        Fields = [new FieldDef("order_id", FieldType.String), new FieldDef("stage", FieldType.String)],
    };

    private static TableDefinition Table(string name, string sql, PipelineStatus status = PipelineStatus.Running, params string[] streamInputs) => new()
    {
        Id = name,
        Name = name,
        Sql = sql,
        Status = status,
        StreamInputs = [.. streamInputs],
    };

    [Fact]
    public void NoConsumersAtAllIsSafe()
    {
        var offender = RetractConsumerValidation.FindNonLatestByConsumer("order_events", [OrderEvents], []);

        Assert.Null(offender);
    }

    [Fact]
    public void ASingleRunningLatestByConsumerIsSafe()
    {
        var t = Table("order_states", "SELECT order_id, stage FROM order_events LATEST BY (order_id)", streamInputs: "order_events");

        var offender = RetractConsumerValidation.FindNonLatestByConsumer("order_events", [OrderEvents], [t]);

        Assert.Null(offender);
    }

    [Fact]
    public void ARunningGroupByConsumerIsRejectedByName()
    {
        var t = Table("order_counts", "SELECT stage, COUNT(*) AS cnt FROM order_events GROUP BY stage", streamInputs: "order_events");

        var offender = RetractConsumerValidation.FindNonLatestByConsumer("order_events", [OrderEvents], [t]);

        Assert.Equal("order_counts", offender);
    }

    [Fact]
    public void ARunningPlainProjectionConsumerIsRejected()
    {
        var t = Table("order_mirror", "SELECT order_id, stage FROM order_events", streamInputs: "order_events");

        var offender = RetractConsumerValidation.FindNonLatestByConsumer("order_events", [OrderEvents], [t]);

        Assert.Equal("order_mirror", offender);
    }

    [Fact]
    public void AStoppedNonLatestByConsumerDoesNotBlockIt()
    {
        // Not currently wired to receive live deltas (see the method's own doc) — its shape carries no
        // risk today, so a retraction to a source it merely used to read (or will read once started)
        // is not blocked on its account.
        var t = Table("order_counts", "SELECT stage, COUNT(*) AS cnt FROM order_events GROUP BY stage", PipelineStatus.Stopped, "order_events");

        var offender = RetractConsumerValidation.FindNonLatestByConsumer("order_events", [OrderEvents], [t]);

        Assert.Null(offender);
    }

    [Fact]
    public void AConsumerOfADifferentSourceIsIgnored()
    {
        var other = new SourceDefinition { Name = "quotes", Kind = SourceKinds.Ingest, Fields = [new FieldDef("symbol", FieldType.String)] };
        var t = Table("quote_mirror", "SELECT symbol FROM quotes", streamInputs: "quotes");

        var offender = RetractConsumerValidation.FindNonLatestByConsumer("order_events", [OrderEvents, other], [t]);

        Assert.Null(offender);
    }

    [Fact]
    public void MultipleConsumersOneBadOneGoodStillRejects()
    {
        var good = Table("order_states", "SELECT order_id, stage FROM order_events LATEST BY (order_id)", streamInputs: "order_events");
        var bad = Table("order_counts", "SELECT stage, COUNT(*) AS cnt FROM order_events GROUP BY stage", streamInputs: "order_events");

        var offender = RetractConsumerValidation.FindNonLatestByConsumer("order_events", [OrderEvents], [good, bad]);

        Assert.Equal("order_counts", offender);
    }

    [Fact]
    public void ATableWhoseSqlNoLongerCompilesIsTreatedAsUnsafe()
    {
        // References a column that doesn't exist on the (test-supplied) current schema — simulates a
        // table whose upstream shape drifted since it was created; "can't prove it's safe" must reject,
        // not skip.
        var t = Table("broken", "SELECT no_such_column FROM order_events LATEST BY (order_id)", streamInputs: "order_events");

        var offender = RetractConsumerValidation.FindNonLatestByConsumer("order_events", [OrderEvents], [t]);

        Assert.Equal("broken", offender);
    }
}
