using StreamsForge.Abstractions;
using StreamsForge.Api;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>Plan 009 B1: <see cref="SourceValidation.Validate"/> for the <c>nats</c> kind — a sibling
/// to orleans/tests/StreamsForge.Host.Tests/SourcesEndpointsLogicTests.cs's grpc/url/file/folder
/// coverage, kept in its own file since that one is not mine to edit (new-files-only convention).</summary>
public class SourceValidationNatsTests
{
    private static SourceDefinition Def(ConnectorConfig? connector = null) => new()
    {
        Name = "s",
        Fields = [new FieldDef("price", FieldType.Double)],
        Kind = SourceKinds.Nats,
        Connector = connector,
    };

    [Fact]
    public void Nats_kind_is_recognized()
    {
        var errors = SourceValidation.Validate(Def());
        Assert.DoesNotContain(errors, e => e.Contains("not recognized"));
    }

    [Fact]
    public void Nats_requires_connector()
    {
        var def = Def();
        Assert.Contains(SourceValidation.Validate(def), e => e.Contains("requires a connector configuration"));
    }

    [Fact]
    public void Nats_requires_connector_nats()
    {
        var def = Def(new ConnectorConfig());
        Assert.Contains(SourceValidation.Validate(def), e => e.Contains("requires connector.nats"));
    }

    [Fact]
    public void Nats_requires_url_and_subject()
    {
        var def = Def(new ConnectorConfig { Nats = new NatsSubConfig { Url = "", Subject = "" } });
        var errors = SourceValidation.Validate(def);
        Assert.Contains(errors, e => e.Contains("connector.nats.url is required"));
        Assert.Contains(errors, e => e.Contains("connector.nats.subject is required"));
    }

    [Fact]
    public void Nats_rejects_unknown_format()
    {
        var def = Def(new ConnectorConfig { Nats = new NatsSubConfig { Url = "nats://x", Subject = "s", Format = "xml" } });
        Assert.Contains(SourceValidation.Validate(def), e => e.Contains("format 'xml' is not recognized"));
    }

    [Theory]
    [InlineData(FileFormats.Ndjson)]
    [InlineData(FileFormats.JsonArray)]
    [InlineData(FileFormats.Csv)]
    public void Nats_accepts_every_known_format(string format)
    {
        var def = Def(new ConnectorConfig { Nats = new NatsSubConfig { Url = "nats://x", Subject = "s", Format = format } });
        Assert.Empty(SourceValidation.Validate(def));
    }

    [Fact]
    public void Nats_accepts_a_well_formed_core_config_with_default_json_format()
    {
        var def = Def(new ConnectorConfig { Nats = new NatsSubConfig { Url = "nats://localhost:4222", Subject = "trades.>" } });
        Assert.Empty(SourceValidation.Validate(def));
    }

    [Fact]
    public void Nats_accepts_a_config_with_a_queueGroup()
    {
        var def = Def(new ConnectorConfig { Nats = new NatsSubConfig { Url = "nats://x", Subject = "s", QueueGroup = "workers" } });
        Assert.Empty(SourceValidation.Validate(def));
    }

    // ------------------------------------------------------------------
    // JetStream sub-config.
    // ------------------------------------------------------------------

    [Fact]
    public void Nats_jetStream_requires_stream_and_durable()
    {
        var def = Def(new ConnectorConfig
        {
            Nats = new NatsSubConfig { Url = "nats://x", Subject = "s", JetStream = new NatsJetStreamConfig { Stream = "", Durable = "" } },
        });
        var errors = SourceValidation.Validate(def);
        Assert.Contains(errors, e => e.Contains("jetStream.stream is required"));
        Assert.Contains(errors, e => e.Contains("jetStream.durable is required"));
    }

    [Fact]
    public void Nats_jetStream_requires_positive_maxAckPending()
    {
        var def = Def(new ConnectorConfig
        {
            Nats = new NatsSubConfig
            {
                Url = "nats://x", Subject = "s",
                JetStream = new NatsJetStreamConfig { Stream = "st", Durable = "d", MaxAckPending = 0 },
            },
        });
        Assert.Contains(SourceValidation.Validate(def), e => e.Contains("maxAckPending must be > 0"));
    }

    [Fact]
    public void Nats_accepts_a_well_formed_jetStream_config()
    {
        var def = Def(new ConnectorConfig
        {
            Nats = new NatsSubConfig { Url = "nats://x", Subject = "s", JetStream = new NatsJetStreamConfig { Stream = "st", Durable = "d" } },
        });
        Assert.Empty(SourceValidation.Validate(def));
    }

    // ------------------------------------------------------------------
    // Schedule is ignored for nats (persistent subscription, same as grpc); Mapping applies.
    // ------------------------------------------------------------------

    [Fact]
    public void Nats_ignores_an_invalid_schedule_it_never_validates_schedule_for_this_kind()
    {
        var def = Def(new ConnectorConfig
        {
            Nats = new NatsSubConfig { Url = "nats://x", Subject = "s" },
            Schedule = new ScheduleSpec { IntervalMs = 10 }, // would fail ScheduleCalc.Validate for a polled kind
        });
        Assert.Empty(SourceValidation.Validate(def));
    }

    [Fact]
    public void Nats_validates_a_configured_mapping()
    {
        var def = Def(new ConnectorConfig
        {
            Nats = new NatsSubConfig { Url = "nats://x", Subject = "s" },
            Mapping = new MappingSpec { ItemsPath = "$", DedupKeyField = "missing-field", Fields = [] },
        });
        Assert.Contains(SourceValidation.Validate(def), e => e.Contains("dedupKeyField"));
    }
}
