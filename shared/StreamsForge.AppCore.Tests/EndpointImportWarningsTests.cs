using StreamsForge.Abstractions;
using StreamsForge.AppCore.Config;
using StreamsForge.AppCore.Discovery;
using Xunit;

namespace StreamsForge.AppCore.Tests;

/// <summary>
/// Plan 016 wave 6, track B — <see cref="EndpointReferenceWarnings.Scan"/>, the pure walk over a
/// <see cref="ConfigDocument"/>'s endpoint-shaped fields. <see cref="NamedEndpoints"/> is process-wide
/// static state (see its own class doc for why), so every test here owns its lifetime end to end:
/// <see cref="NamedEndpoints.Configure"/> in the test body, <see cref="NamedEndpoints.Clear"/> in
/// <see cref="Dispose"/> — the same pattern <c>DiscoveryEndpointsTests</c> already uses for the sibling
/// static, <c>PeerDirectory</c>.
/// </summary>
public sealed class EndpointImportWarningsTests : IDisposable
{
    public void Dispose() => NamedEndpoints.Clear();

    [Fact]
    public void Scan_reports_nothing_for_a_document_with_no_at_sign_values()
    {
        NamedEndpoints.Configure([]);
        var doc = new ConfigDocument
        {
            Sources =
            [
                new SourceDefinition
                {
                    Name = "trades",
                    Kind = SourceKinds.Url,
                    Connector = new ConnectorConfig { Url = new UrlPollConfig { Url = "https://example.com/trades" } },
                },
            ],
        };

        Assert.Empty(EndpointReferenceWarnings.Scan(doc));
    }

    [Fact]
    public void Scan_reports_nothing_when_the_reference_resolves()
    {
        NamedEndpoints.Configure([new("primary-oltp", "db.internal:5432")]);
        var doc = new ConfigDocument
        {
            Sources =
            [
                new SourceDefinition
                {
                    Name = "orders",
                    Kind = SourceKinds.Postgres,
                    Connector = new ConnectorConfig { Db = new DbSourceConfig { Host = "@primary-oltp" } },
                },
            ],
        };

        Assert.Empty(EndpointReferenceWarnings.Scan(doc));
    }

    [Fact]
    public void Scan_warns_on_an_unresolvable_source_db_host_and_names_the_source()
    {
        NamedEndpoints.Configure([new("primary-oltp", "db.internal:5432")]);
        var doc = new ConfigDocument
        {
            Sources =
            [
                new SourceDefinition
                {
                    Name = "orders",
                    Kind = SourceKinds.Postgres,
                    Connector = new ConnectorConfig { Db = new DbSourceConfig { Host = "@nowhere" } },
                },
            ],
        };

        var warnings = EndpointReferenceWarnings.Scan(doc);
        var w = Assert.Single(warnings);
        Assert.Equal("source", w.Kind);
        Assert.Equal("orders", w.Name);
        Assert.Contains("connector.db.host", w.Message);
        Assert.Contains("'@nowhere'", w.Message);
        Assert.Contains("primary-oltp", w.Message); // known names surfaced, from NamedEndpoints.TryResolve's own text
    }

    [Theory]
    [InlineData(nameof(ConnectorConfig.Url))]
    [InlineData(nameof(ConnectorConfig.Nats))]
    [InlineData(nameof(ConnectorConfig.Grpc) + ".Address")]
    [InlineData(nameof(ConnectorConfig.Grpc) + ".RestAddress")]
    [InlineData(nameof(ConnectorConfig.Db) + ".Host")]
    [InlineData(nameof(ConnectorConfig.Db) + ".ConnectionString")]
    [InlineData(nameof(ConnectorConfig.Fix) + ".Host")]
    public void Scan_covers_every_documented_source_endpoint_field(string which)
    {
        NamedEndpoints.Configure([]);
        var connector = which switch
        {
            "Url" => new ConnectorConfig { Url = new UrlPollConfig { Url = "@nowhere" } },
            "Nats" => new ConnectorConfig { Nats = new NatsSubConfig { Url = "@nowhere" } },
            "Grpc.Address" => new ConnectorConfig { Grpc = new GrpcSubConfig { Address = "@nowhere" } },
            "Grpc.RestAddress" => new ConnectorConfig { Grpc = new GrpcSubConfig { Address = "http://x", RestAddress = "@nowhere" } },
            "Db.Host" => new ConnectorConfig { Db = new DbSourceConfig { Host = "@nowhere" } },
            "Db.ConnectionString" => new ConnectorConfig { Db = new DbSourceConfig { ConnectionString = "@nowhere" } },
            "Fix.Host" => new ConnectorConfig { Fix = new FixSourceConfig { Host = "@nowhere" } },
            _ => throw new ArgumentOutOfRangeException(nameof(which)),
        };
        var doc = new ConfigDocument { Sources = [new SourceDefinition { Name = "s", Connector = connector }] };

        Assert.Single(EndpointReferenceWarnings.Scan(doc));
    }

    [Fact]
    public void Scan_warns_on_a_pipeline_sinks_unresolvable_http_url_and_names_the_pipeline_and_sink()
    {
        NamedEndpoints.Configure([]);
        var doc = new ConfigDocument
        {
            Pipelines =
            [
                new ConfigPipeline
                {
                    Name = "fx_desk",
                    Sql = "SELECT symbol FROM trades",
                    Sinks = [new SinkSpec { Kind = SinkKinds.Http, Name = "webhook", Http = new HttpSinkConfig { Url = "@nowhere" } }],
                },
            ],
        };

        var warnings = EndpointReferenceWarnings.Scan(doc);
        var w = Assert.Single(warnings);
        Assert.Equal("pipeline", w.Kind);
        Assert.Equal("fx_desk", w.Name);
        Assert.Contains("sinks[webhook].http.url", w.Message);
    }

    [Fact]
    public void Scan_warns_on_a_tables_sinks_unresolvable_nats_url_and_names_the_table()
    {
        NamedEndpoints.Configure([]);
        var doc = new ConfigDocument
        {
            Tables =
            [
                new ConfigTable
                {
                    Name = "positions",
                    Sql = "SELECT symbol FROM trades",
                    Sinks = [new SinkSpec { Kind = SinkKinds.Nats, Nats = new NatsPubConfig { Url = "@nowhere", Subject = "s" } }],
                },
            ],
        };

        var warnings = EndpointReferenceWarnings.Scan(doc);
        var w = Assert.Single(warnings);
        Assert.Equal("table", w.Kind);
        Assert.Equal("positions", w.Name);
        Assert.Contains("sinks[", w.Message);
        Assert.Contains("nats.url", w.Message);
    }

    [Fact]
    public void Scan_ignores_a_credential_bearing_url_that_merely_contains_an_at_sign()
    {
        // NamedEndpoints.IsReference: only a value that is ENTIRELY "@name" counts. "nats://user@host"
        // is left exactly as authored — the whole point of the sigil rule (see NamedEndpoints' own doc).
        NamedEndpoints.Configure([]);
        var doc = new ConfigDocument
        {
            Sources =
            [
                new SourceDefinition
                {
                    Name = "feed",
                    Kind = SourceKinds.Nats,
                    Connector = new ConnectorConfig { Nats = new NatsSubConfig { Url = "nats://user@host:4222" } },
                },
            ],
        };

        Assert.Empty(EndpointReferenceWarnings.Scan(doc));
    }

    [Fact]
    public void Scan_ignores_file_and_folder_paths_even_when_shaped_like_a_reference()
    {
        // Deliberately out of scope per this class's own doc comment: a local disk path is not an
        // external endpoint, so NamedEndpoints indirection buys nothing there.
        NamedEndpoints.Configure([]);
        var doc = new ConfigDocument
        {
            Sources =
            [
                new SourceDefinition
                {
                    Name = "drop",
                    Kind = SourceKinds.Folder,
                    Connector = new ConnectorConfig { Folder = new FolderPollConfig { Path = "@nowhere" } },
                },
            ],
        };

        Assert.Empty(EndpointReferenceWarnings.Scan(doc));
    }
}
