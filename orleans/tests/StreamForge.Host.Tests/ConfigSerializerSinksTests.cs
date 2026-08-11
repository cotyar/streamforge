using StreamForge.Abstractions;
using StreamForge.AppCore.Config;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// Plan 009 B2: config export/import round-trips a Sink with credentials masked (the wave's own
/// verification gate #4, pinned here as a fast unit test since the live check exercises the same
/// contract only informally). Covers <see cref="ConfigSerializer.FromCatalog"/> masking, the canonical
/// JSON/YAML round-trip carrying Sinks through, and byte-equality staying stable across a masked
/// re-serialize (D-I).
/// </summary>
public class ConfigSerializerSinksTests
{
    private static PipelineDefinition PipelineWithSink(string token = "tok-secret") => new()
    {
        Id = "p1",
        Name = "pipe",
        Sql = "SELECT 1",
        Sinks =
        [
            new SinkSpec
            {
                Kind = SinkKinds.Nats,
                Enabled = true,
                Nats = new NatsPubConfig { Url = "nats://localhost:4222", Subject = "sf.pipeline.{name}", Token = token },
            },
        ],
    };

    private static TableDefinition TableWithSink(string token = "tok-secret") => new()
    {
        Id = "t1",
        Name = "tbl",
        Sql = "SELECT 1",
        Sinks =
        [
            new SinkSpec
            {
                Kind = SinkKinds.Nats,
                Enabled = true,
                Nats = new NatsPubConfig { Url = "nats://localhost:4222", Subject = "sf.table.{name}", Token = token },
            },
        ],
    };

    [Fact]
    public void FromCatalog_WithoutIncludeSecrets_MasksSinkCredentials()
    {
        var doc = ConfigSerializer.FromCatalog([], [PipelineWithSink()], [TableWithSink()], includeSecrets: false);

        Assert.Equal(SourceKinds.SecretMask, doc.Pipelines[0].Sinks[0].Nats!.Token);
        Assert.Equal(SourceKinds.SecretMask, doc.Tables[0].Sinks[0].Nats!.Token);
        // Non-secret fields survive the export untouched.
        Assert.Equal("sf.pipeline.{name}", doc.Pipelines[0].Sinks[0].Nats!.Subject);
    }

    [Fact]
    public void FromCatalog_WithIncludeSecrets_KeepsTheRealCredential()
    {
        var doc = ConfigSerializer.FromCatalog([], [PipelineWithSink()], [TableWithSink()], includeSecrets: true);

        Assert.Equal("tok-secret", doc.Pipelines[0].Sinks[0].Nats!.Token);
        Assert.Equal("tok-secret", doc.Tables[0].Sinks[0].Nats!.Token);
    }

    [Fact]
    public void FromCatalog_NeverMutatesTheSourceCatalogEntities()
    {
        var pipeline = PipelineWithSink();

        ConfigSerializer.FromCatalog([], [pipeline], [], includeSecrets: false);

        Assert.Equal("tok-secret", pipeline.Sinks[0].Nats!.Token);
    }

    [Fact]
    public void CanonicalJson_RoundTripsAMaskedSink()
    {
        var doc = ConfigSerializer.FromCatalog([], [PipelineWithSink()], [], includeSecrets: false);

        var json = ConfigSerializer.ToCanonicalJson(doc);
        Assert.Contains(SourceKinds.SecretMask, json);

        var (parsed, diagnostics) = ConfigSerializer.Parse(json);
        Assert.Empty(diagnostics);
        Assert.NotNull(parsed);
        Assert.Single(parsed!.Pipelines[0].Sinks);
        Assert.Equal(SourceKinds.SecretMask, parsed.Pipelines[0].Sinks[0].Nats!.Token);
        Assert.Equal("nats://localhost:4222", parsed.Pipelines[0].Sinks[0].Nats!.Url);
    }

    [Fact]
    public void CanonicalJson_IsIdempotentAcrossASerializeParseSerializeCycle_WithSinksPresent()
    {
        var doc = ConfigSerializer.FromCatalog([], [PipelineWithSink()], [TableWithSink()], includeSecrets: true);

        var first = ConfigSerializer.ToCanonicalJson(doc);
        var (parsed, _) = ConfigSerializer.Parse(first);
        var second = ConfigSerializer.ToCanonicalJson(parsed!);

        Assert.Equal(first, second);
    }

    [Fact]
    public void CanonicalJson_OmitsSinksWhenEmpty()
    {
        // D-I's "empties omitted" rule extends to Sinks like every other collection on these entities.
        var doc = ConfigSerializer.FromCatalog([], [new PipelineDefinition { Id = "p1", Name = "pipe", Sql = "SELECT 1" }], [], includeSecrets: false);

        var json = ConfigSerializer.ToCanonicalJson(doc);

        Assert.DoesNotContain("\"sinks\"", json);
    }

    [Fact]
    public void Yaml_RoundTripsAMaskedSinkThroughParse()
    {
        var doc = ConfigSerializer.FromCatalog([], [PipelineWithSink()], [], includeSecrets: false);

        var yaml = ConfigSerializer.ToYaml(doc);
        var (parsed, diagnostics) = ConfigSerializer.Parse(yaml);

        Assert.Empty(diagnostics);
        Assert.Equal(SourceKinds.SecretMask, parsed!.Pipelines[0].Sinks[0].Nats!.Token);
    }
}
