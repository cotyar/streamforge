using StreamsForge.Abstractions;
using StreamsForge.Api;
using StreamsForge.AppCore.Transports;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>
/// <c>POST /api/transports/{kind}/probe</c> for a PUSH-shaped kind — the symmetric twin of
/// <see cref="SourceValidationPolledTests"/>'s probe section, which proves the same four outcomes for a
/// polled one.
///
/// <para><b>Why this file exists.</b> <see cref="ISchemaProbe"/> is documented as an optional capability
/// any transport may implement, but <see cref="SourceSchemaService.ProbeAsync"/> used to look only in
/// <see cref="PolledTransports"/> — so an <see cref="IInboundTransport"/> that implemented it (a broker
/// that can describe a topic, a service catalog that lists its own tables' columns) was unreachable: every
/// probe of it answered 404 UnknownKind regardless of what it implemented. The lookup now falls through to
/// <see cref="InboundTransports"/>, and these tests are what keeps it that way.</para>
///
/// <para><b>Registration hygiene</b>, same convention as the polled suite: process-global registries, so
/// the fakes register exactly once from the static constructor under names nothing else in this repo
/// uses ("quuxstream", "quuxstream2").</para>
/// </summary>
public class InboundSchemaProbeTests
{
    private const string StreamKind = "quuxstream";

    /// <summary>A registered inbound kind that does NOT implement <see cref="ISchemaProbe"/> — the "known
    /// but cannot probe" half of the endpoint's two-way distinction, which must stay distinct for a push
    /// kind too.</summary>
    private const string StreamKindNoProbe = "quuxstream2";

    private static readonly TimeSpan ProbeTimeout = TimeSpan.FromSeconds(5);
    private static readonly QuuxStream Shared = new();

    static InboundSchemaProbeTests()
    {
        InboundTransports.Register(Shared);
        InboundTransports.Register(new QuuxStreamNoProbe());
    }

    [Fact]
    public async Task Probe_of_an_inbound_kind_that_implements_ISchemaProbe_reaches_it()
    {
        Shared.Fail = null;

        var outcome = await SourceSchemaService.ProbeAsync(StreamKind, Def(), ProbeTimeout, CancellationToken.None);

        Assert.Equal(ProbeOutcomeKind.Ok, outcome.Kind);
        Assert.Equal(["symbol", "price"], outcome.Result!.Fields.Select(f => f.Name));
        Assert.Empty(outcome.Result.Diagnostics);
    }

    [Fact]
    public async Task Probe_of_a_registered_inbound_kind_that_cannot_probe_is_CannotProbe()
    {
        var outcome = await SourceSchemaService.ProbeAsync(StreamKindNoProbe, Def(kind: StreamKindNoProbe), ProbeTimeout, CancellationToken.None);

        Assert.Equal(ProbeOutcomeKind.CannotProbe, outcome.Kind);
        Assert.Null(outcome.Result);
        Assert.Contains("does not support schema discovery", outcome.Message);
    }

    [Fact]
    public async Task An_unknown_kind_names_the_inbound_registry_too()
    {
        var outcome = await SourceSchemaService.ProbeAsync("no-such-kind-at-all", Def(), ProbeTimeout, CancellationToken.None);

        Assert.Equal(ProbeOutcomeKind.UnknownKind, outcome.Kind);
        // Both registries are listed now — a caller who registered a push kind and typo'd it would
        // otherwise be told only about the polled ones and conclude the seam doesn't exist.
        Assert.Contains(StreamKind, outcome.Message);
    }

    [Fact]
    public async Task A_probe_that_throws_becomes_a_diagnostic_not_an_unhandled_failure()
    {
        Shared.Fail = new InvalidOperationException("daemon unreachable");
        try
        {
            var outcome = await SourceSchemaService.ProbeAsync(StreamKind, Def(), ProbeTimeout, CancellationToken.None);

            // ISchemaProbe's contract, unchanged by the widening: "could not look" is a 200 with a
            // diagnostic an Editor can act on, never a 500.
            Assert.Equal(ProbeOutcomeKind.Ok, outcome.Kind);
            Assert.Empty(outcome.Result!.Fields);
            Assert.Contains("daemon unreachable", Assert.Single(outcome.Result.Diagnostics));
        }
        finally
        {
            Shared.Fail = null;
        }
    }

    private static SourceDefinition Def(string kind = StreamKind) =>
        new()
        {
            Name = "s",
            Kind = kind,
            Fields = [new FieldDef("id", FieldType.String)],
            // The whole point of the settings bag: this fake has no typed config class in
            // StreamsForge.Contracts, exactly like a real out-of-tree kind.
            Connector = new ConnectorConfig { Settings = new() { ["catalog"] = "erates_aggs" } },
        };

    private sealed class QuuxStream : IInboundTransport, ISchemaProbe
    {
        public Exception? Fail { get; set; }

        public string Kind => StreamKind;

        public void Validate(SourceDefinition def, List<string> errors) { }

        public string FormatOf(SourceDefinition def) => FileFormats.Ndjson;

        public IInboundSubscription Open(SourceDefinition def) => throw new NotSupportedException();

        // Fields are non-empty deliberately: TransportRegistryTests asserts every registered descriptor
        // can actually draw a form, and a fake that skipped that would fail a real invariant.
        public TransportDescriptor Describe() => new()
        {
            Kind = StreamKind, Label = "Quux stream", ConfigProperty = "settings", Polled = false, CanProbe = true,
            Fields = [new TransportField { Key = "catalog", Label = "Catalog", Required = true }],
        };

        public Task<SchemaProbeResult> ProbeAsync(SourceDefinition def, CancellationToken ct)
        {
            if (Fail is { } failure)
            {
                throw failure;
            }

            return Task.FromResult(new SchemaProbeResult(
                [new FieldDef("symbol", FieldType.String), new FieldDef("price", FieldType.Double)], []));
        }
    }

    private sealed class QuuxStreamNoProbe : IInboundTransport
    {
        public string Kind => StreamKindNoProbe;

        public void Validate(SourceDefinition def, List<string> errors) { }

        public string FormatOf(SourceDefinition def) => FileFormats.Ndjson;

        public IInboundSubscription Open(SourceDefinition def) => throw new NotSupportedException();

        public TransportDescriptor Describe() => new()
        {
            Kind = StreamKindNoProbe, Label = "Quux stream 2", ConfigProperty = "settings", Polled = false,
            Fields = [new TransportField { Key = "catalog", Label = "Catalog", Required = true }],
        };
    }
}
