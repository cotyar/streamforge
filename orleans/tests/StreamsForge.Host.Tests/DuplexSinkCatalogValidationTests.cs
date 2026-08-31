using StreamsForge.Abstractions;
using StreamsForge.AppCore.Sinks;
using StreamsForge.AppCore.Transports;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>
/// Plan 019 D2 (wave 019-B2): <see cref="DuplexSinkCatalogValidation.ValidateAsync"/> — the catalog-aware
/// half of duplex sink validation that <see cref="DuplexSinkTransport.Validate"/> itself cannot do (no
/// catalog reference on that signature; see that class's own doc comment). This repo has no HTTP-level
/// test harness for the two REST call sites (<c>PipelinesEndpoints.cs</c>, <c>TablesEndpoints.cs</c>) — see
/// <c>SinkTransportsValidateTests</c>'s own doc comment for the identical reasoning — so this file covers
/// the pure logic directly, with a hand-written <see cref="ICatalogFacade"/> fake (same minimal shape as
/// <c>SinkSugarTests.MemoryCatalog</c>) standing in for the registry both endpoint files already hold in
/// scope.
///
/// <para>"Both entry points" (the wave brief's phrase) maps to the two <c>entityName</c> shapes the two
/// endpoint files actually pass: TablesEndpoints always has a known name (<c>req.Name</c>, user-supplied
/// before the table exists) at both create and update; PipelinesEndpoints only has one at update (a
/// pipeline's id is minted by <c>CreatePipelineAsync</c>, so create passes null) — see
/// <see cref="ValidateAsync_NullEntityName_TemplatedSourceName_IsSkippedNotRejected"/> and
/// <see cref="ValidateAsync_NullEntityName_LiteralSourceName_IsStillChecked"/> for that asymmetry, pinned
/// exactly as <see cref="DuplexSinkCatalogValidation.ValidateAsync"/>'s own doc comment describes it.</para>
///
/// <para>Registers its own duplex kind ("dscv") rather than reusing another test file's — same
/// process-global-registry discipline <see cref="DuplexTransportRegistryTests"/>'s own doc comment
/// states, and it also means this file's tests do not depend on another test class's static constructor
/// having already run first.</para>
/// </summary>
public class DuplexSinkCatalogValidationTests
{
    private const string DuplexKind = "dscv";

    private static readonly FakeDuplexTransport Duplex = new();

    static DuplexSinkCatalogValidationTests()
    {
        DuplexTransports.Register(Duplex);
    }

    private sealed class FakeDuplexTransport : IDuplexTransport
    {
        public string Kind => DuplexKind;

        public string FormatOf(SourceDefinition def) => FileFormats.JsonArray;

        public void Validate(SourceDefinition def, List<string> errors)
        {
        }

        public IInboundSubscription Open(SourceDefinition def) => throw new NotImplementedException("not exercised by this file's tests");

        public IDuplexSession OpenDuplex(SourceDefinition def) => throw new NotImplementedException("not exercised by this file's tests");

        public TransportDescriptor Describe() => new()
        {
            Kind = DuplexKind,
            Label = "DSCV fake",
            ConfigProperty = "nats",
            Duplex = true,
            Fields = [new TransportField { Key = "url", Label = "Server URL", Required = true }],
        };
    }

    /// <summary>The smallest <see cref="ICatalogFacade"/> that can answer <c>GetSourceAsync</c> — every
    /// other member throws, matching <c>SinkSugarTests.MemoryCatalog</c>'s "interface conformance only"
    /// convention for members this file's subject never calls.</summary>
    private sealed class SourcesOnlyCatalog(params SourceDefinition[] sources) : ICatalogFacade
    {
        private readonly List<SourceDefinition> _sources = [.. sources];

        public Task<SourceDefinition?> GetSourceAsync(string name) =>
            Task.FromResult(_sources.FirstOrDefault(s => s.Name == name));

        public Task<List<SourceDefinition>> GetSourcesAsync() => throw new NotImplementedException();
        public Task UpsertSourceAsync(SourceDefinition def) => throw new NotImplementedException();
        public Task<bool> DeleteSourceAsync(string name) => throw new NotImplementedException();
        public Task<List<PipelineDefinition>> GetPipelinesAsync() => throw new NotImplementedException();
        public Task<PipelineDefinition?> GetPipelineAsync(string id) => throw new NotImplementedException();
        public Task<PipelineDefinition> CreatePipelineAsync(PipelineDefinition def) => throw new NotImplementedException();
        public Task<PipelineDefinition?> UpdatePipelineAsync(PipelineDefinition def) => throw new NotImplementedException();
        public Task<bool> DeletePipelineAsync(string id) => throw new NotImplementedException();
        public Task<PipelineDefinition?> SetPipelineStatusAsync(string id, PipelineStatus status) => throw new NotImplementedException();
        public Task<List<TableDefinition>> GetTablesAsync() => throw new NotImplementedException();
        public Task<TableDefinition?> GetTableAsync(string id) => throw new NotImplementedException();
        public Task<TableDefinition> CreateTableAsync(TableDefinition def) => throw new NotImplementedException();
        public Task<TableDefinition?> UpdateTableAsync(TableDefinition def) => throw new NotImplementedException();
        public Task<bool> DeleteTableAsync(string id) => throw new NotImplementedException();
        public Task<TableDefinition?> SetTableStatusAsync(string id, PipelineStatus status) => throw new NotImplementedException();
        public Task<string> EnsureFieldNumbersAsync(string entityKey, List<FieldDef> fields) => throw new NotImplementedException();
        public Task<ScenarioRunResult> RunSourceAsync(string name, ScenarioRunRequest request) => throw new NotImplementedException();
    }

    private static SourceDefinition DuplexSource(string name) => new() { Name = name, Kind = DuplexKind };

    private static SourceDefinition NonDuplexSource(string name) => new() { Name = name, Kind = SourceKinds.Generator };

    private static List<SinkSpec> DuplexSinkNaming(string sourceName) =>
        [new SinkSpec { Kind = SinkKinds.Duplex, Duplex = new DuplexSinkConfig { SourceName = sourceName } }];

    // ------------------------------------------------------------------
    // The three scenarios the wave brief names explicitly.
    // ------------------------------------------------------------------

    [Fact]
    public async Task ValidateAsync_SourceDoesNotExist_ProducesNoSuchSourceError()
    {
        var sinks = DuplexSinkNaming("nope");
        var errors = new List<string>();

        await DuplexSinkCatalogValidation.ValidateAsync(sinks, "pl-1", new SourcesOnlyCatalog(), errors);

        var error = Assert.Single(errors);
        Assert.Contains("nope", error);
        Assert.Contains("does not exist", error);
    }

    [Fact]
    public async Task ValidateAsync_SourceExistsButIsNotADuplexKind_ProducesADistinctError()
    {
        var sinks = DuplexSinkNaming("plain-source");
        var catalog = new SourcesOnlyCatalog(NonDuplexSource("plain-source"));
        var errors = new List<string>();

        await DuplexSinkCatalogValidation.ValidateAsync(sinks, "pl-1", catalog, errors);

        var error = Assert.Single(errors);
        Assert.Contains("plain-source", error);
        Assert.Contains("not a duplex kind", error);
        // Distinguishable from the missing-source message — different fixes, per the wave brief.
        Assert.DoesNotContain("does not exist", error);
    }

    [Fact]
    public async Task ValidateAsync_SourceExistsAndIsADuplexKind_PassesValidation()
    {
        var sinks = DuplexSinkNaming("fix-venue1");
        var catalog = new SourcesOnlyCatalog(DuplexSource("fix-venue1"));
        var errors = new List<string>();

        await DuplexSinkCatalogValidation.ValidateAsync(sinks, "pl-1", catalog, errors);

        Assert.Empty(errors);
    }

    // ------------------------------------------------------------------
    // {name} expansion — the two endpoint files' actual entityName shapes.
    // ------------------------------------------------------------------

    [Fact]
    public async Task ValidateAsync_KnownEntityName_ExpandsTheTemplateBeforeLookingUp()
    {
        // Mirrors TablesEndpoints (entityName always known) and PipelinesEndpoints' PUT (entityName =
        // the pre-existing id) — a templated sourceName resolves fully and is checked exactly like a
        // literal one.
        var sinks = DuplexSinkNaming("fix-{name}");
        var catalog = new SourcesOnlyCatalog(DuplexSource("fix-tbl-1"));
        var errors = new List<string>();

        await DuplexSinkCatalogValidation.ValidateAsync(sinks, "tbl-1", catalog, errors);

        Assert.Empty(errors);
    }

    [Fact]
    public async Task ValidateAsync_KnownEntityName_ExpandedTemplateStillReportsAMissingSource()
    {
        var sinks = DuplexSinkNaming("fix-{name}");
        var errors = new List<string>();

        await DuplexSinkCatalogValidation.ValidateAsync(sinks, "tbl-1", new SourcesOnlyCatalog(), errors);

        var error = Assert.Single(errors);
        Assert.Contains("fix-tbl-1", error);
    }

    [Fact]
    public async Task ValidateAsync_NullEntityName_TemplatedSourceName_IsSkippedNotRejected()
    {
        // PipelinesEndpoints' POST handler: the pipeline's id does not exist yet, so entityName is null.
        // A genuinely templated source name cannot be resolved yet — it must be SKIPPED, not misreported
        // as "source '{name}' does not exist" (DuplexSinkCatalogValidation.ValidateAsync's own doc).
        var sinks = DuplexSinkNaming("fix-{name}");
        var errors = new List<string>();

        await DuplexSinkCatalogValidation.ValidateAsync(sinks, entityName: null, new SourcesOnlyCatalog(), errors);

        Assert.Empty(errors);
    }

    [Fact]
    public async Task ValidateAsync_NullEntityName_LiteralSourceName_IsStillChecked()
    {
        // The null-entityName skip is narrowly for an UNRESOLVED template, not a blanket skip for the
        // whole call site — a literal (non-templated) source name is checked exactly the same whether or
        // not the caller happens to know its own entity name yet.
        var sinks = DuplexSinkNaming("fix-venue1");
        var errors = new List<string>();

        await DuplexSinkCatalogValidation.ValidateAsync(sinks, entityName: null, new SourcesOnlyCatalog(), errors);

        var error = Assert.Single(errors);
        Assert.Contains("fix-venue1", error);
        Assert.Contains("does not exist", error);
    }

    // ------------------------------------------------------------------
    // Everything DuplexSinkTransport.Validate already owns must not be double-reported here.
    // ------------------------------------------------------------------

    [Fact]
    public async Task ValidateAsync_NonDuplexSinkKinds_AreIgnored()
    {
        var sinks = new List<SinkSpec> { new() { Kind = SinkKinds.Http, Http = new HttpSinkConfig { Url = "http://x/events" } } };
        var errors = new List<string>();

        await DuplexSinkCatalogValidation.ValidateAsync(sinks, "pl-1", new SourcesOnlyCatalog(), errors);

        Assert.Empty(errors);
    }

    [Fact]
    public async Task ValidateAsync_BlankSourceName_IsSkipped_DuplexSinkTransportValidateOwnsThatError()
    {
        var sinks = new List<SinkSpec> { new() { Kind = SinkKinds.Duplex, Duplex = new DuplexSinkConfig { SourceName = "" } } };
        var errors = new List<string>();

        await DuplexSinkCatalogValidation.ValidateAsync(sinks, "pl-1", new SourcesOnlyCatalog(), errors);

        Assert.Empty(errors);
    }

    [Fact]
    public async Task ValidateAsync_MissingDuplexConfig_IsSkipped_DuplexSinkTransportValidateOwnsThatError()
    {
        var sinks = new List<SinkSpec> { new() { Kind = SinkKinds.Duplex, Duplex = null } };
        var errors = new List<string>();

        await DuplexSinkCatalogValidation.ValidateAsync(sinks, "pl-1", new SourcesOnlyCatalog(), errors);

        Assert.Empty(errors);
    }
}
