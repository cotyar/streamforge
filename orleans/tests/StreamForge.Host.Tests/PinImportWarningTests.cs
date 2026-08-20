using StreamForge.Abstractions;
using StreamForge.Api;
using StreamForge.AppCore.Config;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// Plan 016 wave 7 follow-up. The plan document states a <c>dependsOn</c> pin is checked "at import
/// (against the post-import world <c>ConfigImportService</c> already builds, so <c>mode=validate</c>
/// catches it before anything is applied)". It was not: <c>CatalogRevisions.EvaluatePins</c> was
/// reachable only from each registry's post-write <c>RecomputeStaleReasons</c>, so a violated pin first
/// appeared as a <c>staleReason</c> AFTER a real merge and <c>mode=validate</c> reported nothing at all.
///
/// <para>These tests pin the fixed behaviour, and equally pin its stated limit — a pin against an entity
/// the same document declares is NOT evaluated, because both revision counters are registry-assigned at
/// write time and the post-import value is not knowable at plan time.</para>
/// </summary>
public class PinImportWarningTests
{
    private static readonly List<SourceDefinition> Catalog =
        [new()
        {
            Name = "trades",
            Kind = SourceKinds.Generator,
            SchemaRevision = 3,
            // The table below compiles against this; without a field its entry would be an "error" and
            // pin warnings are deliberately not attached to those.
            Fields = [new FieldDef("a", FieldType.String)],
            GeneratorProfile = "generic",
            EventsPerSecond = 5,
            Enabled = true,
        }];

    private static ConfigDocument DocPinning(long schemaRevision, bool alsoDeclareTheSource = false) => new()
    {
        Sources = alsoDeclareTheSource
            ? [new SourceDefinition
              {
                  Name = "trades", Kind = SourceKinds.Generator, Fields = [new FieldDef("a", FieldType.String)],
                  GeneratorProfile = "generic", EventsPerSecond = 5, Enabled = true,
              }]
            : [],
        Tables =
        [
            new ConfigTable
            {
                Name = "pnl",
                Sql = "SELECT a FROM trades",
                DependsOn = [new EntityPin { Kind = "source", Name = "trades", SchemaRevision = schemaRevision }],
            },
        ],
    };

    private static async Task<ConfigImportReport> RunAsync(ConfigDocument doc, bool apply) =>
        await ConfigImportService.RunImportAsync(
            doc, apply ? "merge" : "validate", "tester", new StubCatalog(Catalog), apply);

    [Fact]
    public async Task ValidateReportsAPinThatWillNotHold_WhichIsTheWholePointOfADryRun()
    {
        var report = await RunAsync(DocPinning(999), apply: false);

        var entry = Assert.Single(report.Entries, e => e.Name == "pnl");
        Assert.Contains(entry.Diagnostics, d => d.Contains("dependsOn", StringComparison.Ordinal)
                                             && d.Contains("999", StringComparison.Ordinal)
                                             && d.Contains("trades", StringComparison.Ordinal));
    }

    [Fact]
    public async Task AHoldingPinIsSilent()
    {
        var report = await RunAsync(DocPinning(3), apply: false);

        var entry = Assert.Single(report.Entries, e => e.Name == "pnl");
        Assert.DoesNotContain(entry.Diagnostics, d => d.Contains("dependsOn", StringComparison.Ordinal));
    }

    [Fact]
    public async Task AViolatedPinWarnsButDoesNotRefuseTheImport()
    {
        // Deliberately NOT a gate, unlike the cycle/plugin/schema checks: wave 2 decided a violated pin
        // badges the entity and lets it keep running, so refusing the import would make promotion
        // stricter than the runtime it promotes into.
        var report = await RunAsync(DocPinning(999), apply: false);

        Assert.True(report.Ok);
    }

    [Fact]
    public async Task APinAgainstAnEntityTheSameDocumentDeclaresIsNotEvaluated()
    {
        // The stated limit: the source's post-import SchemaRevision is registry-assigned and unknowable
        // here, so claiming a mismatch would be confident nonsense.
        var report = await RunAsync(DocPinning(999, alsoDeclareTheSource: true), apply: false);

        var entry = Assert.Single(report.Entries, e => e.Name == "pnl");
        Assert.DoesNotContain(entry.Diagnostics, d => d.Contains("dependsOn", StringComparison.Ordinal));
    }

    [Fact]
    public async Task APinNamingSomethingThatDoesNotExistAnywhereIsReported()
    {
        var doc = new ConfigDocument
        {
            Tables =
            [
                new ConfigTable
                {
                    Name = "pnl",
                    Sql = "SELECT a FROM trades",
                    DependsOn = [new EntityPin { Kind = "source", Name = "ghost", SchemaRevision = 1 }],
                },
            ],
        };

        var report = await RunAsync(doc, apply: false);

        var entry = Assert.Single(report.Entries, e => e.Name == "pnl");
        Assert.Contains(entry.Diagnostics, d => d.Contains("ghost", StringComparison.Ordinal)
                                             && d.Contains("no longer resolves", StringComparison.Ordinal));
    }

    /// <summary>Read-only on purpose: every test here runs <c>mode=validate</c>, which by contract
    /// touches the registry for nothing but the three initial reads — so a write reaching this fake is
    /// itself a failure worth an exception rather than a silently-accepted no-op.</summary>
    private sealed class StubCatalog(List<SourceDefinition> sources) : ICatalogFacade
    {
        public Task<List<SourceDefinition>> GetSourcesAsync() => Task.FromResult(new List<SourceDefinition>(sources));
        public Task<List<PipelineDefinition>> GetPipelinesAsync() => Task.FromResult(new List<PipelineDefinition>());
        public Task<List<TableDefinition>> GetTablesAsync() => Task.FromResult(new List<TableDefinition>());
        public Task<SourceDefinition?> GetSourceAsync(string name) =>
            Task.FromResult(sources.FirstOrDefault(s => s.Name == name));
        public Task<PipelineDefinition?> GetPipelineAsync(string id) => Task.FromResult<PipelineDefinition?>(null);
        public Task<TableDefinition?> GetTableAsync(string id) => Task.FromResult<TableDefinition?>(null);

        public Task UpsertSourceAsync(SourceDefinition def) => throw new NotSupportedException();
        public Task<bool> DeleteSourceAsync(string name) => throw new NotSupportedException();
        public Task<PipelineDefinition> CreatePipelineAsync(PipelineDefinition def) => throw new NotSupportedException();
        public Task<PipelineDefinition?> UpdatePipelineAsync(PipelineDefinition def) => throw new NotSupportedException();
        public Task<bool> DeletePipelineAsync(string id) => throw new NotSupportedException();
        public Task<PipelineDefinition?> SetPipelineStatusAsync(string id, PipelineStatus status) => throw new NotSupportedException();
        public Task<TableDefinition> CreateTableAsync(TableDefinition def) => throw new NotSupportedException();
        public Task<TableDefinition?> UpdateTableAsync(TableDefinition def) => throw new NotSupportedException();
        public Task<bool> DeleteTableAsync(string id) => throw new NotSupportedException();
        public Task<TableDefinition?> SetTableStatusAsync(string id, PipelineStatus status) => throw new NotSupportedException();
        public Task<string> EnsureFieldNumbersAsync(string entityKey, List<FieldDef> fields) => throw new NotSupportedException();
        public Task<ScenarioRunResult> RunSourceAsync(string name, ScenarioRunRequest request) => throw new NotSupportedException();
    }
}
