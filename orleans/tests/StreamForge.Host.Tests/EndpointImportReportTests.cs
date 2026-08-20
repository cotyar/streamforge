using StreamForge.Abstractions;
using StreamForge.Api;
using StreamForge.AppCore.Config;
using StreamForge.AppCore.Discovery;
using StreamForge.Engine;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// Plan 016 wave 6, track B — the wiring half of the endpoint-import-warning feature:
/// <see cref="ConfigImportService.RunImportAsync"/> folding <see cref="EndpointReferenceWarnings.Scan"/>'s
/// findings into the report, matched by (Kind, Name), WITHOUT ever turning the entry into an "error" —
/// the whole point of the plan's own decision ("unresolvable at import is a warning, not an error").
/// <see cref="EndpointReferenceWarnings.Scan"/>'s own field-by-field coverage is pinned separately, in
/// <c>shared/StreamForge.AppCore.Tests/EndpointImportWarningsTests.cs</c> (that project has no reference
/// to <c>StreamForge.Api</c>, so the RunImportAsync-level wiring can only be exercised from here).
///
/// <para><see cref="NamedEndpoints"/> is process-wide static state; every test owns its lifetime
/// end-to-end (<c>Configure</c> in the body, <c>Clear</c> in <see cref="Dispose"/>) — same pattern
/// <c>EndpointImportWarningsTests</c> and (for its own static, <c>PeerDirectory</c>)
/// <c>DiscoveryEndpointsTests</c> already use. A NEW file, per this wave's ownership brief — the
/// <c>FakeCatalogFacade</c>/<c>Source</c> helper below are a third, deliberately independent copy of the
/// same shape <c>ConfigImportGatesTests</c>' own doc comment already explains the reason for (no
/// shared-fixture seam without touching a file this wave does not own).</para>
/// </summary>
public sealed class EndpointImportReportTests : IDisposable
{
    public void Dispose() => NamedEndpoints.Clear();

    [Fact]
    public async Task RunImportAsync_a_reference_that_resolves_produces_no_warning()
    {
        NamedEndpoints.Configure([new("primary-oltp", "db.internal:5432")]);
        var facade = new FakeCatalogFacade();
        var doc = new ConfigDocument
        {
            Sources =
            [
                new SourceDefinition
                {
                    Name = "orders",
                    Kind = SourceKinds.Postgres,
                    Connector = new ConnectorConfig { Db = new DbSourceConfig { Host = "@primary-oltp", Table = "orders", CursorColumn = "id" } },
                },
            ],
        };

        var report = await ConfigImportService.RunImportAsync(doc, "merge", "admin", facade, apply: true);

        Assert.True(report.Ok);
        var entry = Assert.Single(report.Entries);
        Assert.Equal("created", entry.Action);
        Assert.Empty(entry.Diagnostics);
    }

    [Fact]
    public async Task RunImportAsync_an_unresolvable_reference_applies_and_reports_a_warning_naming_it()
    {
        NamedEndpoints.Configure([new("primary-oltp", "db.internal:5432")]);
        var facade = new FakeCatalogFacade();
        var doc = new ConfigDocument
        {
            Sources =
            [
                new SourceDefinition
                {
                    Name = "orders",
                    Kind = SourceKinds.Postgres,
                    Connector = new ConnectorConfig { Db = new DbSourceConfig { Host = "@nowhere", Table = "orders", CursorColumn = "id" } },
                },
            ],
        };

        var report = await ConfigImportService.RunImportAsync(doc, "merge", "admin", facade, apply: true);

        // Applied — this is the whole sales pitch: unresolvable-here does not block an import destined
        // for another environment.
        Assert.True(report.Ok);
        Assert.Single(facade.Sources);
        var entry = Assert.Single(report.Entries);
        Assert.Equal("created", entry.Action); // never "error"
        Assert.Contains(entry.Diagnostics, d => d.Contains("@nowhere") && d.Contains("connector.db.host"));
    }

    [Fact]
    public async Task RunImportAsync_mode_validate_surfaces_the_same_warning_and_applies_nothing()
    {
        NamedEndpoints.Configure([]);
        var facade = new FakeCatalogFacade();
        var doc = new ConfigDocument
        {
            Sources =
            [
                new SourceDefinition
                {
                    Name = "orders",
                    Kind = SourceKinds.Postgres,
                    Connector = new ConnectorConfig { Db = new DbSourceConfig { Host = "@nowhere", Table = "orders", CursorColumn = "id" } },
                },
            ],
        };

        var report = await ConfigImportService.RunImportAsync(doc, "validate", "admin", facade, apply: false);

        Assert.True(report.Ok);
        Assert.Empty(facade.Sources); // validate never writes
        var entry = Assert.Single(report.Entries);
        Assert.Equal("created", entry.Action);
        Assert.Contains(entry.Diagnostics, d => d.Contains("@nowhere"));
    }

    [Fact]
    public async Task RunImportAsync_a_reimport_that_plans_skipped_still_reports_the_warning()
    {
        // "skipped" means byte-identical, not "irrelevant" — an operator running mode=validate to ask
        // "will this land here" needs the answer even for entities the document doesn't change.
        NamedEndpoints.Configure([]);
        var facade = new FakeCatalogFacade();
        var src = new SourceDefinition
        {
            Name = "orders",
            Kind = SourceKinds.Postgres,
            Connector = new ConnectorConfig { Db = new DbSourceConfig { Host = "@nowhere", Table = "orders", CursorColumn = "id" } },
        };
        var doc = new ConfigDocument { Sources = [src] };

        var first = await ConfigImportService.RunImportAsync(doc, "merge", "admin", facade, apply: true);
        Assert.Equal("created", Assert.Single(first.Entries).Action);

        var second = await ConfigImportService.RunImportAsync(doc, "merge", "admin", facade, apply: true);

        var entry = Assert.Single(second.Entries);
        Assert.Equal("skipped", entry.Action);
        Assert.Contains(entry.Diagnostics, d => d.Contains("@nowhere"));
    }

    [Fact]
    public async Task RunImportAsync_a_deleted_entity_is_not_warned_about()
    {
        // Replace mode drops an entity the document omits — its connector config is on the way out of
        // the catalog, not something that will ever connect, so it earns no warning.
        NamedEndpoints.Configure([]);
        var facade = new FakeCatalogFacade();
        facade.Sources.Add(new SourceDefinition
        {
            Name = "orders",
            Kind = SourceKinds.Postgres,
            Connector = new ConnectorConfig { Db = new DbSourceConfig { Host = "@nowhere", Table = "orders", CursorColumn = "id" } },
        });
        var doc = new ConfigDocument(); // empty — replace deletes everything not restated

        var report = await ConfigImportService.RunImportAsync(doc, "replace", "admin", facade, apply: true);

        var entry = Assert.Single(report.Entries);
        Assert.Equal("deleted", entry.Action);
        Assert.Empty(entry.Diagnostics);
    }

    [Fact]
    public async Task RunImportAsync_warns_on_a_pipeline_sink_and_leaves_it_created_not_errored()
    {
        NamedEndpoints.Configure([]);
        var facade = new FakeCatalogFacade();
        facade.Sources.Add(Source("trades", new FieldDef("symbol", FieldType.String)));
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

        var report = await ConfigImportService.RunImportAsync(doc, "merge", "admin", facade, apply: true);

        Assert.True(report.Ok);
        var entry = Assert.Single(report.Entries, e => e.Kind == "pipeline");
        Assert.Equal("created", entry.Action);
        Assert.Contains(entry.Diagnostics, d => d.Contains("sinks[webhook].http.url") && d.Contains("@nowhere"));
    }

    private static SourceDefinition Source(string name, params FieldDef[] fields) => new()
    {
        Name = name,
        Fields = [.. fields],
        GeneratorProfile = "generic",
        EventsPerSecond = 5,
        Enabled = true,
    };

    private sealed class FakeCatalogFacade : ICatalogFacade
    {
        private int _nextId;

        public List<SourceDefinition> Sources { get; } = [];
        public List<PipelineDefinition> Pipelines { get; } = [];
        public List<TableDefinition> Tables { get; } = [];

        public Task<ScenarioRunResult> RunSourceAsync(string name, ScenarioRunRequest request) =>
            Task.FromResult(new ScenarioRunResult { Outcome = ScenarioRunOutcome.NotFound });

        public Task<List<SourceDefinition>> GetSourcesAsync() => Task.FromResult(new List<SourceDefinition>(Sources));

        public Task<SourceDefinition?> GetSourceAsync(string name) => Task.FromResult(Sources.FirstOrDefault(s => s.Name == name));

        public Task UpsertSourceAsync(SourceDefinition def)
        {
            Sources.RemoveAll(s => s.Name == def.Name);
            Sources.Add(def);
            return Task.CompletedTask;
        }

        public Task<bool> DeleteSourceAsync(string name) => Task.FromResult(Sources.RemoveAll(s => s.Name == name) > 0);

        public Task<List<PipelineDefinition>> GetPipelinesAsync() => Task.FromResult(new List<PipelineDefinition>(Pipelines));

        public Task<PipelineDefinition?> GetPipelineAsync(string id) => Task.FromResult(Pipelines.FirstOrDefault(p => p.Id == id));

        public Task<PipelineDefinition> CreatePipelineAsync(PipelineDefinition def)
        {
            def.Id = NextId("pl");
            Pipelines.Add(def);
            return Task.FromResult(def);
        }

        public Task<PipelineDefinition?> UpdatePipelineAsync(PipelineDefinition def)
        {
            var idx = Pipelines.FindIndex(p => p.Id == def.Id);
            if (idx < 0)
            {
                return Task.FromResult<PipelineDefinition?>(null);
            }

            Pipelines[idx] = def;
            return Task.FromResult<PipelineDefinition?>(def);
        }

        public Task<bool> DeletePipelineAsync(string id) => Task.FromResult(Pipelines.RemoveAll(p => p.Id == id) > 0);

        public Task<PipelineDefinition?> SetPipelineStatusAsync(string id, PipelineStatus status)
        {
            var p = Pipelines.FirstOrDefault(p => p.Id == id);
            if (p is null)
            {
                return Task.FromResult<PipelineDefinition?>(null);
            }

            p.Status = status;
            return Task.FromResult<PipelineDefinition?>(p);
        }

        public Task<List<TableDefinition>> GetTablesAsync() => Task.FromResult(new List<TableDefinition>(Tables));

        public Task<TableDefinition?> GetTableAsync(string id) => Task.FromResult(Tables.FirstOrDefault(t => t.Id == id));

        public Task<TableDefinition> CreateTableAsync(TableDefinition def)
        {
            if (Sources.Any(s => s.Name == def.Name) || Tables.Any(t => t.Name == def.Name))
            {
                throw new InvalidOperationException($"name '{def.Name}' already exists");
            }

            def.Id = NextId("tb");
            Compile(def);
            Tables.Add(def);
            return Task.FromResult(def);
        }

        public Task<TableDefinition?> UpdateTableAsync(TableDefinition def)
        {
            var idx = Tables.FindIndex(t => t.Id == def.Id);
            if (idx < 0)
            {
                return Task.FromResult<TableDefinition?>(null);
            }

            Compile(def);
            Tables[idx] = def;
            return Task.FromResult<TableDefinition?>(def);
        }

        public Task<bool> DeleteTableAsync(string id) => Task.FromResult(Tables.RemoveAll(t => t.Id == id) > 0);

        public Task<TableDefinition?> SetTableStatusAsync(string id, PipelineStatus status)
        {
            var t = Tables.FirstOrDefault(t => t.Id == id);
            if (t is null)
            {
                return Task.FromResult<TableDefinition?>(null);
            }

            t.Status = status;
            return Task.FromResult<TableDefinition?>(t);
        }

        public Task<string> EnsureFieldNumbersAsync(string entityKey, List<FieldDef> fields) => throw new NotImplementedException();

        private string NextId(string prefix) => $"{prefix}-{++_nextId}";

        private void Compile(TableDefinition def)
        {
            var streamSchemas = ConfigImportService.BuildSourceSchemas(Sources);
            var tableSchemas = new Dictionary<string, SourceSchema>(StringComparer.Ordinal);
            foreach (var t in Tables.Where(t => t.OutputFields.Count > 0))
            {
                tableSchemas[t.Name] = new SourceSchema(t.Name, t.OutputFields.ToDictionary(f => f.Name, f => ConfigImportService.MapFieldKind(f.Type)));
            }

            var result = SqlCompiler.CompileTable(def.Sql, streamSchemas, tableSchemas);
            if (result.Ok && result.OutputSchema is not null)
            {
                def.OutputFields = [.. result.OutputSchema.Fields.Select(f => new FieldDef(f.Key, f.Value switch
                {
                    FieldKind.String => FieldType.String,
                    FieldKind.Double => FieldType.Double,
                    FieldKind.Long => FieldType.Long,
                    FieldKind.Bool => FieldType.Bool,
                    FieldKind.Timestamp => FieldType.Timestamp,
                    FieldKind.Json => FieldType.Json,
                    _ => FieldType.String,
                }))];
                def.StreamInputs = [.. result.StreamInputs];
                def.TableInputs = [.. result.TableInputs];
                def.Error = null;
            }
            else
            {
                def.Error = string.Join("; ", result.Diagnostics.Select(d => $"{d.Line}:{d.Column} {d.Message}"));
            }
        }
    }
}
