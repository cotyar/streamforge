using StreamForge.Abstractions;
using StreamForge.Api;
using StreamForge.AppCore.Config;
using StreamForge.Engine;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// Plan 006 (W3C, D-I/D-J): unit tests for <see cref="ConfigImportService"/> — the pure body-form
/// detection / composition / report-assembly helpers, plus the end-to-end apply pipeline
/// (<see cref="ConfigImportService.RunImportAsync"/>) against <see cref="FakeCatalogFacade"/>, a
/// full in-memory <see cref="ICatalogFacade"/> (distinct from FakeRegistryGrain.cs, which throws
/// NotImplementedException on every write method and therefore can't exercise an apply). There is
/// no HTTP-level test harness in this repo (see ConfigEndpoints.cs's class doc comment) — this file
/// is the whole test surface for plan 006's config import/export endpoints.
/// </summary>
public class ConfigEndpointsLogicTests
{
    // ------------------------------------------------------------------
    // Body-form detection (pure).
    // ------------------------------------------------------------------

    [Theory]
    [InlineData("multipart/form-data; boundary=abc", true)]
    [InlineData("MULTIPART/FORM-DATA", true)]
    [InlineData("application/json", false)]
    [InlineData(null, false)]
    public void IsMultipartContentType_detects_multipart(string? contentType, bool expected) =>
        Assert.Equal(expected, ConfigImportService.IsMultipartContentType(contentType));

    [Theory]
    [InlineData("application/json", true)]
    [InlineData("application/json; charset=utf-8", true)]
    [InlineData("text/yaml", false)]
    [InlineData("application/x-yaml", false)]
    [InlineData("text/plain", false)]
    [InlineData(null, false)]
    public void IsJsonContentType_detects_json(string? contentType, bool expected) =>
        Assert.Equal(expected, ConfigImportService.IsJsonContentType(contentType));

    // ------------------------------------------------------------------
    // ComposeSingleDocument (pure).
    // ------------------------------------------------------------------

    [Fact]
    public void ComposeSingleDocument_parses_a_plain_json_document()
    {
        var (doc, diagnostics) = ConfigImportService.ComposeSingleDocument(
            """{"version":1,"sources":[{"name":"trades","fields":[{"name":"price","type":"Double"}]}]}""");

        Assert.NotNull(doc);
        Assert.Empty(diagnostics);
        Assert.Single(doc!.Sources);
        Assert.Equal("trades", doc.Sources[0].Name);
    }

    [Fact]
    public void ComposeSingleDocument_rejects_a_non_empty_include_list()
    {
        var (doc, diagnostics) = ConfigImportService.ComposeSingleDocument(
            """{"version":1,"include":["base.json"]}""");

        Assert.Null(doc);
        Assert.Contains(diagnostics, d => d.Contains("include", StringComparison.OrdinalIgnoreCase) && d.Contains("multipart", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ComposeSingleDocument_allows_an_empty_include_list()
    {
        var (doc, diagnostics) = ConfigImportService.ComposeSingleDocument("""{"version":1,"include":[]}""");

        Assert.NotNull(doc);
        Assert.Empty(diagnostics);
    }

    [Fact]
    public void ComposeSingleDocument_surfaces_parse_diagnostics_on_invalid_json()
    {
        var (doc, diagnostics) = ConfigImportService.ComposeSingleDocument("{not valid json");

        Assert.Null(doc);
        Assert.NotEmpty(diagnostics);
    }

    // ------------------------------------------------------------------
    // ComposeJsonBody (pure) — single object vs ordered array.
    // ------------------------------------------------------------------

    [Fact]
    public void ComposeJsonBody_single_object_delegates_to_ComposeSingleDocument()
    {
        var (doc, diagnostics) = ConfigImportService.ComposeJsonBody(
            """{"version":1,"pipelines":[{"name":"p","sql":"SELECT symbol FROM trades"}]}""");

        Assert.NotNull(doc);
        Assert.Empty(diagnostics);
        Assert.Single(doc!.Pipelines);
    }

    [Fact]
    public void ComposeJsonBody_array_composes_in_order_later_wins()
    {
        var (doc, diagnostics) = ConfigImportService.ComposeJsonBody(
            """
            [
              {"version":1,"pipelines":[{"name":"p","sql":"SELECT symbol FROM trades","description":"base"}]},
              {"version":1,"pipelines":[{"name":"p","description":"overlay"}]}
            ]
            """);

        Assert.NotNull(doc);
        Assert.Empty(diagnostics);
        var p = Assert.Single(doc!.Pipelines);
        Assert.Equal("overlay", p.Description);
        Assert.Equal("SELECT symbol FROM trades", p.Sql); // absent from the later doc -> kept from the earlier one.
    }

    [Fact]
    public void ComposeJsonBody_invalid_json_reports_a_diagnostic()
    {
        var (doc, diagnostics) = ConfigImportService.ComposeJsonBody("not json at all");

        Assert.Null(doc);
        Assert.Contains(diagnostics, d => d.Contains("invalid JSON", StringComparison.OrdinalIgnoreCase));
    }

    // ------------------------------------------------------------------
    // ComposeMultipart (pure).
    // ------------------------------------------------------------------

    [Fact]
    public void ComposeMultipart_resolves_includes_within_the_uploaded_set_by_name()
    {
        var files = new Dictionary<string, string>
        {
            ["base.json"] = """{"version":1,"sources":[{"name":"trades","fields":[{"name":"price","type":"Double"}]}]}""",
            ["overlay.json"] = """{"version":1,"include":["base.json"],"sources":[{"name":"trades","eventsPerSecond":9}]}""",
        };

        var (doc, diagnostics) = ConfigImportService.ComposeMultipart("overlay.json", files);

        Assert.NotNull(doc);
        Assert.Empty(diagnostics);
        var src = Assert.Single(doc!.Sources);
        Assert.Equal(9, src.EventsPerSecond);
    }

    [Fact]
    public void ComposeMultipart_no_files_reports_a_diagnostic()
    {
        var (doc, diagnostics) = ConfigImportService.ComposeMultipart(null, new Dictionary<string, string>());

        Assert.Null(doc);
        Assert.Contains(diagnostics, d => d.Contains("no files", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ComposeMultipart_missing_include_is_a_fatal_diagnostic()
    {
        var files = new Dictionary<string, string> { ["root.json"] = """{"version":1,"include":["missing.json"]}""" };

        var (doc, diagnostics) = ConfigImportService.ComposeMultipart("root.json", files);

        Assert.Null(doc);
        Assert.Contains(diagnostics, d => d.Contains("missing include", StringComparison.OrdinalIgnoreCase));
    }

    // ------------------------------------------------------------------
    // DocumentErrorReport (pure).
    // ------------------------------------------------------------------

    [Fact]
    public void DocumentErrorReport_builds_one_document_kind_error_entry_per_diagnostic()
    {
        var report = ConfigImportService.DocumentErrorReport("validate", ["missing include: base.json", "include cycle detected: a -> b -> a"]);

        Assert.Equal("validate", report.Mode);
        Assert.False(report.Ok);
        Assert.Equal(2, report.Entries.Count);
        Assert.All(report.Entries, e =>
        {
            Assert.Equal("document", e.Kind);
            Assert.Equal("error", e.Action);
            Assert.Single(e.Diagnostics);
        });
        Assert.Equal("missing include: base.json", report.Entries[0].Name);
    }

    [Fact]
    public void DocumentErrorReport_falls_back_to_a_generic_message_when_no_diagnostics_given()
    {
        var report = ConfigImportService.DocumentErrorReport("merge", []);

        Assert.False(report.Ok);
        var entry = Assert.Single(report.Entries);
        Assert.Equal("document", entry.Kind);
        Assert.Equal("error", entry.Action);
    }

    // ------------------------------------------------------------------
    // BuildSourceSchemas / MapFieldKind (pure).
    // ------------------------------------------------------------------

    [Fact]
    public void MapFieldKind_maps_every_FieldType_to_its_FieldKind_counterpart()
    {
        Assert.Equal(FieldKind.String, ConfigImportService.MapFieldKind(FieldType.String));
        Assert.Equal(FieldKind.Double, ConfigImportService.MapFieldKind(FieldType.Double));
        Assert.Equal(FieldKind.Long, ConfigImportService.MapFieldKind(FieldType.Long));
        Assert.Equal(FieldKind.Bool, ConfigImportService.MapFieldKind(FieldType.Bool));
        Assert.Equal(FieldKind.Timestamp, ConfigImportService.MapFieldKind(FieldType.Timestamp));
        Assert.Equal(FieldKind.Json, ConfigImportService.MapFieldKind(FieldType.Json));
    }

    [Fact]
    public void BuildSourceSchemas_builds_one_schema_per_source_keyed_by_name()
    {
        var sources = new[]
        {
            Source("trades", new FieldDef("price", FieldType.Double), new FieldDef("symbol", FieldType.String)),
            Source("quotes", new FieldDef("bid", FieldType.Double)),
        };

        var schemas = ConfigImportService.BuildSourceSchemas(sources);

        Assert.Equal(2, schemas.Count);
        Assert.Equal(FieldKind.Double, schemas["trades"].Fields["price"]);
        Assert.Equal(FieldKind.String, schemas["trades"].Fields["symbol"]);
        Assert.Equal(FieldKind.Double, schemas["quotes"].Fields["bid"]);
    }

    // ------------------------------------------------------------------
    // RunImportAsync — end-to-end apply pipeline against a full in-memory ICatalogFacade.
    // ------------------------------------------------------------------

    [Fact]
    public async Task RunImportAsync_validate_mode_never_mutates_the_catalog()
    {
        var facade = new FakeCatalogFacade();
        facade.Sources.Add(Source("trades", new FieldDef("price", FieldType.Double)));

        var doc = new ConfigDocument { Sources = [Source("trades", new FieldDef("price", FieldType.Double))] };

        var report = await ConfigImportService.RunImportAsync(doc, "validate", "admin", facade, apply: false);

        Assert.True(report.Ok);
        var entry = Assert.Single(report.Entries);
        Assert.Equal("skipped", entry.Action);
        Assert.Single(facade.Sources); // untouched.
    }

    [Fact]
    public async Task RunImportAsync_merge_creates_a_new_source_pipeline_and_table_and_starts_running_ones()
    {
        var facade = new FakeCatalogFacade();
        var doc = new ConfigDocument
        {
            Sources = [Source("trades", new FieldDef("symbol", FieldType.String), new FieldDef("price", FieldType.Double))],
            Pipelines = [new ConfigPipeline { Name = "p1", Sql = "SELECT symbol, price FROM trades", Running = true }],
            Tables = [new ConfigTable { Name = "t1", Sql = "SELECT symbol, SUM(price) AS total FROM trades GROUP BY symbol", Running = false }],
        };

        var report = await ConfigImportService.RunImportAsync(doc, "merge", "admin", facade, apply: true);

        Assert.True(report.Ok);
        Assert.All(report.Entries, e => Assert.Equal("created", e.Action));

        var source = Assert.Single(facade.Sources);
        Assert.Equal("trades", source.Name);

        var pipeline = Assert.Single(facade.Pipelines);
        Assert.Equal(PipelineStatus.Running, pipeline.Status); // Running: true honored.

        var table = Assert.Single(facade.Tables);
        Assert.Equal(PipelineStatus.Stopped, table.Status); // Running: false honored.
        Assert.Equal(2, table.OutputFields.Count); // compiled by the real Engine compiler.
    }

    [Fact]
    public async Task RunImportAsync_merge_updates_an_existing_pipeline_description()
    {
        var facade = new FakeCatalogFacade();
        var existing = facade.AddPipeline("p1", "SELECT symbol FROM trades", "old description", PipelineStatus.Stopped);
        facade.Sources.Add(Source("trades", new FieldDef("symbol", FieldType.String)));

        var doc = new ConfigDocument
        {
            Sources = [Source("trades", new FieldDef("symbol", FieldType.String))],
            Pipelines = [new ConfigPipeline { Name = "p1", Sql = "SELECT symbol FROM trades", Description = "new description", Running = false }],
        };

        var report = await ConfigImportService.RunImportAsync(doc, "merge", "admin", facade, apply: true);

        Assert.True(report.Ok);
        var pipelineEntry = Assert.Single(report.Entries, e => e.Kind == "pipeline");
        Assert.Equal("updated", pipelineEntry.Action);
        Assert.Equal("new description", facade.Pipelines.Single(p => p.Id == existing.Id).Description);
    }

    [Fact]
    public async Task RunImportAsync_a_failing_pipeline_compile_is_reported_as_error_and_not_applied()
    {
        var facade = new FakeCatalogFacade();
        var doc = new ConfigDocument
        {
            Pipelines = [new ConfigPipeline { Name = "bad", Sql = "SELECT nope FROM unknown_source" }],
        };

        var report = await ConfigImportService.RunImportAsync(doc, "merge", "admin", facade, apply: true);

        Assert.False(report.Ok);
        var entry = Assert.Single(report.Entries);
        Assert.Equal("error", entry.Action);
        Assert.NotEmpty(entry.Diagnostics);
        Assert.Empty(facade.Pipelines); // never created.
    }

    [Fact]
    public async Task RunImportAsync_a_failing_table_compile_is_reported_as_error_and_not_applied()
    {
        var facade = new FakeCatalogFacade();
        facade.Sources.Add(Source("trades", new FieldDef("symbol", FieldType.String)));
        var doc = new ConfigDocument
        {
            Sources = [Source("trades", new FieldDef("symbol", FieldType.String))],
            Tables = [new ConfigTable { Name = "bad", Sql = "SELECT nope FROM unknown_source" }],
        };

        var report = await ConfigImportService.RunImportAsync(doc, "merge", "admin", facade, apply: true);

        Assert.False(report.Ok);
        var entry = Assert.Single(report.Entries, e => e.Kind == "table");
        Assert.Equal("error", entry.Action);
        Assert.Empty(facade.Tables); // never created.
    }

    [Fact]
    public async Task RunImportAsync_table_dependency_order_lets_a_dependent_table_compile_against_its_freshly_created_input()
    {
        var facade = new FakeCatalogFacade();
        facade.Sources.Add(Source("trades", new FieldDef("symbol", FieldType.String), new FieldDef("price", FieldType.Double)));

        var doc = new ConfigDocument
        {
            Sources = [Source("trades", new FieldDef("symbol", FieldType.String), new FieldDef("price", FieldType.Double))],
            Tables =
            [
                new ConfigTable { Name = "totals", Sql = "SELECT symbol, SUM(price) AS total FROM trades GROUP BY symbol" },
                new ConfigTable { Name = "totals_view", Sql = "SELECT symbol, total FROM totals" },
            ],
        };

        var report = await ConfigImportService.RunImportAsync(doc, "merge", "admin", facade, apply: true);

        Assert.True(report.Ok, string.Join("; ", report.Entries.SelectMany(e => e.Diagnostics)));
        Assert.Equal(2, facade.Tables.Count);
        var view = facade.Tables.Single(t => t.Name == "totals_view");
        Assert.Contains("totals", view.TableInputs);
    }

    [Fact]
    public async Task RunImportAsync_invalid_search_mode_is_reported_as_error()
    {
        var facade = new FakeCatalogFacade();
        facade.Sources.Add(Source("trades", new FieldDef("symbol", FieldType.String)));
        var doc = new ConfigDocument
        {
            Sources = [Source("trades", new FieldDef("symbol", FieldType.String))],
            Tables = [new ConfigTable { Name = "t1", Sql = "SELECT symbol FROM trades", SearchMode = "Bogus" }],
        };

        var report = await ConfigImportService.RunImportAsync(doc, "merge", "admin", facade, apply: true);

        Assert.False(report.Ok);
        var entry = Assert.Single(report.Entries, e => e.Kind == "table");
        Assert.Equal("error", entry.Action);
        Assert.Contains(entry.Diagnostics, d => d.Contains("searchMode", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task RunImportAsync_replace_mode_stops_a_running_pipeline_before_deleting_it()
    {
        var facade = new FakeCatalogFacade();
        var stale = facade.AddPipeline("stale", "SELECT symbol FROM trades", "", PipelineStatus.Running);
        facade.Sources.Add(Source("trades", new FieldDef("symbol", FieldType.String)));

        var doc = new ConfigDocument { Sources = [Source("trades", new FieldDef("symbol", FieldType.String))] }; // no pipelines -> "stale" absent.

        var report = await ConfigImportService.RunImportAsync(doc, "replace", "admin", facade, apply: true);

        Assert.True(report.Ok, string.Join("; ", report.Entries.SelectMany(e => e.Diagnostics)));
        var entry = Assert.Single(report.Entries, e => e.Kind == "pipeline");
        Assert.Equal("deleted", entry.Action);
        Assert.Empty(facade.Pipelines);
        Assert.Contains(stale.Id, facade.StoppedBeforeDeletePipelineIds);
    }

    [Fact]
    public async Task RunImportAsync_source_secrets_merge_keeps_the_stored_header_value_when_the_doc_sends_the_mask()
    {
        var facade = new FakeCatalogFacade();
        var stored = Source("api", new FieldDef("price", FieldType.Double));
        stored.Kind = SourceKinds.Url;
        stored.Connector = new ConnectorConfig { Url = new UrlPollConfig { Url = "http://example.test", Headers = { ["X-Api-Key"] = "real-secret" } } };
        facade.Sources.Add(stored);

        var incoming = Source("api", new FieldDef("price", FieldType.Double));
        incoming.Kind = SourceKinds.Url;
        incoming.Connector = new ConnectorConfig { Url = new UrlPollConfig { Url = "http://example.test", Headers = { ["X-Api-Key"] = SourceKinds.SecretMask } } };
        var doc = new ConfigDocument { Sources = [incoming] };

        var report = await ConfigImportService.RunImportAsync(doc, "merge", "admin", facade, apply: true);

        Assert.True(report.Ok);
        var applied = facade.Sources.Single(s => s.Name == "api");
        Assert.Equal("real-secret", applied.Connector!.Url!.Headers["X-Api-Key"]); // never clobbered with the literal "***".
    }

    // ------------------------------------------------------------------
    // Test fixtures.
    // ------------------------------------------------------------------

    private static SourceDefinition Source(string name, params FieldDef[] fields) => new()
    {
        Name = name,
        Fields = [.. fields],
        GeneratorProfile = "generic",
        EventsPerSecond = 5,
        Enabled = true,
    };

    /// <summary>
    /// A full in-memory <see cref="ICatalogFacade"/> — unlike FakeRegistryGrain.cs (which throws
    /// NotImplementedException on every write member and is a frozen fixture other tests depend on
    /// unmodified), this fake actually applies writes so <see cref="ConfigImportService.RunImportAsync"/>
    /// can be exercised end-to-end. Compiles table SQL for real (via SqlCompiler.CompileTable, exactly
    /// like TablesEndpoints.CreateTableAsync/UpdateTableAsync would), so a table's OutputFields/
    /// StreamInputs/TableInputs reflect a genuine compile, matching production behavior closely enough
    /// for these tests' assertions.
    /// </summary>
    private sealed class FakeCatalogFacade : ICatalogFacade
    {
        private int _nextId;

        public List<SourceDefinition> Sources { get; } = [];
        public List<PipelineDefinition> Pipelines { get; } = [];
        public List<TableDefinition> Tables { get; } = [];
        public List<string> StoppedBeforeDeletePipelineIds { get; } = [];
        public List<string> StoppedBeforeDeleteTableIds { get; } = [];

        public PipelineDefinition AddPipeline(string name, string sql, string description, PipelineStatus status)
        {
            var def = new PipelineDefinition { Id = NextId("pl"), Name = name, Sql = sql, Description = description, Status = status };
            Pipelines.Add(def);
            return def;
        }

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

            if (status == PipelineStatus.Stopped && p.Status != PipelineStatus.Stopped)
            {
                StoppedBeforeDeletePipelineIds.Add(id);
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

            if (status == PipelineStatus.Stopped && t.Status != PipelineStatus.Stopped)
            {
                StoppedBeforeDeleteTableIds.Add(id);
            }

            t.Status = status;
            return Task.FromResult<TableDefinition?>(t);
        }

        public Task<string> EnsureFieldNumbersAsync(string entityKey, List<FieldDef> fields) => throw new NotImplementedException();

        private string NextId(string prefix) => $"{prefix}-{++_nextId}";

        /// <summary>Mirrors TablesEndpoints.CreateTableAsync/UpdateTableAsync's own internal compile
        /// step (real RegistryGrain behavior) — ConfigImportService already precompiles before calling
        /// Create/UpdateTableAsync, so this should always succeed given the same world, but a real
        /// facade recompiles independently and this fake does too for fidelity.</summary>
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
