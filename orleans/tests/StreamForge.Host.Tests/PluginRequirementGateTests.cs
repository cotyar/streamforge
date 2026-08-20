using StreamForge.Abstractions;
using StreamForge.Api;
using StreamForge.AppCore.Config;
using StreamForge.AppCore.Transports;
using StreamForge.Engine;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// Plan 016 wave 4 — the THIRD fatal, whole-import gate <see cref="ConfigImportService.RunImportAsync"/>
/// runs before its apply loop: <see cref="ConfigImportService.DetectUnsatisfiedPluginRequirements"/>,
/// checking <see cref="ConfigDocument.Requires"/> against what this instance actually has registered
/// (<see cref="KindVersions"/>). See that method's doc comment for the fatal-vs-warning argument (it
/// mirrors <c>DetectBreakingSchemaChanges</c>'s reasoning, not the endpoint-alias warning's). A NEW file,
/// same "new tests in a new file" pattern <c>ConfigImportGatesTests.cs</c> already established for this
/// wave family — its <c>FakeCatalogFacade</c> is <c>private</c> to that class, so this file carries an
/// independent copy rather than reaching into a file this wave does not own.
/// </summary>
public class PluginRequirementGateTests
{
    // ------------------------------------------------------------------
    // DetectUnsatisfiedPluginRequirements (pure) — against a synthetic availability map, independent of
    // whatever connectors this particular test process happens to have loaded.
    // ------------------------------------------------------------------

    [Fact]
    public void No_requirements_declared_is_never_gated()
    {
        var doc = new ConfigDocument();

        var entries = ConfigImportService.DetectUnsatisfiedPluginRequirements(doc, new Dictionary<string, string>());

        Assert.Empty(entries);
    }

    [Fact]
    public void A_satisfied_requirement_is_not_gated()
    {
        var doc = new ConfigDocument { Requires = [new ConfigPluginRequirement { Kind = "postgres-cdc", Version = "^2.0.0" }] };
        var available = new Dictionary<string, string> { ["postgres-cdc"] = "2.3.1" };

        var entries = ConfigImportService.DetectUnsatisfiedPluginRequirements(doc, available);

        Assert.Empty(entries);
    }

    [Fact]
    public void An_empty_or_star_requirement_is_satisfied_by_being_present_at_any_version()
    {
        var doc = new ConfigDocument { Requires = [new ConfigPluginRequirement { Kind = "fix", Version = "*" }] };
        var available = new Dictionary<string, string> { ["fix"] = "1.0.0" };

        var entries = ConfigImportService.DetectUnsatisfiedPluginRequirements(doc, available);

        Assert.Empty(entries);
    }

    [Fact]
    public void A_kind_not_registered_at_all_is_gated_as_requires_naming_the_kind()
    {
        var doc = new ConfigDocument { Requires = [new ConfigPluginRequirement { Kind = "mssql-cdc", Version = "^1.0.0" }] };
        var available = new Dictionary<string, string>(); // nothing registered.

        var entries = ConfigImportService.DetectUnsatisfiedPluginRequirements(doc, available);

        var entry = Assert.Single(entries);
        Assert.Equal("requires", entry.Kind);
        Assert.Equal("mssql-cdc", entry.Name);
        Assert.Equal("error", entry.Action);
        Assert.Contains(entry.Diagnostics, d => d.Contains("not registered", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void A_registered_kind_at_an_incompatible_version_is_gated_and_names_both_versions()
    {
        // The category DetectBreakingSchemaChanges's reasoning transfers to: the kind exists and would
        // pass a plain "is it known" check, but the ACTUAL installed version silently disagrees with
        // what the document was authored against.
        var doc = new ConfigDocument { Requires = [new ConfigPluginRequirement { Kind = "postgres-cdc", Version = "^2.0.0" }] };
        var available = new Dictionary<string, string> { ["postgres-cdc"] = "1.4.0" };

        var entries = ConfigImportService.DetectUnsatisfiedPluginRequirements(doc, available);

        var entry = Assert.Single(entries);
        Assert.Equal("requires", entry.Kind);
        Assert.Equal("postgres-cdc", entry.Name);
        Assert.Contains(entry.Diagnostics, d => d.Contains("1.4.0", StringComparison.Ordinal) && d.Contains("^2.0.0", StringComparison.Ordinal));
    }

    [Fact]
    public void A_malformed_range_is_gated_rather_than_ignored_fail_closed()
    {
        var doc = new ConfigDocument { Requires = [new ConfigPluginRequirement { Kind = "fix", Version = "1.2.3 - 4.5.6" }] };
        var available = new Dictionary<string, string> { ["fix"] = "1.0.0" };

        var entries = ConfigImportService.DetectUnsatisfiedPluginRequirements(doc, available);

        var entry = Assert.Single(entries);
        Assert.Equal("requires", entry.Kind);
        Assert.Contains(entry.Diagnostics, d => d.Contains("not a supported version range", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Multiple_unsatisfied_requirements_each_get_their_own_entry_ordered_by_kind()
    {
        var doc = new ConfigDocument
        {
            Requires =
            [
                new ConfigPluginRequirement { Kind = "zzz-kind", Version = "^1.0.0" },
                new ConfigPluginRequirement { Kind = "aaa-kind", Version = "^1.0.0" },
            ],
        };

        var entries = ConfigImportService.DetectUnsatisfiedPluginRequirements(doc, new Dictionary<string, string>());

        Assert.Equal(2, entries.Count);
        Assert.Equal("aaa-kind", entries[0].Name);
        Assert.Equal("zzz-kind", entries[1].Name);
    }

    // ------------------------------------------------------------------
    // KindVersions (live registries) — "nats" is always statically registered
    // (InboundTransports.Registered's built-in list), so this is process-independent unlike the
    // database/FIX connector kinds, which register only when their assembly's host startup runs.
    // ------------------------------------------------------------------

    [Fact]
    public void KindVersions_resolves_a_built_in_source_kind_and_an_always_registered_transport_kind()
    {
        var all = KindVersions.All();

        Assert.Equal("1.0.0", all[SourceKinds.Generator]);
        Assert.Equal("1.0.0", all[SourceKinds.Nats]);
    }

    [Fact]
    public void KindVersions_has_no_entry_for_an_unregistered_kind()
    {
        Assert.Null(KindVersions.Resolve("totally-unregistered-kind-xyz"));
    }

    // ------------------------------------------------------------------
    // RunImportAsync — end-to-end: satisfied requirement lets a real change through; an unsatisfied one
    // refuses the WHOLE import, identically in validate mode and on a real apply, catalog untouched.
    // ------------------------------------------------------------------

    [Fact]
    public async Task RunImportAsync_a_satisfied_requirement_lets_the_rest_of_the_document_apply()
    {
        var facade = new FakeCatalogFacade();
        var doc = new ConfigDocument
        {
            Requires = [new ConfigPluginRequirement { Kind = SourceKinds.Nats, Version = "^1.0.0" }], // nats is always registered at 1.0.0.
            Sources = [Source("trades", new FieldDef("price", FieldType.Double))],
        };

        var report = await ConfigImportService.RunImportAsync(doc, "merge", "admin", facade, apply: true);

        Assert.True(report.Ok, string.Join("; ", report.Entries.SelectMany(e => e.Diagnostics)));
        Assert.Single(facade.Sources);
    }

    [Fact]
    public async Task RunImportAsync_an_unregistered_kind_requirement_refuses_the_whole_import_in_validate_mode()
    {
        var facade = new FakeCatalogFacade();
        var doc = new ConfigDocument
        {
            Requires = [new ConfigPluginRequirement { Kind = "totally-unregistered-kind-xyz", Version = "^1.0.0" }],
            Sources = [Source("trades", new FieldDef("price", FieldType.Double))], // would otherwise apply cleanly.
        };

        var report = await ConfigImportService.RunImportAsync(doc, "validate", "admin", facade, apply: false);

        Assert.False(report.Ok);
        var entry = Assert.Single(report.Entries);
        Assert.Equal("requires", entry.Kind);
        Assert.Equal("totally-unregistered-kind-xyz", entry.Name);
        Assert.Empty(facade.Sources); // nothing applied — validate never writes regardless.
    }

    [Fact]
    public async Task RunImportAsync_an_unsatisfied_version_requirement_refuses_a_real_apply_and_the_catalog_stays_untouched()
    {
        var facade = new FakeCatalogFacade();
        facade.Sources.Add(Source("existing", new FieldDef("x", FieldType.String)));
        var doc = new ConfigDocument
        {
            // nats IS registered, but not at a version satisfying this range — the "silent corruption"
            // category, not the "fails loudly anyway" one. See the gate's doc comment.
            Requires = [new ConfigPluginRequirement { Kind = SourceKinds.Nats, Version = ">=99.0.0" }],
            Sources = [Source("trades", new FieldDef("price", FieldType.Double))],
        };

        var report = await ConfigImportService.RunImportAsync(doc, "merge", "admin", facade, apply: true);

        Assert.False(report.Ok);
        var entry = Assert.Single(report.Entries);
        Assert.Equal("requires", entry.Kind);
        Assert.Equal(SourceKinds.Nats, entry.Name);
        // The catalog is PROVABLY unchanged: still exactly the one pre-existing source, nothing from the
        // refused document made it through.
        var stillStored = Assert.Single(facade.Sources);
        Assert.Equal("existing", stillStored.Name);
    }

    [Fact]
    public async Task RunImportAsync_validate_and_a_real_apply_agree_on_an_unsatisfied_requirement()
    {
        var doc = new ConfigDocument
        {
            Requires = [new ConfigPluginRequirement { Kind = "totally-unregistered-kind-xyz", Version = "*" }],
            Sources = [Source("trades", new FieldDef("price", FieldType.Double))],
        };

        var validateReport = await ConfigImportService.RunImportAsync(doc, "validate", "admin", new FakeCatalogFacade(), apply: false);
        var applyReport = await ConfigImportService.RunImportAsync(doc, "merge", "admin", new FakeCatalogFacade(), apply: true);

        Assert.False(validateReport.Ok);
        Assert.False(applyReport.Ok);
        Assert.Equal(validateReport.Entries.Select(e => (e.Kind, e.Name, e.Action)), applyReport.Entries.Select(e => (e.Kind, e.Name, e.Action)));
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

    /// <summary>A second, deliberately independent copy of <c>ConfigEndpointsLogicTests.FakeCatalogFacade</c>
    /// — that type is <c>private</c> to its own class, and this wave's ownership brief puts new tests in a
    /// NEW file, so there is no shared-fixture seam to reach for without touching a file this wave does
    /// not own. Same shape, same real-compile behavior, for the same reason.</summary>
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
