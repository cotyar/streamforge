using StreamForge.Abstractions;
using StreamForge.Api;
using StreamForge.AppCore.Config;
using StreamForge.Engine;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// Plan 016 wave 3-C: the two fatal, whole-import gates <see cref="ConfigImportService.RunImportAsync"/>
/// runs before its apply loop — a table dependency cycle (job 1) and a <c>schemaPolicy</c>-breaking
/// source schema change (job 2) — plus their pure building blocks
/// (<see cref="ConfigImportService.DetectTableDependencyCycle"/>,
/// <see cref="ConfigImportService.DetectBreakingSchemaChanges"/>,
/// <see cref="ConfigImportService.CycleErrorReport"/>) tested directly. Also covers a live-found bug in
/// the same file, flagged mid-wave: <c>ProcessTableAsync</c>/<c>ProcessPipelineAsync</c> built their
/// create/update definitions without the document's <c>DependsOn</c> pins, so an imported pin silently
/// vanished. A NEW file, per this wave's ownership brief — <c>ConfigEndpointsLogicTests.cs</c> (plan
/// 006) and <c>ImportPlannerTests.cs</c> (wave 3-B) are untouched.
/// </summary>
public class ConfigImportGatesTests
{
    // ------------------------------------------------------------------
    // DetectTableDependencyCycle (pure).
    // ------------------------------------------------------------------

    [Fact]
    public void DetectTableDependencyCycle_returns_null_for_an_acyclic_document()
    {
        var doc = new ConfigDocument
        {
            Tables =
            [
                new ConfigTable { Name = "a", Sql = "SELECT symbol FROM trades" },
                new ConfigTable { Name = "b", Sql = "SELECT symbol FROM a" },
            ],
        };

        var cycle = ConfigImportService.DetectTableDependencyCycle(doc, new Dictionary<string, TableDefinition>());

        Assert.Null(cycle);
    }

    [Fact]
    public void DetectTableDependencyCycle_names_the_full_chain_for_two_brand_new_tables()
    {
        // Neither "a" nor "b" exists in the catalog yet -> dependencies come from SqlCompiler.ExtractReferences
        // over each table's own SQL (ScanNewEntityReferences's job), not from persisted TableInputs.
        var doc = new ConfigDocument
        {
            Tables =
            [
                new ConfigTable { Name = "a", Sql = "SELECT x FROM b" },
                new ConfigTable { Name = "b", Sql = "SELECT x FROM a" },
            ],
        };

        var cycle = ConfigImportService.DetectTableDependencyCycle(doc, new Dictionary<string, TableDefinition>());

        Assert.NotNull(cycle);
        // Full chain named, not merely "a cycle exists" — both directions are a correct DFS answer
        // depending on which node the deterministic (alphabetical) walk starts from.
        Assert.True(cycle == "a -> b -> a" || cycle == "b -> a -> b", $"unexpected cycle text: {cycle}");
    }

    [Fact]
    public void DetectTableDependencyCycle_names_a_three_table_chain()
    {
        var doc = new ConfigDocument
        {
            Tables =
            [
                new ConfigTable { Name = "a", Sql = "SELECT x FROM b" },
                new ConfigTable { Name = "b", Sql = "SELECT x FROM c" },
                new ConfigTable { Name = "c", Sql = "SELECT x FROM a" },
            ],
        };

        var cycle = ConfigImportService.DetectTableDependencyCycle(doc, new Dictionary<string, TableDefinition>());

        Assert.NotNull(cycle);
        Assert.Equal(3, cycle!.Split(" -> ").Length - 1); // 4 tokens: the 3-node loop plus the repeated start.
    }

    [Fact]
    public void DetectTableDependencyCycle_uses_persisted_TableInputs_for_a_table_already_in_the_catalog()
    {
        // "a" is EXISTING (its TableInputs says it reads "b") — the doc's SQL text for "a" says something
        // else entirely, which DetectTableDependencyCycle must ignore in favor of the persisted fact,
        // exactly like ImportPlanner does for existing tables.
        var existingA = new TableDefinition { Id = "tb-a", Name = "a", Sql = "SELECT x FROM nowhere", TableInputs = ["b"] };
        var tableByName = new Dictionary<string, TableDefinition> { ["a"] = existingA };

        var doc = new ConfigDocument
        {
            Tables =
            [
                new ConfigTable { Name = "a", Sql = "SELECT x FROM nowhere" }, // ignored: "a" is existing.
                new ConfigTable { Name = "b", Sql = "SELECT x FROM a" },
            ],
        };

        var cycle = ConfigImportService.DetectTableDependencyCycle(doc, tableByName);

        Assert.NotNull(cycle);
    }

    [Fact]
    public void DetectTableDependencyCycle_ignores_a_dependency_outside_the_document()
    {
        // "a" references "external", which is not in this document's table set -> not a cycle, just an
        // ordinary (possibly dangling, which is a different concern entirely) reference.
        var doc = new ConfigDocument
        {
            Tables = [new ConfigTable { Name = "a", Sql = "SELECT x FROM external" }],
        };

        var cycle = ConfigImportService.DetectTableDependencyCycle(doc, new Dictionary<string, TableDefinition>());

        Assert.Null(cycle);
    }

    [Fact]
    public void DetectTableDependencyCycle_sees_a_JOIN_cycle_a_FROM_only_regex_would_miss()
    {
        // "a" only reaches "b" through a JOIN, not a FROM — SqlCompiler.ExtractReferences is JOIN-aware
        // (wave 3-A), so this is exactly the case the doc comment claims is a strict superset of the
        // planner's old FROM-only regex.
        var doc = new ConfigDocument
        {
            Tables =
            [
                new ConfigTable { Name = "a", Sql = "SELECT x.v FROM trades x JOIN b y ON x.k = y.k" },
                new ConfigTable { Name = "b", Sql = "SELECT x FROM a" },
            ],
        };

        var cycle = ConfigImportService.DetectTableDependencyCycle(doc, new Dictionary<string, TableDefinition>());

        Assert.NotNull(cycle);
    }

    // ------------------------------------------------------------------
    // CycleErrorReport (pure).
    // ------------------------------------------------------------------

    [Fact]
    public void CycleErrorReport_is_a_single_document_level_error_entry_naming_the_chain()
    {
        var report = ConfigImportService.CycleErrorReport("validate", "a -> b -> a");

        Assert.Equal("validate", report.Mode);
        Assert.False(report.Ok);
        var entry = Assert.Single(report.Entries);
        Assert.Equal("document", entry.Kind);
        Assert.Equal("error", entry.Action);
        Assert.Contains("a -> b -> a", entry.Name);
        Assert.Contains(entry.Diagnostics, d => d.Contains("a -> b -> a", StringComparison.Ordinal));
    }

    // ------------------------------------------------------------------
    // DetectBreakingSchemaChanges (pure).
    // ------------------------------------------------------------------

    [Fact]
    public void DetectBreakingSchemaChanges_null_policy_gates_a_removed_field()
    {
        var stored = new Dictionary<string, SourceDefinition>
        {
            ["trades"] = Source("trades", new FieldDef("price", FieldType.Double), new FieldDef("qty", FieldType.Long)),
        };
        var doc = new ConfigDocument { SchemaPolicy = null, Sources = [Source("trades", new FieldDef("price", FieldType.Double))] };

        var entries = ConfigImportService.DetectBreakingSchemaChanges(doc, stored);

        var entry = Assert.Single(entries);
        Assert.Equal("source", entry.Kind);
        Assert.Equal("trades", entry.Name);
        Assert.Equal("error", entry.Action);
        Assert.Contains(entry.Diagnostics, d => d.Contains("qty", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("compatible")]
    [InlineData("compatable")] // the exact typo the plan brief calls out: anything but the literal "any" leaves the gate ON.
    [InlineData("ANY")] // wrong case is not the literal string "any" either.
    public void DetectBreakingSchemaChanges_only_the_literal_string_any_turns_the_gate_off(string? policy)
    {
        var stored = new Dictionary<string, SourceDefinition> { ["trades"] = Source("trades", new FieldDef("price", FieldType.Double)) };
        var doc = new ConfigDocument { SchemaPolicy = policy, Sources = [Source("trades")] }; // price removed.

        var entries = ConfigImportService.DetectBreakingSchemaChanges(doc, stored);

        Assert.Single(entries);
    }

    [Fact]
    public void DetectBreakingSchemaChanges_any_turns_the_gate_off()
    {
        var stored = new Dictionary<string, SourceDefinition> { ["trades"] = Source("trades", new FieldDef("price", FieldType.Double)) };
        var doc = new ConfigDocument { SchemaPolicy = "any", Sources = [Source("trades")] }; // price removed.

        var entries = ConfigImportService.DetectBreakingSchemaChanges(doc, stored);

        Assert.Empty(entries);
    }

    [Fact]
    public void DetectBreakingSchemaChanges_a_type_change_is_breaking_and_names_the_field()
    {
        var stored = new Dictionary<string, SourceDefinition> { ["trades"] = Source("trades", new FieldDef("price", FieldType.Double)) };
        var doc = new ConfigDocument { Sources = [Source("trades", new FieldDef("price", FieldType.String))] };

        var entries = ConfigImportService.DetectBreakingSchemaChanges(doc, stored);

        var entry = Assert.Single(entries);
        Assert.Contains(entry.Diagnostics, d => d.Contains("price", StringComparison.Ordinal) && d.Contains("type", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void DetectBreakingSchemaChanges_an_added_field_only_is_not_breaking()
    {
        var stored = new Dictionary<string, SourceDefinition> { ["trades"] = Source("trades", new FieldDef("price", FieldType.Double)) };
        var doc = new ConfigDocument { Sources = [Source("trades", new FieldDef("price", FieldType.Double), new FieldDef("qty", FieldType.Long))] };

        var entries = ConfigImportService.DetectBreakingSchemaChanges(doc, stored);

        Assert.Empty(entries);
    }

    [Fact]
    public void DetectBreakingSchemaChanges_a_brand_new_source_is_never_gated_against_itself()
    {
        var stored = new Dictionary<string, SourceDefinition>(); // nothing stored under this name.
        var doc = new ConfigDocument { Sources = [Source("new-source", new FieldDef("price", FieldType.Double))] };

        var entries = ConfigImportService.DetectBreakingSchemaChanges(doc, stored);

        Assert.Empty(entries);
    }

    // ------------------------------------------------------------------
    // RunImportAsync — end-to-end, both gates, both apply:false (validate) and apply:true.
    // ------------------------------------------------------------------

    [Fact]
    public async Task RunImportAsync_a_table_cycle_refuses_the_whole_import_in_validate_mode()
    {
        var facade = new FakeCatalogFacade();
        var doc = new ConfigDocument
        {
            Tables =
            [
                new ConfigTable { Name = "a", Sql = "SELECT x FROM b" },
                new ConfigTable { Name = "b", Sql = "SELECT x FROM a" },
            ],
        };

        var report = await ConfigImportService.RunImportAsync(doc, "validate", "admin", facade, apply: false);

        Assert.False(report.Ok);
        var entry = Assert.Single(report.Entries);
        Assert.Equal("document", entry.Kind);
        Assert.Contains("cycle", entry.Diagnostics[0], StringComparison.OrdinalIgnoreCase);
        Assert.Empty(facade.Tables);
    }

    [Fact]
    public async Task RunImportAsync_a_table_cycle_refuses_a_real_apply_and_the_catalog_stays_untouched()
    {
        var facade = new FakeCatalogFacade();
        facade.Sources.Add(Source("trades", new FieldDef("symbol", FieldType.String)));
        var doc = new ConfigDocument
        {
            // A source that would otherwise apply cleanly, alongside the cyclic tables — proving the
            // WHOLE import is refused, not merely the two tables in the cycle.
            Sources = [Source("trades", new FieldDef("symbol", FieldType.String), new FieldDef("price", FieldType.Double))],
            Tables =
            [
                new ConfigTable { Name = "a", Sql = "SELECT x FROM b" },
                new ConfigTable { Name = "b", Sql = "SELECT x FROM a" },
            ],
        };

        var report = await ConfigImportService.RunImportAsync(doc, "merge", "admin", facade, apply: true);

        Assert.False(report.Ok);
        Assert.Empty(facade.Tables);
        // The pre-existing source is untouched too: still its OLD field list, not the document's new one.
        var stillStored = Assert.Single(facade.Sources);
        Assert.Single(stillStored.Fields);
    }

    [Fact]
    public async Task RunImportAsync_a_breaking_source_change_refuses_the_whole_import_by_default()
    {
        var facade = new FakeCatalogFacade();
        facade.Sources.Add(Source("trades", new FieldDef("price", FieldType.Double), new FieldDef("qty", FieldType.Long)));

        var doc = new ConfigDocument
        {
            // "qty" removed from trades: breaking. A brand-new, otherwise-valid pipeline rides along in
            // the SAME document, to prove it is refused too, not applied while the source is skipped.
            Sources = [Source("trades", new FieldDef("price", FieldType.Double))],
            Pipelines = [new ConfigPipeline { Name = "p1", Sql = "SELECT price FROM trades" }],
        };

        var report = await ConfigImportService.RunImportAsync(doc, "merge", "admin", facade, apply: true);

        Assert.False(report.Ok);
        var entry = Assert.Single(report.Entries);
        Assert.Equal("source", entry.Kind);
        Assert.Equal("trades", entry.Name);
        Assert.Contains(entry.Diagnostics, d => d.Contains("qty", StringComparison.Ordinal));
        Assert.Empty(facade.Pipelines); // the whole import refused -- not just the source.
        Assert.Equal(2, facade.Sources.Single().Fields.Count); // untouched: still the OLD (2-field) shape.
    }

    [Fact]
    public async Task RunImportAsync_validate_mode_catches_the_breaking_source_change_before_anything_is_applied()
    {
        var facade = new FakeCatalogFacade();
        facade.Sources.Add(Source("trades", new FieldDef("price", FieldType.Double), new FieldDef("qty", FieldType.Long)));
        var doc = new ConfigDocument { Sources = [Source("trades", new FieldDef("price", FieldType.Double))] };

        var report = await ConfigImportService.RunImportAsync(doc, "validate", "admin", facade, apply: false);

        Assert.False(report.Ok);
        Assert.Equal(2, facade.Sources.Single().Fields.Count); // validate never writes regardless.
    }

    [Fact]
    public async Task RunImportAsync_schemaPolicy_any_lets_the_breaking_change_through()
    {
        var facade = new FakeCatalogFacade();
        facade.Sources.Add(Source("trades", new FieldDef("price", FieldType.Double), new FieldDef("qty", FieldType.Long)));
        var doc = new ConfigDocument
        {
            SchemaPolicy = "any",
            Sources = [Source("trades", new FieldDef("price", FieldType.Double))], // qty dropped: breaking, but the gate is off.
        };

        var report = await ConfigImportService.RunImportAsync(doc, "merge", "admin", facade, apply: true);

        Assert.True(report.Ok, string.Join("; ", report.Entries.SelectMany(e => e.Diagnostics)));
        Assert.Single(facade.Sources.Single().Fields); // the breaking change WAS applied.
    }

    [Fact]
    public async Task RunImportAsync_a_typo_d_schemaPolicy_still_gates_like_Auth_Mode_does()
    {
        var facade = new FakeCatalogFacade();
        facade.Sources.Add(Source("trades", new FieldDef("price", FieldType.Double), new FieldDef("qty", FieldType.Long)));
        var doc = new ConfigDocument
        {
            SchemaPolicy = "cmopatible", // typo — must NOT be treated as "any".
            Sources = [Source("trades", new FieldDef("price", FieldType.Double))],
        };

        var report = await ConfigImportService.RunImportAsync(doc, "merge", "admin", facade, apply: true);

        Assert.False(report.Ok);
        Assert.Equal(2, facade.Sources.Single().Fields.Count); // refused -- untouched.
    }

    [Fact]
    public async Task RunImportAsync_a_non_breaking_source_change_is_not_gated_and_applies_normally()
    {
        var facade = new FakeCatalogFacade();
        facade.Sources.Add(Source("trades", new FieldDef("price", FieldType.Double)));
        var doc = new ConfigDocument { Sources = [Source("trades", new FieldDef("price", FieldType.Double), new FieldDef("qty", FieldType.Long))] }; // additive.

        var report = await ConfigImportService.RunImportAsync(doc, "merge", "admin", facade, apply: true);

        Assert.True(report.Ok);
        Assert.Equal(2, facade.Sources.Single().Fields.Count);
    }

    // ------------------------------------------------------------------
    // DependsOn mapping — the live bug the coordinator flagged mid-wave: ProcessTableAsync/
    // ProcessPipelineAsync built their create/update definitions without the document's pins.
    // ------------------------------------------------------------------

    [Fact]
    public async Task RunImportAsync_maps_a_tables_DependsOn_on_create()
    {
        var facade = new FakeCatalogFacade();
        facade.Sources.Add(Source("trades", new FieldDef("price", FieldType.Double)));
        var doc = new ConfigDocument
        {
            Sources = [Source("trades", new FieldDef("price", FieldType.Double))],
            Tables =
            [
                new ConfigTable
                {
                    Name = "t1",
                    Sql = "SELECT price FROM trades",
                    DependsOn = [new EntityPin { Kind = "source", Name = "trades", SchemaRevision = 1 }],
                },
            ],
        };

        var report = await ConfigImportService.RunImportAsync(doc, "merge", "admin", facade, apply: true);

        Assert.True(report.Ok, string.Join("; ", report.Entries.SelectMany(e => e.Diagnostics)));
        var stored = Assert.Single(facade.Tables);
        var pin = Assert.Single(stored.DependsOn);
        Assert.Equal("source", pin.Kind);
        Assert.Equal("trades", pin.Name);
        Assert.Equal(1, pin.SchemaRevision);
    }

    [Fact]
    public async Task RunImportAsync_maps_a_tables_DependsOn_on_update_and_a_reimport_of_the_identical_document_plans_skipped()
    {
        var facade = new FakeCatalogFacade();
        facade.Sources.Add(Source("trades", new FieldDef("price", FieldType.Double)));
        var doc = new ConfigDocument
        {
            Sources = [Source("trades", new FieldDef("price", FieldType.Double))],
            Tables =
            [
                new ConfigTable
                {
                    Name = "t1",
                    Sql = "SELECT price FROM trades",
                    DependsOn = [new EntityPin { Kind = "source", Name = "trades", SchemaRevision = 1 }],
                },
            ],
        };

        var created = await ConfigImportService.RunImportAsync(doc, "merge", "admin", facade, apply: true);
        Assert.True(created.Ok, string.Join("; ", created.Entries.SelectMany(e => e.Diagnostics)));
        Assert.Equal("created", Assert.Single(created.Entries, e => e.Kind == "table").Action);

        // Re-importing the IDENTICAL document must plan "skipped", not "updated" — the exact permanent-
        // churn shape the coordinator's report described (and the same shape wave 2-A already fixed once
        // for a null-vs-"" description). Only reachable once the pin round-trips through the stored
        // TableDefinition, which is what this create/update fix makes true.
        var reimported = await ConfigImportService.RunImportAsync(doc, "merge", "admin", facade, apply: true);
        Assert.True(reimported.Ok, string.Join("; ", reimported.Entries.SelectMany(e => e.Diagnostics)));
        var entry = Assert.Single(reimported.Entries, e => e.Kind == "table");
        Assert.Equal("skipped", entry.Action);

        var pin = Assert.Single(facade.Tables.Single().DependsOn);
        Assert.Equal("trades", pin.Name);
    }

    [Fact]
    public async Task RunImportAsync_maps_a_pipelines_DependsOn_on_create_and_update()
    {
        var facade = new FakeCatalogFacade();
        facade.Sources.Add(Source("trades", new FieldDef("price", FieldType.Double)));
        var doc = new ConfigDocument
        {
            Sources = [Source("trades", new FieldDef("price", FieldType.Double))],
            Pipelines =
            [
                new ConfigPipeline
                {
                    Name = "p1",
                    Sql = "SELECT price FROM trades",
                    DependsOn = [new EntityPin { Kind = "source", Name = "trades", SchemaRevision = 1 }],
                },
            ],
        };

        var created = await ConfigImportService.RunImportAsync(doc, "merge", "admin", facade, apply: true);
        Assert.True(created.Ok, string.Join("; ", created.Entries.SelectMany(e => e.Diagnostics)));
        var storedAfterCreate = Assert.Single(facade.Pipelines);
        Assert.Single(storedAfterCreate.DependsOn);

        // Update path: bump the pin's SchemaRevision and re-import.
        doc.Pipelines[0].DependsOn = [new EntityPin { Kind = "source", Name = "trades", SchemaRevision = 2 }];
        var updated = await ConfigImportService.RunImportAsync(doc, "merge", "admin", facade, apply: true);

        Assert.True(updated.Ok, string.Join("; ", updated.Entries.SelectMany(e => e.Diagnostics)));
        Assert.Equal("updated", Assert.Single(updated.Entries, e => e.Kind == "pipeline").Action);
        Assert.Equal(2, facade.Pipelines.Single().DependsOn.Single().SchemaRevision);
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
