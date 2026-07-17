using StreamForge.Abstractions;
using StreamForge.Engine;
using StreamForge.Host.Grpc.Dynamic;
using Xunit;

namespace StreamForge.Host.Tests;

public class DynamicDescriptorSetTests
{
    // ------------------------------------------------------------------
    // BuildPlan -- pure, no Orleans / IRegistryGrain needed.
    // ------------------------------------------------------------------

    [Fact]
    public void Sources_are_always_included()
    {
        var sources = new List<SourceDefinition> { new() { Name = "trades", Fields = TestHelpers.FlatFields } };

        var plan = DynamicDescriptorSet.BuildPlan(sources, [], [], new Dictionary<string, SourceSchema>());

        var entry = Assert.Single(plan);
        Assert.Equal("source:trades", entry.EntityKey);
        Assert.Equal("source", entry.Kind);
        Assert.Equal("trades", entry.Name);
        Assert.Same(TestHelpers.FlatFields, entry.Fields);
    }

    [Fact]
    public void Tables_without_a_compiled_output_schema_are_skipped()
    {
        var tables = new List<TableDefinition>
        {
            new() { Id = "t1", Name = "positions", OutputFields = TestHelpers.FlatFields },
            new() { Id = "t2", Name = "never_compiled", OutputFields = [] }, // e.g. seeded/created but SQL never compiled
        };

        var plan = DynamicDescriptorSet.BuildPlan([], tables, [], new Dictionary<string, SourceSchema>());

        var entry = Assert.Single(plan);
        Assert.Equal("table:t1", entry.EntityKey);
        Assert.Equal("table", entry.Kind);
        Assert.Equal("positions", entry.Name);
    }

    [Fact]
    public void Pipelines_whose_sql_compiles_are_included_with_their_output_schema()
    {
        var streamSchemas = new Dictionary<string, SourceSchema>
        {
            ["trades"] = new("trades", new Dictionary<string, FieldKind>
            {
                ["symbol"] = FieldKind.String,
                ["price"] = FieldKind.Double,
            }),
        };
        var pipelines = new List<PipelineDefinition>
        {
            new() { Id = "p1", Name = "vwap", Sql = "SELECT symbol, price FROM trades" },
        };

        var plan = DynamicDescriptorSet.BuildPlan([], [], pipelines, streamSchemas);

        var entry = Assert.Single(plan);
        Assert.Equal("pipeline:p1", entry.EntityKey);
        Assert.Equal("pipeline", entry.Kind);
        Assert.Equal("vwap", entry.Name);
        Assert.Equal(["symbol", "price"], entry.Fields.Select(f => f.Name));
    }

    [Fact]
    public void Broken_pipeline_sql_is_skipped_not_thrown()
    {
        var streamSchemas = new Dictionary<string, SourceSchema>
        {
            ["trades"] = new("trades", new Dictionary<string, FieldKind> { ["symbol"] = FieldKind.String }),
        };
        var pipelines = new List<PipelineDefinition>
        {
            new() { Id = "p-good", Name = "good", Sql = "SELECT symbol FROM trades" },
            new() { Id = "p-bad", Name = "bad", Sql = "SELECT nonexistent_column FROM trades" },
            new() { Id = "p-worse", Name = "worse", Sql = "not even sql" },
        };

        var plan = DynamicDescriptorSet.BuildPlan([], [], pipelines, streamSchemas);

        var entry = Assert.Single(plan);
        Assert.Equal("pipeline:p-good", entry.EntityKey);
    }

    [Fact]
    public void All_three_entity_kinds_combine_in_one_plan()
    {
        var streamSchemas = new Dictionary<string, SourceSchema>
        {
            ["trades"] = new("trades", new Dictionary<string, FieldKind> { ["symbol"] = FieldKind.String }),
        };
        var sources = new List<SourceDefinition> { new() { Name = "trades", Fields = TestHelpers.FlatFields } };
        var tables = new List<TableDefinition> { new() { Id = "t1", Name = "positions", OutputFields = TestHelpers.FlatFields } };
        var pipelines = new List<PipelineDefinition> { new() { Id = "p1", Name = "vwap", Sql = "SELECT symbol FROM trades" } };

        var plan = DynamicDescriptorSet.BuildPlan(sources, tables, pipelines, streamSchemas);

        Assert.Equal(3, plan.Count);
        Assert.Contains(plan, e => e.Kind == "source" && e.EntityKey == "source:trades");
        Assert.Contains(plan, e => e.Kind == "table" && e.EntityKey == "table:t1");
        Assert.Contains(plan, e => e.Kind == "pipeline" && e.EntityKey == "pipeline:p1");
    }

    // ------------------------------------------------------------------
    // BuildAsync -- exercised against a FakeRegistryGrain (no Orleans cluster needed) to prove it goes
    // through IRegistryGrain.EnsureFieldNumbersAsync for numbering (never assigns numbers itself).
    // ------------------------------------------------------------------

    [Fact]
    public async Task BuildAsync_obtains_field_numbers_from_the_registry_not_locally()
    {
        var registry = new FakeRegistryGrain();
        registry.Sources.Add(new SourceDefinition { Name = "trades", Fields = TestHelpers.FlatFields });

        var set = new DynamicDescriptorSet(registry);
        var result = await set.BuildAsync();

        var entry = Assert.Single(result);
        Assert.Equal(1, registry.EnsureFieldNumbersCallCount);
        Assert.True(registry.FieldNumberMaps.ContainsKey("source:trades"));
        // The numbers baked into the generated descriptor must match what the registry now holds.
        var persisted = EntitySchemas.ParseMap(registry.FieldNumberMaps["source:trades"]);
        Assert.Equal(persisted.Active, entry.Schema.FieldNumbers.Active);
    }

    [Fact]
    public async Task BuildAsync_keeps_field_numbers_stable_across_two_calls_when_schema_is_unchanged()
    {
        var registry = new FakeRegistryGrain();
        registry.Sources.Add(new SourceDefinition { Name = "trades", Fields = TestHelpers.FlatFields });
        var set = new DynamicDescriptorSet(registry);

        var first = (await set.BuildAsync()).Single();
        var second = (await set.BuildAsync()).Single();

        Assert.Equal(first.Schema.FieldNumbers.Active, second.Schema.FieldNumbers.Active);
    }

    [Fact]
    public async Task BuildAsync_gives_a_new_field_a_fresh_number_while_keeping_old_ones_stable()
    {
        var registry = new FakeRegistryGrain();
        var v1Fields = new List<FieldDef> { new("symbol", FieldType.String), new("price", FieldType.Double) };
        registry.Sources.Add(new SourceDefinition { Name = "trades", Fields = v1Fields });
        var set = new DynamicDescriptorSet(registry);
        var before = (await set.BuildAsync()).Single();
        var symbolNumber = before.Schema.FieldNumbers.Active["symbol"];
        var priceNumber = before.Schema.FieldNumbers.Active["price"];

        // Simulate an editor adding a field to the source, the same way PUT /api/sources/{name} would.
        registry.Sources[0].Fields = [.. v1Fields, new FieldDef("qty", FieldType.Long)];

        var after = (await set.BuildAsync()).Single();

        Assert.Equal(symbolNumber, after.Schema.FieldNumbers.Active["symbol"]);
        Assert.Equal(priceNumber, after.Schema.FieldNumbers.Active["price"]);
        Assert.True(after.Schema.FieldNumbers.Active.ContainsKey("qty"));
        Assert.DoesNotContain(after.Schema.FieldNumbers.Active["qty"], new[] { symbolNumber, priceNumber });
    }

    [Fact]
    public async Task BuildAsync_skips_broken_pipelines_end_to_end()
    {
        var registry = new FakeRegistryGrain();
        registry.Sources.Add(new SourceDefinition { Name = "trades", Fields = TestHelpers.FlatFields });
        registry.Pipelines.Add(new PipelineDefinition { Id = "p-good", Name = "good", Sql = "SELECT symbol FROM trades" });
        registry.Pipelines.Add(new PipelineDefinition { Id = "p-bad", Name = "bad", Sql = "SELECT nope FROM trades" });

        var set = new DynamicDescriptorSet(registry);
        var result = await set.BuildAsync();

        Assert.Contains(result, e => e.EntityKey == "source:trades");
        Assert.Contains(result, e => e.EntityKey == "pipeline:p-good");
        Assert.DoesNotContain(result, e => e.EntityKey == "pipeline:p-bad");
    }
}
