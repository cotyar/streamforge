using StreamForge.Abstractions;
using StreamForge.Engine;

namespace StreamForge.Host.Grpc.Dynamic;

/// <summary>One entity's generated dynamic descriptor, tagged with the catalog metadata a caller
/// (reflection, .proto downloads) needs alongside the raw <see cref="GeneratedSchema"/>.</summary>
public sealed record DynamicEntityDescriptor(string EntityKey, string Kind, string Name, GeneratedSchema Schema);

/// <summary>
/// Builds the full set of dynamic (runtime-generated) <see cref="GeneratedSchema"/>s for the current
/// catalog — one per stream source, one per table with a compiled output schema, and one per pipeline
/// whose SQL currently compiles (broken pipelines are skipped, not surfaced as an error: reflection is
/// a best-effort snapshot of "what's queryable right now").
///
/// <para>No caching: rebuilt fully on every <see cref="BuildAsync"/> call. Reflection requests are rare
/// (a client fetches a descriptor once, then holds it), so a full registry read + per-pipeline
/// recompile + <see cref="IRegistryGrain.EnsureFieldNumbersAsync"/> round trip per entity is cheap
/// enough for a demo; there is no mid-request invalidation to worry about as a result.</para>
/// </summary>
public sealed class DynamicDescriptorSet(IRegistryGrain registry)
{
    public async Task<IReadOnlyList<DynamicEntityDescriptor>> BuildAsync(CancellationToken cancellationToken = default)
    {
        var sources = await registry.GetSourcesAsync();
        var tables = await registry.GetTablesAsync();
        var pipelines = await registry.GetPipelinesAsync();
        var streamSchemas = await SchemaBuilder.BuildStreamSchemasAsync(registry);

        var plan = BuildPlan(sources, tables, pipelines, streamSchemas);

        var result = new List<DynamicEntityDescriptor>(plan.Count);
        // Two different entities landing on the same generated .proto filename (DescriptorFactory
        // derives it from the entity's Name) would collide in a FileDescriptorProto set keyed by
        // filename. Sources/tables can't collide (CreateTableAsync enforces name uniqueness across
        // sources+tables), but pipeline names aren't namespaced against either — guard defensively:
        // first entity to claim a filename wins, later ones are dropped rather than crashing.
        var usedFileNames = new HashSet<string>(StringComparer.Ordinal);
        foreach (var (entityKey, kind, name, fields) in plan)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var numbersJson = await registry.EnsureFieldNumbersAsync(entityKey, fields);
            var numbers = EntitySchemas.ParseMap(numbersJson);
            var schema = DescriptorFactory.Generate(name, fields, numbers);
            if (!usedFileNames.Add(schema.FileProto.Name))
            {
                continue;
            }

            result.Add(new DynamicEntityDescriptor(entityKey, kind, name, schema));
        }

        return result;
    }

    /// <summary>
    /// Pure planning step, deliberately split out of <see cref="BuildAsync"/> so it's unit-testable
    /// without a live Orleans cluster: decides which entities are in scope and what
    /// <see cref="FieldDef"/> list each one's descriptor should be generated from. Does NOT touch field
    /// numbering — that always goes through <see cref="IRegistryGrain.EnsureFieldNumbersAsync"/>, the
    /// single source of truth, so it can't be short-circuited here.
    /// </summary>
    public static List<(string EntityKey, string Kind, string Name, List<FieldDef> Fields)> BuildPlan(
        IReadOnlyList<SourceDefinition> sources,
        IReadOnlyList<TableDefinition> tables,
        IReadOnlyList<PipelineDefinition> pipelines,
        IReadOnlyDictionary<string, SourceSchema> streamSchemas)
    {
        var plan = new List<(string, string, string, List<FieldDef>)>();

        foreach (var src in sources)
        {
            plan.Add((EntitySchemas.SourceKey(src.Name), "source", src.Name, src.Fields));
        }

        foreach (var t in tables.Where(t => t.OutputFields.Count > 0))
        {
            plan.Add((EntitySchemas.TableKey(t.Id), "table", t.Name, t.OutputFields));
        }

        foreach (var p in pipelines)
        {
            // Pipelines don't persist a compiled OutputSchema (unlike tables) — recompile fresh
            // against the current source catalog, exactly like PipelineGrpcService.Validate does.
            var compiled = SqlCompiler.Compile(p.Sql, streamSchemas);
            if (!compiled.Ok || compiled.OutputSchema is null)
            {
                continue; // broken SQL -- skip, per spec
            }

            plan.Add((EntitySchemas.PipelineKey(p.Id), "pipeline", p.Name, EntitySchemas.FromOutputSchema(compiled.OutputSchema)));
        }

        return plan;
    }
}
