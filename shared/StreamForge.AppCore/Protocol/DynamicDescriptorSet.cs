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
/// recompile + <see cref="ICatalogFacade.EnsureFieldNumbersAsync"/> round trip per entity is cheap
/// enough for a demo; there is no mid-request invalidation to worry about as a result.</para>
///
/// <para>Plan 005 (Dapr sibling runtime) W1: constructed over <see cref="ICatalogFacade"/> rather than
/// <see cref="IRegistryGrain"/> directly — every construction site today happens to pass a real
/// <c>IRegistryGrain</c> (which IS-A ICatalogFacade, so those call sites need no change), but this type
/// itself no longer has any Orleans-grain dependency, making it reusable from a future Dapr host.</para>
/// </summary>
public sealed class DynamicDescriptorSet(ICatalogFacade registry)
{
    public async Task<IReadOnlyList<DynamicEntityDescriptor>> BuildAsync(CancellationToken cancellationToken = default)
    {
        var sources = await registry.GetSourcesAsync();
        var tables = await registry.GetTablesAsync();
        var pipelines = await registry.GetPipelinesAsync();

        // Inlined equivalent of StreamForge.Host.Grpc.SchemaBuilder.BuildStreamSchemasAsync (which is
        // typed over IRegistryGrain, not ICatalogFacade) — same mapping, built from the `sources` list
        // already fetched above instead of a second registry round trip.
        var streamSchemas = new Dictionary<string, SourceSchema>();
        foreach (var src in sources)
        {
            var fields = src.Fields.ToDictionary(f => f.Name, f => MapFieldKind(f.Type));
            streamSchemas[src.Name] = new SourceSchema(src.Name, fields);
        }

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
    /// numbering — that always goes through <see cref="ICatalogFacade.EnsureFieldNumbersAsync"/>, the
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

    /// <summary>Inlined equivalent of <c>StreamForge.Host.Grpc.ProtoMappers.MapFieldKind(FieldType)</c> —
    /// that type also carries a generated-proto (<c>V1</c>) dependency that is Host-only, so rather than
    /// move the whole file, this tiny Orleans-free enum mapping is duplicated here (plan 005 W2).</summary>
    private static FieldKind MapFieldKind(FieldType type) => type switch
    {
        FieldType.String => FieldKind.String,
        FieldType.Double => FieldKind.Double,
        FieldType.Long => FieldKind.Long,
        FieldType.Bool => FieldKind.Bool,
        FieldType.Timestamp => FieldKind.Timestamp,
        FieldType.Json => FieldKind.Json,
        _ => throw new ArgumentOutOfRangeException(nameof(type), type, "Unknown field type"),
    };
}
