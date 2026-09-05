using StreamsForge.Abstractions;
using StreamsForge.Engine;

namespace StreamsForge.Host.Grpc;

/// <summary>Builds the SourceSchema maps SqlCompiler.Compile/CompileTable need from the registry's
/// current sources/tables — same logic as StreamsForge.Host.Api.PipelinesEndpoints/TablesEndpoints,
/// shared here so the gRPC Validate RPCs match REST exactly.
///
/// <para>Plan 025 G1: takes <see cref="ICatalogFacade"/> rather than the Orleans-only
/// <c>IRegistryGrain</c> that inherits it, so the same builder serves both flavors' gRPC hosts.</para></summary>
internal static class SchemaBuilder
{
    /// <param name="includePipelines">Table-over-pipeline: true for a TABLE compile, where a pipeline
    /// with a compiled output schema is a legal relation; false for a PIPELINE compile, which reads
    /// sources only. The parameter exists rather than two methods because every other line is identical
    /// and the two must not drift — and it defaults to false so the pipeline Validate RPC that already
    /// calls this is unchanged.</param>
    public static async Task<Dictionary<string, SourceSchema>> BuildStreamSchemasAsync(
        ICatalogFacade registry, bool includePipelines = false)
    {
        var schemas = new Dictionary<string, SourceSchema>();

        if (includePipelines)
        {
            foreach (var p in await registry.GetPipelinesAsync())
            {
                if (p.OutputFields.Count == 0) continue;
                schemas[p.Name] = new SourceSchema(p.Name, p.OutputFields.ToDictionary(f => f.Name, f => ProtoMappers.MapFieldKind(f.Type)));
            }
        }

        var sources = await registry.GetSourcesAsync();
        foreach (var src in sources)
        {
            var fields = src.Fields.ToDictionary(f => f.Name, f => ProtoMappers.MapFieldKind(f.Type));
            schemas[src.Name] = new SourceSchema(src.Name, fields);
        }

        return schemas;
    }

    public static async Task<Dictionary<string, SourceSchema>> BuildTableSchemasAsync(ICatalogFacade registry)
    {
        var tables = await registry.GetTablesAsync();
        var schemas = new Dictionary<string, SourceSchema>();
        foreach (var t in tables.Where(t => t.OutputFields.Count > 0))
        {
            var fields = t.OutputFields.ToDictionary(f => f.Name, f => ProtoMappers.MapFieldKind(f.Type));
            schemas[t.Name] = new SourceSchema(t.Name, fields);
        }

        return schemas;
    }
}
