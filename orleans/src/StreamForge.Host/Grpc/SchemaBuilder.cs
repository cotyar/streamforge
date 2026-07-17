using StreamForge.Abstractions;
using StreamForge.Engine;

namespace StreamForge.Host.Grpc;

/// <summary>Builds the SourceSchema maps SqlCompiler.Compile/CompileTable need from the registry's
/// current sources/tables — same logic as StreamForge.Host.Api.PipelinesEndpoints/TablesEndpoints,
/// shared here so the gRPC Validate RPCs match REST exactly.</summary>
internal static class SchemaBuilder
{
    public static async Task<Dictionary<string, SourceSchema>> BuildStreamSchemasAsync(IRegistryGrain registry)
    {
        var sources = await registry.GetSourcesAsync();
        var schemas = new Dictionary<string, SourceSchema>();
        foreach (var src in sources)
        {
            var fields = src.Fields.ToDictionary(f => f.Name, f => ProtoMappers.MapFieldKind(f.Type));
            schemas[src.Name] = new SourceSchema(src.Name, fields);
        }

        return schemas;
    }

    public static async Task<Dictionary<string, SourceSchema>> BuildTableSchemasAsync(IRegistryGrain registry)
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
