using System.Text.Json;
using StreamsForge.Abstractions;
using StreamsForge.Engine;

namespace StreamsForge.Host.Grpc.Dynamic;

/// <summary>
/// Shared conventions for dynamic-protobuf entities, used by BOTH the dynamic gRPC reflection
/// service and the .proto download endpoints so the two surfaces can never disagree on entity
/// keys, schemas, or (via IRegistryGrain.EnsureFieldNumbersAsync) field numbers.
/// </summary>
public static class EntitySchemas
{
    public static string SourceKey(string name) => $"source:{name}";
    public static string PipelineKey(string id) => $"pipeline:{id}";
    public static string TableKey(string id) => $"table:{id}";

    /// <summary>Converts a compile-derived flat output schema (pipeline/table) into the FieldDef
    /// shape DescriptorFactory consumes. Json columns carry no declared children here, so they map
    /// to schemaless google.protobuf.Struct fields.</summary>
    public static List<FieldDef> FromOutputSchema(SourceSchema schema) =>
        [.. schema.Fields.Select(f => new FieldDef(f.Key, KindToType(f.Value)))];

    public static FieldNumberMap ParseMap(string json) =>
        JsonSerializer.Deserialize<FieldNumberMap>(json)
        ?? throw new InvalidOperationException($"Invalid FieldNumberMap JSON: {json}");

    private static FieldType KindToType(FieldKind kind) => kind switch
    {
        FieldKind.String => FieldType.String,
        FieldKind.Double => FieldType.Double,
        FieldKind.Long => FieldType.Long,
        FieldKind.Bool => FieldType.Bool,
        FieldKind.Timestamp => FieldType.Timestamp,
        FieldKind.Json => FieldType.Json,
        _ => FieldType.String,
    };
}
