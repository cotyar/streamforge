using Orleans;
using StreamForge.Abstractions;
using StreamForge.Host.Grpc.Dynamic;

namespace StreamForge.Host.Api;

/// <summary>One of the two hand-authored static .proto files StreamForge ships (Protos/), returned
/// verbatim for the API Explorer's "Definition" panel when a static service is selected.</summary>
public sealed record StaticProtoDto(string Name, string Text);

/// <summary>One live-reflectable dynamic entity (source/table/pipeline), enough for the API Explorer
/// to list it, badge its status, and drive its Definition/Connect/Live-data panels without a second
/// round trip. <see cref="Id"/> is the id-or-name segment the REST proto-download routes key off —
/// same value <see cref="ProtoPath"/> is built from (source: name, table/pipeline: id).</summary>
public sealed record DynamicEntityMetaDto(
    string Kind,
    string Id,
    string Name,
    string Status,
    string EntityKey,
    string MessageName,
    string EventMessageName,
    string DeltaMessageName,
    string ProtoPath);

/// <summary>Snapshot of the whole gRPC surface: the fixed static service list plus the current
/// dynamic-entity catalog.</summary>
public sealed record GrpcMetaResponse(int GrpcPort, IReadOnlyList<string> Services, IReadOnlyList<DynamicEntityMetaDto> DynamicEntities);

/// <summary>
/// Read-only metadata endpoints backing the console's API Explorer page: raw text of the static
/// .proto files, plus a consolidated snapshot of the gRPC surface (static service names + the live
/// dynamic-entity catalog with the exact message names a client would see over reflection). Nothing
/// here mutates state — Viewer policy throughout, same as the rest of the read-only catalog surface.
/// </summary>
public static class MetaEndpoints
{
    /// <summary>Fixed list of gRPC services StreamForge always exposes — the four control-plane
    /// services in package streamforge.v1 (Protos/streamforge.proto), the generic dynamic streaming
    /// RPC in streamforge.dynamic.v1 (Protos/streamforge_dynamic.proto), and the hand-implemented
    /// grpc.reflection.v1alpha.ServerReflection (Grpc/Dynamic/DynamicReflectionService.cs) — see
    /// docs/index.html#grpc.</summary>
    private static readonly string[] StaticServiceNames =
    [
        "SourceService", "PipelineService", "TableService", "StreamService", "DynamicStreamService", "ServerReflection",
    ];

    private static readonly string[] StaticProtoFileNames = ["streamforge.proto", "streamforge_dynamic.proto"];

    public static void MapMetaEndpoints(this WebApplication app)
    {
        var group = app.MapGroup("/api/meta");

        // Raw text of the two static .proto files, resolved from the Protos/ dir at runtime the same
        // way Program.cs resolves docs/index.html (ContentRootPath-relative) — Protos/ lives directly
        // under this project (see the <Protobuf> items in StreamForge.Host.csproj), so unlike docs/
        // (two levels up, at the repo root) no ".." segments are needed here.
        group.MapGet("/protos/static", (IWebHostEnvironment env) =>
        {
            var protosDir = Path.Combine(env.ContentRootPath, "Protos");
            var result = new List<StaticProtoDto>(StaticProtoFileNames.Length);
            foreach (var name in StaticProtoFileNames)
            {
                var path = Path.Combine(protosDir, name);
                if (File.Exists(path))
                {
                    result.Add(new StaticProtoDto(name, File.ReadAllText(path)));
                }
            }

            return Results.Ok(result);
        }).RequireAuthorization("Viewer");

        // Snapshot of the whole gRPC surface for the API Explorer: the fixed static service list plus
        // every dynamic entity currently reflectable (sources always; tables with a compiled output
        // schema; pipelines whose SQL currently compiles) — built via the SAME DynamicDescriptorSet
        // reflection/.proto-download endpoints use, so this list can never disagree with what a real
        // grpcurl/reflection client sees.
        group.MapGet("/grpc", async (IClusterClient client, IConfiguration config) =>
        {
            var registry = client.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
            var sources = await registry.GetSourcesAsync();
            var tables = await registry.GetTablesAsync();
            var pipelines = await registry.GetPipelinesAsync();

            // entityKey -> (id-or-name segment used by the REST proto-download route, human status
            // label). DynamicEntityDescriptor itself carries no status, so this is looked up
            // separately from the source catalog lists rather than threaded through BuildAsync.
            var meta = new Dictionary<string, (string Id, string Status)>(StringComparer.Ordinal);
            foreach (var s in sources)
            {
                meta[EntitySchemas.SourceKey(s.Name)] = (s.Name, s.Enabled ? "Enabled" : "Disabled");
            }

            foreach (var t in tables)
            {
                meta[EntitySchemas.TableKey(t.Id)] = (t.Id, t.Status.ToString());
            }

            foreach (var p in pipelines)
            {
                meta[EntitySchemas.PipelineKey(p.Id)] = (p.Id, p.Status.ToString());
            }

            var descriptors = await new DynamicDescriptorSet(registry).BuildAsync();
            var entities = new List<DynamicEntityMetaDto>(descriptors.Count);
            foreach (var d in descriptors)
            {
                var (id, status) = meta.TryGetValue(d.EntityKey, out var m) ? m : (d.Name, "Unknown");
                var protoPath = d.Kind switch
                {
                    "source" => $"/api/sources/{Uri.EscapeDataString(d.Name)}/proto",
                    "table" => $"/api/tables/{Uri.EscapeDataString(id)}/proto",
                    "pipeline" => $"/api/pipelines/{Uri.EscapeDataString(id)}/proto",
                    _ => "",
                };
                entities.Add(new DynamicEntityMetaDto(
                    d.Kind,
                    id,
                    d.Name,
                    status,
                    d.EntityKey,
                    d.Schema.MessageName,
                    d.Schema.EventMessageName,
                    d.Schema.DeltaMessageName,
                    protoPath));
            }

            var grpcPort = config.GetValue("Grpc:Port", 5299);
            return Results.Ok(new GrpcMetaResponse(grpcPort, StaticServiceNames, entities));
        }).RequireAuthorization("Viewer");
    }
}
