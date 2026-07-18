using Orleans;
using StreamForge.Abstractions;
using StreamForge.Engine.Dataflow;
using StreamForge.Host.Grains;
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

/// <summary>Plan 003 M3: one live shared arrangement SET (one per distinct (inputName, keySpec,
/// partitionCount) currently attached by at least one Running table) — <see cref="Consumers"/>/
/// <see cref="TotalRows"/> are summed across every partition.</summary>
public sealed record ArrangementMetaDto(string InputName, string KeySpec, int Partitions, int Consumers, long TotalRows);

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

        // Plan 003 M3: no separate arrangement directory grain exists — deterministically re-derived at
        // query time, the same "recompile-per-grain" philosophy M2's grain topology already relies on (see
        // GrainInterfaces.cs's M2 design note): for every Running Parallelism&gt;=2 table, recompile its
        // dataflow, walk its ArrangeableExternalEdges, and query the live ArrangementGrain set each one
        // resolves to (same input+keySpec+partitionCount ⇒ same grain keys any OTHER table attached to the
        // same shared set would also resolve to — deduplicated here via `seen`). A table that fails to
        // (re)compile (e.g. mid-edit) is skipped, best-effort, like the /grpc endpoint above.
        group.MapGet("/arrangements", async (IClusterClient client) =>
        {
            var registry = client.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
            var tables = await registry.GetTablesAsync();
            var running = tables.Where(t => t.Status == PipelineStatus.Running && t.Parallelism > 1).ToList();

            var seen = new HashSet<string>(StringComparer.Ordinal);
            var result = new List<ArrangementMetaDto>();

            foreach (var def in running)
            {
                TableDataflowPlan dataflow;
                try
                {
                    (_, dataflow) = await TableDataflowFactory.BuildAsync(client, def);
                }
                catch
                {
                    continue; // best-effort — a table that doesn't currently compile just isn't reported
                }

                foreach (var edge in dataflow.ArrangeableExternalEdges)
                {
                    var inputName = dataflow.ExternalInputNameOf(edge);
                    var keySpec = dataflow.KeySpecOf(edge);
                    var hash = ArrangementKeySpec.HashOf(keySpec);
                    int pcount = dataflow.PartitionCountOf(edge.ToStageId);
                    var setKey = $"{inputName}|{hash}|{pcount}";
                    if (!seen.Add(setKey))
                    {
                        continue;
                    }

                    var infos = new List<ArrangementInfo>(pcount);
                    for (int p = 0; p < pcount; p++)
                    {
                        var key = $"{inputName}:{hash}:{p}";
                        infos.Add(await client.GetGrain<IArrangementGrain>(key).GetInfoAsync());
                    }

                    if (infos.All(i => i.ConsumerCount == 0))
                    {
                        continue; // structurally arrangeable but nothing currently attached — not "live"
                    }

                    result.Add(new ArrangementMetaDto(
                        InputName: inputName,
                        KeySpec: keySpec,
                        Partitions: pcount,
                        // Every attaching table attaches ALL P partitions (one consumer id per partition —
                        // see TableGrain.StartCoordinatorAsync's attach loop), so ConsumerCount is uniform
                        // across an arrangement set's partitions; Max is a defensive read (vs. Sum, which
                        // would misleadingly scale with P) in case of a transient in-flight attach/detach.
                        Consumers: infos.Count > 0 ? infos.Max(i => i.ConsumerCount) : 0,
                        TotalRows: infos.Sum(i => i.RowCount)));
                }
            }

            return Results.Ok(result);
        }).RequireAuthorization("Viewer");
    }
}
