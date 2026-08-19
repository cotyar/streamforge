using System.Security.Claims;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using StreamForge.Abstractions;
using StreamForge.Api.Auth;
using StreamForge.Host.Grpc.Dynamic;

namespace StreamForge.Api;

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
/// Plan 005 (Dapr sibling runtime) W3: static protos directory, gRPC port, and the static gRPC
/// service list are host-specific facts carried by <see cref="StreamForgeApiOptions"/> rather than
/// resolved from IWebHostEnvironment/IConfiguration/IClusterClient directly, so this file is
/// identical on both runtimes.
///
/// <para>Plan 015 wave 3-A. Two gates on every route, the pattern <c>AccessEndpoints</c> established:
/// the <c>Viewer</c> policy stays as the compatibility floor, and each handler additionally asks
/// <see cref="AccessGuard"/> for <see cref="Actions.CatalogRead"/> at <c>*</c>. These three routes are
/// PLATFORM metadata — the .proto text this build ships, the gRPC surface, the live arrangement set —
/// so they fold onto <c>catalog.read</c> rather than getting an invented <c>meta.read</c> (wave 1 made
/// that call; see <c>BuiltInRoleCatalog</c>'s class doc). <c>*</c> is the right scope for the same
/// reason: none of the three is about one entity. <c>/grpc</c> does enumerate the catalog, and its
/// entity list is deliberately NOT filtered per entitlement — see the note on that handler.</para>
/// </summary>
public static class MetaEndpoints
{
    private static readonly string[] StaticProtoFileNames = ["streamforge.proto", "streamforge_dynamic.proto"];

    public static void MapMetaEndpoints(this WebApplication app, StreamForgeApiOptions options)
    {
        var group = app.MapGroup("/api/meta");

        // Raw text of the two static .proto files, resolved from options.ProtosDir (host-specific —
        // Protos/ lives directly under the Orleans host project; a future Dapr host can point
        // elsewhere or supply an empty directory).
        group.MapGet("/protos/static", async (ClaimsPrincipal principal, AccessGuard guard) =>
        {
            if (await RefuseAsync(guard, principal) is { } refusal)
            {
                return refusal;
            }

            var result = new List<StaticProtoDto>(StaticProtoFileNames.Length);
            foreach (var name in StaticProtoFileNames)
            {
                var path = Path.Combine(options.ProtosDir, name);
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
        //
        // Plan 015 wave 3-A: the entity list here is NOT filtered per-entitlement, unlike the three
        // catalog LIST routes. Deliberate, and the reason is what this list is for: it is the API
        // Explorer's map of the gRPC surface, whose entries are the reflectable message names a
        // grpcurl client already sees — a caller who reaches the gRPC port learns them from reflection
        // whether or not this REST route repeated them. Filtering here would cost a per-entity guard
        // call on a page-load route and hide nothing that is actually hidden. The per-entity DATA
        // behind each entry is guarded where it is served, which is the /api/{kind}/{id}/... routes.
        group.MapGet("/grpc", async (ClaimsPrincipal principal, AccessGuard guard, ICatalogFacade registry) =>
        {
            if (await RefuseAsync(guard, principal) is { } refusal)
            {
                return refusal;
            }

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

            return Results.Ok(new GrpcMetaResponse(options.GrpcPort, options.GrpcStaticServices, entities));
        }).RequireAuthorization("Viewer");

        // Plan 003 M3 / plan 005 W3: partitioned execution (and therefore shared arrangements) is
        // Orleans-only (decision D-F) — the arrangement-set derivation (recompile each Running
        // Parallelism>=2 table's dataflow, walk ArrangeableExternalEdges, query the live
        // IArrangementGrain set each one resolves to) now lives entirely behind IArrangementMetaFacade
        // (Host-side: Facades/OrleansFacades.cs); a future Dapr facade always returns an empty list.
        group.MapGet("/arrangements", async (ClaimsPrincipal principal, AccessGuard guard, IArrangementMetaFacade arrangements) =>
        {
            if (await RefuseAsync(guard, principal) is { } refusal)
            {
                return refusal;
            }

            return Results.Ok(await arrangements.GetArrangementsAsync());
        }).RequireAuthorization("Viewer");
    }

    /// <summary>Null when the caller may proceed; the ready-made 403 when they may not. Same shape as
    /// <c>AccessEndpoints.RefuseAsync</c>, and — like it — a
    /// <see cref="AccessDecision.RequiresApproval"/> answer is refused here too rather than being
    /// treated as a yes: filing the request is waves 4-5' job and the machinery does not exist yet.
    /// Nothing under <c>/api/meta</c> is a plausible approval subject anyway; the action and the scope
    /// are fixed, so this helper takes neither.</summary>
    private static async Task<IResult?> RefuseAsync(AccessGuard guard, ClaimsPrincipal principal)
    {
        var result = await guard.CheckAsync(principal, Actions.CatalogRead, "*");
        return result.IsAllowed ? null : AccessGuard.Deny(result);
    }
}
