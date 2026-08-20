using System.Reflection;
using System.Security.Claims;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using StreamForge.Abstractions;
using StreamForge.Api.Auth;
using StreamForge.AppCore.Discovery;
using StreamForge.AppCore.Transports;
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

        // Plan 016 wave 5: computed ONCE here (this method runs once at host startup, when the route
        // table is built, not per request) rather than inside the /instance handler below — an id that
        // changed request-to-request would defeat the entire point of a persisted instance identity, and
        // re-reading {DataDir}/instance.json on every probe is pointless I/O for a value that cannot
        // change without a restart.
        var instanceId = InstanceIdentity.LoadOrCreate(options.DataDir);
        var startedAtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        // "The API assembly's informational version" per the wave brief — this file lives in
        // StreamForge.Api, so GetExecutingAssembly() here IS that assembly, unlike ProtoFileBuilder's
        // same-shaped fallback (StreamForge.AppCore) which answers a different question.
        var assemblyVersion =
            Assembly.GetExecutingAssembly().GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion
            ?? Assembly.GetExecutingAssembly().GetName().Version?.ToString()
            ?? "dev";
        // Plan 016 wave 5: honest capability signalling — this instance serves gRPC iff it maps static
        // gRPC services at all (GrpcStaticServices is how StreamForgeApiOptions already tells the two
        // flavors apart: Orleans populates six services, the Dapr host's own Program.cs comment says
        // gRPC serving there is "phase 2" and leaves the list empty). No other capability string is
        // invented here — ponytail: grow this list one real feature-detection need at a time rather than
        // enumerating everything this build happens to support.
        var servesGrpc = options.GrpcStaticServices.Count > 0;

        // Anonymous, like /healthz — the endpoint a peer probes and an operator curls before they have
        // any credential. Must therefore leak nothing sensitive: entity COUNTS, not entity names;
        // registered kind NAMES, not connector configuration.
        group.MapGet("/instance", async (HttpRequest request, ICatalogFacade registry) =>
        {
            var endpoints = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["rest"] = $"{request.Scheme}://{request.Host}",
            };
            if (servesGrpc)
            {
                // gRPC is h2c cleartext on this flavor (see the Host Program.cs Kestrel setup) — same
                // host as the REST request, options.GrpcPort instead of the REST port. Omitted entirely
                // on a flavor that does not actually serve it (see servesGrpc above) rather than
                // reporting a port nothing is listening on.
                endpoints["grpc"] = $"{request.Scheme}://{request.Host.Host}:{options.GrpcPort}";
            }

            var capabilities = new List<string>();
            if (servesGrpc)
            {
                capabilities.Add("grpc");
            }

            var counts = new Dictionary<string, int>(StringComparer.Ordinal);
            var warnings = new List<string>();
            try
            {
                // Plan 016: this route's whole job is being answerable, so a catalog read failure must
                // not fail the response — identity/flavor/version below are still worth having with
                // empty counts rather than a 500 from the one endpoint documented as always up.
                var sources = await registry.GetSourcesAsync();
                var pipelines = await registry.GetPipelinesAsync();
                var tables = await registry.GetTablesAsync();

                counts["sources"] = sources.Count;
                counts["pipelines"] = pipelines.Count;
                counts["tables"] = tables.Count;

                CollectCatalogWarnings(sources, pipelines, tables, warnings);
            }
            catch
            {
                // Swallowed deliberately — see the comment above the try. Counts/warnings stay empty;
                // everything else in the response is unaffected.
            }

            return Results.Ok(new InstanceInfo
            {
                InstanceId = instanceId,
                Name = string.IsNullOrWhiteSpace(options.InstanceName) ? Environment.MachineName : options.InstanceName,
                Flavor = options.Flavor,
                Version = string.IsNullOrWhiteSpace(options.Version) ? assemblyVersion : options.Version,
                Endpoints = endpoints,
                Capabilities = capabilities,
                Plugins = [.. KindVersions.All().Keys],
                CatalogCounts = counts,
                CatalogWarnings = warnings,
                StartedAtMs = startedAtMs,
            });
        }).AllowAnonymous();

        // Viewer + AccessGuard(catalog.read, *) — same two-gate pattern every other route in this file
        // uses. A directory listing is read-only catalog metadata about this instance's configuration,
        // not about any one entity, so catalog.read at * is the right fit (see the class doc's note on
        // why these three pre-existing routes fold onto catalog.read rather than an invented meta.read).
        group.MapGet("/peers", async (ClaimsPrincipal principal, AccessGuard guard) =>
        {
            if (await RefuseAsync(guard, principal) is { } refusal)
            {
                return refusal;
            }

            return Results.Ok(PeerDirectory.All());
        }).RequireAuthorization("Viewer");

        // Gated at least as strictly as the read above, even though it is nominally a GET-shaped action
        // over HTTP POST: probing writes the outcome into PeerDirectory, a process-wide registry, so it
        // is a mutation in the honest sense. catalog.read (not catalog.write) because what it MUTATES is
        // this instance's own bookkeeping about a peer, not the peer or this instance's catalog — the
        // same action a caller already needed to LIST peers, which is the operation this augments.
        group.MapPost("/peers/{name}/probe", async (string name, ClaimsPrincipal principal, AccessGuard guard) =>
        {
            if (await RefuseAsync(guard, principal) is { } refusal)
            {
                return refusal;
            }

            var peer = PeerDirectory.Find(name);
            if (peer is null)
            {
                return Results.NotFound(new { error = $"no peer named '{name}' is configured" });
            }

            await PeerProbe.ProbeAsync(peer);
            return Results.Ok(PeerDirectory.Find(name));
        }).RequireAuthorization("Viewer");

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

    /// <summary>Plan 016's three <c>GET /api/meta/instance</c> catalogWarnings, computed cheaply from the
    /// three catalog lists this route already reads — no extra facade calls, no re-derivation of anything
    /// a wave 2/3 agent already maintains.
    ///
    /// <para><b>Counts and kind names, never entity names.</b> This route is ANONYMOUS, and the rule it
    /// states for itself two screens up is "entity COUNTS, not entity names". A warning reading
    /// <c>pipeline 'fx_desk_pnl' pin is stale</c> would hand an unauthenticated caller the catalog's
    /// contents through the back door — the thing <c>GET /api/pipelines</c> requires a Viewer grant for.
    /// So each condition is reported as a count, and the operator who needs to know WHICH entity reads it
    /// off the catalog routes they are already authorised for. A kind name is not redacted: it names a
    /// connector type, not a business entity, and it is the actionable half of that particular
    /// warning.</para>
    ///
    /// <list type="bullet">
    /// <item>Duplicate pipeline names: pipelines are the one entity NOT unique-checked at the write path
    /// (that guard is against sources+tables only — see the plan's rename-policy section), so this is
    /// the one live symptom worth surfacing.</item>
    /// <item>Broken pins: <see cref="PipelineDefinition.StaleReason"/> / <see cref="TableDefinition.StaleReason"/>
    /// are already maintained by the wave-2 recompile-on-upstream-change path; this reads them, it does
    /// not recompute anything.</item>
    /// <item>Entities referencing an unregistered kind: every <see cref="SourceDefinition.Kind"/> plus
    /// every <see cref="SinkSpec.Kind"/> on every pipeline/table's <c>Sinks</c> list, checked against
    /// <see cref="KindVersions.All"/> — the same live registry snapshot <c>ConfigImportService</c>'s
    /// plugin-requirement gate uses, so an entity counted here is counted for the identical reason an
    /// import of it would be refused.</item>
    /// </list></summary>
    private static void CollectCatalogWarnings(
        List<SourceDefinition> sources, List<PipelineDefinition> pipelines, List<TableDefinition> tables, List<string> warnings)
    {
        var duplicated = pipelines.GroupBy(p => p.Name, StringComparer.Ordinal).Where(g => g.Count() > 1).ToList();
        if (duplicated.Count > 0)
        {
            warnings.Add(
                $"{duplicated.Count} pipeline name(s) are used by more than one pipeline " +
                $"({duplicated.Sum(g => g.Count())} pipelines affected)");
        }

        var stalePipelines = pipelines.Count(p => !string.IsNullOrEmpty(p.StaleReason));
        if (stalePipelines > 0)
        {
            warnings.Add($"{stalePipelines} pipeline(s) have a stale pin");
        }

        var staleTables = tables.Count(t => !string.IsNullOrEmpty(t.StaleReason));
        if (staleTables > 0)
        {
            warnings.Add($"{staleTables} table(s) have a stale pin");
        }

        // One line per unrecognised kind rather than per entity: the count is what an operator acts on,
        // and it bounds this list at the number of distinct kinds no matter how large the catalog is.
        var knownKinds = KindVersions.All();
        var unregistered = new Dictionary<string, int>(StringComparer.Ordinal);
        void Note(string? kind)
        {
            if (!string.IsNullOrEmpty(kind) && !knownKinds.ContainsKey(kind))
            {
                unregistered[kind] = unregistered.GetValueOrDefault(kind) + 1;
            }
        }

        foreach (var s in sources)
        {
            Note(s.Kind);
        }

        foreach (var sink in pipelines.SelectMany(p => p.Sinks).Concat(tables.SelectMany(t => t.Sinks)))
        {
            Note(sink.Kind);
        }

        foreach (var (kind, count) in unregistered.OrderBy(kv => kv.Key, StringComparer.Ordinal))
        {
            warnings.Add($"{count} entit(ies) use kind '{kind}', which this instance has no connector registered for");
        }
    }
}
