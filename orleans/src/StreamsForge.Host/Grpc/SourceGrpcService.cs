using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.AspNetCore.Authorization;
using Orleans;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Environments;
using StreamsForge.Host.Facades;
using StreamsForge.Api.Auth;
using V1 = StreamsForge.Host.Grpc.V1;

namespace StreamsForge.Host.Grpc;

/// <summary>gRPC control-plane mirror of /api/sources — same validation and grain calls as
/// StreamsForge.Host.Api.SourcesEndpoints, translated to gRPC status codes (InvalidArgument for
/// the REST 400s, NotFound for the REST 404s).
///
/// <para>Plan 015 wave 3-B: every method keeps its <c>[Authorize(Policy = …)]</c> — that attribute is the
/// compatibility floor and removing one would be a behaviour change nobody asked for — and additionally
/// asks <see cref="AccessGuard"/> for the SAME action the REST twin asks for, at the entity the method
/// operates on. A grant written for the console therefore means the same thing over gRPC.</para>
///
/// <para><b>Why the entity is fetched before the check on Get/Update/Delete.</b> A <c>tag:finance</c>
/// entitlement can only be evaluated against the entity's <c>Tags</c>, and the only way to have them is
/// to read the definition first. The cost is that an unentitled caller who already passed the Viewer or
/// Editor floor learns whether a name exists (NotFound before PermissionDenied). That is the deliberate
/// trade: existence of a name, to a caller who is already authenticated into the cluster, against
/// tag-scoped entitlements working at all.</para></summary>
public sealed class SourceGrpcService(IClusterClient client, AccessGuard guard) : V1.SourceService.SourceServiceBase
{
    // Plan 021 D4 — a facade/gRPC service answering one request reads the ambient.
    private IRegistryGrain Registry => client.RegistryFor(EnvironmentAmbient.Current);

    [Authorize(Policy = "Viewer")]
    public override async Task<V1.ListSourcesResponse> List(Empty request, ServerCallContext context)
    {
        // Scope "*": a list has no single resource, and asking with "*" is answered only by a
        // "*"-scoped grant — the same reading REST's GET /api/sources uses. Filtering the response down
        // to the entities the caller may see is a different feature (and a different plan).
        await GrpcAccess.EnsureAsync(guard, context, Actions.SourceRead, "*");

        var sources = await Registry.GetSourcesAsync();
        var response = new V1.ListSourcesResponse();
        response.Sources.AddRange(sources.Select(ProtoMappers.ToProto));
        return response;
    }

    [Authorize(Policy = "Viewer")]
    public override async Task<V1.SourceDefinition> Get(V1.GetSourceRequest request, ServerCallContext context)
    {
        var src = await Registry.GetSourceAsync(request.Name);
        if (src is null)
        {
            throw new RpcException(new Status(StatusCode.NotFound, $"source '{request.Name}' not found"));
        }

        await GrpcAccess.EnsureAsync(guard, context, Actions.SourceRead, request.Name, src.Tags);

        return ProtoMappers.ToProto(src);
    }

    [Authorize(Policy = "Editor")]
    public override async Task<V1.SourceDefinition> Create(V1.SourceDefinition request, ServerCallContext context)
    {
        var def = ProtoMappers.FromProto(request);

        if (string.IsNullOrWhiteSpace(def.Name))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "name is required"));
        }

        // Checked after the name validation and before anything else: the scope IS the name, and a check
        // at scope "" would be meaningless. ponytail: no tags — the proto SourceDefinition carries no
        // tags field (see Protos/streamsforge.proto), so a source created over gRPC has none to match on
        // anyway. Ceiling: `tag:` grants cannot authorize a gRPC create. Upgrade path is one repeated
        // string on the proto message plus one line in ProtoMappers, which is additive and safe whenever
        // somebody actually wants it.
        await GrpcAccess.EnsureAsync(guard, context, Actions.SourceWrite, def.Name);

        if (def.Fields.Count == 0)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "at least one field is required"));
        }

        if (def.EventsPerSecond <= 0)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "eventsPerSecond must be > 0"));
        }

        var registry = Registry;
        if (await registry.GetSourceAsync(def.Name) is not null)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "source name already exists"));
        }

        await registry.UpsertSourceAsync(def);
        return ProtoMappers.ToProto(def);
    }

    [Authorize(Policy = "Editor")]
    public override async Task<V1.SourceDefinition> Update(V1.UpdateSourceRequest request, ServerCallContext context)
    {
        var registry = Registry;
        var existing = await registry.GetSourceAsync(request.Name);
        if (existing is null)
        {
            throw new RpcException(new Status(StatusCode.NotFound, $"source '{request.Name}' not found"));
        }

        // The EXISTING entity's tags, not the incoming body's: the caller must be entitled to the thing
        // as it stands, or editing the tags would be the way to escape a tag-scoped entitlement.
        await GrpcAccess.EnsureAsync(guard, context, Actions.SourceWrite, request.Name, existing.Tags);

        var def = ProtoMappers.FromProto(request.Definition);

        if (def.Fields.Count == 0)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "at least one field is required"));
        }

        if (def.EventsPerSecond <= 0)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "eventsPerSecond must be > 0"));
        }

        def.Name = request.Name;
        await registry.UpsertSourceAsync(def);
        return ProtoMappers.ToProto(def);
    }

    [Authorize(Policy = "Editor")]
    public override async Task<Empty> Delete(V1.DeleteSourceRequest request, ServerCallContext context)
    {
        var registry = Registry;
        // One extra read before the delete, purely so the tags exist to check against. A delete is rare
        // and already writes the whole registry, so the read is not worth avoiding.
        var existing = await registry.GetSourceAsync(request.Name);
        if (existing is null)
        {
            throw new RpcException(new Status(StatusCode.NotFound, $"source '{request.Name}' not found"));
        }

        await GrpcAccess.EnsureAsync(guard, context, Actions.SourceDelete, request.Name, existing.Tags);

        var removed = await registry.DeleteSourceAsync(request.Name);
        if (!removed)
        {
            throw new RpcException(new Status(StatusCode.NotFound, $"source '{request.Name}' not found"));
        }

        return new Empty();
    }
}
