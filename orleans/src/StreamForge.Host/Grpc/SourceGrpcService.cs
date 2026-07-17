using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.AspNetCore.Authorization;
using Orleans;
using StreamForge.Abstractions;
using V1 = StreamForge.Host.Grpc.V1;

namespace StreamForge.Host.Grpc;

/// <summary>gRPC control-plane mirror of /api/sources — same validation and grain calls as
/// StreamForge.Host.Api.SourcesEndpoints, translated to gRPC status codes (InvalidArgument for
/// the REST 400s, NotFound for the REST 404s).</summary>
public sealed class SourceGrpcService(IClusterClient client) : V1.SourceService.SourceServiceBase
{
    private IRegistryGrain Registry => client.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);

    [Authorize(Policy = "Viewer")]
    public override async Task<V1.ListSourcesResponse> List(Empty request, ServerCallContext context)
    {
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
        if (await registry.GetSourceAsync(request.Name) is null)
        {
            throw new RpcException(new Status(StatusCode.NotFound, $"source '{request.Name}' not found"));
        }

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
        var removed = await Registry.DeleteSourceAsync(request.Name);
        if (!removed)
        {
            throw new RpcException(new Status(StatusCode.NotFound, $"source '{request.Name}' not found"));
        }

        return new Empty();
    }
}
