using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.AspNetCore.Authorization;
using Orleans;
using StreamForge.Abstractions;
using StreamForge.Engine;
using V1 = StreamForge.Host.Grpc.V1;

namespace StreamForge.Host.Grpc;

/// <summary>gRPC control-plane mirror of /api/pipelines — see StreamForge.Host.Api.PipelinesEndpoints
/// for the REST semantics this reproduces.</summary>
public sealed class PipelineGrpcService(IClusterClient client) : V1.PipelineService.PipelineServiceBase
{
    private IRegistryGrain Registry => client.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);

    [Authorize(Policy = "Viewer")]
    public override async Task<V1.ListPipelinesResponse> List(Empty request, ServerCallContext context)
    {
        var pipelines = await Registry.GetPipelinesAsync();
        var response = new V1.ListPipelinesResponse();
        response.Pipelines.AddRange(pipelines.Select(ProtoMappers.ToProto));
        return response;
    }

    [Authorize(Policy = "Viewer")]
    public override async Task<V1.PipelineDefinition> Get(V1.GetPipelineRequest request, ServerCallContext context)
    {
        var p = await Registry.GetPipelineAsync(request.Id);
        if (p is null)
        {
            throw new RpcException(new Status(StatusCode.NotFound, $"pipeline '{request.Id}' not found"));
        }

        return ProtoMappers.ToProto(p);
    }

    [Authorize(Policy = "Editor")]
    public override async Task<V1.PipelineDefinition> Create(V1.CreatePipelineRequest request, ServerCallContext context)
    {
        if (string.IsNullOrWhiteSpace(request.Name) || string.IsNullOrWhiteSpace(request.Sql))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "name and sql are required"));
        }

        var registry = Registry;

        // Compile-check for diagnostics; draft-friendly — never blocks creation beyond the empty check above.
        var schemas = await SchemaBuilder.BuildStreamSchemasAsync(registry);
        _ = SqlCompiler.Compile(request.Sql, schemas);

        var def = new PipelineDefinition
        {
            Name = request.Name,
            Description = request.Description,
            Sql = request.Sql,
            CreatedBy = context.GetHttpContext().User.Identity?.Name ?? "",
        };
        var created = await registry.CreatePipelineAsync(def);
        return ProtoMappers.ToProto(created);
    }

    [Authorize(Policy = "Editor")]
    public override async Task<V1.PipelineDefinition> Update(V1.UpdatePipelineRequest request, ServerCallContext context)
    {
        var registry = Registry;
        var existing = await registry.GetPipelineAsync(request.Id);
        if (existing is null)
        {
            throw new RpcException(new Status(StatusCode.NotFound, $"pipeline '{request.Id}' not found"));
        }

        existing.Name = request.Name;
        existing.Description = request.Description;
        existing.Sql = request.Sql;
        var updated = await registry.UpdatePipelineAsync(existing);
        if (updated is null)
        {
            throw new RpcException(new Status(StatusCode.NotFound, $"pipeline '{request.Id}' not found"));
        }

        return ProtoMappers.ToProto(updated);
    }

    [Authorize(Policy = "Editor")]
    public override async Task<Empty> Delete(V1.DeletePipelineRequest request, ServerCallContext context)
    {
        var removed = await Registry.DeletePipelineAsync(request.Id);
        if (!removed)
        {
            throw new RpcException(new Status(StatusCode.NotFound, $"pipeline '{request.Id}' not found"));
        }

        return new Empty();
    }

    [Authorize(Policy = "Editor")]
    public override async Task<V1.PipelineDefinition> Start(V1.StartPipelineRequest request, ServerCallContext context)
    {
        var updated = await Registry.SetPipelineStatusAsync(request.Id, PipelineStatus.Running);
        if (updated is null)
        {
            throw new RpcException(new Status(StatusCode.NotFound, $"pipeline '{request.Id}' not found"));
        }

        return ProtoMappers.ToProto(updated);
    }

    [Authorize(Policy = "Editor")]
    public override async Task<V1.PipelineDefinition> Stop(V1.StopPipelineRequest request, ServerCallContext context)
    {
        var updated = await Registry.SetPipelineStatusAsync(request.Id, PipelineStatus.Stopped);
        if (updated is null)
        {
            throw new RpcException(new Status(StatusCode.NotFound, $"pipeline '{request.Id}' not found"));
        }

        return ProtoMappers.ToProto(updated);
    }

    [Authorize(Policy = "Editor")]
    public override async Task<V1.ValidateResponse> Validate(V1.ValidateRequest request, ServerCallContext context)
    {
        var schemas = await SchemaBuilder.BuildStreamSchemasAsync(Registry);
        var result = SqlCompiler.Compile(request.Sql, schemas);
        return ProtoMappers.ToProtoValidateResponse(result);
    }
}
