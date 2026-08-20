using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.AspNetCore.Authorization;
using Orleans;
using StreamForge.Abstractions;
using StreamForge.AppCore.Environments;
using StreamForge.Host.Facades;
using StreamForge.Api.Auth;
using StreamForge.Engine;
using V1 = StreamForge.Host.Grpc.V1;

namespace StreamForge.Host.Grpc;

/// <summary>gRPC control-plane mirror of /api/pipelines — see StreamForge.Host.Api.PipelinesEndpoints
/// for the REST semantics this reproduces.
///
/// <para>Plan 015 wave 3-B: the <c>[Authorize(Policy = …)]</c> attributes stay as the compatibility
/// floor; each method additionally asks <see cref="AccessGuard"/> for the same action its REST twin asks
/// for, at the pipeline it operates on and with that pipeline's <c>Tags</c>. See
/// <see cref="SourceGrpcService"/>'s class doc for why the entity is read before the check.</para></summary>
public sealed class PipelineGrpcService(IClusterClient client, AccessGuard guard) : V1.PipelineService.PipelineServiceBase
{
    // Plan 021 D4 — a facade/gRPC service answering one request reads the ambient.
    private IRegistryGrain Registry => client.RegistryFor(EnvironmentAmbient.Current);

    [Authorize(Policy = "Viewer")]
    public override async Task<V1.ListPipelinesResponse> List(Empty request, ServerCallContext context)
    {
        await GrpcAccess.EnsureAsync(guard, context, Actions.PipelineRead, "*");

        var pipelines = await Registry.GetPipelinesAsync();
        var response = new V1.ListPipelinesResponse();
        response.Pipelines.AddRange(pipelines.Select(ProtoMappers.ToProto));
        return response;
    }

    [Authorize(Policy = "Viewer")]
    public override async Task<V1.PipelineDefinition> Get(V1.GetPipelineRequest request, ServerCallContext context)
    {
        // Plan 016 wave 1: read RPCs take an id OR a name, through the one resolver.
        var p = await GrpcEntityRef.RequireAsync(
            await GrpcEntityRef.PipelineAsync(Registry, request.Id), guard, context, Actions.PipelineRead);

        await GrpcAccess.EnsureAsync(guard, context, Actions.PipelineRead, p.Name, p.Tags);

        return ProtoMappers.ToProto(p);
    }

    [Authorize(Policy = "Editor")]
    public override async Task<V1.PipelineDefinition> Create(V1.CreatePipelineRequest request, ServerCallContext context)
    {
        if (string.IsNullOrWhiteSpace(request.Name) || string.IsNullOrWhiteSpace(request.Sql))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "name and sql are required"));
        }

        // Scoped by NAME, not by id: the id does not exist until the registry mints one, so the name is
        // the only thing an entitlement could have been written against. ponytail: no tags — see
        // SourceGrpcService.Create's note; CreatePipelineRequest carries none either.
        await GrpcAccess.EnsureAsync(guard, context, Actions.PipelineWrite, request.Name);

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

        await GrpcAccess.EnsureAsync(guard, context, Actions.PipelineWrite, existing.Name, existing.Tags);

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
        var registry = Registry;
        var existing = await registry.GetPipelineAsync(request.Id);
        if (existing is null)
        {
            throw new RpcException(new Status(StatusCode.NotFound, $"pipeline '{request.Id}' not found"));
        }

        await GrpcAccess.EnsureAsync(guard, context, Actions.PipelineDelete, existing.Name, existing.Tags);

        var removed = await registry.DeletePipelineAsync(request.Id);
        if (!removed)
        {
            throw new RpcException(new Status(StatusCode.NotFound, $"pipeline '{request.Id}' not found"));
        }

        return new Empty();
    }

    [Authorize(Policy = "Editor")]
    public override async Task<V1.PipelineDefinition> Start(V1.StartPipelineRequest request, ServerCallContext context)
        => await SetStatusAsync(request.Id, PipelineStatus.Running, context);

    [Authorize(Policy = "Editor")]
    public override async Task<V1.PipelineDefinition> Stop(V1.StopPipelineRequest request, ServerCallContext context)
        => await SetStatusAsync(request.Id, PipelineStatus.Stopped, context);

    [Authorize(Policy = "Editor")]
    public override async Task<V1.ValidateResponse> Validate(V1.ValidateRequest request, ServerCallContext context)
    {
        // Scope "*": validating SQL touches no pipeline — it is the gRPC twin of
        // POST /api/pipelines/validate, which the wave-1 equivalence matrix also pins at "*".
        await GrpcAccess.EnsureAsync(guard, context, Actions.PipelineWrite, "*");

        var schemas = await SchemaBuilder.BuildStreamSchemasAsync(Registry);
        var result = SqlCompiler.Compile(request.Sql, schemas);
        return ProtoMappers.ToProtoValidateResponse(result);
    }

    /// <summary>Start and Stop differ only in the status they set and are otherwise the same four lines
    /// — one read for the tags, one entitlement check at <see cref="Actions.PipelineControl"/>, one
    /// grain call, one 404.</summary>
    private async Task<V1.PipelineDefinition> SetStatusAsync(string id, PipelineStatus status, ServerCallContext context)
    {
        var registry = Registry;
        var existing = await registry.GetPipelineAsync(id);
        if (existing is null)
        {
            throw new RpcException(new Status(StatusCode.NotFound, $"pipeline '{id}' not found"));
        }

        await GrpcAccess.EnsureAsync(guard, context, Actions.PipelineControl, existing.Name, existing.Tags);

        var updated = await registry.SetPipelineStatusAsync(id, status);
        if (updated is null)
        {
            throw new RpcException(new Status(StatusCode.NotFound, $"pipeline '{id}' not found"));
        }

        return ProtoMappers.ToProto(updated);
    }
}
