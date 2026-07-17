using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.AspNetCore.Authorization;
using Orleans;
using StreamForge.Abstractions;
using StreamForge.Engine;
using V1 = StreamForge.Host.Grpc.V1;

namespace StreamForge.Host.Grpc;

/// <summary>gRPC control-plane mirror of /api/tables — see StreamForge.Host.Api.TablesEndpoints for
/// the REST semantics this reproduces. InvalidOperationException from the registry (REST 409
/// Conflict) maps to FailedPrecondition here.</summary>
public sealed class TableGrpcService(IClusterClient client) : V1.TableService.TableServiceBase
{
    private IRegistryGrain Registry => client.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);

    [Authorize(Policy = "Viewer")]
    public override async Task<V1.ListTablesResponse> List(Empty request, ServerCallContext context)
    {
        var tables = await Registry.GetTablesAsync();
        var response = new V1.ListTablesResponse();
        response.Tables.AddRange(tables.Select(ProtoMappers.ToProto));
        return response;
    }

    [Authorize(Policy = "Viewer")]
    public override async Task<V1.TableDefinition> Get(V1.GetTableRequest request, ServerCallContext context)
    {
        var t = await Registry.GetTableAsync(request.Id);
        if (t is null)
        {
            throw new RpcException(new Status(StatusCode.NotFound, $"table '{request.Id}' not found"));
        }

        return ProtoMappers.ToProto(t);
    }

    [Authorize(Policy = "Editor")]
    public override async Task<V1.TableDefinition> Create(V1.CreateTableRequest request, ServerCallContext context)
    {
        if (string.IsNullOrWhiteSpace(request.Name) || string.IsNullOrWhiteSpace(request.Sql))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "name and sql are required"));
        }

        try
        {
            var def = new TableDefinition
            {
                Name = request.Name,
                Description = request.Description,
                Sql = request.Sql,
                CreatedBy = context.GetHttpContext().User.Identity?.Name ?? "",
                SearchEnabled = request.SearchEnabled,
                SearchMode = ProtoMappers.FromProto(request.SearchMode),
            };
            var created = await Registry.CreateTableAsync(def);
            return ProtoMappers.ToProto(created);
        }
        catch (InvalidOperationException ex)
        {
            throw new RpcException(new Status(StatusCode.FailedPrecondition, ex.Message));
        }
    }

    [Authorize(Policy = "Editor")]
    public override async Task<V1.TableDefinition> Update(V1.UpdateTableRequest request, ServerCallContext context)
    {
        var registry = Registry;
        var existing = await registry.GetTableAsync(request.Id);
        if (existing is null)
        {
            throw new RpcException(new Status(StatusCode.NotFound, $"table '{request.Id}' not found"));
        }

        existing.Name = request.Name;
        existing.Description = request.Description;
        existing.Sql = request.Sql;
        existing.SearchEnabled = request.SearchEnabled;
        existing.SearchMode = ProtoMappers.FromProto(request.SearchMode);

        try
        {
            var updated = await registry.UpdateTableAsync(existing);
            if (updated is null)
            {
                throw new RpcException(new Status(StatusCode.NotFound, $"table '{request.Id}' not found"));
            }

            return ProtoMappers.ToProto(updated);
        }
        catch (InvalidOperationException ex)
        {
            throw new RpcException(new Status(StatusCode.FailedPrecondition, ex.Message));
        }
    }

    [Authorize(Policy = "Editor")]
    public override async Task<Empty> Delete(V1.DeleteTableRequest request, ServerCallContext context)
    {
        try
        {
            var removed = await Registry.DeleteTableAsync(request.Id);
            if (!removed)
            {
                throw new RpcException(new Status(StatusCode.NotFound, $"table '{request.Id}' not found"));
            }

            return new Empty();
        }
        catch (InvalidOperationException ex)
        {
            throw new RpcException(new Status(StatusCode.FailedPrecondition, ex.Message));
        }
    }

    [Authorize(Policy = "Editor")]
    public override async Task<V1.TableDefinition> Start(V1.StartTableRequest request, ServerCallContext context)
    {
        try
        {
            var updated = await Registry.SetTableStatusAsync(request.Id, PipelineStatus.Running);
            if (updated is null)
            {
                throw new RpcException(new Status(StatusCode.NotFound, $"table '{request.Id}' not found"));
            }

            return ProtoMappers.ToProto(updated);
        }
        catch (InvalidOperationException ex)
        {
            throw new RpcException(new Status(StatusCode.FailedPrecondition, ex.Message));
        }
    }

    [Authorize(Policy = "Editor")]
    public override async Task<V1.TableDefinition> Stop(V1.StopTableRequest request, ServerCallContext context)
    {
        try
        {
            var updated = await Registry.SetTableStatusAsync(request.Id, PipelineStatus.Stopped);
            if (updated is null)
            {
                throw new RpcException(new Status(StatusCode.NotFound, $"table '{request.Id}' not found"));
            }

            return ProtoMappers.ToProto(updated);
        }
        catch (InvalidOperationException ex)
        {
            throw new RpcException(new Status(StatusCode.FailedPrecondition, ex.Message));
        }
    }

    [Authorize(Policy = "Editor")]
    public override async Task<V1.ValidateTableResponse> Validate(V1.ValidateRequest request, ServerCallContext context)
    {
        var registry = Registry;
        var streamSchemas = await SchemaBuilder.BuildStreamSchemasAsync(registry);
        var tableSchemas = await SchemaBuilder.BuildTableSchemasAsync(registry);
        var result = SqlCompiler.CompileTable(request.Sql, streamSchemas, tableSchemas);
        return ProtoMappers.ToProtoValidateTableResponse(result);
    }

    [Authorize(Policy = "Viewer")]
    public override async Task<V1.TableRowsResponse> Rows(V1.GetTableRowsRequest request, ServerCallContext context)
    {
        var def = await Registry.GetTableAsync(request.Id);
        if (def is null)
        {
            throw new RpcException(new Status(StatusCode.NotFound, $"table '{request.Id}' not found"));
        }

        var grain = client.GetGrain<ITableGrain>(def.Name);
        var limit = request.Limit > 0 ? request.Limit : 100;
        var rows = await grain.GetRowsAsync(limit, request.Offset);
        var total = await grain.GetRowCountAsync();
        var seq = await grain.GetSeqAsync();

        var response = new V1.TableRowsResponse { TotalRows = total, Seq = seq };
        response.Rows.AddRange(rows.Select(ProtoMappers.ToProto));
        return response;
    }

    [Authorize(Policy = "Viewer")]
    public override async Task<V1.SearchTableResponse> Search(V1.SearchTableRequest request, ServerCallContext context)
    {
        var def = await Registry.GetTableAsync(request.Id);
        if (def is null)
        {
            throw new RpcException(new Status(StatusCode.NotFound, $"table '{request.Id}' not found"));
        }

        if (!def.SearchEnabled)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "Search is not enabled for this table."));
        }

        var limit = request.Limit > 0 ? request.Limit : 100;
        List<TableRowDto> rows = string.IsNullOrWhiteSpace(request.Query)
            ? []
            : await client.GetGrain<ITableGrain>(def.Name).SearchAsync(request.Query, limit);

        var response = new V1.SearchTableResponse
        {
            Mode = ProtoMappers.ToProto(def.SearchMode),
            Enabled = def.SearchEnabled,
            Total = rows.Count,
        };
        response.Rows.AddRange(rows.Select(ProtoMappers.ToProto));
        return response;
    }
}
