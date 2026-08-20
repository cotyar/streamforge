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

/// <summary>gRPC control-plane mirror of /api/tables — see StreamForge.Host.Api.TablesEndpoints for
/// the REST semantics this reproduces. InvalidOperationException from the registry (REST 409
/// Conflict) maps to FailedPrecondition here.
///
/// <para>Plan 015 wave 3-B: the <c>[Authorize(Policy = …)]</c> attributes stay as the compatibility
/// floor; each method additionally asks <see cref="AccessGuard"/> for the same action its REST twin asks
/// for, at the table it operates on and with that table's <c>Tags</c>. See
/// <see cref="SourceGrpcService"/>'s class doc for why the entity is read before the check.</para>
///
/// <para>One collision worth naming: this service already answers <see cref="StatusCode.FailedPrecondition"/>
/// for the registry's 409-style <see cref="InvalidOperationException"/> (a Running dependant), and
/// <see cref="GrpcAccess"/> uses the same code for a <see cref="AccessDecision.RequiresApproval"/>
/// refusal. They are told apart by the status detail, which is the reason string in both cases and says
/// which one happened. Inventing a private status code for one of them would be worse.</para></summary>
public sealed class TableGrpcService(IClusterClient client, AccessGuard guard) : V1.TableService.TableServiceBase
{
    // Plan 021 D4 — a facade/gRPC service answering one request reads the ambient.
    private IRegistryGrain Registry => client.RegistryFor(EnvironmentAmbient.Current);

    [Authorize(Policy = "Viewer")]
    public override async Task<V1.ListTablesResponse> List(Empty request, ServerCallContext context)
    {
        await GrpcAccess.EnsureAsync(guard, context, Actions.TableRead, "*");

        var tables = await Registry.GetTablesAsync();
        var response = new V1.ListTablesResponse();
        response.Tables.AddRange(tables.Select(ProtoMappers.ToProto));
        return response;
    }

    [Authorize(Policy = "Viewer")]
    public override async Task<V1.TableDefinition> Get(V1.GetTableRequest request, ServerCallContext context)
    {
        // Plan 016 wave 1: read RPCs take an id OR a name, through the one resolver.
        var t = await GrpcEntityRef.RequireAsync(
            await GrpcEntityRef.TableAsync(Registry, request.Id), guard, context, Actions.TableRead);

        await GrpcAccess.EnsureAsync(guard, context, Actions.TableRead, t.Name, t.Tags);

        return ProtoMappers.ToProto(t);
    }

    [Authorize(Policy = "Editor")]
    public override async Task<V1.TableDefinition> Create(V1.CreateTableRequest request, ServerCallContext context)
    {
        if (string.IsNullOrWhiteSpace(request.Name) || string.IsNullOrWhiteSpace(request.Sql))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "name and sql are required"));
        }

        // By NAME — the id does not exist yet. ponytail: no tags, CreateTableRequest carries none (see
        // SourceGrpcService.Create).
        await GrpcAccess.EnsureAsync(guard, context, Actions.TableWrite, request.Name);

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

        await GrpcAccess.EnsureAsync(guard, context, Actions.TableWrite, existing.Name, existing.Tags);

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
        var registry = Registry;
        var existing = await registry.GetTableAsync(request.Id);
        if (existing is null)
        {
            throw new RpcException(new Status(StatusCode.NotFound, $"table '{request.Id}' not found"));
        }

        await GrpcAccess.EnsureAsync(guard, context, Actions.TableDelete, existing.Name, existing.Tags);

        try
        {
            var removed = await registry.DeleteTableAsync(request.Id);
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
        => await SetStatusAsync(request.Id, PipelineStatus.Running, context);

    [Authorize(Policy = "Editor")]
    public override async Task<V1.TableDefinition> Stop(V1.StopTableRequest request, ServerCallContext context)
        => await SetStatusAsync(request.Id, PipelineStatus.Stopped, context);

    [Authorize(Policy = "Editor")]
    public override async Task<V1.ValidateTableResponse> Validate(V1.ValidateRequest request, ServerCallContext context)
    {
        // Scope "*" — the gRPC twin of POST /api/tables/validate, which touches no table.
        await GrpcAccess.EnsureAsync(guard, context, Actions.TableWrite, "*");

        var registry = Registry;
        var streamSchemas = await SchemaBuilder.BuildStreamSchemasAsync(registry);
        var tableSchemas = await SchemaBuilder.BuildTableSchemasAsync(registry);
        var result = SqlCompiler.CompileTable(request.Sql, streamSchemas, tableSchemas);
        return ProtoMappers.ToProtoValidateTableResponse(result);
    }

    [Authorize(Policy = "Viewer")]
    public override async Task<V1.TableRowsResponse> Rows(V1.GetTableRowsRequest request, ServerCallContext context)
    {
        // Plan 016 wave 1: read RPCs take an id OR a name, through the one resolver.
        var def = await GrpcEntityRef.RequireAsync(
            await GrpcEntityRef.TableAsync(Registry, request.Id), guard, context, Actions.TableRead);

        await GrpcAccess.EnsureAsync(guard, context, Actions.TableRead, def.Name, def.Tags);

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
        // Plan 016 wave 1: read RPCs take an id OR a name, through the one resolver.
        var def = await GrpcEntityRef.RequireAsync(
            await GrpcEntityRef.TableAsync(Registry, request.Id), guard, context, Actions.TableRead);

        // Search returns table ROWS, so it is table.read at the table — the same action GET
        // /api/tables/{id}/search asks for. The "search is not enabled" answer below stays a 400-style
        // InvalidArgument and is reached only by a caller entitled to read the table in the first place.
        await GrpcAccess.EnsureAsync(guard, context, Actions.TableRead, def.Name, def.Tags);

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

    /// <summary>Start and Stop differ only in the status. Both are <see cref="Actions.TableControl"/> at
    /// the table, and both keep the registry's InvalidOperationException → FailedPrecondition mapping
    /// (starting a table whose inputs are not Running, stopping one a Running table depends on).</summary>
    private async Task<V1.TableDefinition> SetStatusAsync(string id, PipelineStatus status, ServerCallContext context)
    {
        var registry = Registry;
        var existing = await registry.GetTableAsync(id);
        if (existing is null)
        {
            throw new RpcException(new Status(StatusCode.NotFound, $"table '{id}' not found"));
        }

        await GrpcAccess.EnsureAsync(guard, context, Actions.TableControl, existing.Name, existing.Tags);

        try
        {
            var updated = await registry.SetTableStatusAsync(id, status);
            if (updated is null)
            {
                throw new RpcException(new Status(StatusCode.NotFound, $"table '{id}' not found"));
            }

            return ProtoMappers.ToProto(updated);
        }
        catch (InvalidOperationException ex)
        {
            throw new RpcException(new Status(StatusCode.FailedPrecondition, ex.Message));
        }
    }
}
