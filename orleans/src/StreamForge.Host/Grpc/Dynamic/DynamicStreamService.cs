using Google.Protobuf;
using Grpc.Core;
using Microsoft.AspNetCore.Authorization;
using Orleans;
using Orleans.Runtime;
using Orleans.Streams;
using StreamForge.Abstractions;
using StreamForge.Engine;
using V1 = StreamForge.Host.Grpc.Dynamic.V1;

namespace StreamForge.Host.Grpc.Dynamic;

/// <summary>
/// Tier 2's single generic streaming RPC for runtime ("dynamic") entities — see
/// Protos/streamforge_dynamic.proto. One RPC (<see cref="SubscribeEntity"/>) replaces Tier 1's
/// StreamService.SubscribeSource/SubscribePipeline/SubscribeTable trio: which Orleans stream to
/// subscribe to and how to encode each row is resolved at call time from <paramref name="entityKey"/>'s
/// "source:{name}" / "pipeline:{id}" / "table:{id}" prefix, following the same Orleans
/// stream-subscription + cancellation pattern as <see cref="StreamGrpcService"/>.
///
/// <para><b>Snapshot semantics</b>: the entity's field list + <see cref="FieldNumberMap"/> are fetched
/// ONCE at subscribe time (matching whatever <see cref="DynamicReflectionService"/> would return for the
/// same entity at that moment) and reused for every frame of the subscription's lifetime. A schema edit
/// made to the entity AFTER the subscription starts is not tracked — the stream keeps encoding against
/// the field numbers it captured at subscribe time; a client that wants the new shape must re-subscribe
/// (which fetches the updated schema/reflection descriptor fresh). This mirrors typed-client reality:
/// the client already generated code against a single descriptor version before starting the call.</para>
/// </summary>
public sealed class DynamicStreamService(IClusterClient client) : V1.DynamicStreamService.DynamicStreamServiceBase
{
    private IRegistryGrain Registry => client.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);

    [Authorize(Policy = "Viewer")]
    public override async Task SubscribeEntity(
        V1.EntitySubscribeRequest request,
        IServerStreamWriter<V1.DynamicFrame> responseStream,
        ServerCallContext context)
    {
        var (kind, ident) = ParseEntityKey(request.EntityKey);
        var registry = Registry;

        switch (kind)
        {
            case "source":
                await StreamSourceAsync(registry, request.EntityKey, ident, responseStream, context);
                break;
            case "table":
                await StreamTableAsync(registry, request.EntityKey, ident, responseStream, context);
                break;
            case "pipeline":
                await StreamPipelineAsync(registry, request.EntityKey, ident, responseStream, context);
                break;
            default:
                throw new RpcException(new Status(StatusCode.NotFound, $"unknown entity kind in '{request.EntityKey}'"));
        }
    }

    private async Task StreamSourceAsync(
        IRegistryGrain registry, string entityKey, string name, IServerStreamWriter<V1.DynamicFrame> responseStream, ServerCallContext context)
    {
        var src = await registry.GetSourceAsync(name);
        if (src is null)
        {
            throw new RpcException(new Status(StatusCode.NotFound, $"source '{name}' not found"));
        }

        var fields = src.Fields;
        var numbers = await FetchNumbersAsync(registry, entityKey, fields);

        var streamProvider = client.GetStreamProvider(StreamConstants.ProviderName);
        var stream = streamProvider.GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, name));

        long seq = 0;
        var handle = await stream.SubscribeAsync(async (evt, _) =>
        {
            seq++;
            var payload = ProtoWireEncoder.EncodeEvent(fields, numbers, evt, seq, evt.Timestamp);
            await responseStream.WriteAsync(new V1.DynamicFrame
            {
                EntityKey = entityKey,
                Payload = ByteString.CopyFrom(payload),
                Seq = seq,
            });
        });

        await WaitForCancellationThenUnsubscribeAsync(handle, context.CancellationToken);
    }

    private async Task StreamTableAsync(
        IRegistryGrain registry, string entityKey, string id, IServerStreamWriter<V1.DynamicFrame> responseStream, ServerCallContext context)
    {
        var table = await registry.GetTableAsync(id)
            ?? (await registry.GetTablesAsync()).FirstOrDefault(t => t.Name == id); // names are unique across sources+tables
        if (table is null)
        {
            throw new RpcException(new Status(StatusCode.NotFound, $"table '{id}' not found"));
        }

        // Canonicalize: the field-number map must live under one key regardless of whether the
        // caller subscribed by id or by name.
        entityKey = EntitySchemas.TableKey(table.Id);

        if (table.OutputFields.Count == 0)
        {
            throw new RpcException(new Status(StatusCode.FailedPrecondition, $"table '{id}' has no compiled output schema"));
        }

        var fields = table.OutputFields;
        var numbers = await FetchNumbersAsync(registry, entityKey, fields);

        // Table delta streams are keyed by table NAME, not id (see TableGrain / StreamGrpcService.SubscribeTable) —
        // entity_key carries the id (stable across renames), so resolve id -> name via the definition above.
        var streamProvider = client.GetStreamProvider(StreamConstants.ProviderName);
        var stream = streamProvider.GetStream<List<TableDeltaDto>>(StreamId.Create(StreamConstants.TableDeltaNamespace, table.Name));

        long seq = 0;
        var handle = await stream.SubscribeAsync(async (deltas, _) =>
        {
            seq++; // one seq per batch, mirroring StreamGrpcService.SubscribeTable's TableDeltaBatch.Seq
            foreach (var delta in deltas)
            {
                var payload = ProtoWireEncoder.EncodeDelta(fields, numbers, delta.Row, delta.Weight, seq);
                await responseStream.WriteAsync(new V1.DynamicFrame
                {
                    EntityKey = entityKey,
                    Payload = ByteString.CopyFrom(payload),
                    Seq = seq,
                });
            }
        });

        await WaitForCancellationThenUnsubscribeAsync(handle, context.CancellationToken);
    }

    private async Task StreamPipelineAsync(
        IRegistryGrain registry, string entityKey, string id, IServerStreamWriter<V1.DynamicFrame> responseStream, ServerCallContext context)
    {
        var pipeline = await registry.GetPipelineAsync(id);
        if (pipeline is null)
        {
            // Name fallback (pipeline names aren't enforced unique — only resolve an unambiguous match).
            var byName = (await registry.GetPipelinesAsync()).Where(p => p.Name == id).ToList();
            if (byName.Count == 1) pipeline = byName[0];
        }
        if (pipeline is null)
        {
            throw new RpcException(new Status(StatusCode.NotFound, $"pipeline '{id}' not found"));
        }

        // Canonicalize the field-number-map key (see StreamTableAsync) and re-point id at the real
        // pipeline id — the output stream below is keyed by id, not name.
        entityKey = EntitySchemas.PipelineKey(pipeline.Id);
        id = pipeline.Id;

        var streamSchemas = await SchemaBuilder.BuildStreamSchemasAsync(registry);
        var compiled = SqlCompiler.Compile(pipeline.Sql, streamSchemas);
        if (!compiled.Ok || compiled.OutputSchema is null)
        {
            throw new RpcException(new Status(StatusCode.FailedPrecondition, $"pipeline '{id}' SQL does not currently compile"));
        }

        var fields = EntitySchemas.FromOutputSchema(compiled.OutputSchema);
        var numbers = await FetchNumbersAsync(registry, entityKey, fields);

        var streamProvider = client.GetStreamProvider(StreamConstants.ProviderName);
        var stream = streamProvider.GetStream<List<ResultEnvelope>>(StreamId.Create(StreamConstants.OutputNamespace, id));

        var handle = await stream.SubscribeAsync(async (rows, _) =>
        {
            foreach (var row in rows)
            {
                var payload = ProtoWireEncoder.EncodeEvent(fields, numbers, row.Row, row.Seq, row.TimestampMs);
                await responseStream.WriteAsync(new V1.DynamicFrame
                {
                    EntityKey = entityKey,
                    Payload = ByteString.CopyFrom(payload),
                    Seq = row.Seq,
                });
            }
        });

        await WaitForCancellationThenUnsubscribeAsync(handle, context.CancellationToken);
    }

    private static async Task<FieldNumberMap> FetchNumbersAsync(IRegistryGrain registry, string entityKey, List<FieldDef> fields)
        => EntitySchemas.ParseMap(await registry.EnsureFieldNumbersAsync(entityKey, fields));

    private static (string Kind, string Ident) ParseEntityKey(string entityKey)
    {
        var idx = entityKey.IndexOf(':');
        if (idx <= 0 || idx == entityKey.Length - 1)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, $"malformed entity_key '{entityKey}' (expected 'kind:id')"));
        }

        return (entityKey[..idx], entityKey[(idx + 1)..]);
    }

    /// <summary>Keeps the RPC alive until the client disconnects/cancels, then unsubscribes the Orleans
    /// stream handle — same pattern as StreamGrpcService.WaitForCancellationThenUnsubscribeAsync.</summary>
    private static async Task WaitForCancellationThenUnsubscribeAsync<T>(
        StreamSubscriptionHandle<T> handle, CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            // expected on client disconnect / call cancellation
        }
        finally
        {
            try
            {
                await handle.UnsubscribeAsync();
            }
            catch
            {
                // best-effort, mirrors StreamGrpcService's unsubscribe try/catch
            }
        }
    }
}
