using Grpc.Core;
using Microsoft.AspNetCore.Authorization;
using Orleans;
using Orleans.Runtime;
using Orleans.Streams;
using StreamForge.Abstractions;
using StreamForge.Engine;
using V1 = StreamForge.Host.Grpc.V1;

namespace StreamForge.Host.Grpc;

/// <summary>gRPC server-streaming mirror of the SignalR StreamHub/StreamBridgeService relays: raw
/// source events, pipeline results, and table deltas. Subscribes directly to the same Orleans
/// streams StreamBridgeService relays to SignalR groups, for the lifetime of the gRPC call —
/// client disconnect cancels ServerCallContext.CancellationToken, which unsubscribes.</summary>
public sealed class StreamGrpcService(IClusterClient client) : V1.StreamService.StreamServiceBase
{
    [Authorize(Policy = "Viewer")]
    public override async Task SubscribeSource(
        V1.SubscribeSourceRequest request,
        IServerStreamWriter<V1.SourceEvent> responseStream,
        ServerCallContext context)
    {
        var streamProvider = client.GetStreamProvider(StreamConstants.ProviderName);
        var stream = streamProvider.GetStream<EventRecord>(
            StreamId.Create(StreamConstants.SourcesNamespace, request.Name));

        long seq = 0;
        var handle = await stream.SubscribeAsync(async (evt, _) =>
        {
            seq++;
            await responseStream.WriteAsync(new V1.SourceEvent
            {
                SourceName = request.Name,
                Seq = seq,
                TimestampMs = evt.Timestamp,
                Row = GrpcValueConverter.ToStruct(evt),
            });
        });

        await WaitForCancellationThenUnsubscribeAsync(handle, context.CancellationToken);
    }

    [Authorize(Policy = "Viewer")]
    public override async Task SubscribePipeline(
        V1.SubscribePipelineRequest request,
        IServerStreamWriter<V1.ResultEnvelope> responseStream,
        ServerCallContext context)
    {
        var streamProvider = client.GetStreamProvider(StreamConstants.ProviderName);
        var stream = streamProvider.GetStream<List<ResultEnvelope>>(
            StreamId.Create(StreamConstants.OutputNamespace, request.Id));

        var handle = await stream.SubscribeAsync(async (rows, _) =>
        {
            foreach (var row in rows)
            {
                await responseStream.WriteAsync(new V1.ResultEnvelope
                {
                    PipelineId = row.PipelineId,
                    Seq = row.Seq,
                    TimestampMs = row.TimestampMs,
                    Row = GrpcValueConverter.ToStruct(row.Row),
                });
            }
        });

        await WaitForCancellationThenUnsubscribeAsync(handle, context.CancellationToken);
    }

    [Authorize(Policy = "Viewer")]
    public override async Task SubscribeTable(
        V1.SubscribeTableRequest request,
        IServerStreamWriter<V1.TableDeltaBatch> responseStream,
        ServerCallContext context)
    {
        var streamProvider = client.GetStreamProvider(StreamConstants.ProviderName);
        var stream = streamProvider.GetStream<List<TableDeltaDto>>(
            StreamId.Create(StreamConstants.TableDeltaNamespace, request.Name));

        long seq = 0;
        var handle = await stream.SubscribeAsync(async (deltas, _) =>
        {
            seq++;
            var batch = new V1.TableDeltaBatch { TableName = request.Name, Seq = seq };
            batch.Deltas.AddRange(deltas.Select(d => new V1.TableDelta
            {
                Row = GrpcValueConverter.ToStruct(d.Row),
                Weight = d.Weight,
            }));
            await responseStream.WriteAsync(batch);
        });

        await WaitForCancellationThenUnsubscribeAsync(handle, context.CancellationToken);
    }

    /// <summary>Keeps the RPC alive until the client disconnects/cancels (context.CancellationToken),
    /// then unsubscribes the Orleans stream handle — mirrors StreamBridgeService's
    /// subscribe-once-per-name lifecycle, but scoped to a single gRPC call instead of the whole
    /// process.</summary>
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
                // best-effort, mirrors StreamBridgeService's unsubscribe try/catch
            }
        }
    }
}
