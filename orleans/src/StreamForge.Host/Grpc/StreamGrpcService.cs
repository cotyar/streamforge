using Grpc.Core;
using Microsoft.AspNetCore.Authorization;
using Orleans;
using Orleans.Runtime;
using Orleans.Streams;
using StreamForge.Abstractions;
using StreamForge.Api.Auth;
using StreamForge.Engine;
using V1 = StreamForge.Host.Grpc.V1;

namespace StreamForge.Host.Grpc;

/// <summary>gRPC server-streaming mirror of the SignalR StreamHub/StreamBridgeService relays: raw
/// source events, pipeline results, and table deltas. Subscribes directly to the same Orleans
/// streams StreamBridgeService relays to SignalR groups, for the lifetime of the gRPC call —
/// client disconnect cancels ServerCallContext.CancellationToken, which unsubscribes.
///
/// <para>Plan 015 wave 3-B: each subscription keeps its <c>[Authorize(Policy = "Viewer")]</c> floor and
/// additionally asks <see cref="AccessGuard"/> for the READ action on the entity being subscribed to —
/// the same action the REST route that returns that entity's rows asks for. Subscribing to a stream is
/// reading the entity, continuously, so anything weaker than <c>{source,pipeline,table}.read</c> would
/// make the streaming surface the way around every read entitlement.</para>
///
/// <para><b>The entity is looked up only for its Tags, and a miss is not a 404.</b> Subscribing to a
/// name that does not exist has always been legal here (the Orleans stream simply never fires; a client
/// may subscribe before the entity is created), and turning that into a NotFound would be a behaviour
/// change this wave has no business making. So the lookup is best-effort: found → check with its tags,
/// absent → check with none, which can only ever narrow the answer.</para>
///
/// <para><b>Revocation does not reach a live subscription.</b> The check happens at subscribe time and
/// nothing re-checks per frame — see the identical note on <c>StreamHub</c>, which states the ceiling and
/// the upgrade path once for both transports.</para></summary>
public sealed class StreamGrpcService(IClusterClient client, AccessGuard guard) : V1.StreamService.StreamServiceBase
{
    private IRegistryGrain Registry => client.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);

    [Authorize(Policy = "Viewer")]
    public override async Task SubscribeSource(
        V1.SubscribeSourceRequest request,
        IServerStreamWriter<V1.SourceEvent> responseStream,
        ServerCallContext context)
    {
        await GrpcAccess.EnsureAsync(
            guard, context, Actions.SourceRead, request.Name,
            (await Registry.GetSourceAsync(request.Name))?.Tags);

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
        // Scope is the pipeline's NAME, not the id in the request: an id is a Guid("n") the registry
        // minted, so a `prod-*` scope written by an operator would match nothing at all. Same rule as
        // the REST routes and the chat tools — a grant has to mean one thing on every transport.
        var subscribed = await Registry.GetPipelineAsync(request.Id);
        await GrpcAccess.EnsureAsync(
            guard, context, Actions.PipelineRead, subscribed?.Name ?? request.Id, subscribed?.Tags);

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
        // Delta streams are keyed by table NAME (see TableGrain), and IRegistryGrain.GetTableAsync takes
        // an id — hence the list scan, the same one DynamicStreamService does for the same reason. The
        // entitlement is checked against the NAME the caller asked for, because that is the string an
        // operator would have written into a scope.
        var table = (await Registry.GetTablesAsync()).FirstOrDefault(t => t.Name == request.Name);
        await GrpcAccess.EnsureAsync(guard, context, Actions.TableRead, request.Name, table?.Tags);

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
