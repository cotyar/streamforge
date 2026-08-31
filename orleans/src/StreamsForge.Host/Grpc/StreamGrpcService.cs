using Grpc.Core;
using Microsoft.AspNetCore.Authorization;
using Orleans;
using Orleans.Runtime;
using Orleans.Streams;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Environments;
using StreamsForge.Host.Facades;
using StreamsForge.Api.Auth;
using StreamsForge.AppCore;
using StreamsForge.Engine;
using V1 = StreamsForge.Host.Grpc.V1;

namespace StreamsForge.Host.Grpc;

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
    // Plan 021 D4 — a facade/gRPC service answering one request reads the ambient.
    private IRegistryGrain Registry => client.RegistryFor(EnvironmentAmbient.Current);

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
            StreamId.Create(StreamConstants.SourcesNamespace, EnvKeys.Qualify(EnvironmentAmbient.Current, request.Name)));

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
        //
        // Plan 016 wave 1: the request field takes an id OR a name. This RPC deliberately does NOT fail
        // on an unknown id (an unknown key just yields a silent stream), so that stays — only an
        // AMBIGUOUS name is answered, and only after the guard, so the candidate ids are not an
        // enumeration oracle for a caller entitled to read neither.
        var hit = await GrpcEntityRef.PipelineAsync(Registry, request.Id);
        var subscribed = hit.Value;
        await GrpcAccess.EnsureAsync(
            guard, context, Actions.PipelineRead, subscribed?.Name ?? request.Id, subscribed?.Tags);
        if (hit.Outcome == EntityRefOutcome.Ambiguous)
        {
            throw new RpcException(new Status(StatusCode.FailedPrecondition, hit.Message));
        }

        // The output stream is keyed by pipeline ID, so a name-addressed subscription resolves to one.
        var pipelineId = subscribed?.Id ?? request.Id;

        var streamProvider = client.GetStreamProvider(StreamConstants.ProviderName);
        var stream = streamProvider.GetStream<List<ResultEnvelope>>(
            StreamId.Create(StreamConstants.OutputNamespace, EnvKeys.Qualify(subscribed?.Environment ?? EnvironmentAmbient.Current, pipelineId)));

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
        // Delta streams are keyed by table NAME (see TableGrain). Plan 016 wave 1: the request field
        // now takes an id OR a name through the one resolver, and the delta-stream key is the RESOLVED
        // name — so a caller holding only the id (what the console and the config export carry) can
        // subscribe without a round trip to translate it. The entitlement is checked against that same
        // resolved name, because that is the string an operator would have written into a scope, and
        // against the raw request when nothing resolved. Like SubscribePipeline above, an unknown key
        // is NOT an error here (it yields a silent stream); only an ambiguous name is, after the guard.
        var hit = await GrpcEntityRef.TableAsync(Registry, request.Name);
        var table = hit.Value;
        var tableName = table?.Name ?? request.Name;
        await GrpcAccess.EnsureAsync(guard, context, Actions.TableRead, tableName, table?.Tags);
        if (hit.Outcome == EntityRefOutcome.Ambiguous)
        {
            throw new RpcException(new Status(StatusCode.FailedPrecondition, hit.Message));
        }

        var streamProvider = client.GetStreamProvider(StreamConstants.ProviderName);
        var stream = streamProvider.GetStream<List<TableDeltaDto>>(
            StreamId.Create(StreamConstants.TableDeltaNamespace, EnvKeys.Qualify(table?.Environment ?? EnvironmentAmbient.Current, tableName)));

        long seq = 0;
        var handle = await stream.SubscribeAsync(async (deltas, _) =>
        {
            seq++;
            var batch = new V1.TableDeltaBatch { TableName = tableName, Seq = seq };
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
