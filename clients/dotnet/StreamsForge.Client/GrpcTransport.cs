using System.Runtime.CompilerServices;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Grpc.Net.Client;
using StreamsForge.Host.Grpc.V1;

namespace StreamsForge.Client;

/// <summary>
/// Tier 1 gRPC transport: <c>StreamService.SubscribeTable</c> for deltas, <c>TableService.Rows</c>
/// for the snapshot, <c>TableService.List</c> for id resolution and as the Auto-connect probe, and
/// the bidi <c>IngestService.Ingest</c> for pushes. One insecure h2c channel, prior knowledge -- no
/// TLS negotiation, matching how the engine is actually run from source (the <c>--urls</c> trap:
/// starting the host with <c>--urls</c> trips its guard so no gRPC port is bound at all).
///
/// Row payloads travel as <c>google.protobuf.Struct</c>; <see cref="RowCodec"/> is the whole
/// "typing" story -- see its class doc for the Struct-number precision hazard.
/// </summary>
internal sealed class GrpcTransport : ITransport, IAsyncDisposable
{
    public string Name => "grpc";

    private readonly GrpcChannel _channel;
    private readonly TableService.TableServiceClient _tables;
    private readonly StreamService.StreamServiceClient _stream;
    private readonly IngestService.IngestServiceClient _ingest;
    private readonly Func<CancellationToken, ValueTask<string>> _tokenProvider;

    public GrpcTransport(string target, Func<CancellationToken, ValueTask<string>> tokenProvider)
    {
        // h2c: the engine serves gRPC in cleartext locally (--Grpc:Port), so the client must ask
        // for HTTP/2 over plain HTTP explicitly -- .NET's SocketsHttpHandler otherwise refuses to
        // negotiate HTTP/2 without TLS ("prior knowledge" h2c).
        AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
        _channel = GrpcChannel.ForAddress($"http://{target}", new GrpcChannelOptions
        {
            HttpHandler = new SocketsHttpHandler(),
        });
        _tables = new TableService.TableServiceClient(_channel);
        _stream = new StreamService.StreamServiceClient(_channel);
        _ingest = new IngestService.IngestServiceClient(_channel);
        _tokenProvider = tokenProvider;
    }

    private async Task<Metadata> AuthMetadataAsync(CancellationToken ct)
    {
        var token = await _tokenProvider(ct).ConfigureAwait(false);
        return new Metadata { { "authorization", $"Bearer {token}" } };
    }

    // ---- catalog (id resolution + the Auto-connect "does the channel and JWT actually work" probe) ----

    public async Task<IReadOnlyList<TableSummary>> ListTablesAsync(CancellationToken ct)
    {
        var md = await AuthMetadataAsync(ct).ConfigureAwait(false);
        var resp = await _tables.ListAsync(new Empty(), md, cancellationToken: ct).ConfigureAwait(false);
        return resp.Tables.Select(t => new TableSummary(t.Id, t.Name)).ToList();
    }

    private async Task<string> ResolveTableIdAsync(string name, CancellationToken ct)
    {
        foreach (var t in await ListTablesAsync(ct).ConfigureAwait(false))
        {
            if (t.Name == name) return t.Id;
        }
        throw new StreamsForgeException($"no such table '{name}'");
    }

    // ---- ITransport ----

    public async Task<(IReadOnlyList<RowDelta> Rows, long Seq)> SnapshotAsync(string table, int limit, CancellationToken ct)
    {
        var id = await ResolveTableIdAsync(table, ct).ConfigureAwait(false);
        var md = await AuthMetadataAsync(ct).ConfigureAwait(false);
        var resp = await _tables.RowsAsync(new GetTableRowsRequest { Id = id, Limit = limit }, md, cancellationToken: ct)
            .ConfigureAwait(false);
        var rows = resp.Rows.Select(r => new RowDelta(RowCodec.FromStruct(r.Row), r.Weight)).ToList();
        return (rows, resp.Seq);
    }

    /// <summary>Creates the server-streaming call object eagerly, in a plain <c>async Task</c>
    /// method, rather than inside a lazy <c>async IAsyncEnumerable</c> iterator -- see
    /// <see cref="ITransport.SubscribeAsync"/>'s doc for why laziness here is a correctness bug,
    /// not just a style nit.
    ///
    /// WHAT THIS DOES AND DOES NOT GUARANTEE, checked against Grpc.Net.Client's actual behavior
    /// (not assumed): creating an <see cref="AsyncServerStreamingCall{TResponse}"/> via the
    /// generated client stub starts the underlying HTTP/2 request immediately -- GrpcCall's
    /// server-streaming path kicks off its send as part of the synchronous prefix of call
    /// creation, it does not wait for the first <c>MoveNextAsync()</c> on the response stream the
    /// way a lazy C# iterator would. So moving call creation out of the iterator (this change)
    /// closes the gap this client itself introduced. What it does NOT give is a hard guarantee
    /// that the SERVER has finished registering the subscription by the time this method returns:
    /// <c>StreamGrpcService.SubscribeTable</c> (Grpc/StreamGrpcService.cs) only calls
    /// <c>stream.SubscribeAsync(...)</c> (the actual Orleans registration) after its handler
    /// starts running, and does not write anything to the response stream -- and ASP.NET Core does
    /// not reliably flush response headers before that either -- until the first delta arrives, so
    /// there is no cheap, hang-free signal to await here that would make this exact. This is the
    /// same best-effort level Python settled for (its grpc.py creates the call object synchronously
    /// and does not wait for any further readiness signal either): the remaining window is a
    /// network round trip plus server-side dispatch, not "however long until this LiveTable's
    /// consumer happens to start reading," which is what the lazy-iterator bug actually cost.</summary>
    public async Task<IAsyncEnumerable<(IReadOnlyList<RowDelta> Deltas, long Seq)>> SubscribeAsync(string table, CancellationToken ct)
    {
        var md = await AuthMetadataAsync(ct).ConfigureAwait(false);
        AsyncServerStreamingCall<TableDeltaBatch> call;
        try
        {
            call = _stream.SubscribeTable(new SubscribeTableRequest { Name = table }, md, cancellationToken: ct);
        }
        catch
        {
            // Nothing to dispose here: SubscribeTable() throwing means no call object was ever
            // created (auth metadata failures surface earlier, above). Kept as a catch/rethrow
            // only to document that this branch was considered, matching SignalRTransport's.
            throw;
        }
        return Consume(call, ct);
    }

    private static async IAsyncEnumerable<(IReadOnlyList<RowDelta> Deltas, long Seq)> Consume(
        AsyncServerStreamingCall<TableDeltaBatch> call, [EnumeratorCancellation] CancellationToken ct)
    {
        try
        {
            await foreach (var batch in call.ResponseStream.ReadAllAsync(ct).ConfigureAwait(false))
            {
                var deltas = batch.Deltas.Select(d => new RowDelta(RowCodec.FromStruct(d.Row), d.Weight)).ToList();
                yield return (deltas, batch.Seq);
            }
        }
        finally
        {
            call.Dispose();
        }
    }

    // ---- ingest ----

    /// <summary>One request, one ack, over a fresh bidi stream -- real HTTP/2 backpressure from
    /// the bidi RPC itself (the server does not ack until the push completes) without holding a
    /// stream open across calls. A long-lived streaming session (sustained backpressure across
    /// many pushes) is future work, not needed for <see cref="StreamsForgeClient.PushAsync"/>.</summary>
    public async Task<IngestAckResult> IngestAsync(
        string source, IReadOnlyList<IReadOnlyDictionary<string, object?>> rows, string? idempotencyKey, bool partial, CancellationToken ct)
    {
        var md = await AuthMetadataAsync(ct).ConfigureAwait(false);
        using var call = _ingest.Ingest(md, cancellationToken: ct);
        var request = new IngestRequest
        {
            SourceName = source,
            Partial = partial,
            IdempotencyKey = idempotencyKey ?? "",
        };
        request.Rows.AddRange(rows.Select(RowCodec.ToStruct));
        await call.RequestStream.WriteAsync(request).ConfigureAwait(false);
        await call.RequestStream.CompleteAsync().ConfigureAwait(false);

        if (!await call.ResponseStream.MoveNext(ct).ConfigureAwait(false))
            throw new StreamsForgeException($"gRPC Ingest('{source}') stream closed with no ack");

        var ack = call.ResponseStream.Current;
        // Thrown HERE rather than left to the caller to notice: the C# codegen's enum member
        // naming (PascalCase, e.g. IngestOutcomeAccepted) is a codegen-convention detail that
        // should not leak into StreamsForgeClient's transport-agnostic error handling -- REST's
        // 202/non-202 split makes the same decision at this same layer, on its own wire's terms.
        if (ack.Outcome != IngestOutcome.Accepted)
        {
            throw new IngestRejectedException(
                string.IsNullOrEmpty(ack.Error) ? $"{source} ingest push rejected: {ack.Outcome}" : ack.Error,
                ack.RowErrors.ToList());
        }
        return new IngestAckResult("INGEST_OUTCOME_ACCEPTED", ack.Accepted, ack.Dropped, ack.Invalid, null, Array.Empty<string>());
    }

    public async ValueTask DisposeAsync()
    {
        await _channel.ShutdownAsync().ConfigureAwait(false);
        _channel.Dispose();
    }
}
