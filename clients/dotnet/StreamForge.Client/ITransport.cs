namespace StreamForge.Client;

/// <summary>
/// The one interface every live transport implements (gRPC Tier 1, SignalR). <see cref="ZSet"/>
/// and <see cref="LiveTable"/> are written against THIS and never know which is underneath --
/// that is the whole point of the contract-test suite running once per transport: implementations
/// that agree on every assertion are interchangeable, and one that drifts fails on the same line
/// the others pass.
/// </summary>
public interface ITransport
{
    /// <summary>"grpc", "signalr:ws", "signalr:sse" or "signalr:lp" -- logged on connect so a
    /// caller on <see cref="TransportKind.Auto"/> never silently rides a fallback.</summary>
    string Name { get; }

    /// <summary>Establishes the subscription and returns once it is live, THEN hands back an
    /// enumerable of (deltas, seq) batches until the subscription ends (error or a clean
    /// server-initiated close). No backfill: the first item is whatever arrives after the
    /// subscription goes live, not the table's current contents -- callers pair this with
    /// <see cref="SnapshotAsync"/> and buffer/replay (see <see cref="LiveTable"/>), never rely on
    /// this alone.
    ///
    /// THE OUTER <see cref="Task"/> IS LOAD-BEARING, not a style choice: a plain
    /// <c>IAsyncEnumerable&lt;T&gt; SubscribeAsync(...)</c> implemented as a C# async iterator
    /// (<c>async IAsyncEnumerable&lt;T&gt;</c> with <c>yield return</c>) is entirely LAZY -- none
    /// of its body runs until the consumer's first <c>MoveNextAsync()</c>. A caller that spawns a
    /// background reader task and then immediately calls <see cref="SnapshotAsync"/> would race:
    /// the subscription might not even be registered on the server yet when the snapshot read
    /// lands. That is a different failure than the "arrived before the snapshot" case buffering
    /// exists for -- a delta the server emits before OUR subscription is registered is never sent
    /// to us at all, buffered or not, and a fresh subscription gets no backfill for it. Returning
    /// <c>Task&lt;IAsyncEnumerable&lt;T&gt;&gt;</c> forces establishment (connect, and for
    /// SignalR the server-acknowledged <c>SubscribeTable</c> invocation) to complete, awaited, in
    /// a plain async method BEFORE the enumerable is handed back -- only mechanical reads from an
    /// already-live subscription happen lazily. Implementations must dispose any half-open
    /// connection if establishment itself fails.</summary>
    Task<IAsyncEnumerable<(IReadOnlyList<RowDelta> Deltas, long Seq)>> SubscribeAsync(string table, CancellationToken ct);

    /// <summary>One-shot read of the table's current consolidated rows (weight already summed
    /// server-side) plus the read's own sequence number. NOT comparable to <see cref="SubscribeAsync"/>'s
    /// seq -- see <see cref="ZSet"/>'s class doc.</summary>
    Task<(IReadOnlyList<RowDelta> Rows, long Seq)> SnapshotAsync(string table, int limit, CancellationToken ct);
}
