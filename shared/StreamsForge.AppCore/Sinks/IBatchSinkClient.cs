namespace StreamsForge.AppCore.Sinks;

/// <summary>
/// Plan 014: an OPTIONAL capability an <see cref="ISinkClient"/> may additionally implement — "give me a
/// whole delivered batch at once, not one message at a time". Nothing about <see cref="ISinkClient"/>'s own
/// contract changes: a sink that does not implement this interface is unaffected, and one that does is
/// still bound by the exact same rules <see cref="PublishAsync{T}"/> already restates in <see
/// cref="NatsSinkClient"/>'s class doc — <see cref="PublishBatchAsync{T}"/> must NEVER throw, must not
/// block the caller past the same ~3s budget <c>NatsSinkClient.PublishTimeout</c> /
/// <c>FileSinkClient.PublishTimeout</c> already spend, and delivery stays fire-and-forget, at-most-once,
/// with no backpressure into the pipeline. A batch buys throughput and transactional grouping on the way
/// out; it does not buy reliability, acknowledgement or retry that <see cref="ISinkClient"/> didn't already
/// have. Restated honestly rather than left implied, the same way <c>NatsPubConfig</c>'s own doc comment
/// states its delivery ceiling instead of letting a reader assume "batch" means "safer".
///
/// <para><b>Why this exists at all.</b> For a sink whose natural unit of work is a <i>transaction</i> —
/// plan 014's database sink, which the four un-batching call sites this wave is about exist to eventually
/// stop starving — the batch <see cref="SinkFanout.PublishAllAsync{T}"/> delivers already <i>is</i> the
/// transaction. Un-batching it into N serial <see cref="ISinkClient.PublishAsync{T}"/> calls, which is
/// exactly what <c>NatsPublisherService</c> and <c>NatsSinkPublisherService</c> do today (there was never a
/// reason not to — NATS and the file sink have no notion of a transaction), means N separate transactions
/// for a database sink: N round-trips, N commits, and partial-batch visibility if the host dies midway
/// through. One call carrying the whole batch is the difference between "the batch" and "N batches of
/// one".</para>
///
/// <para><b>One delivered batch = one transaction. No time-based buffering — this is deliberate, not a
/// gap.</b> <see cref="SinkSelection.Signature"/> tears a client down and rebuilds it on ANY edit to its
/// owning <see cref="StreamsForge.Abstractions.SinkSpec"/> (see that method's doc). A sink that accumulated
/// a time-based (linger) buffer across calls would silently lose whatever sat in that buffer at the moment
/// of teardown — unless <see cref="ISinkClient.DisposeAsync"/> flushed it, and a flush-on-dispose racing the
/// periodic refresh that is doing the tearing-down is exactly the kind of concurrency bug that is easy to
/// introduce and brutal to reproduce. Binding a client's buffer to nothing but the batch that was just
/// handed to it — never accumulating across separate <see cref="PublishBatchAsync{T}"/> calls — makes that
/// entire class of bug structurally impossible: there is never anything left to lose at teardown. A
/// calendar/size-based linger buffer (<c>LingerMs</c>) is the named follow-up this plan defers, not
/// something this interface forgot.</para>
/// </summary>
public interface IBatchSinkClient : ISinkClient
{
    /// <summary>Publishes <paramref name="payloads"/> as one unit — one transaction for a sink whose sink
    /// kind has one. NEVER throws, and never blocks past the same budget <see
    /// cref="ISinkClient.PublishAsync{T}"/> already promises — see this interface's doc comment. An empty
    /// <paramref name="payloads"/> is a valid no-op call, not an error.</summary>
    Task PublishBatchAsync<T>(IReadOnlyList<T> payloads, CancellationToken ct);
}
