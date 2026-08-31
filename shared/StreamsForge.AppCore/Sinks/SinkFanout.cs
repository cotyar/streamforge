namespace StreamsForge.AppCore.Sinks;

/// <summary>
/// Plan 014: the batching rule from ONE place instead of four. <c>NatsPublisherService</c> (Orleans) and
/// <c>NatsSinkPublisherService</c> (Dapr) each have two call sites — pipeline results and table deltas —
/// that receive a batch of rows/deltas and fan it out to that entity's active <see cref="ISinkClient"/>
/// list. All four currently do the same thing the same way: for each message, for each client, one
/// <see cref="ISinkClient.PublishAsync{T}"/> call. That is fine for NATS and the file sink, which have no
/// notion of a batch, but it is the wrong shape for a sink whose delivery unit is a transaction (see
/// <see cref="IBatchSinkClient"/>'s doc) — and four independent call sites are four independent places that
/// would need to learn the "is this client a batch client" branch, and four places that have already, in
/// this exact repo, drifted from each other once (Orleans subscribes per-entity, Dapr polls a fixed topic;
/// read their class docs). Putting the branch here means it is learned once and waves E/F each swap one
/// loop body for one call to <see cref="PublishAllAsync{T}"/>.
///
/// <para><b>Per client:</b> an <see cref="IBatchSinkClient"/> gets exactly ONE
/// <see cref="IBatchSinkClient.PublishBatchAsync{T}"/> call carrying every message in <c>messages</c>;
/// anything else gets <c>messages.Count</c> serial <see cref="ISinkClient.PublishAsync{T}"/> calls, in
/// order. Clients are processed in list order, one at a time — this is a different loop nesting from the
/// four call sites' "per message, per client" (this is "per client, per message"), but the two orderings
/// are observably identical for NATS and file clients: they are independent connections with no ordering
/// dependency on each other, and <see cref="ISinkClient.PublishAsync{T}"/> never throws for them, so no
/// reordering of interleaving between clients is visible from outside. That equivalence is what makes
/// "waves E and F swap their loop bodies for this call with no behaviour change" true.</para>
///
/// <para><b>Failure discipline — deliberately NOT what the doc comments elsewhere in this file family
/// might suggest.</b> The four existing call sites wrap no try/catch around their
/// <c>PublishAsync</c> calls at all (verified by reading <c>NatsPublisherService.SubscribePipelineAsync</c>
/// / <c>SubscribeTableAsync</c> and <c>NatsSinkPublisherService.OnPipelineResultsAsync</c> /
/// <c>OnTableDeltaAsync</c>) — they lean entirely on <see cref="ISinkClient.PublishAsync{T}"/>'s own
/// contract of never throwing. This method reproduces that literally: no try/catch here either. A client
/// that violates its contract and throws propagates immediately out of this call, exactly as it would
/// propagate out of any of the four existing loops today, and any client not yet reached in
/// <paramref name="clients"/> at that point is NOT called for this invocation. This is not "resilient
/// fan-out with per-client isolation" — that behavior does not exist at the four call sites this replaces,
/// so inventing it here would be a silent behavior change, not a like-for-like extraction.</para>
/// </summary>
public static class SinkFanout
{
    /// <summary>Fans <paramref name="messages"/> out to every client in <paramref name="clients"/>, batching
    /// where the client supports it. See this class's doc comment for the exact per-client behavior and the
    /// (deliberate) failure discipline. Both empty inputs are no-ops.</summary>
    public static async Task PublishAllAsync<T>(
        IReadOnlyList<ISinkClient> clients, IReadOnlyList<T> messages, CancellationToken ct)
    {
        if (clients.Count == 0 || messages.Count == 0)
        {
            return;
        }

        foreach (var client in clients)
        {
            if (client is IBatchSinkClient batch)
            {
                await batch.PublishBatchAsync(messages, ct).ConfigureAwait(false);
                continue;
            }

            foreach (var message in messages)
            {
                await client.PublishAsync(message, ct).ConfigureAwait(false);
            }
        }
    }
}
