using Orleans.Runtime;

namespace StreamsForge.Host.Streaming;

/// <summary>
/// The grain-side landing pad of the push transport. It is an Orleans GRAIN EXTENSION — the same mechanism
/// Orleans' own streaming uses to deliver into a grain (<c>IStreamConsumerExtension</c>) — installed on an
/// activation the first time that activation subscribes to a push stream.
///
/// Why an extension rather than new members on the existing grain interfaces: delivery must arrive as a
/// real Orleans message so it is queued and dispatched under the activation's normal concurrency rules
/// (non-reentrant by default, honoring any <c>[MayInterleave]</c> predicate), which is exactly what memory
/// streams do today. An extension gets that with ZERO changes to StreamsForge.Abstractions — no new grain
/// interface members, no new <c>[Id(n)]</c>s, nothing added to a frozen contract (CLAUDE.md hard rule 1).
/// </summary>
[Alias("StreamsForge.Host.Streaming.IPushDeliveryExtension")]
public interface IPushDeliveryExtension : IGrainExtension
{
    /// <summary>Called by the bus's pump task, from OUTSIDE any grain context, so the payload lands in the
    /// subscribing activation's own turn. <paramref name="item"/> is typed <c>object</c> because one
    /// extension serves every stream this activation subscribes to (EventRecord,
    /// List&lt;TableDeltaDto&gt;, ... ); the subscription id selects the right callback.</summary>
    [Alias("Deliver")]
    Task DeliverAsync(Guid subscriptionId, object? item);
}

/// <inheritdoc cref="IPushDeliveryExtension"/>
public sealed class PushDeliveryExtension(PushStreamBus bus, IGrainContext context) : IPushDeliveryExtension
{
    public Task DeliverAsync(Guid subscriptionId, object? item) => bus.DispatchAsync(subscriptionId, item, context);
}
