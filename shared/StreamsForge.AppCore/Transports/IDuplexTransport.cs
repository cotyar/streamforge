using StreamsForge.Abstractions;

namespace StreamsForge.AppCore.Transports;

/// <summary>
/// Plan 019 (wave A): the third seam, after <see cref="IInboundTransport"/> and
/// <see cref="IPolledTransport"/> — one live session with two halves, for a protocol where ingress and
/// egress are not independent (a FIX session's <c>NewOrderSingle</c> out and <c>ExecutionReport</c> back
/// travel the same TCP connection, the same <c>SenderCompID</c>/<c>TargetCompID</c> pair, the same
/// sequence-number streams; two independent connections would be two logons a real counterparty rejects).
///
/// <para><b>Its inbound half IS an <see cref="IInboundSubscription"/></b>, so <see cref="SubscriberCore"/>
/// drives it completely unchanged — no duplex-specific branch anywhere in the reconnect/backoff/parse/
/// mapping/ack loop. Its outbound half is <see cref="SendAsync"/>, reached by a stateless proxy sink (wave
/// 019-B) that resolves this session by source name and forwards, rather than opening a second
/// connection.</para>
/// </summary>
public interface IDuplexSession : IInboundSubscription
{
    /// <summary>True when the session is established and can accept a send right now. A proxy sink with
    /// <c>requireSession: true</c> (wave 019-B/G) refuses to start a pipeline while this is false rather
    /// than accepting rows it cannot deliver.</summary>
    bool IsReady { get; }

    /// <summary>Hands rows to the session's outbound half. MUST NOT throw for an ordinary delivery
    /// failure (not logged on, queue full, session mid-reconnect) — report it in the returned
    /// <see cref="DuplexSendOutcome"/> instead, so the caller's never-throw <c>ISinkClient.PublishAsync</c>
    /// contract (<c>ISinkTransport.cs</c>) holds without a try/catch at the call site. May throw only when
    /// the session itself is dead, which the driver treats exactly like
    /// <see cref="IInboundTransport.Open"/> throwing: "this connection attempt failed", reconnected with
    /// backoff.</summary>
    Task<DuplexSendOutcome> SendAsync(IReadOnlyList<Dictionary<string, object?>> rows, CancellationToken ct);

    /// <summary>Plan 019 D3 (wave 019-B2): cumulative ROWS this session has accepted via
    /// <see cref="SendAsync"/> — a running sum of every <see cref="DuplexSendOutcome.Sent"/> this instance
    /// has returned, NOT a count of <see cref="SendAsync"/> calls. Rows, not batches, because that is what
    /// <see cref="ConnectorRuntimeStatus.DuplexSentTotal"/>'s own doc promises ("rows this source's
    /// outbound half accepted") and what an operator actually wants to know: how many orders went out, not
    /// how many wire calls it took.
    ///
    /// <para><b>Scope: the life of THIS session instance, not the source.</b>
    /// <see cref="IDuplexTransport.OpenDuplex"/> mints a fresh session per connection attempt — the
    /// connect/backoff loop in <c>SubscriberCore</c> calls <see cref="IInboundTransport.Open"/> (which
    /// delegates to <see cref="OpenDuplex"/>) once per attempt — so a reconnect produces a brand-new object
    /// and this counter starts back at zero on it. The driver (<c>ConnectorGrain</c>/<c>ConnectorActor</c>)
    /// does not merge counts across generations; it reads this straight off whichever session is currently
    /// published in <c>DuplexSessions</c>, the same read-through-no-local-accumulation shape
    /// <c>ConnectorRuntimeStatus.DuplexReady</c> already uses. Losing the tally across a reconnect is an
    /// accepted trade for that simplicity: "is the CURRENT session healthy" is the question an operator is
    /// actually asking, not "how many orders has this source sent since the dawn of time" — the latter
    /// would need persistence this wave does not add (see plan 019 D5 for where that belongs
    /// instead).</para></summary>
    long SentTotal { get; }

    /// <summary>Cumulative ROWS this session's <see cref="SendAsync"/> could not deliver — every
    /// <see cref="DuplexSendOutcome.Failed"/> this instance has returned, summed, PLUS the whole batch size
    /// on a call that threw instead of returning (the session's one documented exception case: "the
    /// session itself is dead"). Same rows-not-batches counting and same per-instance,
    /// reset-on-reconnect scope as <see cref="SentTotal"/> — see that member's doc for why.</summary>
    long FailedTotal { get; }

    /// <summary>The most recent row this session's <see cref="SendAsync"/> could not deliver, or null if
    /// this instance has never failed a send. Same per-instance, reset-on-reconnect scope as
    /// <see cref="SentTotal"/>.
    ///
    /// <para>Exists because <see cref="ConnectorRuntimeStatus.LastDuplexFailure"/> must carry more than a
    /// count: plan 019 D3 explicitly rejects "counted in a failure counter" as an acceptable outcome for a
    /// <c>NewOrderSingle</c>. The driver formats this into that field with
    /// <see cref="DuplexSendFailure.CorrelationId"/> first — for a FIX session, the order's
    /// <c>ClOrdID</c> — because the correlation id is the part an operator can actually act on; the reason
    /// string alone is not.</para></summary>
    DuplexSendFailure? LastFailure { get; }
}

/// <summary>
/// Plan 019 (wave A): a source kind whose live session also accepts sends — <c>fix</c> will be the first
/// (wave 019-E).
///
/// <para><b><see cref="IDuplexTransport"/> extends <see cref="IInboundTransport"/> deliberately</b>, rather
/// than standing alone: <see cref="Open"/> and <see cref="OpenDuplex"/> both exist so a duplex kind still
/// satisfies every existing inbound code path (<c>ConnectorGrain.ArmForKind</c>'s
/// <c>InboundTransports.Find</c> arm, <c>ConnectorActor</c>'s twin, <c>SourceValidation</c>, <c>GET
/// /api/transports</c>) with no change to any of them — see <see cref="DuplexTransports"/>'s doc for how
/// registration makes that automatic. <b>A duplex transport's <see cref="Open"/> is expected to delegate to
/// <see cref="OpenDuplex"/></b> (typically <c>IInboundSubscription Open(def) =&gt; OpenDuplex(def);</c>) so
/// the two entry points always return the same live object rather than two independently-connected
/// ones.</para>
/// </summary>
public interface IDuplexTransport : IInboundTransport
{
    /// <summary>Opens ONE session covering both directions. The driver (Orleans <c>ConnectorGrain</c>, Dapr
    /// <c>ConnectorActor</c> — wave 019-C/D) keeps exactly one alive per source and disposes it on stop; the
    /// proxy sink (wave 019-B) reaches this same object rather than opening one of its own.</summary>
    IDuplexSession OpenDuplex(SourceDefinition def);
}

/// <summary>One <see cref="IDuplexSession.SendAsync"/> call's result: how many of the offered rows the
/// session accepted, how many it did not, and — for the ones it did not — enough to act on
/// (<see cref="DuplexSendFailure.CorrelationId"/> for a FIX session is the order's <c>ClOrdID</c>).</summary>
public readonly record struct DuplexSendOutcome(int Sent, int Failed, IReadOnlyList<DuplexSendFailure> Failures);

/// <summary>One row <see cref="IDuplexSession.SendAsync"/> could not deliver. <see cref="CorrelationId"/>
/// and <see cref="SequenceNumber"/> are both nullable because what identifies a failed send is protocol-
/// specific (a FIX order has a <c>ClOrdID</c>; a session with no application-level identity has neither) —
/// <see cref="Reason"/> is the one field every duplex kind can always supply.</summary>
public sealed record DuplexSendFailure(string? CorrelationId, long? SequenceNumber, string Reason);

/// <summary>Plan 019 D3 (wave 019-B2): the one formatting rule for
/// <see cref="ConnectorRuntimeStatus.LastDuplexFailure"/>, shared so Orleans' <c>ConnectorGrain</c> and
/// Dapr's <c>ConnectorActor</c> render <see cref="IDuplexSession.LastFailure"/> identically rather than
/// each inventing its own string shape.</summary>
public static class DuplexSendFailureFormatting
{
    /// <summary>Correlation id first (a FIX order's <c>ClOrdID</c>) because that is the part an operator
    /// acts on; the sequence number, when known, follows in parentheses; <paramref name="failure"/> null
    /// (no failure yet on this session) formats as null, matching
    /// <see cref="ConnectorRuntimeStatus.LastDuplexFailure"/>'s own nullability.</summary>
    public static string? Format(this DuplexSendFailure? failure)
    {
        if (failure is null)
        {
            return null;
        }

        var seq = failure.SequenceNumber is { } n ? $" (seq {n})" : "";
        return failure.CorrelationId is null
            ? $"{failure.Reason}{seq}"
            : $"{failure.CorrelationId}: {failure.Reason}{seq}";
    }
}
