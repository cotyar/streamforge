using StreamForge.Abstractions;

namespace StreamForge.AppCore.Transports;

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
