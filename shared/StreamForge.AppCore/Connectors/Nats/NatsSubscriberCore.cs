using StreamForge.Abstractions;
using StreamForge.AppCore.Connectors.Polling;
using StreamForge.AppCore.Transports;

namespace StreamForge.AppCore.Connectors.Nats;

/// <summary>
/// The <c>nats</c>-kind subscriber, plan 009 B1. Since plan 010 the loop itself — reconnect, backoff,
/// per-message parse/extract/coerce/dedup/stamp, coercion-failure reporting, ack discipline — is
/// <see cref="SubscriberCore"/>'s and is shared by every message transport; NATS's own share is
/// <see cref="NatsInboundTransport"/> (which config, what makes it valid, how to open a subscription).
///
/// <para><b>Why this type still exists rather than call sites constructing a
/// <see cref="SubscriberCore"/> directly.</b> Its constructor is the pinned surface a 390-line test suite
/// drives — including the fake <see cref="INatsMessageSource"/> that makes the loop testable without a
/// broker. Keeping the signature byte-identical is what let plan 010 generalize the loop and prove it
/// behavior-preserving with those tests untouched; the alternative (rewriting the tests to the new shape)
/// would have discarded exactly the evidence that mattered.</para>
/// </summary>
public sealed class NatsSubscriberCore
{
    private readonly SubscriberCore _core;

    public NatsSubscriberCore(
        SourceDefinition def,
        DedupTracker dedup,
        Func<IReadOnlyList<Dictionary<string, object?>>, long, Task> onRows,
        Action<string, string?> onStatus,
        Func<INatsMessageSource>? sourceFactory = null,
        Action<int>? onCoercionFailures = null)
    {
        ArgumentNullException.ThrowIfNull(def);

        // Eager, at construction, exactly as before: a nats-kind source with no nats config is a
        // configuration error the caller should see when it arms the subscriber, not a connect-time one
        // reported as a transient status.
        _ = def.Connector?.Nats ?? throw new InvalidOperationException($"source '{def.Name}' has kind 'nats' but no nats config");

        _core = new SubscriberCore(
            def, new NatsInboundTransport(sourceFactory), dedup, onRows, onStatus, onCoercionFailures);
    }

    public Task RunAsync(CancellationToken ct) => _core.RunAsync(ct);
}
