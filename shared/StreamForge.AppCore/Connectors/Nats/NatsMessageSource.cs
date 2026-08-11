using System.Runtime.CompilerServices;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NATS.Net;
using StreamForge.Abstractions;
using StreamForge.AppCore.Nats;

namespace StreamForge.AppCore.Connectors.Nats;

/// <summary>One received NATS message: the subject it actually arrived on (a wildcard subscription's
/// subject may differ from <see cref="NatsSubConfig.Subject"/>), the raw payload bytes, and — for a
/// JetStream delivery only — the ack callback. Null <see cref="AckAsync"/> means core NATS: there is no
/// redelivery to ack, same "at-most-once, nothing left behind" contract <see cref="NatsSubConfig.JetStream"/>'s
/// doc comment describes.</summary>
public sealed record NatsInboundMessage(string Subject, byte[] Payload, Func<Task>? AckAsync);

/// <summary>
/// The thin seam plan 009 B1 asks for: everything <see cref="NatsSubscriberCore"/> needs from an actual
/// NATS connection, factored out of the reconnect/backoff/dispatch loop so that loop is unit-testable
/// with a fake — there is no NATS server in this sandbox (verified once; Docker Hub was unreachable in
/// earlier sessions too), so every test exercising <see cref="NatsSubscriberCore"/> substitutes an
/// <see cref="INatsMessageSource"/> fake instead of dialing a broker. <see cref="NatsClientMessageSource"/>
/// is the one real implementation, built on <c>NATS.Net</c>; it is exercised only by the live-check step
/// of this wave's verification (a `nats`-kind source reporting a "degraded, not crashed" status against
/// an unreachable broker), never by the test suite.
/// </summary>
public interface INatsMessageSource : IAsyncDisposable
{
    /// <summary>Connects (core NATS: <see cref="NatsConnection.ConnectAsync"/> on first iteration;
    /// JetStream: also creates/updates the durable consumer named by
    /// <see cref="NatsJetStreamConfig.Durable"/>) and yields messages until <paramref name="ct"/> is
    /// cancelled or the underlying subscription/consumer enumerable ends on its own. Never returns
    /// more than once per instance — callers construct a fresh <see cref="INatsMessageSource"/> per
    /// (re)connect attempt, mirroring <c>GrpcSubscriberCore</c>'s per-attempt channel.</summary>
    IAsyncEnumerable<NatsInboundMessage> SubscribeAsync(NatsSubConfig config, CancellationToken ct);
}

/// <summary>Real <see cref="INatsMessageSource"/> over <c>NATS.Net</c>. <see cref="NatsConnectionSettings.Build"/>
/// (plan 009, already written — do not reimplement) turns <see cref="NatsSubConfig"/>'s credential
/// fields into the <see cref="NatsOpts"/> this connects with. <see cref="NatsSubConfig.JetStream"/> null
/// (the default) subscribes core NATS — at-most-once, no cursor, nothing left behind on the server —
/// honoring <see cref="NatsSubConfig.QueueGroup"/> so two replicas sharing one group split the subject
/// instead of both ingesting everything (this path's answer to the per-replica problem
/// <c>IngestStatus.Aggregated</c> documents on the push-ingress side). Non-null JetStream instead
/// creates-or-updates ("Creates new consumer if it doesn't exist or updates an existing one with the
/// same name" — <c>INatsJSContext.CreateOrUpdateConsumerAsync</c>'s own doc) an EXPLICIT-ack durable
/// consumer and yields its messages with an ack callback wired to <c>INatsJSMsg.AckAsync</c>; a message
/// the caller never acks (e.g. a RejectBatch coercion rejection) is redelivered per the consumer's
/// AckWait — the honest at-least-once cost of asking for redelivery at all.</summary>
public sealed class NatsClientMessageSource(string clientName) : INatsMessageSource
{
    private NatsConnection? _connection;

    public NatsClientMessageSource() : this("streamforge-nats-source")
    {
    }

    public async IAsyncEnumerable<NatsInboundMessage> SubscribeAsync(NatsSubConfig config, [EnumeratorCancellation] CancellationToken ct)
    {
        var opts = NatsConnectionSettings.Build(config.Url, config.Token, config.Username, config.Password, config.Credentials, clientName);
        var connection = new NatsConnection(opts);
        _connection = connection;
        await connection.ConnectAsync().ConfigureAwait(false);

        if (config.JetStream is { } js)
        {
            await foreach (var msg in ConsumeJetStreamAsync(connection, config, js, ct).ConfigureAwait(false))
            {
                yield return msg;
            }
        }
        else
        {
            var queueGroup = string.IsNullOrEmpty(config.QueueGroup) ? null : config.QueueGroup;
            await foreach (var msg in connection.SubscribeAsync<byte[]>(config.Subject, queueGroup, cancellationToken: ct).ConfigureAwait(false))
            {
                yield return new NatsInboundMessage(msg.Subject, msg.Data ?? [], AckAsync: null);
            }
        }
    }

    private static async IAsyncEnumerable<NatsInboundMessage> ConsumeJetStreamAsync(
        NatsConnection connection, NatsSubConfig config, NatsJetStreamConfig js, [EnumeratorCancellation] CancellationToken ct)
    {
        var jsContext = connection.CreateJetStreamContext();
        var consumerConfig = new ConsumerConfig(js.Durable)
        {
            DurableName = js.Durable,
            FilterSubject = config.Subject,
            AckPolicy = ConsumerConfigAckPolicy.Explicit,
            MaxAckPending = js.MaxAckPending,
        };
        var consumer = await jsContext.CreateOrUpdateConsumerAsync(js.Stream, consumerConfig, ct).ConfigureAwait(false);

        await foreach (var msg in consumer.ConsumeAsync<byte[]>(cancellationToken: ct).ConfigureAwait(false))
        {
            yield return new NatsInboundMessage(msg.Subject, msg.Data ?? [], AckAsync: () => msg.AckAsync(cancellationToken: ct).AsTask());
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_connection is not null)
        {
            await _connection.DisposeAsync().ConfigureAwait(false);
        }
    }
}
