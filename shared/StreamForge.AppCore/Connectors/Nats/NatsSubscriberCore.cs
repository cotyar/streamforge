using System.Text;
using StreamForge.Abstractions;
using StreamForge.AppCore.Connectors.Polling;

namespace StreamForge.AppCore.Connectors.Nats;

/// <summary>
/// The reconnecting subscribe loop behind a <c>nats</c>-kind source (plan 009 B1) — modeled directly on
/// <c>GrpcSubscriberCore</c> (read that class's doc comment first; this one only calls out where NATS
/// differs): a persistent background subscriber whose callbacks route back through a captured grain/
/// actor reference rather than touching runtime state directly. <see cref="INatsMessageSource"/> is the
/// seam that keeps this loop's reconnect/backoff/dispatch logic unit-testable without a live broker (see
/// that interface's doc comment).
///
/// <para><b>Payload → row</b> goes through the EXACT SAME format/mapping path a polled HTTP body uses —
/// <see cref="ConnectorPollCycle.ExecuteNatsMessage"/> (parse per <see cref="NatsSubConfig.Format"/>,
/// extract per <see cref="ConnectorPollCycle.EffectiveMapping"/>, coerce per
/// <see cref="SourceDefinition.OnCoercionFailure"/>, dedup per <c>MappingSpec.DedupKeyField</c>, stamp
/// "_source"/"_ts") — there is deliberately no second extraction path for NATS.</para>
///
/// <para><b>Reconnection</b>: forever, honoring <paramref name="ct"/> at every await, with the exact same
/// LOCALLY-computed D-E backoff formula (<c>min(30s * 2^(k-1), 15 min)</c>) as <c>GrpcSubscriberCore</c>
/// — duplicated for the same "keep this file's ownership self-contained" reason that class's doc
/// comment gives, pinned identically so both copies agree. A clean end of the underlying subscription/
/// consumer enumerable (the connection closed without an error) reconnects immediately with no backoff
/// and resets the failure counter, same as a successful poll cycle would.</para>
///
/// <para><b>One bad message never tears down the subscription</b>: a per-message parse/mapping/coercion
/// failure (including a <see cref="CoercionFailurePolicy.RejectBatch"/> rejection) is reported via
/// <paramref name="onStatus"/>("error", …) and the loop continues to the next message — only a
/// CONNECTION-level failure (thrown out of the enumerable itself) triggers the reconnect/backoff path.
/// A JetStream message that was never acked because of such a failure is redelivered per the consumer's
/// AckWait — core NATS has no redelivery to skip in the first place.</para>
/// </summary>
public sealed class NatsSubscriberCore
{
    private static readonly TimeSpan BaseDelay = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan MaxDelay = TimeSpan.FromMinutes(15);

    private readonly SourceDefinition _def;
    private readonly NatsSubConfig _config;
    private readonly DedupTracker _dedup;
    private readonly Func<IReadOnlyList<Dictionary<string, object?>>, long, Task> _onRows;
    private readonly Action<string, string?> _onStatus;
    private readonly Func<INatsMessageSource> _sourceFactory;

    public NatsSubscriberCore(
        SourceDefinition def,
        DedupTracker dedup,
        Func<IReadOnlyList<Dictionary<string, object?>>, long, Task> onRows,
        Action<string, string?> onStatus,
        Func<INatsMessageSource>? sourceFactory = null)
    {
        _def = def ?? throw new ArgumentNullException(nameof(def));
        _config = def.Connector?.Nats ?? throw new InvalidOperationException($"source '{def.Name}' has kind 'nats' but no nats config");
        _dedup = dedup ?? throw new ArgumentNullException(nameof(dedup));
        _onRows = onRows ?? throw new ArgumentNullException(nameof(onRows));
        _onStatus = onStatus ?? throw new ArgumentNullException(nameof(onStatus));
        _sourceFactory = sourceFactory ?? (() => new NatsClientMessageSource($"streamforge-source-{def.Name}"));
    }

    public async Task RunAsync(CancellationToken ct)
    {
        var failures = 0;
        long seq = 0;

        while (!ct.IsCancellationRequested)
        {
            _onStatus("connecting", null);
            var source = _sourceFactory();
            try
            {
                _onStatus("ok", null);

                await foreach (var msg in source.SubscribeAsync(_config, ct).ConfigureAwait(false))
                {
                    await ProcessMessageAsync(msg, ++seq).ConfigureAwait(false);
                }

                if (ct.IsCancellationRequested)
                {
                    return;
                }

                failures = 0; // reached here without throwing -> clean disconnect, reconnect with no backoff
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                failures++;
                _onStatus("error", $"{ex.GetType().Name}: {ex.Message}");

                try
                {
                    await Task.Delay(ComputeBackoffDelay(failures), ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
            finally
            {
                await source.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    /// <summary>One message: decode → run the shared poll-cycle pipeline (parse/extract/coerce/dedup/
    /// stamp) → hand surviving rows to <paramref name="_onRows"/> → ack (JetStream only, and only on a
    /// clean outcome). Never throws for a message-level problem — those become <c>onStatus("error", …)</c>
    /// calls so the subscription itself stays up; only lets a genuine callback exception (from
    /// <paramref name="_onRows"/> itself) propagate, exactly like GrpcSubscriberCore's pump loop does,
    /// so the outer reconnect/backoff machinery sees it.</summary>
    private async Task ProcessMessageAsync(NatsInboundMessage msg, long seq)
    {
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        string text;
        try
        {
            text = Encoding.UTF8.GetString(msg.Payload);
        }
        catch (Exception ex)
        {
            _onStatus("error", $"malformed payload on subject '{msg.Subject}': {ex.Message}");
            return;
        }

        PollCycleResult result;
        try
        {
            result = ConnectorPollCycle.ExecuteNatsMessage(_def, _config.Format, text, _dedup, nowMs);
        }
        catch (Exception ex)
        {
            _onStatus("error", $"{ex.GetType().Name}: {ex.Message}");
            return;
        }

        if (result.Error is not null)
        {
            // Parse failure, or a CoercionFailurePolicy.RejectBatch rejection — coerce-before-admission:
            // nothing from this message is emitted, and it is deliberately left unacked below (a
            // JetStream redelivery is the honest "at least once, will retry" outcome; core NATS has no
            // redelivery to skip).
            _onStatus("error", result.Error);
            return;
        }

        if (result.Rows.Count > 0)
        {
            await _onRows(result.Rows, seq).ConfigureAwait(false);
        }

        if (result.CoercionFailures > 0)
        {
            // Plan 009 C2: the failure is counted and surfaced even under the lenient Null/DropRow
            // policies, which never produce a non-null Error above — an "ok" status carrying a note is
            // this loop's only channel back to the driver (ConnectorGrain/ConnectorActor), which folds
            // it into ConnectorRuntimeStatus.LastError.
            _onStatus("ok", $"{result.CoercionFailures} field coercion failure(s) on this message; policy={_def.OnCoercionFailure}");
        }

        if (msg.AckAsync is not null)
        {
            await msg.AckAsync().ConfigureAwait(false);
        }
    }

    private static TimeSpan ComputeBackoffDelay(int consecutiveFailures)
    {
        if (consecutiveFailures <= 0)
        {
            return TimeSpan.Zero;
        }
        var exponent = Math.Min(consecutiveFailures - 1, 30); // clamp well before double overflow
        var scaledMs = BaseDelay.TotalMilliseconds * Math.Pow(2, exponent);
        return TimeSpan.FromMilliseconds(Math.Min(scaledMs, MaxDelay.TotalMilliseconds));
    }
}
