using System.Text;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Connectors;
using StreamsForge.AppCore.Connectors.Polling;

namespace StreamsForge.AppCore.Transports;

/// <summary>
/// Plan 010: the reconnecting subscribe loop shared by EVERY message transport — generalized from plan 009
/// B1's <c>NatsSubscriberCore</c>, whose logic turned out to be NATS-specific in exactly two expressions
/// (which config object to read, and which format field to parse payloads as). Both now come from
/// <see cref="IInboundTransport"/>, so this loop — reconnect, backoff, per-message parse/coerce/dedup,
/// coercion-failure reporting, ack discipline — exists once for all transports instead of once per
/// transport. <c>NatsSubscriberCore</c> is a thin wrapper over this class and its 390-line test suite
/// covers this behavior unchanged, which is the proof the generalization preserved it.
///
/// <para><b>Payload → row</b> goes through the shared format/mapping path
/// (<see cref="ConnectorPollCycle.ExecuteMessage"/>: parse per <see cref="IInboundTransport.FormatOf"/>,
/// extract per <c>ConnectorPollCycle.EffectiveMapping</c>, coerce per
/// <see cref="SourceDefinition.OnCoercionFailure"/>, dedup per <c>MappingSpec.DedupKeyField</c>, stamp
/// "_source"/"_ts") — the same one a polled HTTP body uses. There is deliberately no per-transport
/// extraction path.</para>
///
/// <para><b>Reconnection</b>: forever, honoring <c>ct</c> at every await, with the D-E backoff formula
/// (<c>min(30s * 2^(k-1), 15 min)</c>) — pinned identically to <c>GrpcSubscriberCore</c>'s own copy. A clean
/// end of the underlying subscription (the connection closed without an error) reconnects immediately with
/// no backoff and resets the failure counter, same as a successful poll cycle would.</para>
///
/// <para><b>One bad message never tears down the subscription</b>: a per-message parse/mapping/coercion
/// failure (including a <see cref="CoercionFailurePolicy.RejectBatch"/> rejection) is reported via
/// <c>onStatus("error", …)</c> and the loop continues — only a CONNECTION-level failure (thrown out of the
/// enumerable itself) triggers reconnect/backoff. A message left unacked because of such a failure is
/// redelivered if the transport supports redelivery at all; that is the honest at-least-once cost of asking
/// for acks, and a transport without them has nothing to skip.</para>
/// </summary>
public sealed class SubscriberCore
{
    private static readonly TimeSpan BaseDelay = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan MaxDelay = TimeSpan.FromMinutes(15);

    private readonly SourceDefinition _def;
    private readonly IInboundTransport _transport;
    private readonly DedupTracker _dedup;
    private readonly Func<IReadOnlyList<Dictionary<string, object?>>, long, Task> _onRows;
    private readonly Action<string, string?> _onStatus;

    /// <summary>Plan 009 C2: the COUNTING half of "counted and surfaced", separate from
    /// <see cref="_onStatus"/> on purpose. Folding a count into the status callback would have widened a
    /// shape several call sites and tests already depend on, to carry a value that is zero on every
    /// status transition and non-zero only here. Optional: a driver that does not count still gets the
    /// note through the status channel.</summary>
    private readonly Action<int>? _onCoercionFailures;

    public SubscriberCore(
        SourceDefinition def,
        IInboundTransport transport,
        DedupTracker dedup,
        Func<IReadOnlyList<Dictionary<string, object?>>, long, Task> onRows,
        Action<string, string?> onStatus,
        Action<int>? onCoercionFailures = null)
    {
        _def = def ?? throw new ArgumentNullException(nameof(def));
        _transport = transport ?? throw new ArgumentNullException(nameof(transport));
        _dedup = dedup ?? throw new ArgumentNullException(nameof(dedup));
        _onRows = onRows ?? throw new ArgumentNullException(nameof(onRows));
        _onStatus = onStatus ?? throw new ArgumentNullException(nameof(onStatus));
        _onCoercionFailures = onCoercionFailures;
    }

    public async Task RunAsync(CancellationToken ct)
    {
        var failures = 0;
        long seq = 0;

        while (!ct.IsCancellationRequested)
        {
            _onStatus("connecting", null);
            var subscription = _transport.Open(_def);
            try
            {
                _onStatus("ok", null);

                await foreach (var msg in subscription.SubscribeAsync(ct).ConfigureAwait(false))
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
                await subscription.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    /// <summary>One message: decode → run the shared poll-cycle pipeline (parse/extract/coerce/dedup/
    /// stamp) → hand surviving rows to <c>onRows</c> → ack (only where the transport supports it, and only
    /// on a clean outcome). Never throws for a message-level problem — those become
    /// <c>onStatus("error", …)</c> calls so the subscription itself stays up; only lets a genuine callback
    /// exception (from <c>onRows</c> itself) propagate, exactly like <c>GrpcSubscriberCore</c>'s pump loop
    /// does, so the outer reconnect/backoff machinery sees it.</summary>
    private async Task ProcessMessageAsync(InboundMessage msg, long seq)
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
            result = ConnectorPollCycle.ExecuteMessage(_def, _transport.FormatOf(_def), text, _dedup, nowMs);
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
            // redelivery is the honest "at least once, will retry" outcome on a transport that has one).
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
            _onCoercionFailures?.Invoke(result.CoercionFailures);
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
