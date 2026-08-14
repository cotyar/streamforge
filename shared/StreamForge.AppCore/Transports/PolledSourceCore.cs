using StreamForge.Abstractions;
using StreamForge.AppCore.Connectors;
using StreamForge.AppCore.Connectors.Polling;

namespace StreamForge.AppCore.Transports;

/// <summary>One driven cycle: the rows-and-error shape every connector path already reports through, the
/// cursor to persist, and whether to re-arm immediately. <see cref="Cursor"/> is ALWAYS a value the caller
/// can persist unconditionally — on a failed cycle it is the cursor that was passed in, not null and not a
/// partially-advanced one.</summary>
public sealed record PolledCycleOutcome(PollCycleResult Result, string? Cursor, bool HasMore);

/// <summary>
/// Plan 014: the part of driving an <see cref="IPolledTransport"/> that is genuinely identical on both
/// runtimes — poll, turn rows into events, decide the next cursor — so that each flavour driver
/// (<c>ConnectorGrain</c> on Orleans, <c>ConnectorActor</c> on Dapr) needs one switch arm and a
/// <see cref="PolledCycleOutcome.HasMore"/> check rather than a second copy of the cursor rules.
///
/// <para><b>What is deliberately NOT here:</b> the timer, the backoff, status marshalling, emission and
/// persistence. Those look similar across the flavours and are not: Orleans reminders/timers and grain
/// state are not Dapr timers and actor state, the two persist at different points, and pretending
/// otherwise would produce an abstraction with two implementations and no third — the cost without the
/// benefit. The invariants that actually matter live here; the plumbing stays flavour-owned.</para>
///
/// <para><b>The one load-bearing rule: a failed cycle keeps the OLD cursor.</b> Advancing past rows that
/// were never emitted skips data permanently and silently, and the failure that would do it is a transport
/// bug — which is exactly why the rule is enforced out here rather than inside each transport. "Failed"
/// includes both a throwing <see cref="IPolledTransport.PollAsync"/> and a batch that
/// <see cref="ConnectorPollCycle.ExecuteRows"/> rejects (a
/// <see cref="CoercionFailurePolicy.RejectBatch"/> rejection emits nothing, so its rows must stay
/// re-readable, same coerce-before-admission rule every other inbound path follows). The cost is honest and
/// stated: re-reading a batch is at-least-once, and a batch that fails deterministically is re-read every
/// cycle until the operator fixes it — visible in the error status, which is the outcome to prefer over a
/// gap nobody notices.</para>
/// </summary>
public static class PolledSourceCore
{
    /// <summary>Runs one cycle. Never throws — a transport failure comes back as
    /// <see cref="PollCycleResult.Error"/> with the incoming <paramref name="cursor"/> intact, because the
    /// caller's next act is to persist whatever it is handed.</summary>
    /// <param name="cursor">The persisted cursor, or null on this source's first ever cycle.</param>
    /// <param name="dedup">The driver's persisted tracker; mutated in place on a successful cycle exactly
    /// as every other <c>ConnectorPollCycle</c> entry point mutates it.</param>
    /// <param name="dedupKeyField">Which emitted field dedups re-read rows — for a database source, the
    /// companion to a <c>&gt;=</c> cursor that re-reads its own watermark. Null disables dedup for this
    /// cycle. It is supplied by the caller rather than read from <c>MappingSpec.DedupKeyField</c> because a
    /// polled row source has no mapping document at all; the driver reads it from the kind's own config.
    /// (Additive trailing parameter — the four-argument-plus-ct call in plan 014's pinned signature
    /// compiles unchanged and simply dedups nothing.)</param>
    public static async Task<PolledCycleOutcome> RunCycleAsync(
        IPolledTransport transport,
        SourceDefinition def,
        string? cursor,
        DedupTracker dedup,
        long nowMs,
        CancellationToken ct,
        string? dedupKeyField = null)
    {
        ArgumentNullException.ThrowIfNull(transport);
        ArgumentNullException.ThrowIfNull(def);
        ArgumentNullException.ThrowIfNull(dedup);

        PolledBatch batch;
        try
        {
            batch = await transport.PollAsync(def, cursor, ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // A shutdown is not a data problem. Nothing was read, nothing advances, and the driver is
            // already tearing down — reporting an error here would leave a spurious one on the status.
            throw;
        }
        catch (Exception ex)
        {
            return Failed(cursor, $"{ex.GetType().Name}: {ex.Message}");
        }

        if (batch is null)
        {
            // A transport returning null instead of an empty batch is a bug in the transport, not a reason
            // to NullReferenceException out of a driver's timer callback.
            return Failed(cursor, $"transport '{transport.Kind}' returned a null batch");
        }

        PollCycleResult result;
        try
        {
            result = ConnectorPollCycle.ExecuteRows(def, batch.Rows, dedupKeyField, dedup, nowMs);
        }
        catch (Exception ex)
        {
            return Failed(cursor, $"{ex.GetType().Name}: {ex.Message}");
        }

        if (result.Error is not null)
        {
            // Rows were read but none admitted. Keep the cursor so they are re-read, and do NOT re-arm —
            // HasMore on a rejected batch would spin the driver against the same failing rows at full speed.
            return new PolledCycleOutcome(result, cursor, HasMore: false);
        }

        // null Cursor = "leave it unchanged", which is what an empty poll returns. It is not "reset".
        return new PolledCycleOutcome(result, batch.Cursor ?? cursor, batch.HasMore);
    }

    private static PolledCycleOutcome Failed(string? cursor, string error)
        => new(new PollCycleResult([], error), cursor, HasMore: false);
}
