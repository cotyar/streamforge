using StreamForge.Abstractions;

namespace StreamForge.AppCore.Ingest;

/// <summary>
/// Pure admission-control policy for one client-push batch against a source's ingress buffer (plan
/// 008 W4) — ALL of <see cref="IngressOverflowPolicy"/>'s semantics live here and nowhere else, so
/// REST/gRPC/whatever transport comes next, and both runtime flavors, decide identically.
/// Infrastructure-free (no locks, no I/O, no wall-clock) so it is exhaustively testable: every
/// policy x under/at/over capacity x oversized batch, with zero setup — mirrors this repo's
/// SourceValidation convention of pure, static, side-effect-free rule evaluation.
/// </summary>
public static class IngressAdmission
{
    /// <summary>Server cap on <see cref="IngestConfig.MaxWaitMs"/> for
    /// <see cref="IngressOverflowPolicy.Block"/> — a client cannot make the server thread wait longer
    /// than this no matter how it configures the source.</summary>
    public const int MaxBlockWaitMs = 30_000;

    public enum AdmissionKind
    {
        /// <summary>Admit <see cref="Decision.Admit"/> rows — all of the batch, or fewer under
        /// DropNewest/DropOldest (see <see cref="Decision.Drop"/> / <see cref="Decision.Evict"/>).</summary>
        Admit,

        /// <summary>429: no room and the policy is Reject, or a Block wait's deadline passed.</summary>
        Reject,

        /// <summary>413: batch exceeds MaxBatchRows or total capacity — always whole-batch, never a
        /// partial admit, regardless of policy.</summary>
        TooLarge,

        /// <summary>Policy is Block and there is no room yet; the caller must wait for space to free
        /// up and re-<see cref="Decide"/> (see <see cref="SourceIngressBuffer"/>).</summary>
        Wait,
    }

    /// <param name="Kind">What happens to the batch.</param>
    /// <param name="Admit">Rows to admit from the FRONT of the incoming batch.</param>
    /// <param name="Drop">Rows discarded from the incoming batch itself (DropNewest) — always
    /// reported on the 202, never silent.</param>
    /// <param name="Evict">Existing buffered rows to evict from the buffer's HEAD to make room
    /// (DropOldest) — reported via the same "dropped" count as <see cref="Drop"/>.</param>
    /// <param name="RetryAfterMs">Honest milliseconds derived from the observed drain rate; the REST
    /// layer clamps this to whole seconds in [1,30] — this returns the un-clamped estimate.</param>
    public readonly record struct Decision(AdmissionKind Kind, int Admit, int Drop, int Evict, int RetryAfterMs);

    /// <summary>Decides what happens to one incoming batch of <paramref name="batchSize"/> rows
    /// against a buffer currently holding <paramref name="depth"/> rows, per <paramref name="config"/>.
    /// <paramref name="drainRowsPerMs"/> is the caller's observed drain rate (0 when unknown, e.g. a
    /// freshly created buffer) — used only to estimate <see cref="Decision.RetryAfterMs"/>.</summary>
    public static Decision Decide(int depth, int batchSize, IngestConfig config, double drainRowsPerMs)
    {
        if (batchSize <= 0)
        {
            return new Decision(AdmissionKind.Admit, 0, 0, 0, 0);
        }

        if (config.Policy == IngressOverflowPolicy.Inline)
        {
            // No buffer at all, so total capacity is meaningless; only the single-batch ceiling applies.
            return batchSize > config.MaxBatchRows
                ? new Decision(AdmissionKind.TooLarge, 0, 0, 0, 0)
                : new Decision(AdmissionKind.Admit, batchSize, 0, 0, 0);
        }

        if (batchSize > config.MaxBatchRows || batchSize > config.CapacityRows)
        {
            return new Decision(AdmissionKind.TooLarge, 0, 0, 0, 0);
        }

        var free = Math.Max(0, config.CapacityRows - depth);
        if (batchSize <= free)
        {
            return new Decision(AdmissionKind.Admit, batchSize, 0, 0, 0);
        }

        var deficit = batchSize - free;
        return config.Policy switch
        {
            IngressOverflowPolicy.Reject =>
                new Decision(AdmissionKind.Reject, 0, 0, 0, EstimateRetryAfterMs(deficit, drainRowsPerMs)),
            IngressOverflowPolicy.Block =>
                new Decision(AdmissionKind.Wait, 0, 0, 0, EstimateRetryAfterMs(deficit, drainRowsPerMs)),
            IngressOverflowPolicy.DropNewest =>
                new Decision(AdmissionKind.Admit, free, deficit, 0, 0),
            IngressOverflowPolicy.DropOldest =>
                new Decision(AdmissionKind.Admit, batchSize, deficit, deficit, 0),
            _ => throw new ArgumentOutOfRangeException(nameof(config), config.Policy, "unknown overflow policy"),
        };
    }

    /// <summary>Deficit rows / observed drain rate, ceilinged to a whole millisecond. No observed
    /// drain yet (rate &lt;= 0, e.g. a source that has never published) returns a sane 1s default
    /// hint rather than a false-precision zero.</summary>
    private static int EstimateRetryAfterMs(int deficitRows, double drainRowsPerMs)
    {
        if (drainRowsPerMs <= 0)
        {
            return 1000;
        }

        var ms = deficitRows / drainRowsPerMs;
        return ms switch
        {
            <= 0 => 1,
            > int.MaxValue => int.MaxValue,
            _ => (int)Math.Ceiling(ms),
        };
    }
}
