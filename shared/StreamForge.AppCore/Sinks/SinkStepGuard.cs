using System.Text.Json;

namespace StreamForge.AppCore.Sinks;

/// <summary>
/// Wishlist #9: the "bounded feedback loop" maxDepth guard, extracted from <see cref="HttpSinkClient"/>
/// (option (a), wishlist #9(a) — the first sink kind to need it) so <see cref="LoopbackSinkClient"/>
/// (option (b), the native in-process pair, wishlist #9(b)) reuses the EXACT same drop rule rather than
/// reimplementing it — 9(b)'s own brief is explicit: "the maxDepth guard must work exactly as it does in
/// the HTTP sink … reuse that logic rather than reimplementing it." Both <see cref="HttpSinkConfig"/> and
/// <see cref="LoopbackSinkConfig"/> carry the identical StepField/MaxDepth pair with the identical
/// semantics; this class is where that semantics actually lives now, called by both.
///
/// <para><b>The rule, restated once instead of twice.</b> maxDepth 0 = the guard is off — most sinks of
/// either kind are not a loop's own feedback edge, and a guard armed by default (dropping every row once
/// some unrelated field happened to be named "step") would be a worse default than none. A row missing
/// <c>StepField</c>, or carrying a non-numeric value there, is NOT dropped — the guard only fires on a row
/// that actually carries a recognizable step counter. Otherwise: dropped iff the row's step value is
/// &gt;= maxDepth. The check runs BEFORE any network call / hub write — a dropped row never leaves this
/// method.</para>
/// </summary>
internal static class SinkStepGuard
{
    /// <summary>True if a row carrying <paramref name="step"/> at <paramref name="stepField"/> must be
    /// DROPPED under <paramref name="maxDepth"/> — see this class's doc comment for the exact rule.
    /// <paramref name="step"/> is populated whenever a numeric value was found, regardless of the drop
    /// verdict, so a caller building a "dropped by maxDepth guard: field=N &gt;= maxDepth=M" message never
    /// needs a second lookup.</summary>
    public static bool ShouldDrop(Dictionary<string, object?> row, string stepField, int maxDepth, out long step)
    {
        var hasStep = TryGetStep(row, stepField, out step);
        return maxDepth > 0 && hasStep && step >= maxDepth;
    }

    /// <summary>Flattens a sink message to the row a loopback/HTTP sink forwards. Shared for the same
    /// reason as <see cref="ShouldDrop"/> — one flattening rule (including stamping <c>_weight</c> for a
    /// table delta, so a retraction is not indistinguishable from an insert downstream), not two copies
    /// that can silently drift.</summary>
    public static Dictionary<string, object?> RowOf<T>(T payload) => payload switch
    {
        NatsTableDeltaMessage d => new Dictionary<string, object?>(d.Row, StringComparer.Ordinal) { ["_weight"] = d.Weight },
        NatsPipelineRowMessage p => new Dictionary<string, object?>(p.Row, StringComparer.Ordinal),
        // No other payload type reaches a sink today; round-tripping it through JSON keeps a future one
        // visible in the forwarded row instead of silently empty.
        _ => JsonSerializer.Deserialize<Dictionary<string, object?>>(JsonSerializer.SerializeToUtf8Bytes(payload))
             ?? new Dictionary<string, object?>(StringComparer.Ordinal),
    };

    /// <summary>Reads <paramref name="field"/> off <paramref name="row"/> as an integer step counter.
    /// Absent, null, or a value that is not one of the numeric CLR shapes the engine's own coercion
    /// produces (see <c>FieldValueConversion</c>) returns false — a row without a recognizable step is
    /// let through, not dropped, per this class's doc comment. Double→long uses the same UNCHECKED
    /// narrowing cast <c>FieldValueConversion</c> documents using for the identical conversion elsewhere
    /// in this codebase, for the same consistency reason.</summary>
    private static bool TryGetStep(Dictionary<string, object?> row, string field, out long step)
    {
        step = 0;
        if (!row.TryGetValue(field, out var value) || value is null)
        {
            return false;
        }

        switch (value)
        {
            case long l:
                step = l;
                return true;
            case int i:
                step = i;
                return true;
            case double d:
                step = (long)d;
                return true;
            case decimal m:
                step = (long)m;
                return true;
            case string s when long.TryParse(s, out var parsed):
                step = parsed;
                return true;
            default:
                return false;
        }
    }
}
