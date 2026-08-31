using StreamsForge.Abstractions;
using StreamsForge.AppCore.Ingest;

namespace StreamsForge.AppCore.Connectors;

/// <summary>
/// Plan 009 C2: applies a source's declared field types to already-extracted connector rows, honoring
/// <see cref="SourceDefinition.OnCoercionFailure"/> — the piece push ingress already had
/// (<c>AppCore.Ingest.IngressRowAcceptance</c>) and the connector/mapping path did not. Uses
/// <see cref="FieldValueCoercion.TryCoerce"/> (the one canonical conversion implementation; do not
/// re-derive the rules here) so a value that fails to coerce is judged identically on every inbound
/// path.
///
/// <para>Deliberately a SEPARATE type from <c>IngressRowAcceptance</c> rather than a shared call into
/// it: that class is owned by the Ingest wave (plan 009 A1/A1.1) and lives under
/// <c>AppCore/Ingest/</c>, which this wave (B1/C2) does not touch (see plan 009's file-ownership
/// split) — also, its row-acceptance shape (fail-the-whole-row, unknown-field handling, "_ts"/"_source"
/// stamping) answers a different question than a POLICY-driven per-field coercion over rows that are
/// already fully shaped by <c>RecordExtractor</c>/<c>ProtoWireDecoder</c>.</para>
/// </summary>
public static class ConnectorRowCoercion
{
    /// <summary><see cref="Rows"/> is the surviving row set (Null policy: same rows, failing fields set
    /// to null; DropRow: rows with any failure removed entirely) — empty and meaningless when
    /// <see cref="BatchRejected"/> is true. <see cref="FailureCount"/> counts every field-level failure
    /// encountered before any RejectBatch short-circuit (so it is always &gt;= 1 when BatchRejected is
    /// true, even though the batch's remaining rows/fields were never examined).</summary>
    public sealed record Result(List<Dictionary<string, object?>> Rows, int FailureCount, bool BatchRejected, string? RejectReason);

    /// <summary>Coerces every DECLARED field (<paramref name="fields"/>) present on each row in
    /// <paramref name="rows"/> to its <see cref="FieldDef.Type"/>, mutating rows in place (array fields
    /// coerce element-by-element, a null element is skipped — same convention
    /// <c>IngressRowAcceptance.TryCoerceField</c> uses). A field absent from a row, or already null, is
    /// left alone (nothing to coerce) — same "absent = omitted" convention as every extraction path in
    /// this codebase. Undeclared keys (e.g. "_source"/"_ts") are never touched.
    ///
    /// <list type="bullet">
    /// <item><see cref="CoercionFailurePolicy.Null"/>: the failing field becomes null; the row is kept
    /// (the pre-009 lenient default, formalized).</item>
    /// <item><see cref="CoercionFailurePolicy.DropRow"/>: the whole row is dropped from
    /// <see cref="Result.Rows"/> — remaining fields on that row are still visited for counting, but
    /// their values are irrelevant since the row won't be emitted.</item>
    /// <item><see cref="CoercionFailurePolicy.RejectBatch"/>: the FIRST failure anywhere in the batch
    /// rejects the whole thing — <see cref="Result.Rows"/> comes back empty and
    /// <see cref="Result.BatchRejected"/> is true, so the caller can refuse admission with nothing left
    /// behind (same "coerce before admission" rule plan 009 A1.1 states for push ingress).</item>
    /// </list>
    /// </summary>
    public static Result Apply(IReadOnlyList<FieldDef> fields, List<Dictionary<string, object?>> rows, CoercionFailurePolicy policy)
    {
        var failureCount = 0;
        var kept = new List<Dictionary<string, object?>>(rows.Count);

        foreach (var row in rows)
        {
            var rowHadFailure = false;
            foreach (var f in fields)
            {
                if (!row.TryGetValue(f.Name, out var value) || value is null)
                {
                    continue; // absent/null -> nothing to coerce
                }

                if (TryCoerceField(f, value, out var coerced))
                {
                    row[f.Name] = coerced;
                    continue;
                }

                failureCount++;
                rowHadFailure = true;

                if (policy == CoercionFailurePolicy.RejectBatch)
                {
                    return new Result([], failureCount, true, $"field \"{f.Name}\" cannot be coerced to {f.Type}");
                }

                row[f.Name] = null; // Null policy keeps this; DropRow's row is dropped below regardless
            }

            if (rowHadFailure && policy == CoercionFailurePolicy.DropRow)
            {
                continue;
            }

            kept.Add(row);
        }

        return new Result(kept, failureCount, false, null);
    }

    private static bool TryCoerceField(FieldDef f, object value, out object? coerced)
    {
        if (!f.IsArray)
        {
            return FieldValueCoercion.TryCoerce(f.Type, value, out coerced);
        }

        if (value is not IEnumerable<object?> list)
        {
            coerced = null;
            return false;
        }

        var result = new List<object?>();
        foreach (var element in list)
        {
            if (element is null)
            {
                continue; // proto3 convention: a null list element carries no value
            }

            if (!FieldValueCoercion.TryCoerce(f.Type, element, out var c))
            {
                coerced = null;
                return false;
            }

            result.Add(c);
        }

        coerced = result;
        return true;
    }
}
