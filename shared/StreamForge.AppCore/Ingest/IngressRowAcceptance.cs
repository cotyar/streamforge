using StreamForge.Abstractions;
using StreamForge.AppCore.Json;

namespace StreamForge.AppCore.Ingest;

/// <summary>Result of running one client-pushed row through <see cref="IngressRowAcceptance.Accept"/>.
/// <see cref="Row"/> is non-null exactly when the row was accepted (<see cref="Accepted"/>);
/// <see cref="Error"/> is non-null exactly when it was not.</summary>
public sealed record RowAcceptanceResult(Dictionary<string, object?>? Row, int UnknownFieldsDropped, string? Error)
{
    public bool Accepted => Row is not null;
}

/// <summary>Result of running a whole batch through <see cref="IngressRowAcceptance.AcceptBatch"/>:
/// every row is coerced before any admission decision is made (IngestModels.cs's header — coercion
/// happens BEFORE admission so a rejected/invalid batch never leaves partial state), so by the time
/// this returns both <see cref="Accepted"/> and <see cref="RowErrors"/> are final and the caller can
/// decide whole-batch-reject vs. <c>partial</c> admit with no further row inspection.</summary>
public sealed record BatchAcceptanceResult(List<Dictionary<string, object?>> Accepted, int UnknownFieldsDropped, List<string> RowErrors);

/// <summary>
/// The row-acceptance pipeline every client-push ingest transport (REST today; whatever bidi/gRPC
/// path comes next) runs a row through before it is eligible for admission into a
/// <see cref="SourceIngressBuffer"/>. Order, per row:
/// <list type="number">
/// <item><see cref="JsonValueNormalizer.NormalizeInPlace"/> — JsonElement leaves become plain CLR
/// values, exactly like every other pub/sub ingress boundary in this codebase.</item>
/// <item>Coerce every DECLARED field via <see cref="FieldValueCoercion.TryCoerce"/> (array fields
/// coerce element-by-element; a null element is skipped, matching ProtoWireEncoder's repeated-field
/// writer). Any failure fails the WHOLE row, not just the field — coercion is all-or-nothing per row
/// so a batch's accepted/invalid split never depends on how far through a bad row we got.</item>
/// <item>Keys that are neither a declared field nor "_ts"/"_source"/"_retract" are unknown:
/// dropped-and-counted, or — when <c>rejectUnknownFields</c> — fail the row.</item>
/// <item>"_ts": honoured (via <see cref="RowTimestamp.Resolve"/>) if the client sent one, otherwise
/// stamped with <c>arrivalMs</c> — mirrors RecordExtractor/ConnectorPollCycle's own stamping.</item>
/// <item>"_source": ALWAYS overwritten with the source's own name — a security property, not
/// tidiness: an attacker-controlled "_source" would inject rows into another source's SignalR group
/// and routing (see IngestModels.cs's <see cref="IIngressFacade"/> doc).</item>
/// <item>"_retract": wishlist "explicit key retraction through ingest" — a fourth reserved key, opt-
/// in and additive (absent = every pre-existing row is byte-identical). Present and truthy, it is
/// coerced to <c>bool</c> and carried into the accepted row exactly like "_ts"/"_source" are, so it
/// survives all the way to <see cref="StreamForge.Engine.Runtime.Ops"/>'s TableIngestOp — the one
/// place downstream that still sees this row before TableExecutorImpl's hardcoded assert-weight
/// takes over (see that op's own doc). A present-but-uncoercible value fails the ROW, the same as a
/// bad declared field — a retraction that doesn't parse must never be accepted and silently ignored
/// (that is the whole failure mode this feature exists to avoid; see docs/cdc.md's "Operational
/// hazards" for the historical version of that problem this closes for LATEST BY consumers). Whether
/// this source even HAS a LATEST BY consumer to retract from is not decidable here — this method has
/// no catalog access and runs identically on every ingest transport (REST, gRPC, whatever comes
/// next) — so that check is the REST ingest endpoint's job (SourcesEndpoints.cs, backed by
/// RetractConsumerValidation); this layer only guarantees the flag itself is well-formed.</item>
/// </list>
/// </summary>
public static class IngressRowAcceptance
{
    /// <summary>The reserved ingest-row key TableIngestOp looks for to flip a row's Z-set weight from
    /// the assert TableExecutorImpl otherwise hardcodes every stream event to. Named once, here, so
    /// the one other place that has to agree on the literal (TableIngestOp.cs) can reference the same
    /// constant instead of a second copy of the string.</summary>
    public const string RetractField = "_retract";

    public static RowAcceptanceResult Accept(
        IReadOnlyList<FieldDef> fields, string sourceName, bool rejectUnknownFields,
        Dictionary<string, object?> rawRow, long arrivalMs)
    {
        JsonValueNormalizer.NormalizeInPlace(rawRow);

        var row = new Dictionary<string, object?>();
        foreach (var f in fields)
        {
            if (!rawRow.TryGetValue(f.Name, out var value) || value is null)
            {
                continue; // absent/null -> omitted, same convention as ProtoWireEncoder/RecordExtractor
            }

            if (!TryCoerceField(f, value, out var coerced))
            {
                return new RowAcceptanceResult(null, 0, $"field \"{f.Name}\" cannot be coerced to {f.Type}");
            }

            row[f.Name] = coerced;
        }

        if (rawRow.TryGetValue(RetractField, out var retractRaw) && retractRaw is not null)
        {
            if (!FieldValueCoercion.TryCoerce(FieldType.Bool, retractRaw, out var coercedRetract))
            {
                return new RowAcceptanceResult(null, 0, $"field \"{RetractField}\" cannot be coerced to {FieldType.Bool}");
            }

            row[RetractField] = coercedRetract;
        }

        var unknownDropped = 0;
        foreach (var key in rawRow.Keys)
        {
            if (key is "_ts" or "_source" or RetractField || ContainsField(fields, key))
            {
                continue;
            }

            unknownDropped++;
            if (rejectUnknownFields)
            {
                return new RowAcceptanceResult(null, unknownDropped, $"unknown field \"{key}\"");
            }
        }

        row["_ts"] = rawRow.TryGetValue("_ts", out var ts) ? RowTimestamp.Resolve(ts, arrivalMs) : arrivalMs;
        row["_source"] = sourceName; // always overwritten, even if the client set one

        return new RowAcceptanceResult(row, unknownDropped, null);
    }

    /// <summary>Runs <see cref="Accept"/> over every row; see <see cref="BatchAcceptanceResult"/> for
    /// why this exists as a batch operation rather than leaving the "coerce every row first" ordering
    /// to be re-derived by each host.</summary>
    public static BatchAcceptanceResult AcceptBatch(
        IReadOnlyList<FieldDef> fields, string sourceName, bool rejectUnknownFields,
        IReadOnlyList<Dictionary<string, object?>> rawRows, long arrivalMs)
    {
        var accepted = new List<Dictionary<string, object?>>(rawRows.Count);
        var errors = new List<string>();
        var unknownDropped = 0;

        for (var i = 0; i < rawRows.Count; i++)
        {
            var result = Accept(fields, sourceName, rejectUnknownFields, rawRows[i], arrivalMs);
            unknownDropped += result.UnknownFieldsDropped;
            if (result.Accepted)
            {
                accepted.Add(result.Row!);
            }
            else
            {
                errors.Add($"row {i}: {result.Error}");
            }
        }

        return new BatchAcceptanceResult(accepted, unknownDropped, errors);
    }

    private static bool ContainsField(IReadOnlyList<FieldDef> fields, string name)
    {
        foreach (var f in fields)
        {
            if (f.Name == name) return true;
        }
        return false;
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
            if (element is null) continue; // proto3 convention: a null list element carries no value
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
