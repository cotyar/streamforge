namespace StreamForge.Connectors.Database;

/// <summary>What one <c>ReplicationValue</c> resolved to, already collapsed to the three outcomes a
/// decoder needs — <see cref="PgTupleDecoder"/>'s only input, so it never takes a dependency on
/// <c>Npgsql.Replication.PgOutput.TupleDataKind</c> or on any type that needs a live connection to
/// construct. <see cref="PgCdcSource"/> is the only producer: it awaits a <c>ReplicationValue</c>'s
/// <c>Get()</c> for <see cref="Value"/> and the streaming discipline (full tuple, in order) lives there,
/// not here.</summary>
public enum PgTupleValueKind
{
    /// <summary>The column is NULL.</summary>
    Null,

    /// <summary>An ordinary value — <c>Value</c> holds it.</summary>
    Value,

    /// <summary>Postgres omitted this column from the WAL because it is an unchanged, TOASTed value and
    /// the table's replica identity did not force it in. <see cref="PgTupleDecoder.Decode"/> turns this
    /// into <see cref="CdcStamp.UnavailableValue"/>, Debezium's own sentinel — see that constant's doc for
    /// why the literal is reused rather than invented.</summary>
    UnchangedToast,
}

/// <summary>One already-materialized field of a replication tuple: its column name (from
/// <c>ReplicationValue.GetFieldName()</c>), what kind of value it turned out to be, and — for
/// <see cref="PgTupleValueKind.Value"/> only — the decoded CLR value. <see cref="PgCdcSource"/> builds a
/// list of these by enumerating a <c>ReplicationTuple</c> fully and in order (the discipline that loses
/// data if skipped — see its class doc) BEFORE calling <see cref="PgTupleDecoder.Decode"/>, so decoding
/// itself never touches the network.</summary>
public readonly record struct PgTupleField(string FieldName, PgTupleValueKind Kind, object? Value);

/// <summary>The outcome of decoding one tuple: the row, plus a diagnostic when the tuple's shape did not
/// match what the cached <see cref="PgRelation"/> declared. <see cref="Diagnostic"/> is not a reason to
/// discard <see cref="Row"/> — see <see cref="PgTupleDecoder.Decode"/>'s doc for why the row is trustworthy
/// even when it fires.</summary>
public sealed record PgTupleDecodeResult(Dictionary<string, object?> Row, string? Diagnostic);

/// <summary>
/// Turns an already-materialized <see cref="PgTupleField"/> sequence plus the <see cref="PgRelation"/>
/// <see cref="PgRelationCache"/> resolved for it into the row dictionary <c>ConnectorPollCycle.ExecuteRows</c>
/// admits. Pure, synchronous, no Npgsql type in its signature — the whole point of splitting this out of
/// <see cref="PgCdcSource"/>'s streaming loop, per that class's own doc comment: it is what makes this logic
/// unit-testable with no server and no live connection.
///
/// <para><b>Per-value mapping, exactly <see cref="CdcStamp"/>'s doc restates for the native path:</b>
/// <see cref="PgTupleValueKind.Null"/> → <c>null</c>; <see cref="PgTupleValueKind.UnchangedToast"/> →
/// <see cref="CdcStamp.UnavailableValue"/>; <see cref="PgTupleValueKind.Value"/> → the value, unchanged.</para>
///
/// <para><b>Decoding is by FIELD NAME, never by position</b> — <paramref name="fields"/>'s own
/// <see cref="PgTupleField.FieldName"/>, taken from <c>ReplicationValue.GetFieldName()</c>, is the
/// dictionary key. This is what makes the "column count disagrees with the relation" case safe to report
/// rather than fatal: since nothing here ever zips two lists together positionally, a tuple that is
/// genuinely shorter or longer than <see cref="PgRelation.ColumnNames"/> (a DDL change mid-session, a
/// dropped column pgoutput still remembers) cannot misalign a row's values under the wrong names — it can
/// only mean the row has fewer or more entries than the relation's declared column count, which
/// <see cref="PgTupleDecodeResult.Diagnostic"/> says plainly rather than staying silent about.</para>
/// </summary>
public static class PgTupleDecoder
{
    public static PgTupleDecodeResult Decode(PgRelation relation, IReadOnlyList<PgTupleField> fields)
    {
        ArgumentNullException.ThrowIfNull(relation);
        ArgumentNullException.ThrowIfNull(fields);

        var row = new Dictionary<string, object?>(fields.Count, StringComparer.Ordinal);
        foreach (var field in fields)
        {
            row[field.FieldName] = field.Kind switch
            {
                PgTupleValueKind.Null => null,
                PgTupleValueKind.UnchangedToast => CdcStamp.UnavailableValue,
                _ => field.Value,
            };
        }

        var diagnostic = fields.Count == relation.ColumnNames.Count
            ? null
            : $"relation '{relation.QualifiedName}' has {relation.ColumnNames.Count} known column(s) but this " +
              $"tuple carried {fields.Count} value(s) — decoded by field name, so nothing was zipped short, " +
              "but the mismatch itself is worth an operator's attention (a DDL change mid-session is the usual cause)";

        return new PgTupleDecodeResult(row, diagnostic);
    }
}
