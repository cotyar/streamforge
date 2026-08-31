namespace StreamsForge.Connectors.Database;

/// <summary>
/// The ONE place a native CDC reader writes change-event metadata onto a row dictionary. The
/// Postgres reader (logical replication) and the SQL Server reader (CDC capture tables)
/// both call this instead of setting <c>_op</c>/<c>_weight</c>/<c>_ts</c> by hand, so the two
/// dialects cannot drift apart from each other — and, just as important, cannot drift from
/// <see cref="StreamsForge.AppCore.Connectors.Mapping.CdcEnvelope"/>, the existing Debezium-envelope
/// unwrapper: the op letters, the weight sign, and the column names here are reproduced from that
/// class on purpose, so a row a native reader stamps is indistinguishable downstream from one a
/// Debezium-fed source produced. An operator's <c>LATEST BY &lt;key&gt; WHERE _op &lt;&gt; 'd'</c>
/// table SQL does not know or care which path the row came in on.
///
/// <para><b>The honest limit, restated for the native path:</b> a StreamsForge source is an
/// append-only <c>EventRecord</c> stream — <c>_weight</c> on an inbound row is just a column, not a
/// retraction the Engine's Z-sets act on. A native delete does not free the key it deleted; it
/// arrives as one more event stamped <c>_op = "d"</c>, <c>_weight = -1</c>, sitting in the stream
/// next to every insert and update that came before it. <c>LATEST BY &lt;key&gt;</c> +
/// <c>WHERE _op &lt;&gt; 'd'</c> hides a deleted key from query results; it does not free it from the
/// source's history. See <see cref="StreamsForge.AppCore.Connectors.Mapping.CdcEnvelope"/>'s own class
/// doc for the longer version of this same limit.</para>
/// </summary>
public static class CdcStamp
{
    /// <summary>The op letter, matching <see cref="StreamsForge.AppCore.Connectors.Mapping.CdcEnvelope"/>'s
    /// own vocabulary exactly.</summary>
    public const string OpColumn = "_op";

    /// <summary>The Z-set weight. Must equal <see cref="DbSinkPlanner.WeightColumn"/> — a CDC row is a
    /// sink row like any other by the time it reaches a sink, and the two constants existing
    /// independently is exactly how they'd drift; a test asserts the equality directly.</summary>
    public const string WeightColumn = "_weight";

    /// <summary>The event's commit-time clock (Debezium's <c>ts_ms</c>, Postgres's commit timestamp,
    /// SQL Server's <c>__$start_lsn</c>-derived time), more trustworthy than connector arrival time.</summary>
    public const string TsColumn = "_ts";

    /// <summary>The qualified source table a row came from. Unlike <c>_op</c>/<c>_weight</c>/<c>_ts</c>,
    /// this one has no Debezium-envelope counterpart in this codebase today — the existing unwrapper
    /// never populates it — but a native reader can poll several tables into one stream, so it needs
    /// somewhere honest to say which table a given row is from.</summary>
    public const string TableColumn = "_table";

    /// <summary>The sentinel a native Postgres reader writes for an unchanged, TOASTed column value —
    /// deliberately Debezium's OWN literal (<c>__debezium_unavailable_value</c>), not a StreamsForge
    /// invention. Postgres logical replication omits an unchanged TOASTed column from the WAL entirely
    /// when the table's replica identity doesn't force it in; Debezium fills the gap with this exact
    /// string so a consumer can tell "unchanged, value omitted" apart from "actually NULL". Reusing the
    /// literal means an operator's SQL written against a Debezium-fed table — anything that filters or
    /// special-cases this sentinel — keeps working unmodified against the native path. Inventing our own
    /// sentinel here would be a needless migration for every existing Debezium consumer.</summary>
    public const string UnavailableValue = "__debezium_unavailable_value";

    /// <summary>Create.</summary>
    public const string OpCreate = "c";

    /// <summary>Update.</summary>
    public const string OpUpdate = "u";

    /// <summary>Delete.</summary>
    public const string OpDelete = "d";

    /// <summary>Stamps <paramref name="row"/> in place with <see cref="OpColumn"/>,
    /// <see cref="WeightColumn"/>, and — when present — <see cref="TableColumn"/> and
    /// <see cref="TsColumn"/>. <paramref name="qualifiedTable"/> is written only when non-empty;
    /// <paramref name="tsMs"/> only when non-null — same "stamp only what we actually know" rule
    /// <c>ConnectorPollCycle</c> already follows for the Debezium path, so a reader that has no
    /// table name or no event time handy doesn't fabricate one.</summary>
    public static void Apply(Dictionary<string, object?> row, string op, string? qualifiedTable, long? tsMs)
    {
        ArgumentNullException.ThrowIfNull(row);
        ArgumentNullException.ThrowIfNull(op);

        row[OpColumn] = op;
        row[WeightColumn] = WeightOf(op);

        if (!string.IsNullOrEmpty(qualifiedTable))
        {
            row[TableColumn] = qualifiedTable;
        }

        if (tsMs is not null)
        {
            row[TsColumn] = tsMs.Value;
        }
    }

    /// <summary><c>-1</c> for a delete, <c>+1</c> for everything else — including an op letter this
    /// method has never seen before. Matching <c>CdcEnvelope</c>'s own reasoning verbatim: one honest
    /// guess at ingest time beats throwing over an op vocabulary a future dialect might extend.</summary>
    public static int WeightOf(string op) => op == OpDelete ? -1 : 1;
}
