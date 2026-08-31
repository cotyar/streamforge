using StreamsForge.Abstractions;
using StreamsForge.AppCore.Transports;

namespace StreamsForge.Connectors.Database;

/// <summary>What one poll cycle should execute. <see cref="Seed"/> distinguishes the two shapes: a normal
/// read (<c>false</c>) returns rows, a seed (<c>true</c>) returns ONE scalar which becomes the cursor and
/// emits no rows at all.</summary>
public sealed record DbPollPlan(string Sql, IReadOnlyList<KeyValuePair<string, object?>> Parameters, bool Seed);

/// <summary>
/// The pure half of a database source: given the config and the persisted cursor, what SQL runs and what
/// the resulting rows mean. Separated from <see cref="DbSource"/> — which does nothing but open a
/// connection and execute this — for one blunt reason: <b>every rule worth testing lives here, and none of
/// them needs a database.</b> The cursor rules are the part of this connector that can lose data silently,
/// so they are the part that must be covered by tests that run in the ordinary suite rather than only
/// against a container.
///
/// <para><b>The four states of "where do I start", in the order they are decided:</b></para>
/// <list type="number">
/// <item>A persisted cursor exists → read strictly after it. The ordinary case.</item>
/// <item>No cursor, but <see cref="DbSourceConfig.InitialCursor"/> is set → start there.
/// <b>This is the transport's job, not the driver's</b> — neither <c>ConnectorGrain</c> nor
/// <c>ConnectorActor</c> seeds its persisted cursor from the config, so if this were not implemented here
/// the field would be silently inert.</item>
/// <item>No cursor, no initial, <see cref="DbSourceConfig.Snapshot"/> → read the whole table ordered by the
/// cursor column, one page per driver cycle. Page 1 has no predicate at all; the cursor it persists sends
/// page 2 down branch 1, so the snapshot continues through the ordinary path and a restart mid-snapshot
/// resumes rather than starting over.</item>
/// <item>No cursor, no initial, no snapshot → <c>SELECT MAX(cursor)</c>, persist it, emit nothing. The
/// next cycle tails. On an empty table MAX is NULL, the cursor stays unset, and the cycle after tries
/// again — which is right: seeding to "nothing" would make the first ever inserted row invisible.</item>
/// </list>
///
/// <para><b>Comparison operator.</b> <c>&gt;=</c> when a <see cref="DbSourceConfig.DedupKeyColumn"/> is
/// configured, <c>&gt;</c> otherwise. That is not a hidden mode: it is exactly the shape
/// <c>CursorColumn</c>'s own doc recommends for a timestamp watermark ("<c>&gt;=</c> plus a
/// DedupKeyColumn"), expressed through the fields that exist rather than through a fourth one nobody would
/// set correctly. With <c>&gt;</c> a timestamp loses every row sharing the watermark's millisecond; with
/// <c>&gt;=</c> it re-reads them and <c>ConnectorPollCycle</c>'s dedup tracker drops the duplicates.</para>
///
/// <para><b>Custom <see cref="DbSourceConfig.Query"/> mode requires an <c>InitialCursor</c></b> and is
/// rejected in validation without one. There is no MAX to seed from in arbitrary SQL and no safe sentinel
/// to invent (<c>long.MinValue</c> is a real key in someone's table; <c>DateTime.MinValue</c> will not even
/// bind to a <c>timestamptz</c>), so the honest move is to make the operator name the starting point.
/// Query mode also gets no generated ORDER BY and no page clause: the SQL is the operator's, and a
/// connector rewriting it would be a connector guessing.</para>
/// </summary>
public static class DbPollPlanner
{
    /// <summary>Builds the plan for this cycle. <paramref name="cursor"/> is the driver's persisted value,
    /// null on this source's first ever cycle. Throws when the config cannot express a starting point —
    /// which <c>PolledSourceCore</c> turns into an error status with the cursor untouched.</summary>
    public static DbPollPlan Plan(DbSourceConfig config, ISqlDialect dialect, string? cursor)
    {
        ArgumentNullException.ThrowIfNull(config);
        ArgumentNullException.ThrowIfNull(dialect);

        var effective = string.IsNullOrWhiteSpace(cursor)
            ? (string.IsNullOrWhiteSpace(config.InitialCursor) ? null : config.InitialCursor.Trim())
            : cursor;

        var page = config.BatchSize > 0 ? config.BatchSize : 1000;

        if (!string.IsNullOrWhiteSpace(config.Query))
        {
            if (effective is null)
            {
                throw new InvalidOperationException(
                    "a custom query needs an initialCursor: there is no MAX(cursorColumn) to seed from in query mode, " +
                    "and no starting value this connector could invent would be safe for every column type");
            }

            return new DbPollPlan(
                config.Query,
                [Bind(config, effective)],
                Seed: false);
        }

        var table = dialect.QualifiedTable(config.Schema, config.Table);
        var order = dialect.QuoteIdent(config.CursorColumn.Trim());
        var extra = string.IsNullOrWhiteSpace(config.Where) ? null : $"({config.Where.Trim()})";

        if (effective is null && !config.Snapshot)
        {
            // Branch 4. The Where clause is applied here too, so tailing starts at the top of the FILTERED
            // set — seeding past rows the source would never have emitted anyway.
            var seedWhere = extra is null ? "" : $" WHERE {extra}";
            return new DbPollPlan($"SELECT MAX({order}) FROM {table}{seedWhere}", [], Seed: true);
        }

        // Branch 3 (snapshot page 1) has no cursor predicate; branches 1 and 2 do. Everything else about
        // the statement is identical, which is the point — a snapshot is not a second code path.
        var predicates = new List<string>();
        List<KeyValuePair<string, object?>> parameters = [];
        if (effective is not null)
        {
            predicates.Add($"{order} {Operator(config)} {DbCursor.Placeholder}");
            parameters.Add(Bind(config, effective));
        }

        if (extra is not null)
        {
            predicates.Add(extra);
        }

        var where = predicates.Count == 0 ? "" : " WHERE " + string.Join(" AND ", predicates);
        return new DbPollPlan(
            $"SELECT * FROM {table}{where} ORDER BY {order} ASC {dialect.PageClause(page)}",
            parameters,
            Seed: false);
    }

    /// <summary>Turns a completed read into the batch the driver persists against. Pure, and the other
    /// half of what makes this connector testable without a server.
    ///
    /// <para><b>Cursor is the MAXIMUM over the batch, not the last row.</b> Table mode orders ascending so
    /// the two agree; query mode is the operator's SQL and may not order at all, where taking the last row
    /// would move the watermark backwards and re-read forever.</para>
    ///
    /// <para><b>HasMore is <c>rows.Count &gt;= BatchSize</c></b> — a full page means there is very likely
    /// more waiting, so re-arm now instead of sleeping the schedule. The one exception is the <c>&gt;=</c>
    /// case where a full page did not move the cursor at all (every row shares the watermark value): re-arming
    /// there would spin the driver against the same page at full speed forever, so it falls back to the
    /// schedule. That page is a genuine dead end — no cursor this connector can mint escapes it — and the
    /// honest response is to keep polling at the configured rate rather than to burn a core.</para></summary>
    public static PolledBatch Complete(DbSourceConfig config, IReadOnlyList<Dictionary<string, object?>> rows, string? incoming)
    {
        ArgumentNullException.ThrowIfNull(config);
        ArgumentNullException.ThrowIfNull(rows);

        if (rows.Count == 0)
        {
            // Nothing read. Null cursor = leave the persisted one alone.
            return new PolledBatch([], null, HasMore: false);
        }

        var column = config.CursorColumn.Trim();
        if (!rows.Any(r => r.ContainsKey(column)))
        {
            // Almost always a custom Query whose SELECT list omits the cursor column. Without it the
            // watermark can never advance and the same rows are re-emitted every cycle — loud beats that.
            throw new InvalidOperationException(
                $"no row in this batch has the cursor column '{column}'; the SELECT list must include it for the cursor to advance");
        }

        var next = DbCursor.Encode(Max(rows, column), config.CursorKind);
        var full = rows.Count >= (config.BatchSize > 0 ? config.BatchSize : 1000);
        var stalled = next is not null && incoming is not null && string.Equals(next, incoming, StringComparison.Ordinal);
        return new PolledBatch(rows, next, HasMore: full && !stalled);
    }

    /// <summary>See this class's doc — the <c>&gt;=</c>/<c>&gt;</c> choice is <see cref="DbSourceConfig.DedupKeyColumn"/>.</summary>
    internal static string Operator(DbSourceConfig config)
        => string.IsNullOrWhiteSpace(config.DedupKeyColumn) ? ">" : ">=";

    private static KeyValuePair<string, object?> Bind(DbSourceConfig config, string cursor)
        => new(DbCursor.ParameterName, DbCursor.Decode(cursor, config.CursorKind));

    private static object? Max(IReadOnlyList<Dictionary<string, object?>> rows, string column)
    {
        object? best = null;
        foreach (var row in rows)
        {
            if (!row.TryGetValue(column, out var value) || value is null)
            {
                continue;
            }

            if (best is null)
            {
                best = value;
                continue;
            }

            try
            {
                if (Comparer<object>.Default.Compare(value, best) > 0)
                {
                    best = value;
                }
            }
            catch (ArgumentException)
            {
                // Mixed or non-comparable CLR types in one column (a Query UNIONing two shapes). Last one
                // wins, which is the pre-comparison behaviour and still monotonic for an ordered result.
                best = value;
            }
        }

        return best;
    }
}
