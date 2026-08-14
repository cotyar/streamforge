using System.Globalization;
using StreamForge.Abstractions;

namespace StreamForge.Connectors.Database;

/// <summary>One row on its way out: the cells, and the Z-set weight that decides what happens to it. A
/// pipeline result has no weight and arrives as +1 — see <see cref="DbSinkPlanner"/> on why that makes
/// upsert mode meaningless for a pipeline.</summary>
public sealed record SinkRow(Dictionary<string, object?> Values, long Weight);

/// <summary>One statement and the values bound into it, in <c>@p0</c>… order.</summary>
public sealed record DbStatement(string Sql, IReadOnlyList<object?> Parameters);

/// <summary>The statements one delivered batch becomes, executed IN ORDER inside one transaction, plus
/// the rows that could not be planned at all.</summary>
public sealed record DbSinkPlan(IReadOnlyList<DbStatement> Statements, int Skipped, string? SkipReason);

/// <summary>
/// The pure half of the database sink: a delivered batch in, the statements of one transaction out. Same
/// split, and the same reason, as <see cref="DbPollPlanner"/> — the SQL text and the parameter ORDER are
/// where the subtle bugs are, and neither needs a server to test.
///
/// <para><b>Append</b> is chunked parameterized <c>INSERT … VALUES (…),(…)</c>. The chunk size is
/// <see cref="ISqlDialect.MaxCommandParameters"/> ÷ column count, not a round number of rows: SQL Server
/// caps a batch at 2100 PARAMETERS, so a 40-column row hits the ceiling at 52 rows while a 3-column row
/// does not until 700.</para>
///
/// <para><b>Upsert resolves each key to its LAST delta in the batch, then upserts the survivors and
/// deletes the rest, deletes last, all in the one transaction.</b> That collapsing step is not an
/// optimization, it is required twice over. Correctness: a table UPDATE arrives as two deltas — <c>-1</c>
/// carrying the old row and <c>+1</c> carrying the new one, same key — and a sink that ran the delete
/// after the upsert because deletes go last would delete the row the update just wrote. Mechanics: both
/// dialects REFUSE a batch that names the same key twice (SQL Server error 8672 "MERGE attempted to update
/// or delete the same row more than once"; PostgreSQL "ON CONFLICT DO UPDATE command cannot affect row a
/// second time"), so the duplicate has to go before the statement is built either way. Resolving to the
/// last delta does both, and is what makes a delete-then-reinsert of one key inside a batch land the way
/// the caller meant.</para>
///
/// <para><b>The limit that buys:</b> it assumes the delivered batch is in causal order. It is — a table's
/// delta batch is emitted in the order the engine produced it — but a sink fed an arbitrarily reordered
/// batch would resolve the key to the wrong delta, and nothing here could detect that.</para>
///
/// <para><b>A row missing one of its key columns is skipped, not guessed at.</b> A null key matches
/// nothing in a DELETE and collides with every other null in an upsert; writing it would corrupt exactly
/// the identity the operator declared. The count comes back in <see cref="DbSinkPlan.Skipped"/> and lands
/// in the sink's failure counters.</para>
/// </summary>
public static class DbSinkPlanner
{
    /// <summary>The Z-set weight column, written on append when
    /// <see cref="DbSinkConfig.IncludeWeight"/> is set. Never written in upsert mode, where the weight IS
    /// the operation and persisting it would store a number already spent.</summary>
    public const string WeightColumn = "_weight";

    public static DbSinkPlan Plan(DbSinkConfig config, ISqlDialect dialect, string qualifiedTable, IReadOnlyList<SinkRow> rows)
    {
        ArgumentNullException.ThrowIfNull(config);
        ArgumentNullException.ThrowIfNull(dialect);
        ArgumentNullException.ThrowIfNull(rows);

        if (rows.Count == 0)
        {
            return new DbSinkPlan([], 0, null);
        }

        return string.Equals(config.Mode, DbSinkModes.Upsert, StringComparison.OrdinalIgnoreCase)
            ? PlanUpsert(config, dialect, qualifiedTable, rows)
            : PlanAppend(config, dialect, qualifiedTable, rows);
    }

    /// <summary>The configured key columns, trimmed and de-blanked.</summary>
    public static List<string> Keys(DbSinkConfig config)
        => [.. config.KeyColumns.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)];

    private static DbSinkPlan PlanAppend(DbSinkConfig config, ISqlDialect dialect, string table, IReadOnlyList<SinkRow> rows)
    {
        var columns = Columns(config, rows);
        if (config.IncludeWeight && !columns.Contains(WeightColumn, StringComparer.Ordinal))
        {
            columns.Add(WeightColumn);
        }

        if (columns.Count == 0)
        {
            return new DbSinkPlan([], rows.Count, "no columns to write");
        }

        var quoted = dialect.QuotedList(columns);
        List<DbStatement> statements = [];
        foreach (var chunk in Chunk(rows, ChunkSize(dialect, columns.Count)))
        {
            List<object?> parameters = [];
            var tuples = new List<string>(chunk.Count);
            foreach (var row in chunk)
            {
                tuples.Add(dialect.ParameterTuple(parameters.Count, columns.Count));
                parameters.AddRange(columns.Select(c => Value(config, row, c)));
            }

            statements.Add(new DbStatement(
                $"INSERT INTO {table} ({quoted}) VALUES {string.Join(", ", tuples)}",
                parameters));
        }

        return new DbSinkPlan(statements, 0, null);
    }

    private static DbSinkPlan PlanUpsert(DbSinkConfig config, ISqlDialect dialect, string table, IReadOnlyList<SinkRow> rows)
    {
        var keys = Keys(config);
        if (keys.Count == 0)
        {
            // Validation rejects this, so reaching it means a config that bypassed validation. Refusing the
            // whole batch beats inventing an identity.
            return new DbSinkPlan([], rows.Count, "upsert mode needs keyColumns");
        }

        // Resolve each key to its LAST delta — see this class's doc for why this is required, not clever.
        Dictionary<string, SinkRow> latest = new(StringComparer.Ordinal);
        List<string> order = [];
        var skipped = 0;
        foreach (var row in rows)
        {
            if (keys.Any(k => !row.Values.TryGetValue(k, out var v) || v is null))
            {
                skipped++;
                continue;
            }

            // Joined on the ASCII unit separator so ("a","bc") and ("ab","c") are two identities and
            // not one — a plain concatenation would silently merge them.
            var identity = string.Join('\u001F', keys.Select(k => Convert.ToString(row.Values[k], CultureInfo.InvariantCulture)));
            if (!latest.ContainsKey(identity))
            {
                order.Add(identity);
            }

            latest[identity] = row;
        }

        var surviving = order.Select(k => latest[k]).ToList();
        var upserts = surviving.Where(r => r.Weight > 0).ToList();
        var deletes = surviving.Where(r => r.Weight <= 0).ToList();

        List<DbStatement> statements = [];

        if (upserts.Count > 0)
        {
            var columns = Columns(config, upserts);
            foreach (var key in keys.Where(k => !columns.Contains(k, StringComparer.Ordinal)))
            {
                // An explicit Columns list that omits a key column: the statement would be built with an
                // ON CONFLICT / MERGE target that isn't in the projection at all.
                return new DbSinkPlan([], rows.Count, $"key column '{key}' is not among the written columns");
            }

            foreach (var chunk in Chunk(upserts, ChunkSize(dialect, columns.Count)))
            {
                List<object?> parameters = [];
                foreach (var row in chunk)
                {
                    parameters.AddRange(columns.Select(c => Value(config, row, c)));
                }

                statements.Add(new DbStatement(
                    dialect.UpsertStatement(table, columns, keys, chunk.Count, 0),
                    parameters));
            }
        }

        // Deletes last, inside the same transaction. With the key resolution above there is no overlap
        // between the two sets, so the order is a stated convention rather than a load-bearing one — but
        // it is stated, because an implementation that quietly flipped it would be indistinguishable
        // until the day the resolution changed.
        foreach (var chunk in Chunk(deletes, ChunkSize(dialect, keys.Count)))
        {
            List<object?> parameters = [];
            foreach (var row in chunk)
            {
                parameters.AddRange(keys.Select(k => row.Values[k]));
            }

            statements.Add(new DbStatement(dialect.DeleteStatement(table, keys, chunk.Count, 0), parameters));
        }

        return new DbSinkPlan(statements, skipped, skipped == 0 ? null : "row(s) had a null or absent key column");
    }

    /// <summary>The column list to write: <see cref="DbSinkConfig.Columns"/> when the operator set one,
    /// otherwise the union of the batch's keys in first-seen order. Union rather than first-row-only
    /// because a batch whose later rows carry an extra column would otherwise drop it silently — the
    /// opposite trade-off from the file sink, whose header is physically fixed once written.</summary>
    private static List<string> Columns(DbSinkConfig config, IReadOnlyList<SinkRow> rows)
    {
        var declared = config.Columns.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (declared.Length > 0)
        {
            return [.. declared];
        }

        List<string> columns = [];
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var name in rows.SelectMany(r => r.Values.Keys))
        {
            if (seen.Add(name))
            {
                columns.Add(name);
            }
        }

        return columns;
    }

    private static object? Value(DbSinkConfig config, SinkRow row, string column)
    {
        if (config.IncludeWeight && string.Equals(column, WeightColumn, StringComparison.Ordinal) && !row.Values.ContainsKey(column))
        {
            return row.Weight;
        }

        return row.Values.TryGetValue(column, out var value) ? value : null;
    }

    /// <summary>Rows per statement. At least one — a row wider than the parameter ceiling cannot be split
    /// and is better off failing at the server with its own message than being silently truncated here.</summary>
    public static int ChunkSize(ISqlDialect dialect, int columnCount)
        => Math.Max(1, dialect.MaxCommandParameters / Math.Max(1, columnCount));

    private static IEnumerable<List<T>> Chunk<T>(IReadOnlyList<T> items, int size)
    {
        for (var i = 0; i < items.Count; i += size)
        {
            yield return [.. items.Skip(i).Take(size)];
        }
    }
}
