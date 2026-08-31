using System.Data.Common;
using Microsoft.Data.SqlClient;

namespace StreamsForge.Connectors.Database;

/// <summary>
/// Microsoft SQL Server. Bracketed identifiers, <c>OFFSET 0 ROWS FETCH NEXT n ROWS ONLY</c>, and
/// <c>MERGE</c> for the upsert.
///
/// <para><b>Three things here are not stylistic differences from PostgreSQL, they are correctness
/// rules.</b> (1) <see cref="MaxCommandParameters"/> is 2100 — a SERVER limit on parameters per batch, and
/// the reason <see cref="DbSinkPlanner"/> chunks by column count at all rather than by a round number of
/// rows. (2) A <c>MERGE</c> statement must be terminated by a semicolon; without it SQL Server raises a
/// syntax error that names the NEXT statement, which is a genuinely confusing way to find out. (3)
/// <c>MERGE</c> raises error 8672 when two source rows carry the same key, so the planner de-duplicates a
/// batch by key before it gets here — see <see cref="DbSinkPlanner"/>.</para>
///
/// <para><b>MERGE's reputation is acknowledged, not dismissed.</b> It has a history of concurrency and
/// optimizer bugs under high contention. The alternative — <c>UPDATE</c> then <c>INSERT … WHERE NOT
/// EXISTS</c> in the same transaction — is two round-trips and two statements to keep in step, and this
/// sink already runs each batch in its own transaction. MERGE is chosen for one statement per chunk; if a
/// deployment hits one of those bugs, the failure is visible in the sink's counters rather than silent.</para>
/// </summary>
public sealed class SqlServerDialect : ISqlDialect
{
    public string Kind => "mssql";

    public string Label => "Microsoft SQL Server";

    public int DefaultPort => 1433;

    public string DefaultSchema => "dbo";

    /// <summary>2100, the documented per-batch parameter ceiling. Exceeding it fails the whole command.</summary>
    public int MaxCommandParameters => 2100;

    public string QuoteIdent(string ident) => '[' + ident.Replace("]", "]]", StringComparison.Ordinal) + ']';

    public string Parameter(int index) => "@p" + index.ToString(System.Globalization.CultureInfo.InvariantCulture);

    /// <summary>Requires an ORDER BY on the SELECT it tails — every SELECT this connector generates orders
    /// by the cursor column, and a source without one is rejected before it can reach here.</summary>
    public string PageClause(int rows) => $"OFFSET 0 ROWS FETCH NEXT {rows} ROWS ONLY";

    public string UpsertStatement(string qualifiedTable, IReadOnlyList<string> columns, IReadOnlyList<string> keys, int rowCount, int firstParameter)
    {
        var tuples = string.Join(", ", Enumerable.Range(0, rowCount)
            .Select(r => this.ParameterTuple(firstParameter + (r * columns.Count), columns.Count)));

        var on = string.Join(" AND ", keys.Select(k => $"t.{QuoteIdent(k)} = s.{QuoteIdent(k)}"));
        var updatable = columns.Where(c => !keys.Contains(c, StringComparer.Ordinal)).ToList();

        var matched = updatable.Count == 0
            ? ""
            : " WHEN MATCHED THEN UPDATE SET " + string.Join(", ", updatable.Select(c => $"t.{QuoteIdent(c)} = s.{QuoteIdent(c)}"));

        return $"MERGE {qualifiedTable} AS t USING (VALUES {tuples}) AS s ({this.QuotedList(columns)}) ON ({on})" +
               matched +
               $" WHEN NOT MATCHED THEN INSERT ({this.QuotedList(columns)}) VALUES ({string.Join(", ", columns.Select(c => $"s.{QuoteIdent(c)}"))});";
    }

    public string DeleteStatement(string qualifiedTable, IReadOnlyList<string> keys, int rowCount, int firstParameter)
    {
        if (keys.Count == 1)
        {
            var values = string.Join(", ", Enumerable.Range(firstParameter, rowCount).Select(Parameter));
            return $"DELETE FROM {qualifiedTable} WHERE {QuoteIdent(keys[0])} IN ({values})";
        }

        // No row-value IN on SQL Server, so a composite key becomes one OR'd AND-group per row. Verbose,
        // and the reason the delete chunk size is bounded by the same parameter ceiling as everything else.
        var groups = Enumerable.Range(0, rowCount).Select(r =>
            "(" + string.Join(" AND ", keys.Select((k, i) => $"{QuoteIdent(k)} = {Parameter(firstParameter + (r * keys.Count) + i)}")) + ")");
        return $"DELETE FROM {qualifiedTable} WHERE {string.Join(" OR ", groups)}";
    }

    public TypeMapping MapType(string? dataTypeName, Type? clrType) => SqlTypeTables.MapSqlServer(dataTypeName, clrType);

    public DbConnection CreateConnection(DbEndpoint endpoint)
    {
        // Plan 016 wave 6: resolved HERE, at the actual connect site — see DbEndpoint.Resolved's doc (and
        // PostgresDialect.CreateConnection's identical comment) for why not earlier and not cached.
        endpoint = endpoint.Resolved();

        if (!string.IsNullOrWhiteSpace(endpoint.ConnectionString))
        {
            return new SqlConnection(endpoint.ConnectionString);
        }

        var builder = new SqlConnectionStringBuilder
        {
            DataSource = endpoint.Port > 0 ? $"{endpoint.Host},{endpoint.Port}" : endpoint.Host,
            InitialCatalog = endpoint.Database,
            UserID = endpoint.Username,
            Password = endpoint.Password ?? "",
            Encrypt = true,
            // Microsoft.Data.SqlClient 4+ encrypts by default and validates the chain by default, which
            // breaks every self-signed dev instance. Tls=false here means "encrypt, don't verify" rather
            // than "plaintext": downgrading the wire is not something a checkbox labelled TLS should do.
            // A verified chain needs a real certificate and is configured through the ConnectionString.
            TrustServerCertificate = !endpoint.Tls,
        };
        return new SqlConnection(builder.ConnectionString);
    }

    public bool IsTransient(Exception ex) => ex is SqlException { IsTransient: true };
}
