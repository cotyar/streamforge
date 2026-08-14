using System.Data.Common;
using Npgsql;

namespace StreamForge.Connectors.Database;

/// <summary>
/// PostgreSQL. Double-quoted identifiers, <c>LIMIT n</c>, and <c>INSERT … ON CONFLICT (k) DO UPDATE</c>
/// for the upsert — the one dialect where the upsert is genuinely a single statement rather than a
/// bolted-on merge, which is why it reads as one.
///
/// <para><b>The <c>ON CONFLICT</c> target must be a unique index</b>, not merely the columns the operator
/// nominated as keys. This connector cannot check that without DDL rights it deliberately does not have,
/// so a <c>KeyColumns</c> that no unique constraint covers fails at the server with PostgreSQL's own
/// message ("there is no unique or exclusion constraint matching the ON CONFLICT specification"), counted
/// and surfaced through the sink's failure callback. That is a better outcome than a pre-flight catalog
/// query per batch, and a far better one than silently degrading to plain INSERT.</para>
/// </summary>
public sealed class PostgresDialect : ISqlDialect
{
    public string Kind => "postgres";

    public string Label => "PostgreSQL";

    public int DefaultPort => 5432;

    public string DefaultSchema => "public";

    /// <summary>PostgreSQL's wire protocol caps parameters at 65535 (an unsigned 16-bit count). Far above
    /// anything a sane batch reaches, but the chunker asks the dialect rather than assuming.</summary>
    public int MaxCommandParameters => 65535;

    public string QuoteIdent(string ident) => '"' + ident.Replace("\"", "\"\"", StringComparison.Ordinal) + '"';

    public string Parameter(int index) => "@p" + index.ToString(System.Globalization.CultureInfo.InvariantCulture);

    public string PageClause(int rows) => $"LIMIT {rows}";

    public string UpsertStatement(string qualifiedTable, IReadOnlyList<string> columns, IReadOnlyList<string> keys, int rowCount, int firstParameter)
    {
        var tuples = string.Join(", ", Enumerable.Range(0, rowCount)
            .Select(r => this.ParameterTuple(firstParameter + (r * columns.Count), columns.Count)));

        var updatable = columns.Where(c => !keys.Contains(c, StringComparer.Ordinal)).ToList();

        // Every column is a key: there is nothing an UPDATE could change, and `DO UPDATE SET` with an
        // empty assignment list is a syntax error. DO NOTHING is the honest equivalent — the row that is
        // already there is byte-identical to the one being written.
        var action = updatable.Count == 0
            ? "DO NOTHING"
            : "DO UPDATE SET " + string.Join(", ", updatable.Select(c => $"{QuoteIdent(c)} = EXCLUDED.{QuoteIdent(c)}"));

        return $"INSERT INTO {qualifiedTable} ({this.QuotedList(columns)}) VALUES {tuples} " +
               $"ON CONFLICT ({this.QuotedList(keys)}) {action}";
    }

    public string DeleteStatement(string qualifiedTable, IReadOnlyList<string> keys, int rowCount, int firstParameter)
    {
        // Single key reads as a plain IN list; a composite key uses PostgreSQL's row constructor, which
        // SQL Server has no equivalent of — the entire reason this method is on the dialect.
        if (keys.Count == 1)
        {
            var values = string.Join(", ", Enumerable.Range(firstParameter, rowCount).Select(Parameter));
            return $"DELETE FROM {qualifiedTable} WHERE {QuoteIdent(keys[0])} IN ({values})";
        }

        var tuples = string.Join(", ", Enumerable.Range(0, rowCount)
            .Select(r => this.ParameterTuple(firstParameter + (r * keys.Count), keys.Count)));
        return $"DELETE FROM {qualifiedTable} WHERE ({this.QuotedList(keys)}) IN ({tuples})";
    }

    public TypeMapping MapType(string? dataTypeName, Type? clrType) => SqlTypeTables.MapPostgres(dataTypeName, clrType);

    public DbConnection CreateConnection(DbEndpoint endpoint)
    {
        if (!string.IsNullOrWhiteSpace(endpoint.ConnectionString))
        {
            return new NpgsqlConnection(endpoint.ConnectionString);
        }

        var builder = new NpgsqlConnectionStringBuilder
        {
            Host = endpoint.Host,
            Port = endpoint.Port > 0 ? endpoint.Port : DefaultPort,
            Database = endpoint.Database,
            Username = endpoint.Username,
            Password = endpoint.Password,
            // Require encrypts without demanding a verifiable chain. Verify-full is the correct setting for
            // a hostile network and is NOT reachable through the structured fields — that is what the
            // ConnectionString escape hatch is for, and saying so beats offering a checkbox that lies.
            SslMode = endpoint.Tls ? SslMode.Require : SslMode.Prefer,
        };
        return new NpgsqlConnection(builder.ConnectionString);
    }

    /// <summary>Delegates to Npgsql's own classification. Deliberately not a message-substring list of
    /// our own: the driver knows which SQLSTATEs it will survive a reconnect on, and a hand-maintained
    /// copy of that knowledge would be wrong within one minor version.</summary>
    public bool IsTransient(Exception ex) => ex is NpgsqlException { IsTransient: true };
}
