using System.Data.Common;
using StreamForge.Abstractions;

namespace StreamForge.Connectors.Database;

/// <summary>Everything a database connection needs, lifted out of <see cref="DbSourceConfig"/> and
/// <see cref="DbSinkConfig"/> — which carry the same seven fields deliberately (see
/// <c>DbSinkConfig</c>'s doc) but are two unrelated types. Lifting them here is what keeps
/// <see cref="ISqlDialect"/> from taking a union of two contract types, or from being implemented twice.
///
/// <para><see cref="ConnectionString"/> WINS over every other field when set, which is the contract's
/// own rule restated where it is enforced rather than only where it is declared.</para></summary>
public sealed record DbEndpoint(
    string Host,
    int Port,
    string Database,
    string Username,
    string? Password,
    bool Tls,
    string? ConnectionString)
{
    public static DbEndpoint From(DbSourceConfig c) =>
        new(c.Host, c.Port, c.Database, c.Username, c.Password, c.Tls, c.ConnectionString);

    public static DbEndpoint From(DbSinkConfig c) =>
        new(c.Host, c.Port, c.Database, c.Username, c.Password, c.Tls, c.ConnectionString);

    /// <summary>True when there is enough here to attempt a connection at all — the
    /// <c>ISinkTransport.IsConfigured</c> half of the question, shared so the source's validation and the
    /// sink's configured-check cannot drift apart.</summary>
    public bool Addressable =>
        !string.IsNullOrWhiteSpace(ConnectionString) ||
        (!string.IsNullOrWhiteSpace(Host) && !string.IsNullOrWhiteSpace(Database));
}

/// <summary>A column's mapped platform type, plus what mapping it cost. <see cref="Note"/> is non-null
/// only when the mapping LOSES something the operator can act on — today exactly one case,
/// <c>numeric</c>/<c>decimal</c> → <see cref="FieldType.Double"/>. It is surfaced through
/// <c>SchemaProbeResult.Diagnostics</c> rather than swallowed, because a probe that silently rounds
/// money is worse than one that says it will.</summary>
public sealed record TypeMapping(FieldType Type, string? Note = null);

/// <summary>
/// The whole of what differs between PostgreSQL and Microsoft SQL Server for this connector: identifier
/// quoting, the paging tail, the two statements whose syntax genuinely diverges (upsert and multi-key
/// delete), the type table, the parameter ceiling, and how to open a connection.
///
/// <para><b>Why one interface and one project rather than two projects.</b> Cursor arithmetic, snapshot
/// paging, type coercion, schema probing, batching and upsert planning are ~90% identical between the two
/// databases. Two projects would need a third shared one to hold that (three artefacts to express two
/// connectors), or a copy — and a copied cursor rule is a cursor rule that will diverge under maintenance,
/// which is the one thing this connector cannot afford. <b>Stated cost:</b> a Postgres-only deployment
/// still ships <c>Microsoft.Data.SqlClient</c> and its <c>Azure.Identity</c> /
/// <c>Microsoft.IdentityModel.*</c> tail, roughly a few MB it will never load. That is the price of the
/// single cursor implementation, paid knowingly.</para>
///
/// <para><b>Parameters are named <c>@p0</c>, <c>@p1</c>, … in both dialects</b> — Npgsql accepts the
/// <c>@name</c> form as readily as <c>$n</c>, so the generated SQL is the same text modulo quoting and the
/// tails below, and the tests can assert it as such.</para>
/// </summary>
public interface ISqlDialect
{
    /// <summary>The <c>SourceKinds</c>/<c>SinkKinds</c> value this dialect serves — "postgres" | "mssql".</summary>
    string Kind { get; }

    /// <summary>Human name for the descriptor label.</summary>
    string Label { get; }

    /// <summary>Port used when <c>Port</c> is 0.</summary>
    int DefaultPort { get; }

    /// <summary>Schema used when <c>Schema</c> is empty — "public" / "dbo".</summary>
    string DefaultSchema { get; }

    /// <summary>Hard ceiling on parameters in ONE command. 2100 on SQL Server (a documented server limit,
    /// not a driver one), 65535 on PostgreSQL. This is what <see cref="DbSinkPlanner"/> chunks against; get
    /// it wrong and a large batch fails at the server with a message that names neither the batch nor the
    /// column count.</summary>
    int MaxCommandParameters { get; }

    /// <summary>Quotes one identifier, escaping the closing quote character by doubling it. Applied to
    /// every schema, table and column name this connector emits — the operator's <c>Where</c> clause and
    /// <c>Query</c> are the two things it does NOT touch, because those are SQL by definition.</summary>
    string QuoteIdent(string ident);

    /// <summary>The placeholder for the <paramref name="index"/>'th bound value.</summary>
    string Parameter(int index);

    /// <summary>The tail that limits a SELECT to <paramref name="rows"/> rows. On SQL Server the
    /// OFFSET/FETCH form REQUIRES an ORDER BY, which every SELECT this connector generates has — a source
    /// with no ordering column has no cursor either and is rejected in validation.</summary>
    string PageClause(int rows);

    /// <summary>"Insert these rows, replacing any that already exist on <paramref name="keys"/>" —
    /// <c>ON CONFLICT DO UPDATE</c> on PostgreSQL, <c>MERGE</c> on SQL Server. Parameters are consumed in
    /// row-major order starting at <paramref name="firstParameter"/>: row 0's columns in
    /// <paramref name="columns"/> order, then row 1's, and so on.</summary>
    string UpsertStatement(string qualifiedTable, IReadOnlyList<string> columns, IReadOnlyList<string> keys, int rowCount, int firstParameter);

    /// <summary>"Delete the rows identified by these key tuples". Diverges because SQL Server has no
    /// row-value <c>IN</c> constructor, so a composite key becomes OR'd AND-groups there and a row
    /// constructor here.</summary>
    string DeleteStatement(string qualifiedTable, IReadOnlyList<string> keys, int rowCount, int firstParameter);

    /// <summary>Maps a result column onto the platform's six field types. <paramref name="dataTypeName"/>
    /// is the driver's own name for the type (<c>numeric</c>, <c>jsonb</c>, <c>datetimeoffset</c>); it is
    /// consulted FIRST because it is the only thing that distinguishes an exact <c>numeric</c> from a
    /// <c>float8</c> once both have become <see cref="double"/>-ish CLR values, and that distinction is the
    /// one diagnostic this probe owes the operator. <paramref name="clrType"/> is the fallback for a type
    /// name neither table knows.</summary>
    TypeMapping MapType(string? dataTypeName, Type? clrType);

    /// <summary>A closed connection for <paramref name="endpoint"/>. Opening is the caller's job so the
    /// failure lands inside its own try/catch with its own timeout.</summary>
    DbConnection CreateConnection(DbEndpoint endpoint);

    /// <summary>True when <paramref name="ex"/> is the driver's own classification of a TRANSIENT fault —
    /// a pooled connection the server closed between batches, a failover, a momentary refusal. This is the
    /// entire basis for the sink's single retry: anything the driver will not vouch for as transient is
    /// counted and dropped rather than replayed, because a replayed non-transient failure is just the same
    /// failure twice plus the chance of a partial double-write.</summary>
    bool IsTransient(Exception ex);
}

/// <summary>Shared helpers neither dialect needs to say twice.</summary>
internal static class SqlDialectExtensions
{
    /// <summary><c>"schema"."table"</c>, with the dialect's default schema when none is configured.</summary>
    public static string QualifiedTable(this ISqlDialect dialect, string schema, string table)
    {
        var effective = string.IsNullOrWhiteSpace(schema) ? dialect.DefaultSchema : schema.Trim();
        return $"{dialect.QuoteIdent(effective)}.{dialect.QuoteIdent(table.Trim())}";
    }

    /// <summary>The comma-separated <c>(@p0, @p1, …)</c> tuple for one row of <paramref name="width"/>
    /// columns starting at <paramref name="first"/>.</summary>
    public static string ParameterTuple(this ISqlDialect dialect, int first, int width)
        => "(" + string.Join(", ", Enumerable.Range(first, width).Select(dialect.Parameter)) + ")";

    public static string QuotedList(this ISqlDialect dialect, IEnumerable<string> idents)
        => string.Join(", ", idents.Select(dialect.QuoteIdent));
}
