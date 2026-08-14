using StreamForge.AppCore.Sinks;
using StreamForge.AppCore.Transports;

namespace StreamForge.Connectors.Database;

/// <summary>
/// The entire wiring surface of this assembly: six registrations behind one call — two polled sources
/// (<see cref="DbSource"/> for postgres/mssql), two CDC-polled sources (<see cref="PgCdcSource"/> and
/// <see cref="MsSqlCdcSource"/>), and two sinks (<see cref="DbSink"/>) — made from each host's startup.
/// That single explicit call is the point of plan 014's out-of-core connector — it is the second real
/// call site <c>InboundTransports.Register</c> / <c>SinkTransports.Register</c> never had, and it is what
/// makes "a transport this platform's core has never heard of" a thing the acceptance tests can
/// demonstrate rather than a claim in a doc comment.
///
/// <para><b>The CDC pair is registered outside the dialect loop, deliberately.</b> The loop below is
/// dialect-symmetric — every <see cref="ISqlDialect"/> gets a <see cref="DbSource"/> and a
/// <see cref="DbSink"/> — but <see cref="PgCdcSource"/> and <see cref="MsSqlCdcSource"/> are not
/// interchangeable across dialects: one speaks Postgres logical replication, the other SQL Server capture
/// tables, and there is no CDC sink at all (CDC is ingress only). Forcing that into the loop would mean
/// inventing a capability interface or a registry abstraction just to keep one `foreach` uniform — two
/// explicit registrations below is the least clever expression of the actual shape.</para>
///
/// <para><b>Not an <see cref="System.Runtime.Loader.AssemblyLoadContext"/> plugin, deliberately.</b>
/// <c>Microsoft.Data.SqlClient</c> drags <c>Azure.Identity</c> and <c>Microsoft.IdentityModel.*</c>; a
/// diamond conflict against a plugin's own copies is precisely what ALC isolation is famous for, and this
/// repo has no diagnostic surface for one. A compile-time reference plus this call keeps runtime loading
/// open as a future option and costs nothing today.</para>
///
/// <para><b>Both dialects ship together, in one project.</b> A Postgres-only deployment still carries
/// <c>Microsoft.Data.SqlClient</c> and its dependency tail. See <see cref="ISqlDialect"/> for why that is
/// the cheaper of the two mistakes available: the cursor, snapshot-paging, coercion, probe, batching and
/// upsert-planning logic is one implementation, and a copied cursor rule is a cursor rule that diverges.</para>
///
/// <para><b>Registration is process-global and permanent</b> (both registries throw on a duplicate kind),
/// so <see cref="RegisterAll"/> is idempotent by choice rather than by luck: calling it twice — two hosts
/// in one test process, a re-entrant startup — is a no-op the second time rather than an exception that
/// takes the host down for a reason that has nothing to do with the operator.</para>
/// </summary>
public static class DatabaseConnectors
{
    private static readonly Lock Gate = new();
    private static bool _registered;

    /// <summary>Registers the postgres and mssql source and sink transports, plus the postgres-cdc and
    /// mssql-cdc sources. Call once from host startup, before any source or sink is opened.</summary>
    public static void RegisterAll()
    {
        lock (Gate)
        {
            if (_registered)
            {
                return;
            }

            _registered = true;

            foreach (ISqlDialect dialect in Dialects)
            {
                PolledTransports.Register(new DbSource(dialect));
                SinkTransports.Register(new DbSink(dialect));
            }

            // Not part of the loop above — see the class doc for why. One CDC source per dialect, ingress
            // only (no CDC sink exists).
            PolledTransports.Register(new PgCdcSource(new PostgresDialect()));
            PolledTransports.Register(new MsSqlCdcSource(new SqlServerDialect()));
        }
    }

    /// <summary>The dialects this assembly serves. Exposed so tests can drive both without re-deriving
    /// the list, and so a reader can see at a glance that "two databases" is two entries and nothing else.</summary>
    public static IReadOnlyList<ISqlDialect> Dialects { get; } = [new PostgresDialect(), new SqlServerDialect()];
}
