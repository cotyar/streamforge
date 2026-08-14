using StreamForge.AppCore.Sinks;
using StreamForge.AppCore.Transports;

namespace StreamForge.Connectors.Database;

/// <summary>
/// The entire wiring surface of this assembly: four registrations behind one call, made from each host's
/// startup. That single explicit call is the point of plan 014's out-of-core connector — it is the second
/// real call site <c>InboundTransports.Register</c> / <c>SinkTransports.Register</c> never had, and it is
/// what makes "a transport this platform's core has never heard of" a thing the acceptance tests can
/// demonstrate rather than a claim in a doc comment.
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

    /// <summary>Registers the postgres and mssql source and sink transports. Call once from host startup,
    /// before any source or sink is opened.</summary>
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
        }
    }

    /// <summary>The dialects this assembly serves. Exposed so tests can drive both without re-deriving
    /// the list, and so a reader can see at a glance that "two databases" is two entries and nothing else.</summary>
    public static IReadOnlyList<ISqlDialect> Dialects { get; } = [new PostgresDialect(), new SqlServerDialect()];
}
