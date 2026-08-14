using System.Data.Common;
using System.Globalization;

namespace StreamForge.Connectors.Database.Tests.Integration;

/// <summary>
/// The tests' own tiny SQL runner — seed a table, then read it back to see what the connector did. It is
/// separate from anything in the production assembly on purpose: a test that verified the sink by asking
/// the sink's own planner what it wrote would be verifying the planner against itself. These four methods
/// go to the server directly and believe only what it says.
///
/// <para>Parameters are bound, and named <c>@p0…</c> in both dialects for the same reason the connector
/// names them that way — Npgsql accepts the <c>@name</c> form as readily as SQL Server does.</para>
/// </summary>
public static class Sql
{
    public static async Task ExecAsync(DbConnection connection, string sql, params object?[] values)
    {
        await using var command = Command(connection, sql, values);
        await command.ExecuteNonQueryAsync().ConfigureAwait(false);
    }

    public static async Task<object?> ScalarAsync(DbConnection connection, string sql, params object?[] values)
    {
        await using var command = Command(connection, sql, values);
        var result = await command.ExecuteScalarAsync().ConfigureAwait(false);
        return result is DBNull ? null : result;
    }

    /// <summary>The scalar as a long — row counts, sums, MAX(id).</summary>
    public static async Task<long> CountAsync(DbConnection connection, string sql, params object?[] values)
        => Convert.ToInt64(await ScalarAsync(connection, sql, values).ConfigureAwait(false) ?? 0L, CultureInfo.InvariantCulture);

    public static async Task<List<Dictionary<string, object?>>> QueryAsync(DbConnection connection, string sql, params object?[] values)
    {
        await using var command = Command(connection, sql, values);
        await using var reader = await command.ExecuteReaderAsync().ConfigureAwait(false);

        List<Dictionary<string, object?>> rows = [];
        while (await reader.ReadAsync().ConfigureAwait(false))
        {
            Dictionary<string, object?> row = new(StringComparer.Ordinal);
            for (var i = 0; i < reader.FieldCount; i++)
            {
                row[reader.GetName(i)] = await reader.IsDBNullAsync(i).ConfigureAwait(false) ? null : reader.GetValue(i);
            }

            rows.Add(row);
        }

        return rows;
    }

    private static DbCommand Command(DbConnection connection, string sql, object?[] values)
    {
        var command = connection.CreateCommand();
        command.CommandText = sql;
        command.CommandTimeout = 60;
        for (var i = 0; i < values.Length; i++)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = "p" + i.ToString(CultureInfo.InvariantCulture);
            parameter.Value = values[i] ?? DBNull.Value;
            command.Parameters.Add(parameter);
        }

        return command;
    }
}
