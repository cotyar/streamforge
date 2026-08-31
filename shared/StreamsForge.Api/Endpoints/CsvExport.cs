using StreamsForge.Abstractions;
using StreamsForge.AppCore.Connectors.Formats;

namespace StreamsForge.Api;

/// <summary>
/// Plan 012: what the two CSV download routes (<c>/api/tables/{id}/rows.csv</c>,
/// <c>/api/pipelines/{id}/results.csv</c>) share — column selection. The rendering itself is
/// <see cref="CsvFormatter"/>, the same writer the file sink uses, so a table exported by hand and the
/// same table written by a sink produce byte-identical lines.
/// </summary>
public static class CsvExport
{
    /// <summary>The name the multiplicity of a table row goes out under — the same column the file sink
    /// writes for a delta, so the two are the same document.</summary>
    public const string WeightColumn = "_weight";

    /// <summary>A table's rows. Columns come from the table's compiled <see cref="TableDefinition.OutputFields"/>
    /// — the authoritative order, and stable across exports even when a particular page of rows happens
    /// to be missing a value — falling back to what the rows themselves carry for a table that has not
    /// compiled yet. <see cref="WeightColumn"/> is always the last column: a Z-set row's weight is part
    /// of its meaning (a −1 is a retraction, not a duplicate), and dropping it would export something
    /// that is not the table.</summary>
    public static string Table(TableDefinition def, IReadOnlyList<TableRowDto> rows)
    {
        var columns = def.OutputFields.Count > 0
            ? def.OutputFields.Select(f => f.Name).ToList()
            : UnionOfKeys(rows.Select(r => r.Row));
        columns.Add(WeightColumn);

        return CsvFormatter.Table(
            columns,
            rows.Select(r => (IReadOnlyDictionary<string, object?>)new Dictionary<string, object?>(r.Row, StringComparer.Ordinal)
            {
                [WeightColumn] = r.Weight,
            }));
    }

    /// <summary>Loose rows (a pipeline's recent-results buffer). No definition to take columns from —
    /// a pipeline's output schema isn't stored on its definition the way a table's is — so the header is
    /// the union of the keys present, in first-seen order.</summary>
    public static string Rows(IEnumerable<Dictionary<string, object?>> rows)
    {
        var materialized = rows.ToList();
        return CsvFormatter.Table(
            UnionOfKeys(materialized),
            materialized.Select(r => (IReadOnlyDictionary<string, object?>)r));
    }

    private static List<string> UnionOfKeys(IEnumerable<Dictionary<string, object?>> rows)
    {
        var columns = new List<string>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var row in rows)
        {
            foreach (var key in row.Keys)
            {
                if (seen.Add(key))
                {
                    columns.Add(key);
                }
            }
        }

        return columns;
    }
}
