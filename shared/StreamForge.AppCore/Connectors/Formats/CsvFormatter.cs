using System.Globalization;
using System.Text;

namespace StreamForge.AppCore.Connectors.Formats;

/// <summary>
/// Plan 012: the write half of <see cref="FormatParsers.ParseCsv"/> — RFC 4180 rendering, shared by the
/// file sink (<c>StreamForge.AppCore.Sinks.FileSinkClient</c>) and the CSV download routes on
/// <c>/api/tables/{id}/rows.csv</c> and <c>/api/pipelines/{id}/results.csv</c>, so egress CSV is one
/// implementation rather than one per caller. Anything this writes, <see cref="FormatParsers.ParseCsv"/>
/// reads back — <c>CsvFormatterTests</c> pins that round trip, which is the only property of a CSV writer
/// actually worth guaranteeing.
/// </summary>
public static class CsvFormatter
{
    /// <summary>Line ending. CRLF is what RFC 4180 specifies and what Excel expects; every reader that
    /// accepts LF accepts CRLF too, so this is the strictly safer of the two.</summary>
    public const string NewLine = "\r\n";

    /// <summary>Renders one record. A field is quoted only when it has to be (it contains the delimiter,
    /// a quote, CR or LF, or leading/trailing whitespace) — quoting everything would be simpler but makes
    /// the common file noisier for no gain to any parser.</summary>
    public static string Row(IEnumerable<object?> values, char delimiter = ',')
    {
        var sb = new StringBuilder();
        var first = true;
        foreach (var value in values)
        {
            if (!first)
            {
                sb.Append(delimiter);
            }

            first = false;
            AppendField(sb, Render(value), delimiter);
        }

        sb.Append(NewLine);
        return sb.ToString();
    }

    /// <summary>Header + one line per row, columns taken in <paramref name="columns"/> order; a row
    /// missing a column writes an empty cell.</summary>
    public static string Table(IReadOnlyList<string> columns, IEnumerable<IReadOnlyDictionary<string, object?>> rows, char delimiter = ',')
    {
        var sb = new StringBuilder();
        sb.Append(Row(columns, delimiter));
        foreach (var row in rows)
        {
            sb.Append(Row(columns.Select(c => row.TryGetValue(c, out var v) ? v : null), delimiter));
        }

        return sb.ToString();
    }

    /// <summary>Cell text for one value. Invariant culture throughout (a decimal comma inside a
    /// comma-delimited file is exactly the kind of locale bug that only shows up on someone else's
    /// machine); <c>true</c>/<c>false</c> lower-cased so <see cref="FormatParsers.ParseCsv"/>'s own
    /// <c>bool.TryParse</c> round-trips it; null is an empty cell — which means null and "" are the same
    /// cell on the way out, an inherent property of CSV rather than a choice made here.</summary>
    public static string Render(object? value) => value switch
    {
        null => "",
        string s => s,
        bool b => b ? "true" : "false",
        double d => d.ToString("R", CultureInfo.InvariantCulture),
        float f => f.ToString("R", CultureInfo.InvariantCulture),
        decimal m => m.ToString(CultureInfo.InvariantCulture),
        DateTimeOffset dto => dto.ToString("O", CultureInfo.InvariantCulture),
        DateTime dt => dt.ToString("O", CultureInfo.InvariantCulture),
        IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
        _ => value.ToString() ?? "",
    };

    private static void AppendField(StringBuilder sb, string text, char delimiter)
    {
        var needsQuotes = text.Length > 0
            && (text.IndexOf(delimiter) >= 0
                || text.IndexOf('"') >= 0
                || text.IndexOf('\n') >= 0
                || text.IndexOf('\r') >= 0
                || char.IsWhiteSpace(text[0])
                || char.IsWhiteSpace(text[^1]));

        if (!needsQuotes)
        {
            sb.Append(text);
            return;
        }

        sb.Append('"');
        foreach (var c in text)
        {
            if (c == '"')
            {
                sb.Append('"');
            }

            sb.Append(c);
        }

        sb.Append('"');
    }
}
