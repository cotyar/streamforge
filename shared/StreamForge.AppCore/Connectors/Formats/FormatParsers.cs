using System.Buffers;
using System.Globalization;
using System.Text;
using System.Text.Json;

namespace StreamForge.AppCore.Connectors.Formats;

/// <summary>
/// The three file/URL response body formats file/folder/url connector sources accept
/// (<see cref="StreamForge.Abstractions.FileFormats"/>), each turning raw text into a flat list of
/// <see cref="JsonElement"/> items ready for <see cref="StreamForge.AppCore.Connectors.Mapping.RecordExtractor"/>.
/// Malformed input throws <see cref="FormatException"/> with a message that names the offending line
/// wherever the format has a natural notion of "line" (NDJSON always; CSV for its own structural
/// errors; the underlying <see cref="JsonException"/> for JSON-array parsing already reports its own
/// line/position, which is propagated as-is).
/// </summary>
public static class FormatParsers
{
    /// <summary>Newline-delimited JSON: one JSON value per non-blank line. Blank/whitespace-only
    /// lines are skipped (tolerated), matching how NDJSON producers commonly pad output.</summary>
    public static List<JsonElement> ParseNdjson(string text)
    {
        var items = new List<JsonElement>();
        var lineNumber = 0;

        using var reader = new StringReader(text);
        string? line;
        while ((line = reader.ReadLine()) is not null)
        {
            lineNumber++;
            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }

            try
            {
                using var doc = JsonDocument.Parse(line);
                items.Add(doc.RootElement.Clone());
            }
            catch (JsonException ex)
            {
                throw new FormatException($"NDJSON parse error on line {lineNumber}: {ex.Message}", ex);
            }
        }

        return items;
    }

    /// <summary>A JSON document whose root is either an array (each element becomes one item) or a
    /// single object/scalar (becomes the sole item).</summary>
    public static List<JsonElement> ParseJsonArray(string text)
    {
        JsonDocument doc;
        try
        {
            doc = JsonDocument.Parse(text);
        }
        catch (JsonException ex)
        {
            throw new FormatException($"JSON parse error: {ex.Message}", ex);
        }

        using (doc)
        {
            var root = doc.RootElement;
            if (root.ValueKind == JsonValueKind.Array)
            {
                var items = new List<JsonElement>(root.GetArrayLength());
                foreach (var element in root.EnumerateArray())
                {
                    items.Add(element.Clone());
                }

                return items;
            }

            return [root.Clone()];
        }
    }

    /// <summary>RFC 4180 CSV with a header row: quoted fields (double-quote-escaped via <c>""</c>),
    /// commas and newlines inside quoted fields, CRLF/LF/CR line endings. Each data row becomes a
    /// flat JSON object keyed by the header. A short row is padded with empty-string values; a long
    /// row's extra columns are dropped. Values are typed <see cref="long"/>/<see cref="double"/>/
    /// <see cref="bool"/> when they parse exactly (invariant culture), else kept as
    /// <see cref="string"/> — including the empty string for an empty cell.
    ///
    /// <para><b>Duplicate header dedup</b>: when the header row repeats a column name, only the
    /// LAST occurrence of that name is kept (using that occurrence's own column value) — earlier
    /// duplicates are dropped entirely rather than silently overwritten in an unpredictable order.
    /// </para></summary>
    public static List<JsonElement> ParseCsv(string text)
    {
        var records = ParseCsvRecords(text);
        if (records.Count == 0)
        {
            return [];
        }

        var header = records[0];
        var lastIndexOfName = new Dictionary<string, int>(StringComparer.Ordinal);
        for (var c = 0; c < header.Count; c++)
        {
            lastIndexOfName[header[c]] = c;
        }

        var items = new List<JsonElement>(records.Count - 1);
        for (var r = 1; r < records.Count; r++)
        {
            items.Add(BuildCsvRowElement(header, records[r], lastIndexOfName));
        }

        return items;
    }

    private static JsonElement BuildCsvRowElement(List<string> header, List<string> fields, Dictionary<string, int> lastIndexOfName)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            for (var c = 0; c < header.Count; c++)
            {
                if (lastIndexOfName[header[c]] != c)
                {
                    continue; // an earlier duplicate of a name that reappears later in the header — skip.
                }

                var raw = c < fields.Count ? fields[c] : "";
                writer.WritePropertyName(header[c]);
                WriteSniffedCsvValue(writer, raw);
            }

            writer.WriteEndObject();
        }

        using var doc = JsonDocument.Parse(buffer.WrittenMemory);
        return doc.RootElement.Clone();
    }

    private static void WriteSniffedCsvValue(Utf8JsonWriter writer, string raw)
    {
        if (long.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var l))
        {
            writer.WriteNumberValue(l);
            return;
        }

        if (double.TryParse(raw, NumberStyles.Float, CultureInfo.InvariantCulture, out var d))
        {
            writer.WriteNumberValue(d);
            return;
        }

        if (bool.TryParse(raw, out var b))
        {
            writer.WriteBooleanValue(b);
            return;
        }

        writer.WriteStringValue(raw);
    }

    /// <summary>Tokenizes RFC 4180 CSV text into rows of raw field strings. A quote is only
    /// recognized as the start of a quoted field at the very start of a field (RFC 4180 doesn't allow
    /// quotes to appear inside an unquoted field) — anything else involving a stray <c>"</c> is
    /// rejected as malformed rather than silently mangled.</summary>
    private static List<List<string>> ParseCsvRecords(string text)
    {
        var records = new List<List<string>>();
        var field = new StringBuilder();
        var record = new List<string>();
        var inQuotes = false;
        var quoteStartLine = 0;
        var lineNumber = 1;
        var i = 0;
        var n = text.Length;

        while (i < n)
        {
            var c = text[i];

            if (inQuotes)
            {
                if (c == '"')
                {
                    if (i + 1 < n && text[i + 1] == '"')
                    {
                        field.Append('"');
                        i += 2;
                        continue;
                    }

                    inQuotes = false;
                    i++;
                    continue;
                }

                if (c == '\n')
                {
                    lineNumber++;
                }

                field.Append(c);
                i++;
                continue;
            }

            switch (c)
            {
                case '"' when field.Length == 0:
                    inQuotes = true;
                    quoteStartLine = lineNumber;
                    i++;
                    break;

                case '"':
                    throw new FormatException(
                        $"CSV parse error on line {lineNumber}: unexpected '\"' inside an unquoted field.");

                case ',':
                    record.Add(field.ToString());
                    field.Clear();
                    i++;
                    break;

                case '\r':
                    record.Add(field.ToString());
                    field.Clear();
                    records.Add(record);
                    record = [];
                    i++;
                    if (i < n && text[i] == '\n')
                    {
                        i++;
                    }
                    lineNumber++;
                    break;

                case '\n':
                    record.Add(field.ToString());
                    field.Clear();
                    records.Add(record);
                    record = [];
                    i++;
                    lineNumber++;
                    break;

                default:
                    field.Append(c);
                    i++;
                    break;
            }
        }

        if (inQuotes)
        {
            throw new FormatException($"CSV parse error: unterminated quoted field starting on line {quoteStartLine}.");
        }

        if (field.Length > 0 || record.Count > 0)
        {
            record.Add(field.ToString());
            records.Add(record);
        }

        return records;
    }
}
