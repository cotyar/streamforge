using System.Text.Json;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Ingest;
using StreamsForge.AppCore.Json;

namespace StreamsForge.AppCore.Connectors.Mapping;

/// <summary>
/// Turns one fetched response body (already parsed into a <see cref="JsonElement"/>) into the row
/// dictionaries the rest of the connector pipeline (dedup, ledger, emission) works with, per a
/// <see cref="MappingSpec"/> (plan 006, D-B/D-D).
/// </summary>
public static class RecordExtractor
{
    /// <summary>Extracts every item's row. <see cref="MappingSpec.ItemsPath"/> is resolved via
    /// <see cref="JsonPathLite"/>; each resulting match that is itself a JSON array is spread into one
    /// item per element (this is what makes plain <c>"$"</c> mean "each element" on an array root and
    /// "the single object" on an object root — the same rule generalizes to any path, so
    /// <c>"$.data"</c> pointing at an array works the same way without needing a trailing
    /// <c>[*]</c>). Rows never contain a "_source" key — the driver stamps that.</summary>
    public static List<Dictionary<string, object?>> Extract(JsonElement root, MappingSpec spec, long arrivalMs)
    {
        var itemsPath = string.IsNullOrEmpty(spec.ItemsPath) ? "$" : spec.ItemsPath;
        var matches = JsonPathLite.Select(root, itemsPath);

        var items = new List<JsonElement>();
        foreach (var match in matches)
        {
            if (match.ValueKind == JsonValueKind.Array)
            {
                foreach (var element in match.EnumerateArray())
                {
                    items.Add(element);
                }
            }
            else
            {
                items.Add(match);
            }
        }

        var rows = new List<Dictionary<string, object?>>(items.Count);
        foreach (var item in items)
        {
            rows.Add(ExtractRow(item, spec, arrivalMs));
        }

        return rows;
    }

    private static Dictionary<string, object?> ExtractRow(JsonElement item, MappingSpec spec, long arrivalMs)
    {
        var row = new Dictionary<string, object?>();

        foreach (var entry in spec.Fields)
        {
            var path = entry.SourcePath ?? entry.Field.Name;
            var matches = JsonPathLite.Select(item, path);
            if (matches.Count == 0)
            {
                continue; // missing path -> key omitted entirely (proto-encoder-compatible absence).
            }

            if (entry.Field.IsArray)
            {
                var values = new List<object?>();
                if (matches.Count == 1 && matches[0].ValueKind == JsonValueKind.Array)
                {
                    // A single match that is itself an array: its elements become the list.
                    foreach (var element in matches[0].EnumerateArray())
                    {
                        values.Add(JsonValueNormalizer.Normalize(element));
                    }
                }
                else
                {
                    // Multiple matches (e.g. from a [*] SourcePath), or a single non-array match:
                    // every match becomes one list element.
                    foreach (var m in matches)
                    {
                        values.Add(JsonValueNormalizer.Normalize(m));
                    }
                }

                row[entry.Field.Name] = values;
            }
            else
            {
                row[entry.Field.Name] = JsonValueNormalizer.Normalize(matches[0]);
            }
        }

        row["_ts"] = ResolveTimestamp(row, spec, arrivalMs);
        return row;
    }

    /// <summary>"_ts" resolution: the value already extracted under the emitted field named by
    /// <see cref="MappingSpec.TimestampField"/>, resolved via <see cref="RowTimestamp.Resolve"/>
    /// (extracted plan 008 W4 so this and the client-push ingest path agree on what a timestamp
    /// value means) — a number is epoch-ms, a string is parsed as ISO-8601 (UTC), anything else
    /// (missing field, unparseable string, no TimestampField configured) falls back to
    /// <paramref name="arrivalMs"/>.</summary>
    private static long ResolveTimestamp(Dictionary<string, object?> row, MappingSpec spec, long arrivalMs)
    {
        if (spec.TimestampField is { Length: > 0 } key && row.TryGetValue(key, out var value))
        {
            return RowTimestamp.Resolve(value, arrivalMs);
        }

        return arrivalMs;
    }
}
