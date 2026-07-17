using System.Globalization;
using StreamForge.Abstractions;

namespace StreamForge.Host.Grains;

// ============================================================================
// Row-identity key derivation + retention math for Feature B (ROW HISTORY).
// Pure, Orleans-free classes — unit-testable without a cluster. See TableHistoryGrain
// for how they're wired into a grain.
// ============================================================================

/// <summary>
/// Best-effort extraction of a table's GROUP BY identity columns from its SQL text.
///
/// DESIGN NOTE (key-derivation choice): the natural candidate for a stable per-row identity would be
/// something the Engine already computes — but StreamForge.Engine is FROZEN (read-only) and its public
/// surface (PublicApi.cs) does not expose GROUP BY column info. TableExecutor.CanonicalRowKey (and the
/// internal, assembly-inaccessible TableKeyEncoding it wraps) key by a canonical serialization of the
/// ENTIRE output row, including aggregate columns — that changes on every update to a group (e.g. a new
/// trade changes "positions".avg_price), so it is unusable as a stable per-group identity: it would never
/// let two versions of the same symbol collide under one key, defeating the point of a version *history*.
/// The compiler's own internal matching (TablePlanner.AssignGroupByIndexes: structural equality between a
/// SELECT item's expression and a GROUP BY expression) is exactly the right idea, but it lives on internal
/// types (OutputItem, CompiledTablePlan) with no InternalsVisibleTo from Engine to Host.
///
/// So this class re-derives the same mapping textually from the table's own SQL string (which IS
/// available to Host via TableDefinition.Sql): find the GROUP BY clause, find the SELECT list, and match
/// each GROUP BY expression to a SELECT item by normalized expression text — mirroring the compiler's rule
/// well enough for the bare-column and JSON-path patterns this SQL dialect actually supports (no
/// subqueries, no derived expressions in GROUP BY beyond what's also projected). It intentionally does not
/// re-implement a full SQL parser: on any ambiguity (an unparsentable clause, a GROUP BY expression that
/// doesn't textually match any SELECT item) it returns null, and callers fall back to whole-row identity
/// (see RowKeyCodec.EncodeIdentity) — a safe degradation the task spec itself calls out as acceptable for
/// tables with "no group-by identity".
/// </summary>
public static class TableGroupKeyExtractor
{
    /// <summary>Returns the output column names (aliases) that make up this table's GROUP BY identity, in
    /// GROUP BY clause order — or null if there's no GROUP BY, or the SQL couldn't be confidently parsed
    /// well enough to map every GROUP BY expression to a SELECT output column.</summary>
    public static List<string>? ExtractIdentityColumns(string? sql)
    {
        if (string.IsNullOrWhiteSpace(sql)) return null;

        var selectIdx = FindWord(sql, "SELECT", 0);
        if (selectIdx < 0) return null;
        var afterSelect = selectIdx + "SELECT".Length;

        var fromIdx = FindWord(sql, "FROM", afterSelect);
        if (fromIdx < 0) return null;

        var groupByIdx = FindGroupBy(sql);
        if (groupByIdx < 0) return null; // no GROUP BY => no group identity; caller falls back to whole-row

        var selectListText = sql.Substring(afterSelect, fromIdx - afterSelect);
        var groupByStart = groupByIdx + GroupByKeywordLength(sql, groupByIdx);
        var groupByListText = sql[groupByStart..];

        var selectItems = SplitTopLevel(selectListText, ',')
            .Select(s => s.Trim())
            .Where(s => s.Length > 0)
            .Select(ParseSelectItem)
            .ToList();

        var groupExprs = SplitTopLevel(groupByListText, ',')
            .Select(s => Normalize(s))
            .Where(s => s.Length > 0)
            .ToList();

        if (groupExprs.Count == 0) return null;

        var identity = new List<string>();
        foreach (var g in groupExprs)
        {
            var match = selectItems.FirstOrDefault(it => it.Alias is not null && Normalize(it.ExprText) == g);
            if (match.Alias is null)
            {
                return null; // couldn't confidently map this GROUP BY expression -> fall back to whole-row
            }
            identity.Add(match.Alias);
        }
        return identity;
    }

    private static (string ExprText, string? Alias) ParseSelectItem(string raw)
    {
        var asIdx = FindWord(raw, "AS", 0);
        if (asIdx >= 0)
        {
            var expr = raw[..asIdx].Trim();
            var alias = raw[(asIdx + 2)..].Trim().Trim('"', '\'');
            return (expr, alias.Length > 0 ? alias : null);
        }

        var exprTrim = raw.Trim();
        return (exprTrim, DefaultAlias(exprTrim));
    }

    /// <summary>Mirrors TablePlanner.DefaultName's non-aggregate cases (bare/qualified identifier, JSON
    /// access chain) well enough for identity-column matching purposes — aggregate/function-call defaults
    /// are irrelevant here since AssignGroupByIndexes never matches an aggregate expression anyway.</summary>
    private static string? DefaultAlias(string expr)
    {
        if (expr.Length == 0) return null;

        // JSON access chain 'a -> b ->> "key"' (or ' -> "key"') -> alias = last string-literal key.
        var lastArrow = LastTopLevelArrow(expr);
        if (lastArrow >= 0)
        {
            var tail = expr[(lastArrow)..].TrimStart('-', '>').Trim();
            if (tail.Length >= 2 && tail[0] == '\'' && tail[^1] == '\'')
            {
                return tail[1..^1];
            }
            return null; // numeric/other JSON keys not needed for this dialect's demo patterns
        }

        // Qualified identifier "alias.field" (no function call parens) -> alias = last dotted segment.
        if (!expr.Contains('(') && expr.Contains('.'))
        {
            var lastDot = expr.LastIndexOf('.');
            var candidate = expr[(lastDot + 1)..].Trim();
            return IsSimpleIdentifier(candidate) ? candidate : null;
        }

        // Bare identifier.
        return IsSimpleIdentifier(expr) ? expr : null;
    }

    private static bool IsSimpleIdentifier(string s) =>
        s.Length > 0 && (char.IsLetter(s[0]) || s[0] == '_') && s.All(c => char.IsLetterOrDigit(c) || c == '_');

    private static int LastTopLevelArrow(string s)
    {
        var idx = -1;
        var inQuote = false;
        for (var i = 0; i < s.Length; i++)
        {
            if (s[i] == '\'') { inQuote = !inQuote; continue; }
            if (inQuote) continue;
            if (s[i] == '-' && i + 1 < s.Length && s[i + 1] == '>')
            {
                idx = i;
            }
        }
        return idx;
    }

    private static string Normalize(string s) =>
        string.Join(" ", s.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));

    /// <summary>Splits on <paramref name="sep"/> at paren-depth 0, outside single-quoted string literals.</summary>
    private static List<string> SplitTopLevel(string s, char sep)
    {
        var result = new List<string>();
        var depth = 0;
        var inQuote = false;
        var start = 0;
        for (var i = 0; i < s.Length; i++)
        {
            var c = s[i];
            if (c == '\'') { inQuote = !inQuote; continue; }
            if (inQuote) continue;
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (c == sep && depth == 0)
            {
                result.Add(s[start..i]);
                start = i + 1;
            }
        }
        result.Add(s[start..]);
        return result;
    }

    /// <summary>Finds a whole-word, case-insensitive keyword outside single-quoted literals.</summary>
    private static int FindWord(string s, string word, int from)
    {
        var inQuote = false;
        for (var i = from; i <= s.Length - word.Length; i++)
        {
            var c = s[i];
            if (c == '\'') { inQuote = !inQuote; continue; }
            if (inQuote) continue;
            if (string.Compare(s, i, word, 0, word.Length, StringComparison.OrdinalIgnoreCase) != 0) continue;
            var leftOk = i == 0 || !char.IsLetterOrDigit(s[i - 1]);
            var after = i + word.Length;
            var rightOk = after >= s.Length || !char.IsLetterOrDigit(s[after]);
            if (leftOk && rightOk) return i;
        }
        return -1;
    }

    /// <summary>Finds "GROUP" followed (after whitespace) by "BY", outside quoted literals — flexible on
    /// internal whitespace, unlike a literal FindWord(s, "GROUP BY", ...) would be.</summary>
    private static int FindGroupBy(string s)
    {
        var inQuote = false;
        for (var i = 0; i <= s.Length - 5; i++)
        {
            var c = s[i];
            if (c == '\'') { inQuote = !inQuote; continue; }
            if (inQuote) continue;
            if (string.Compare(s, i, "GROUP", 0, 5, StringComparison.OrdinalIgnoreCase) != 0) continue;
            var leftOk = i == 0 || !char.IsLetterOrDigit(s[i - 1]);
            if (!leftOk) continue;
            var j = i + 5;
            if (j < s.Length && char.IsLetterOrDigit(s[j])) continue; // e.g. "GROUPING"
            while (j < s.Length && char.IsWhiteSpace(s[j])) j++;
            if (j + 2 > s.Length || string.Compare(s, j, "BY", 0, 2, StringComparison.OrdinalIgnoreCase) != 0) continue;
            var afterBy = j + 2;
            if (afterBy < s.Length && char.IsLetterOrDigit(s[afterBy])) continue;
            return i;
        }
        return -1;
    }

    private static int GroupByKeywordLength(string s, int groupByIdx)
    {
        var j = groupByIdx + 5;
        while (j < s.Length && char.IsWhiteSpace(s[j])) j++;
        return j + 2 - groupByIdx;
    }
}

/// <summary>Encodes a row's identity into a stable, deterministic string key. When
/// <paramref name="identityColumns"/> is non-empty, the key covers only those columns' values (the
/// table's GROUP BY identity — see TableGroupKeyExtractor). Otherwise it falls back to a canonical
/// encoding of the WHOLE row (minus the "_ts"/"_source" transport metadata fields) — the documented
/// behavior for tables with no derivable group-by identity: each distinct combination of output values
/// gets its own key, so history only "accumulates" versions when literally nothing in the row changed
/// between updates (rare, but harmless — see TableHistoryGrain's class comment).</summary>
public static class RowKeyCodec
{
    public static string EncodeIdentity(IReadOnlyDictionary<string, object?> row, IReadOnlyList<string>? identityColumns)
    {
        if (identityColumns is { Count: > 0 })
        {
            return string.Join("", identityColumns.Select(c => $"{c}{EncodeValue(row.GetValueOrDefault(c))}"));
        }

        var ordered = row
            .Where(kv => kv.Key != EventRecordFields.TimestampField && kv.Key != EventRecordFields.SourceField)
            .OrderBy(kv => kv.Key, StringComparer.Ordinal);
        return string.Join("", ordered.Select(kv => $"{kv.Key}{EncodeValue(kv.Value)}"));
    }

    private static string EncodeValue(object? v) => v switch
    {
        null => "N",
        long l => $"L:{l.ToString(CultureInfo.InvariantCulture)}",
        int i => $"L:{i.ToString(CultureInfo.InvariantCulture)}",
        double d => $"D:{d.ToString("R", CultureInfo.InvariantCulture)}",
        string s => $"S:{s}",
        bool b => $"B:{b}",
        Dictionary<string, object?> dict => $"J:{{{EncodeIdentity(dict, null)}}}",
        System.Collections.IEnumerable list => $"A:[{string.Join(",", list.Cast<object?>().Select(EncodeValue))}]",
        _ => $"?:{v}",
    };
}

/// <summary>Mirrors the two reserved EventRecord field names (StreamForge.Engine.EventRecord.TimestampField
/// / SourceField) without taking a dependency on the Engine's EventRecord type here — this file only deals
/// in plain Dictionary&lt;string, object?&gt; rows (as carried by TableDeltaDto), not EventRecord itself.</summary>
internal static class EventRecordFields
{
    public const string TimestampField = "_ts";
    public const string SourceField = "_source";
}

/// <summary>Pure (no Orleans, no I/O) retention engine for one row-identity's version history. See
/// TableDefinition.HistoryMode for the policy semantics and TableHistoryGrain for how this is wired to the
/// delta stream.</summary>
public static class TableRowHistoryRetention
{
    /// <summary>Safety cap applied to All-mode history so a single hot key can't grow its version list
    /// without bound — documented, not configurable.</summary>
    public const int AllModeCap = 1000;

    /// <summary>Appends one new ASSERTION version to <paramref name="entry"/> per the configured mode,
    /// applying HistoryWindowMs pruning first. Mutates <paramref name="entry"/> in place.</summary>
    public static void Append(
        RowHistoryEntry entry,
        HistoryVersion version,
        TableHistoryMode mode,
        int limit,
        string? byField,
        long windowMs)
    {
        PruneWindow(entry, version.TsMs, windowMs);

        switch (mode)
        {
            case TableHistoryMode.All:
                entry.Versions.Add(version);
                if (entry.Versions.Count > AllModeCap)
                {
                    entry.Versions.RemoveRange(0, entry.Versions.Count - AllModeCap);
                }
                break;

            case TableHistoryMode.LastN:
                entry.Versions.Add(version);
                var cap = Math.Max(1, limit);
                if (entry.Versions.Count > cap)
                {
                    entry.Versions.RemoveRange(0, entry.Versions.Count - cap);
                }
                break;

            case TableHistoryMode.FirstN:
                if (entry.Versions.Count < Math.Max(1, limit))
                {
                    entry.Versions.Add(version);
                }
                break;

            case TableHistoryMode.MinBy:
            case TableHistoryMode.MaxBy:
                ApplyExtremeLatest(entry, version, mode, byField);
                break;
        }
    }

    /// <summary>Drops versions older than (nowMs - windowMs). windowMs &lt;= 0 means unbounded (no-op).
    /// Exposed separately so callers (TableHistoryGrain.GetHistoryAsync) can also prune purely for the
    /// passage of time, on read, without appending anything.</summary>
    public static void PruneWindow(RowHistoryEntry entry, long nowMs, long windowMs)
    {
        if (windowMs <= 0) return;
        var cutoff = nowMs - windowMs;
        entry.Versions.RemoveAll(v => v.TsMs < cutoff);
    }

    /// <summary>MinBy/MaxBy: keep at most 2 entries — [extreme, latest] — collapsing to a single entry
    /// when the new version IS the extreme (including the very first version ever seen for this key).
    /// A version whose HistoryByField value is missing/non-numeric never displaces the current extreme
    /// (NaN comparisons are always false) but is still recorded as "latest".</summary>
    private static void ApplyExtremeLatest(RowHistoryEntry entry, HistoryVersion version, TableHistoryMode mode, string? byField)
    {
        var currentExtreme = entry.Versions.Count > 0 ? entry.Versions[0] : null;
        var extreme = currentExtreme;

        if (extreme is null)
        {
            extreme = version;
        }
        else
        {
            var extVal = ExtractValue(extreme, byField);
            var newVal = ExtractValue(version, byField);
            var newIsMoreExtreme = mode == TableHistoryMode.MinBy ? newVal < extVal : newVal > extVal;
            if (newIsMoreExtreme)
            {
                extreme = version;
            }
        }

        entry.Versions = ReferenceEquals(extreme, version)
            ? [version]
            : [extreme, version];
    }

    private static double ExtractValue(HistoryVersion v, string? byField)
    {
        if (byField is null || !v.Row.TryGetValue(byField, out var raw))
        {
            return double.NaN;
        }
        return raw switch
        {
            long l => l,
            int i => i,
            double d => d,
            bool b => b ? 1 : 0,
            _ => double.NaN,
        };
    }
}
