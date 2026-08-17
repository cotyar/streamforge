using System.Globalization;
using System.Text.Json;
using StreamForge.Abstractions;
using StreamForge.Engine;

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
///
/// THE FALLBACK IS NO LONGER SILENT (post-011 D). The degradation above is safe but it is NOT free, and
/// the two cases that reach it are not the same thing:
///   * a table with NO GROUP BY / LATEST BY at all has always been keyed by the whole row, that IS its
///     identity, and nothing is wrong — this is the case the fallback was designed for;
///   * a table that DOES declare a key the mapping could not resolve (an expression key like
///     <c>ts_ms - ts_ms % 43000</c>, a CAST, a JSON path that doesn't match its own projection
///     character-for-character) silently loses the very thing history was enabled for: each row version
///     lands under its own whole-row key, so the version trail never forms.
/// <see cref="Describe"/> tells the two apart — it reports the clause and the raw declared keys alongside
/// the mapped columns — and <see cref="TableRowIdentityWarning.For(TableDefinition)"/> turns the second
/// case into a human-readable warning. That warning is reported in two places, both in the SHARED API
/// layer so the two runtime flavors cannot drift: the server log at table create/update
/// (TablesEndpoints.WarnOnDegradedRowIdentity — it warns, it never rejects), and
/// <see cref="TableMetrics.RowIdentityWarning"/> on <c>GET /api/tables/{id}/metrics</c>, which the
/// console polls and renders verbatim on the Row history card and the Sharding panel — the same object
/// and the same idiom <c>TableMetrics.Rebuilding</c> already uses. Derived on every read rather than
/// persisted, so it can never disagree with the SQL it describes.
/// </summary>
public static class TableGroupKeyExtractor
{
    /// <summary>Returns the output column names (aliases) that make up this table's GROUP BY identity, in
    /// GROUP BY clause order — or null if there's no GROUP BY, or the SQL couldn't be confidently parsed
    /// well enough to map every GROUP BY expression to a SELECT output column. Unchanged in behavior: it
    /// is now the <see cref="TableRowIdentity.Columns"/> half of <see cref="Describe"/>.</summary>
    public static List<string>? ExtractIdentityColumns(string? sql) => Describe(sql).Columns;

    /// <summary>The same derivation <see cref="ExtractIdentityColumns"/> performs, reporting WHAT IT SAW as
    /// well as what it resolved: which clause (if any) declared a row identity, the raw declared key
    /// expressions, and the mapped output columns (null on the whole-row fallback). Callers that only want
    /// the key use ExtractIdentityColumns; callers that want to tell "no declared identity" (fine) from
    /// "declared an identity we could not map" (degraded) use this — see the class doc.</summary>
    public static TableRowIdentity Describe(string? sql)
    {
        if (string.IsNullOrWhiteSpace(sql)) return TableRowIdentity.None;

        var selectIdx = FindWord(sql, "SELECT", 0);
        if (selectIdx < 0) return TableRowIdentity.None;
        var afterSelect = selectIdx + "SELECT".Length;

        var fromIdx = FindWord(sql, "FROM", afterSelect);
        if (fromIdx < 0) return TableRowIdentity.None;

        var groupByIdx = FindGroupBy(sql);
        if (groupByIdx < 0)
        {
            // No GROUP BY — a LATEST BY (col, ...) clause (plan 002) also defines a row identity: its key
            // columns, which the engine requires to be plainly projected. Same best-effort contract: map
            // each key to a SELECT output column, whole-row fallback on any ambiguity.
            return DescribeLatestBy(sql, afterSelect, fromIdx);
        }

        var groupByStart = groupByIdx + GroupByKeywordLength(sql, groupByIdx);
        var groupExprs = SplitTopLevel(sql[groupByStart..], ',')
            .Select(Normalize)
            .Where(s => s.Length > 0)
            .ToList();

        if (groupExprs.Count == 0) return TableRowIdentity.None;

        return new TableRowIdentity(
            TableRowIdentity.GroupByClause,
            groupExprs,
            MapKeysToColumns(sql, afterSelect, fromIdx, groupExprs));
    }

    private static TableRowIdentity DescribeLatestBy(string sql, int afterSelect, int fromIdx)
    {
        var latestIdx = FindWord(sql, "LATEST", fromIdx);
        if (latestIdx < 0) return TableRowIdentity.None;
        var byIdx = FindWord(sql, "BY", latestIdx + "LATEST".Length);
        if (byIdx < 0) return TableRowIdentity.None;

        var open = sql.IndexOf('(', byIdx);
        if (open < 0) return TableRowIdentity.None;
        var close = sql.IndexOf(')', open);
        if (close < 0) return TableRowIdentity.None;

        var keys = SplitTopLevel(sql[(open + 1)..close], ',')
            .Select(Normalize)
            .Where(s => s.Length > 0)
            .ToList();
        if (keys.Count == 0) return TableRowIdentity.None;

        return new TableRowIdentity(
            TableRowIdentity.LatestByClause,
            keys,
            MapKeysToColumns(sql, afterSelect, fromIdx, keys));
    }

    /// <summary>Maps every declared key expression to the SELECT item projecting it, by normalized
    /// expression text — all-or-nothing, exactly as before: one unmappable key means whole-row identity for
    /// the table, not a partial key.</summary>
    private static List<string>? MapKeysToColumns(string sql, int afterSelect, int fromIdx, List<string> keys)
    {
        var selectListText = sql.Substring(afterSelect, fromIdx - afterSelect);
        var selectItems = SplitTopLevel(selectListText, ',')
            .Select(s => s.Trim())
            .Where(s => s.Length > 0)
            .Select(ParseSelectItem)
            .ToList();

        var identity = new List<string>();
        foreach (var k in keys)
        {
            var match = selectItems.FirstOrDefault(it => it.Alias is not null && Normalize(it.ExprText) == k);
            if (match.Alias is null)
            {
                return null; // couldn't confidently map this key -> fall back to whole-row
            }
            identity.Add(match.Alias);
        }
        return identity;
    }

    private static (string ExprText, string? Alias) ParseSelectItem(string raw)
    {
        // TOP-LEVEL "AS" only. A select item's alias marker is the AS at paren depth 0; an AS *inside*
        // parentheses belongs to the expression (`cast(x AS long)`, and likewise EXTRACT/TRIM-style
        // constructs). Splitting on the first AS at any depth — which this did until plan 011 — parsed
        // `cast(bucket AS long) AS b` as expression "cast(bucket" with alias "long) AS b", so the item
        // never matched its own GROUP BY/LATEST BY key and EVERY cast key silently fell back to whole-row
        // identity, even when written character-for-character identically in both places.
        var asIdx = FindWordTopLevel(raw, "AS");
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
    /// <summary>Like <see cref="FindWord"/>, but only matches at parenthesis depth 0 — see
    /// <see cref="ParseSelectItem"/> for why that distinction is load-bearing. Returns the FIRST such
    /// match: within one select item, the first depth-0 AS is the alias marker by construction, since
    /// anything before it is the expression.</summary>
    private static int FindWordTopLevel(string s, string word)
    {
        var inQuote = false;
        var depth = 0;
        for (var i = 0; i <= s.Length - word.Length; i++)
        {
            var c = s[i];
            if (c == '\'') { inQuote = !inQuote; continue; }
            if (inQuote) continue;
            if (c == '(') { depth++; continue; }
            if (c == ')') { depth--; continue; }
            if (depth != 0) continue;
            if (string.Compare(s, i, word, 0, word.Length, StringComparison.OrdinalIgnoreCase) != 0) continue;
            var leftOk = i == 0 || !char.IsLetterOrDigit(s[i - 1]);
            var after = i + word.Length;
            var rightOk = after >= s.Length || !char.IsLetterOrDigit(s[after]);
            if (leftOk && rightOk) return i;
        }
        return -1;
    }

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

/// <summary>Wishlist #18 — derives <c>TableDefinition.KeyFields</c> from the same compile that already
/// produces <c>OutputFields</c>, so both are recomputed together and neither can go stale relative to the
/// other. See that field's own doc comment for the full null/[]/non-empty wire contract; in short: a
/// resolved GROUP BY/LATEST BY maps to its output columns, an unkeyed global aggregate (no GROUP BY,
/// still an aggregate — see <see cref="TablePlan.IsGlobalAggregate"/>) maps to an empty list, and every
/// other case — including the declared-but-unmappable fallback <see cref="TableRowIdentityWarning"/>
/// already reports — maps to null, because RowKeyCodec keys those rows by their whole content regardless
/// of what was declared, and null is the answer that matches actual behavior.</summary>
public static class TableKeyFields
{
    /// <summary><paramref name="plan"/> is the successful compile's <c>TableCompileResult.Plan</c> (null
    /// when the compile failed or hasn't happened, in which case this always returns null — the same
    /// fail-safe whole-row default a table gets before its first successful compile).</summary>
    public static List<string>? Describe(string? sql, TablePlan? plan)
    {
        var identity = TableGroupKeyExtractor.Describe(sql);
        if (identity.Columns is { Count: > 0 } cols)
        {
            return cols;
        }

        return plan is { IsGlobalAggregate: true } ? [] : null;
    }
}

/// <summary>What <see cref="TableGroupKeyExtractor.Describe"/> saw in a table's SQL.
/// <see cref="Clause"/> is "GROUP BY"/"LATEST BY" when the SQL declares a row identity at all and null
/// when it doesn't; <see cref="DeclaredKeys"/> are that clause's key expressions verbatim (normalized
/// whitespace only); <see cref="Columns"/> are the output columns they mapped to, or null for the
/// whole-row fallback. The pair is what separates "no identity declared, whole row IS the identity"
/// (fine — <see cref="None"/>) from "identity declared but unmappable" (degraded —
/// <see cref="FellBackToWholeRow"/>).</summary>
public sealed record TableRowIdentity(string? Clause, IReadOnlyList<string> DeclaredKeys, List<string>? Columns)
{
    public const string GroupByClause = "GROUP BY";
    public const string LatestByClause = "LATEST BY";

    /// <summary>The SQL declares no row identity this extractor can see — the pre-existing, perfectly
    /// healthy whole-row-identity case.</summary>
    public static readonly TableRowIdentity None = new(null, [], null);

    /// <summary>The SQL declares a GROUP BY / LATEST BY row identity.</summary>
    public bool IsDeclared => Clause is not null;

    /// <summary>A row identity WAS declared but could not be mapped to output columns, so
    /// <see cref="RowKeyCodec.EncodeIdentity"/> will key rows by their whole content. This is the
    /// materially degraded state — see TableGroupKeyExtractor's class doc.</summary>
    public bool FellBackToWholeRow => IsDeclared && Columns is null;
}

/// <summary>Turns <see cref="TableRowIdentity.FellBackToWholeRow"/> into the sentence a user reads.
///
/// Called from the shared API layer only — at table create/update (logged) and on the table's metrics
/// (rendered by the console) — so both runtime flavors report identically, from one implementation, for
/// every table including ones seeded or persisted long before this existed. It never persists a computed
/// verdict anywhere: recomputing from the definition is cheap and cannot go stale, and TableDefinition is
/// a client-owned record whose every writable field must round-trip an update untouched (asserted by the
/// Dapr flavor's CatalogUpdateRoundTripTests), which a server-computed field would break.
/// It WARNS, it never rejects: the whole-row fallback is pre-existing behavior, catalogs
/// legitimately depend on it, and refusing it would break working installs (which is also why a table
/// with no GROUP BY / LATEST BY at all is never flagged — that table's identity IS its whole row and
/// always was).
///
/// The trigger is narrow on purpose: the fallback only COSTS anything where something actually consumes
/// the row identity, i.e. when row history is enabled (the table-wide TableHistoryGrain/TableHistoryActor
/// trail) or the table is sharded (each TableShardGrain groups versions of one logical row by the same
/// identity — see TableShardRouterGrain.BuildConfig). A table with neither stores no version trail for
/// the degradation to ruin, so it says nothing.</summary>
public static class TableRowIdentityWarning
{
    /// <summary>The warning for <paramref name="def"/>, or null when there is nothing to report.</summary>
    public static string? For(TableDefinition def) =>
        For(def.Sql, def.HistoryEnabled, def.ShardBy.Count > 0);

    /// <summary>Definition-free overload (same rule), for callers holding the three inputs directly.</summary>
    public static string? For(string? sql, bool historyEnabled, bool sharded)
    {
        if (!historyEnabled && !sharded) return null;

        var identity = TableGroupKeyExtractor.Describe(sql);
        if (!identity.FellBackToWholeRow) return null;

        var keys = string.Join(", ", identity.DeclaredKeys);
        var plural = identity.DeclaredKeys.Count == 1 ? "" : "s";
        var affected = historyEnabled && sharded
            ? "Row history and each shard's version trail are"
            : historyEnabled
                ? "Row history is"
                : "Each shard's version trail is";

        return $"{affected} degraded: this table's {identity.Clause} key{plural} ({keys}) could not be " +
               $"matched to an output column, so rows are identified by their WHOLE content instead. " +
               $"Successive versions of the same row therefore never group together — every version sits " +
               $"alone under its own key, and no version trail forms. Fix: key on a plainly projected " +
               $"column — compute the expression upstream (a CTE or a derived query) and SELECT it as-is — " +
               $"or write the {identity.Clause} expression character-for-character identically to the " +
               $"SELECT item that projects it.";
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

    // BUGFIX (found live via the web UI's row-history sheet, POST /api/tables/{id}/history/lookup):
    // a request-bound `Dictionary<string, object?>` (HistoryLookupRequest.Row) deserializes its values
    // as boxed System.Text.Json.JsonElement — System.Text.Json's default behavior for an untyped
    // `object` property, since Program.cs registers no ObjectToInferredTypesConverter — NOT as the
    // plain string/long/double/bool the live delta path stores when TableHistoryGrain derives the same
    // key from a table row already living in memory. Every one of those JsonElement values used to fall
    // through to the `_ => $"?:{v}"` case below, so a client-submitted lookup key never matched the key
    // recorded from live deltas: GetHistoryAsync came back keyFound=false for every row, even though
    // GetStatsAsync showed the history WAS being retained (keyCount/totalVersions > 0). Unwrapping the
    // JsonElement into the same primitive shape the live path uses fixes it for both call sites without
    // touching global JSON options (which could affect unrelated Dictionary<string, object?> endpoints).
    private static string EncodeValue(object? v) => v switch
    {
        null => "N",
        long l => $"L:{l.ToString(CultureInfo.InvariantCulture)}",
        int i => $"L:{i.ToString(CultureInfo.InvariantCulture)}",
        double d => $"D:{d.ToString("R", CultureInfo.InvariantCulture)}",
        string s => $"S:{s}",
        bool b => $"B:{b}",
        Dictionary<string, object?> dict => $"J:{{{EncodeIdentity(dict, null)}}}",
        JsonElement je => EncodeValue(FromJsonElement(je)),
        System.Collections.IEnumerable list => $"A:[{string.Join(",", list.Cast<object?>().Select(EncodeValue))}]",
        _ => $"?:{v}",
    };

    /// <summary>Unwraps a JsonElement into the plain CLR value EncodeValue's other cases expect. Numbers
    /// prefer Int64 when the value is whole (matching how the live path stores an aggregate COUNT/SUM as
    /// long) and fall back to double otherwise — a residual ambiguity for a whole-valued Double output
    /// column (e.g. a "high" price that happens to land on an exact integer) is possible but out of scope
    /// here: identity columns in this dialect are overwhelmingly GROUP BY keys (strings/longs), and a
    /// false mismatch on a non-identity field is harmless since only identity columns feed EncodeIdentity.</summary>
    private static object? FromJsonElement(JsonElement je) => je.ValueKind switch
    {
        JsonValueKind.Null or JsonValueKind.Undefined => null,
        JsonValueKind.String => je.GetString(),
        JsonValueKind.True => true,
        JsonValueKind.False => false,
        // Plan 011 D1 BUGFIX (found by ShardKeyCodecTests' live-vs-REST agreement test): the cast to
        // (object) is load-bearing. Without it C#'s conditional-expression typing unifies `long` and
        // `double` to DOUBLE, so EVERY whole JSON number came back boxed as a double and encoded as
        // "D:2" while the live delta path — which holds a real long — encoded "L:2". The two keys never
        // matched, so a POSTed lookup against a table whose identity/shard column is a LONG always
        // answered keyFound=false. It went unnoticed because the demo tables all key on strings, which
        // this method has always handled correctly.
        JsonValueKind.Number => je.TryGetInt64(out var l) ? (object)l : je.GetDouble(),
        JsonValueKind.Object => je.EnumerateObject().ToDictionary(p => p.Name, p => FromJsonElement(p.Value)),
        JsonValueKind.Array => je.EnumerateArray().Select(FromJsonElement).ToList(),
        _ => je.ToString(),
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
