using System.Globalization;
using System.Text.Json;
using StreamForge.Abstractions;

namespace StreamForge.Host.Search;

/// <summary>
/// A reverse (inverted) search index over a materialized table's current rows. Pure/testable — no Orleans
/// dependency, so it can be unit-tested directly and driven incrementally from TableGrain as Z-set deltas
/// land (Add when a row's consolidated weight goes 0→positive, Remove when it returns to 0).
///
/// Rows are identified by their canonical row key — the same key TableExecutor.Snapshot() /
/// TableExecutor.CanonicalRowKey() use for a table's consolidated Z-set, so the index's identity always
/// matches the table's.
///
/// Two independent matching strategies, selected per-table by <see cref="TableSearchMode"/>:
///
///   Exact — token/prefix lookup via the inverted map (token → set&lt;rowKey&gt;, backed by a SortedSet
///   for O(log n + k) prefix range queries), falling back to a substring scan only for query words that
///   miss the token index outright (e.g. a fragment that lives mid-token, like "2024" inside "AAPL2024Q1").
///   Multi-word queries AND their per-word results; the whole (unsplit) query is also tried as a literal
///   substring of a row's full text, as an OR alternative. Complexity: O(log V + k) per word via the token
///   index (V = vocabulary size, k = matches), degrading to O(R) only for the substring fallback / paths
///   (R = row count) — which is the same order of work FlushAsync's snapshot copy already does every 2s.
///
///   Fuzzy — trigram index (token → its trigrams, and trigram → tokens) for typo-tolerant matching
///   ("gogle" ~ "google"). The query's trigrams gather a candidate token set via trigram overlap; each
///   candidate is scored by the max of trigram Jaccard similarity and normalized Levenshtein similarity, kept
///   above a threshold, then mapped back to rows and ranked by best matching-token score. Complexity:
///   O(Q·C) where Q = query trigram count (~query length) and C = candidate tokens gathered by overlap —
///   in practice a small slice of the vocabulary, not the whole row set.
/// </summary>
public sealed class TableSearchIndex
{
    private const double FuzzyThreshold = 0.3;

    private readonly TableSearchMode _mode;

    // rowKey -> the row's field values (a defensive copy — TableGrain may mutate its own dictionaries).
    private readonly Dictionary<string, IReadOnlyDictionary<string, object?>> _rows = new(StringComparer.Ordinal);

    // rowKey -> lowercased "all column values stringified and joined" (used for substring fallback/checks).
    private readonly Dictionary<string, string> _rowText = new(StringComparer.Ordinal);

    // rowKey -> the distinct tokens that row contributed (needed to unwind Remove() correctly).
    private readonly Dictionary<string, HashSet<string>> _rowTokens = new(StringComparer.Ordinal);

    // token -> set of rowKeys containing it (the inverted index proper).
    private readonly Dictionary<string, HashSet<string>> _tokenToRowKeys = new(StringComparer.Ordinal);

    // Sorted view of _tokenToRowKeys.Keys for O(log n + k) prefix range queries (exact mode).
    private readonly SortedSet<string> _sortedTokens = new(StringComparer.Ordinal);

    // Fuzzy mode only: trigram -> set of tokens containing it (global vocabulary), maintained alongside
    // _tokenToRowKeys so a token enters/leaves the trigram index exactly when it enters/leaves the vocabulary.
    private readonly Dictionary<string, HashSet<string>> _trigramToTokens = new(StringComparer.Ordinal);

    public TableSearchIndex(TableSearchMode mode) => _mode = mode;

    public TableSearchMode Mode => _mode;

    public int RowCount => _rows.Count;

    /// <summary>Upserts a row (removes any prior occurrence of the same key first, so this is safe to call
    /// on an update as well as a fresh insert).</summary>
    public void Add(string rowKey, IReadOnlyDictionary<string, object?> row)
    {
        Remove(rowKey);

        var text = BuildSearchableText(row);
        var tokens = Tokenize(text).ToHashSet(StringComparer.Ordinal);

        _rows[rowKey] = row;
        _rowText[rowKey] = text;
        _rowTokens[rowKey] = tokens;

        foreach (var token in tokens)
        {
            if (!_tokenToRowKeys.TryGetValue(token, out var rowKeys))
            {
                rowKeys = new HashSet<string>(StringComparer.Ordinal);
                _tokenToRowKeys[token] = rowKeys;
                _sortedTokens.Add(token);
                if (_mode == TableSearchMode.Fuzzy) AddTokenToTrigramIndex(token);
            }
            rowKeys.Add(rowKey);
        }
    }

    public void Remove(string rowKey)
    {
        if (!_rowTokens.TryGetValue(rowKey, out var tokens)) return;

        foreach (var token in tokens)
        {
            if (!_tokenToRowKeys.TryGetValue(token, out var rowKeys)) continue;
            rowKeys.Remove(rowKey);
            if (rowKeys.Count == 0)
            {
                _tokenToRowKeys.Remove(token);
                _sortedTokens.Remove(token);
                if (_mode == TableSearchMode.Fuzzy) RemoveTokenFromTrigramIndex(token);
            }
        }

        _rows.Remove(rowKey);
        _rowText.Remove(rowKey);
        _rowTokens.Remove(rowKey);
    }

    public void Clear()
    {
        _rows.Clear();
        _rowText.Clear();
        _rowTokens.Clear();
        _tokenToRowKeys.Clear();
        _sortedTokens.Clear();
        _trigramToTokens.Clear();
    }

    /// <summary>Clears and re-adds every row in the snapshot — used on activation/rebuild.</summary>
    public void Rebuild(IEnumerable<KeyValuePair<string, IReadOnlyDictionary<string, object?>>> snapshot)
    {
        Clear();
        foreach (var (rowKey, row) in snapshot) Add(rowKey, row);
    }

    /// <summary>Ordered (best match first) search results, capped at <paramref name="limit"/>. Empty query
    /// or empty index both yield an empty list.</summary>
    public List<(string RowKey, IReadOnlyDictionary<string, object?> Row, double Score)> Search(string query, int limit)
    {
        if (string.IsNullOrWhiteSpace(query) || limit <= 0 || _rows.Count == 0) return [];

        var matches = _mode == TableSearchMode.Exact ? SearchExact(query) : SearchFuzzy(query);

        return matches
            .OrderByDescending(m => m.Score)
            .ThenBy(m => m.RowKey, StringComparer.Ordinal) // deterministic tie-break
            .Take(limit)
            .Select(m => (m.RowKey, _rows[m.RowKey], m.Score))
            .ToList();
    }

    // ------------------------------------------------------------------
    // Exact mode
    // ------------------------------------------------------------------

    private List<(string RowKey, double Score)> SearchExact(string query)
    {
        var words = Tokenize(query).ToList();
        if (words.Count == 0) return [];

        HashSet<string>? andResult = null;
        foreach (var word in words)
        {
            var candidates = TokenCandidates(word);
            if (candidates.Count == 0)
            {
                // Fall back to substring only for the word(s) the token index missed — catches matches
                // that live mid-token (e.g. "2024" inside "aapl2024q1"), not just token-prefix hits.
                candidates = SubstringCandidates(word);
            }
            andResult = andResult is null ? candidates : Intersect(andResult, candidates);
            if (andResult.Count == 0) break;
        }
        andResult ??= [];

        // Whole-query-as-substring is an OR alternative to the per-word AND above (e.g. punctuation makes
        // the query fail to tokenize the same way the row text did).
        var wholeQuery = query.Trim().ToLowerInvariant();
        var result = new HashSet<string>(andResult, StringComparer.Ordinal);
        if (wholeQuery.Length > 0) result.UnionWith(SubstringCandidates(wholeQuery));

        return result.Select(rk => (rk, 1.0)).ToList();
    }

    private HashSet<string> TokenCandidates(string word)
    {
        var result = new HashSet<string>(StringComparer.Ordinal);
        if (_tokenToRowKeys.TryGetValue(word, out var exact)) result.UnionWith(exact);

        // Prefix range query: tokens lexicographically in [word, word + '￿'] are exactly those with
        // prefix `word` (plus `word` itself, already covered above).
        foreach (var token in _sortedTokens.GetViewBetween(word, word + '\uffff'))
        {
            if (token.Length > word.Length) result.UnionWith(_tokenToRowKeys[token]);
        }
        return result;
    }

    private HashSet<string> SubstringCandidates(string needle)
    {
        var result = new HashSet<string>(StringComparer.Ordinal);
        foreach (var (rowKey, text) in _rowText)
        {
            if (text.Contains(needle, StringComparison.Ordinal)) result.Add(rowKey);
        }
        return result;
    }

    private static HashSet<string> Intersect(HashSet<string> a, HashSet<string> b)
    {
        var result = new HashSet<string>(StringComparer.Ordinal);
        var (small, large) = a.Count <= b.Count ? (a, b) : (b, a);
        foreach (var item in small)
        {
            if (large.Contains(item)) result.Add(item);
        }
        return result;
    }

    // ------------------------------------------------------------------
    // Fuzzy mode
    // ------------------------------------------------------------------

    private List<(string RowKey, double Score)> SearchFuzzy(string query)
    {
        var q = query.Trim().ToLowerInvariant();
        if (q.Length == 0) return [];

        var queryTrigrams = Trigrams(q);

        var candidateTokens = new HashSet<string>(StringComparer.Ordinal);
        foreach (var tri in queryTrigrams)
        {
            if (_trigramToTokens.TryGetValue(tri, out var tokens)) candidateTokens.UnionWith(tokens);
        }
        // A candidate exactly equal to the query never showed up via trigram overlap if it's absent from
        // the vocabulary, but if it IS in the vocabulary its trigrams necessarily overlap query's — no gap.

        var rowBestScore = new Dictionary<string, double>(StringComparer.Ordinal);
        foreach (var token in candidateTokens)
        {
            double score = FuzzyScore(q, queryTrigrams, token);
            if (score < FuzzyThreshold) continue;

            if (!_tokenToRowKeys.TryGetValue(token, out var rowKeys)) continue;
            foreach (var rowKey in rowKeys)
            {
                if (!rowBestScore.TryGetValue(rowKey, out var best) || score > best)
                {
                    rowBestScore[rowKey] = score;
                }
            }
        }

        return rowBestScore.Select(kv => (kv.Key, kv.Value)).ToList();
    }

    /// <summary>Combines the two signals with max rather than average: trigram Jaccard rewards shared
    /// substrings anywhere in the token (order-independent), Levenshtein rewards overall closeness
    /// (order-sensitive) — a candidate that's clearly close by either measure should pass, not get
    /// penalized by the other measure disagreeing. This matters most for short tokens (e.g. ticker
    /// symbols): "gogle" vs "goog" only shares one trigram (low Jaccard, since "goog" is short) but is a
    /// close edit distance away (high Levenshtein similarity) — a straight average would wrongly dilute
    /// the clear typo match.</summary>
    private static double FuzzyScore(string query, HashSet<string> queryTrigrams, string token)
    {
        var tokenTrigrams = Trigrams(token);
        double jaccard = JaccardSimilarity(queryTrigrams, tokenTrigrams);
        double levSim = LevenshteinSimilarity(query, token);
        return Math.Max(jaccard, levSim);
    }

    private static double JaccardSimilarity(HashSet<string> a, HashSet<string> b)
    {
        if (a.Count == 0 && b.Count == 0) return 1.0;
        int intersection = 0;
        var (small, large) = a.Count <= b.Count ? (a, b) : (b, a);
        foreach (var item in small)
        {
            if (large.Contains(item)) intersection++;
        }
        int union = a.Count + b.Count - intersection;
        return union == 0 ? 0.0 : (double)intersection / union;
    }

    private static double LevenshteinSimilarity(string a, string b)
    {
        int maxLen = Math.Max(a.Length, b.Length);
        if (maxLen == 0) return 1.0;
        int dist = LevenshteinDistance(a, b);
        return 1.0 - (double)dist / maxLen;
    }

    private static int LevenshteinDistance(string a, string b)
    {
        int[] prev = new int[b.Length + 1];
        int[] curr = new int[b.Length + 1];
        for (int j = 0; j <= b.Length; j++) prev[j] = j;

        for (int i = 1; i <= a.Length; i++)
        {
            curr[0] = i;
            for (int j = 1; j <= b.Length; j++)
            {
                int cost = a[i - 1] == b[j - 1] ? 0 : 1;
                curr[j] = Math.Min(Math.Min(curr[j - 1] + 1, prev[j] + 1), prev[j - 1] + cost);
            }
            (prev, curr) = (curr, prev);
        }
        return prev[b.Length];
    }

    private void AddTokenToTrigramIndex(string token)
    {
        foreach (var tri in Trigrams(token))
        {
            if (!_trigramToTokens.TryGetValue(tri, out var tokens))
            {
                tokens = new HashSet<string>(StringComparer.Ordinal);
                _trigramToTokens[tri] = tokens;
            }
            tokens.Add(token);
        }
    }

    private void RemoveTokenFromTrigramIndex(string token)
    {
        foreach (var tri in Trigrams(token))
        {
            if (!_trigramToTokens.TryGetValue(tri, out var tokens)) continue;
            tokens.Remove(token);
            if (tokens.Count == 0) _trigramToTokens.Remove(tri);
        }
    }

    /// <summary>Boundary-padded consecutive 3-char windows (e.g. "cat" -> "$ca","cat","at$") — always
    /// yields at least one trigram for any non-empty input, even single-character tokens.</summary>
    private static HashSet<string> Trigrams(string s)
    {
        var padded = "$" + s + "$";
        var result = new HashSet<string>(StringComparer.Ordinal);
        for (int i = 0; i + 3 <= padded.Length; i++)
        {
            result.Add(padded.Substring(i, 3));
        }
        return result;
    }

    // ------------------------------------------------------------------
    // Row text / tokenization
    // ------------------------------------------------------------------

    private static readonly JsonSerializerOptions CompactJsonOptions = new() { WriteIndented = false };

    /// <summary>Lowercased stringification of all column values, joined with spaces. Numbers render
    /// invariant-culture; JSON objects/arrays render as compact JSON text.</summary>
    internal static string BuildSearchableText(IReadOnlyDictionary<string, object?> row)
    {
        var parts = row.Values.Select(Stringify).Where(s => s.Length > 0);
        return string.Join(" ", parts).ToLowerInvariant();
    }

    private static string Stringify(object? value) => value switch
    {
        null => "",
        string s => s,
        bool b => b ? "true" : "false",
        double d => d.ToString(CultureInfo.InvariantCulture),
        float f => f.ToString(CultureInfo.InvariantCulture),
        long l => l.ToString(CultureInfo.InvariantCulture),
        int i => i.ToString(CultureInfo.InvariantCulture),
        IDictionary<string, object?> => JsonSerializer.Serialize(value, CompactJsonOptions),
        System.Collections.IEnumerable => JsonSerializer.Serialize(value, CompactJsonOptions),
        _ => value.ToString() ?? "",
    };

    /// <summary>Splits on any run of non-alphanumeric characters; empty tokens dropped.</summary>
    internal static IEnumerable<string> Tokenize(string text)
    {
        int start = -1;
        for (int i = 0; i <= text.Length; i++)
        {
            bool isAlnum = i < text.Length && char.IsLetterOrDigit(text[i]);
            if (isAlnum)
            {
                if (start < 0) start = i;
            }
            else if (start >= 0)
            {
                yield return text[start..i].ToLowerInvariant();
                start = -1;
            }
        }
    }
}
