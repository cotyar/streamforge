using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace StreamForge.Host.Grains;

/// <summary>
/// Plan 011 wave D1: the pure (no Orleans, no I/O) key math for sharded tables — shard-key derivation,
/// grain-key composition, and the within-shard canonical row key. Lives next to
/// <see cref="RowKeyCodec"/>/<see cref="TableGroupKeyExtractor"/> in AppCore for the same reason those do:
/// the REST layer and the grains must derive byte-identical keys, and the REST layer is runtime-neutral.
///
/// THREE DIFFERENT KEYS, and confusing them is the whole class of bugs this file exists to prevent:
///
///  * The SHARD key (<see cref="EncodeShardKey"/>) decides WHICH GRAIN OWNS A ROW. Derived only from the
///    explicit <c>TableDefinition.ShardBy</c> columns, which are validated against the table's compiled
///    output fields at upsert. Never from <c>TableGroupKeyExtractor</c>'s textual GROUP BY/LATEST BY
///    matching: that is best-effort and returns null on any ambiguity, and a null-shaped guess about
///    grain ownership would silently scatter one instrument's rows across shards (or collapse two
///    instruments into one) with no error anywhere.
///  * The ROW-IDENTITY key (<c>RowKeyCodec.EncodeIdentity</c>) groups VERSIONS of the same logical row
///    INSIDE an already-chosen shard. Best-effort extraction IS acceptable there — it is the existing
///    history tier's own contract, and the worst case (whole-row fallback) degrades to "a version trail
///    per distinct row content", not to a wrong owner.
///  * The CANONICAL ROW key (<see cref="CanonicalRowKey"/>) is the Z-set consolidation key inside one
///    shard's ledger. It covers the WHOLE row, deliberately: Z-set consolidation nets weights for
///    identical rows, so two rows that differ in any column are different ledger entries.
/// </summary>
public static class TableShardKeys
{
    /// <summary>Separates the table name from the encoded shard key in a shard grain's primary key.</summary>
    public const char Separator = '|';

    /// <summary>Grain-key tokens longer than this are replaced by a hash — see <see cref="GrainKey"/>.</summary>
    private const int MaxInlineTokenLength = 96;

    /// <summary>Marks a hashed grain-key token. Deliberately outside the base64url alphabet
    /// (A-Z a-z 0-9 - _) so a hashed token can never be confused with an inline one.</summary>
    private const string HashedTokenPrefix = "~";

    /// <summary>The shard key for a row: the values of <paramref name="shardBy"/>, in the declared column
    /// order, through the same encoder the history tier keys rows with (so a REST-submitted row whose
    /// values arrive as <c>JsonElement</c> derives the identical key a live delta does — see
    /// RowKeyCodec.EncodeValue's own bugfix note). An empty <paramref name="shardBy"/> is a programming
    /// error, not a fallback: callers must check <c>ShardBy.Count &gt; 0</c> first, because the whole-row
    /// fallback RowKeyCodec applies for a null identity would make every distinct row its own shard.</summary>
    public static string EncodeShardKey(IReadOnlyDictionary<string, object?> row, IReadOnlyList<string> shardBy)
    {
        ArgumentNullException.ThrowIfNull(shardBy);
        if (shardBy.Count == 0)
        {
            throw new ArgumentException("A shard key needs at least one ShardBy column.", nameof(shardBy));
        }
        return RowKeyCodec.EncodeIdentity(row, shardBy);
    }

    /// <summary>Composes a <c>TableShardGrain</c> primary key from the owning table's name and an encoded
    /// shard key: <c>{table}|{token}</c>.
    ///
    /// The token is base64url of the shard key's UTF-8 bytes rather than the raw key, for one concrete
    /// reason: a grain key becomes a FILE NAME under <c>JsonFileGrainStorage</c> (it sanitizes anything
    /// outside <c>[A-Za-z0-9_.-]</c> to <c>_</c>), and a raw key carrying a slash, a quote or a run of
    /// characters that all sanitize to the same thing would either escape the state directory or collide
    /// two distinct instruments onto one file. base64url's alphabet passes that sanitizer untouched.
    ///
    /// Tokens longer than <see cref="MaxInlineTokenLength"/> are replaced by
    /// <c>~{base64url(SHA-256(key))}</c>, because a file name has a hard length limit (255 bytes on both
    /// APFS and ext4) and a wide composite shard key would otherwise make the state write fail at
    /// runtime rather than at upsert. The mapping is one-way and never needs inverting: the raw shard
    /// keys live in the shard directory and in each shard's own persisted state, and every caller
    /// (router, REST lookup) derives the grain key forward from the row.</summary>
    public static string GrainKey(string tableName, string shardKey)
    {
        var token = Base64Url(Encoding.UTF8.GetBytes(shardKey));
        if (token.Length > MaxInlineTokenLength)
        {
            token = HashedTokenPrefix + Base64Url(SHA256.HashData(Encoding.UTF8.GetBytes(shardKey)));
        }
        return string.Concat(tableName, Separator.ToString(), token);
    }

    /// <summary>Splits a shard grain key back into its table name and opaque token. The token is NOT the
    /// shard key (see <see cref="GrainKey"/>) — this exists for diagnostics and for a grain to learn which
    /// table it belongs to, nothing else.</summary>
    public static (string TableName, string Token) ParseGrainKey(string grainKey)
    {
        var idx = grainKey.LastIndexOf(Separator);
        return idx < 0 ? (grainKey, "") : (grainKey[..idx], grainKey[(idx + 1)..]);
    }

    /// <summary>The Z-set consolidation key for one row inside a shard's ledger: a canonical JSON dump
    /// with keys in ordinal order, covering every column.
    ///
    /// Deliberately NOT <c>TableExecutor.CanonicalRowKey</c>, which is the Engine's own equivalent: that
    /// requires a compiled <c>TablePlan</c>, and a shard grain reactivating from cold storage has no plan
    /// and must not compile SQL to answer a read. The precedent is <c>ArrangementGrain</c>, which computes
    /// its own sorted-keys dump for exactly this reason — the key never crosses a grain boundary and is
    /// never compared against another grain's canonicalization, so it only has to be internally
    /// consistent and deterministic, which a sorted dump is.</summary>
    public static string CanonicalRowKey(IReadOnlyDictionary<string, object?> row)
    {
        var ordered = new SortedDictionary<string, object?>(StringComparer.Ordinal);
        foreach (var (k, v) in row)
        {
            ordered[k] = v;
        }
        return JsonSerializer.Serialize(ordered);
    }

    private static string Base64Url(byte[] bytes) =>
        Convert.ToBase64String(bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_');
}
