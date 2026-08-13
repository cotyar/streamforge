using System.Text.Json;
using StreamForge.Host.Grains;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// Plan 011 wave D1: the pure key math behind sharded tables (<see cref="TableShardKeys"/>). No cluster —
/// these are the properties everything else rests on, so they are worth pinning where a failure points
/// straight at the cause rather than at a downstream symptom.
///
/// The property that matters most is the REST/live-delta agreement: a client posting a row to
/// <c>/shard/lookup</c> sends JSON, whose untyped values deserialize as <c>JsonElement</c>, while the
/// router derives the same key from a live delta holding plain CLR primitives. If those two disagree,
/// every per-key lookup answers "not found" while the tier is demonstrably full of data — the exact bug
/// RowKeyCodec's own JsonElement note records having been found live, one layer down.
/// </summary>
public class ShardKeyCodecTests
{
    private static Dictionary<string, object?> Row(params (string Key, object? Value)[] pairs) =>
        pairs.ToDictionary(p => p.Key, p => p.Value);

    [Fact]
    public void ShardKey_IsStable_AndIndependentOfColumnOrderInTheRow()
    {
        var a = TableShardKeys.EncodeShardKey(Row(("instrument", "XS123"), ("leg", 1L), ("noise", "x")), ["instrument", "leg"]);
        var b = TableShardKeys.EncodeShardKey(Row(("noise", "y"), ("leg", 1L), ("instrument", "XS123")), ["instrument", "leg"]);
        Assert.Equal(a, b);
    }

    [Fact]
    public void ShardKey_DistinguishesDifferentKeyValues()
    {
        var a = TableShardKeys.EncodeShardKey(Row(("instrument", "XS123")), ["instrument"]);
        var b = TableShardKeys.EncodeShardKey(Row(("instrument", "XS124")), ["instrument"]);
        Assert.NotEqual(a, b);
    }

    [Fact]
    public void ShardKey_RespectsColumnOrder_SoAMultiColumnKeyIsNotAmbiguous()
    {
        // "ab"+"c" must not collide with "a"+"bc": the encoding tags each value with its column name,
        // which is what prevents that whole family of composite-key collisions.
        var a = TableShardKeys.EncodeShardKey(Row(("x", "ab"), ("y", "c")), ["x", "y"]);
        var b = TableShardKeys.EncodeShardKey(Row(("x", "a"), ("y", "bc")), ["x", "y"]);
        Assert.NotEqual(a, b);
    }

    [Fact]
    public void ShardKey_FromJsonDeserializedRow_MatchesTheKeyFromALiveDelta()
    {
        // The live path: values are plain CLR primitives straight off the delta stream.
        var live = Row(("instrument", "XS123"), ("leg", 2L));

        // The REST path: HistoryLookupRequest/ShardLookupRequest bind Dictionary<string, object?>, whose
        // values System.Text.Json materializes as boxed JsonElement.
        var posted = JsonSerializer.Deserialize<Dictionary<string, object?>>("""{"instrument":"XS123","leg":2}""")!;

        Assert.Equal(
            TableShardKeys.EncodeShardKey(live, ["instrument", "leg"]),
            TableShardKeys.EncodeShardKey(posted, ["instrument", "leg"]));
    }

    [Fact]
    public void ShardKey_RefusesAnEmptyColumnList_RatherThanFallingBackToWholeRow()
    {
        // RowKeyCodec's own contract treats a null/empty identity as "key by the whole row". That is a
        // sane fallback for history and a catastrophic one for sharding — every distinct row would become
        // its own shard — so this layer refuses instead of inheriting it.
        Assert.Throws<ArgumentException>(() => TableShardKeys.EncodeShardKey(Row(("a", "b")), []));
    }

    [Fact]
    public void GrainKey_SurvivesTheStorageProvidersFileNameSanitizer()
    {
        // JsonFileGrainStorage replaces anything outside [A-Za-z0-9_.-] with '_'. A raw shard key carrying
        // a slash or a quote would either escape the state directory or collide two distinct keys onto one
        // file; the base64url token cannot.
        var key = TableShardKeys.EncodeShardKey(Row(("instrument", "a/b\"c d:e")), ["instrument"]);
        var grainKey = TableShardKeys.GrainKey("my_table", key);
        var token = TableShardKeys.ParseGrainKey(grainKey).Token;

        Assert.Equal("my_table", TableShardKeys.ParseGrainKey(grainKey).TableName);
        Assert.All(token, c => Assert.True(char.IsLetterOrDigit(c) || c is '-' or '_' or '~', $"unexpected char '{c}' in grain-key token"));
    }

    [Fact]
    public void GrainKey_HashesOverlongKeys_SoAWideCompositeKeyCannotBlowTheFileNameLimit()
    {
        var wide = TableShardKeys.EncodeShardKey(Row(("instrument", new string('x', 4000))), ["instrument"]);
        var grainKey = TableShardKeys.GrainKey("t", wide);
        var token = TableShardKeys.ParseGrainKey(grainKey).Token;

        Assert.StartsWith("~", token);
        Assert.True(token.Length < 64, $"hashed token should be short, was {token.Length}");
        // Still deterministic, and still distinct from a different overlong key.
        Assert.Equal(grainKey, TableShardKeys.GrainKey("t", wide));
        var other = TableShardKeys.EncodeShardKey(Row(("instrument", new string('y', 4000))), ["instrument"]);
        Assert.NotEqual(grainKey, TableShardKeys.GrainKey("t", other));
    }

    [Fact]
    public void CanonicalRowKey_CoversEveryColumn_AndIsOrderIndependent()
    {
        var a = TableShardKeys.CanonicalRowKey(Row(("b", 2L), ("a", 1L)));
        var b = TableShardKeys.CanonicalRowKey(Row(("a", 1L), ("b", 2L)));
        Assert.Equal(a, b);

        // Two rows sharing a shard key but differing in a non-key column are DIFFERENT ledger entries —
        // Z-set consolidation nets weights per identical row, not per key.
        Assert.NotEqual(
            TableShardKeys.CanonicalRowKey(Row(("k", "same"), ("v", 1L))),
            TableShardKeys.CanonicalRowKey(Row(("k", "same"), ("v", 2L))));
    }
}
