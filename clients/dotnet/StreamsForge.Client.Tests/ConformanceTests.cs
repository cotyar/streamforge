using System.Text.Json;
using StreamsForge.Client;
using Xunit;
using Xunit.Abstractions;

namespace StreamsForge.Client.Tests;

/// <summary>
/// Runs clients/conformance/zset-cases.json -- the cross-language conformance suite for the Z-set
/// reducer -- against this client's own <see cref="ZSet"/>. Every StreamsForge client (Python,
/// .NET, TypeScript, Kotlin, plus the two in-app copies) implements the same reducer semantics
/// independently; this fixture is what turns "these agree" into something that fails on the same
/// named case everywhere instead of drifting silently.
///
/// Runner contract (clients/conformance/README.md), implemented literally:
///
///   z = ZSet(case.keyFields)
///   z.seed(case.snapshot)
///   for b in case.bufferedBatches: if not z.alreadyReflected(b.deltas): z.apply(b.deltas)
///   for b in case.liveBatches: z.apply(b.deltas)
///   assert rows(z) == case.expectedRows, ignoring order
/// </summary>
public sealed class ConformanceTests
{
    private readonly ITestOutputHelper _output;

    public ConformanceTests(ITestOutputHelper output) => _output = output;

    private static string FixturePath()
    {
        var dir = AppContext.BaseDirectory;
        var path = Path.Combine(dir, "conformance", "zset-cases.json");
        if (!File.Exists(path))
            throw new FileNotFoundException($"conformance fixture not found at {path} -- was it copied to the output dir?");
        return path;
    }

    public static IEnumerable<object[]> Cases()
    {
        using var doc = JsonDocument.Parse(File.ReadAllText(FixturePath()));
        foreach (var c in doc.RootElement.GetProperty("cases").EnumerateArray())
            yield return new object[] { c.GetProperty("name").GetString()! };
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public void ReducesToExpectedRows(string caseName)
    {
        using var doc = JsonDocument.Parse(File.ReadAllText(FixturePath()));
        var caseEl = doc.RootElement.GetProperty("cases").EnumerateArray().First(c => c.GetProperty("name").GetString() == caseName);

        var keyFields = ParseKeyFields(caseEl);
        var zset = new ZSet(keyFields);

        zset.Seed(ParseDeltaList(caseEl.GetProperty("snapshot")));

        foreach (var batch in caseEl.GetProperty("bufferedBatches").EnumerateArray())
        {
            var deltas = ParseDeltaList(batch.GetProperty("deltas"));
            if (!zset.AlreadyReflected(deltas)) zset.Apply(deltas);
        }

        foreach (var batch in caseEl.GetProperty("liveBatches").EnumerateArray())
        {
            zset.Apply(ParseDeltaList(batch.GetProperty("deltas")));
        }

        var actual = zset.Rows().Select(RowIdentityKeyFor).ToHashSet();
        var expected = caseEl.GetProperty("expectedRows").EnumerateArray()
            .Select(r => RowIdentityKeyFor(RowCodec.FromJson(r)))
            .ToHashSet();

        _output.WriteLine($"{caseName}: actual={actual.Count} rows, expected={expected.Count} rows");
        Assert.Equal(expected, actual);
    }

    // Order-insensitive row equality: two rows are "the same" iff their canonical (sorted-key)
    // JSON-ish serialization matches -- exactly the identity ZSet itself uses internally, reused
    // here as the assertion's equality rather than a separate deep-equality implementation.
    private static string RowIdentityKeyFor(IReadOnlyDictionary<string, object?> row) => RowIdentity.CanonicalKey(row);

    private static IReadOnlyList<string>? ParseKeyFields(JsonElement caseEl)
    {
        var el = caseEl.GetProperty("keyFields");
        if (el.ValueKind == JsonValueKind.Null) return null;
        return el.EnumerateArray().Select(e => e.GetString()!).ToList();
    }

    private static IReadOnlyList<RowDelta> ParseDeltaList(JsonElement arrayEl) =>
        arrayEl.EnumerateArray()
            .Select(e => new RowDelta(RowCodec.FromJson(e.GetProperty("row")), e.GetProperty("weight").GetInt64()))
            .ToList();
}
